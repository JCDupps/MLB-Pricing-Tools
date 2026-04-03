import html
import json
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from html.parser import HTMLParser
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, quote, unquote, urlparse
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from openpyxl import Workbook
from openpyxl.utils import get_column_letter


BASE_URL = "https://www.fangraphs.com/roster-resource/platoon-lineups"
BULLPEN_URL = "https://www.fangraphs.com/roster-resource/closer-depth-chart"
MLB_SCORES_URL = "https://www.mlb.com/scores"
SAVANT_PITCH_ARSENALS_URL = "https://baseballsavant.mlb.com/leaderboard/pitch-arsenals?year={year}&min={minimum}&type={metric_type}&hand="
APP_DIR = Path(__file__).resolve().parent
HOME_TEMPLATE_PATH = APP_DIR / "templates" / "index.html"
LINEUP_TEMPLATE_PATH = APP_DIR / "templates" / "platoon_lineups.html"
BULLPEN_TEMPLATE_PATH = APP_DIR / "templates" / "bullpens.html"
VELOCITY_TEMPLATE_PATH = APP_DIR / "templates" / "velocity_comparison.html"
PITCHERS_TEMPLATE_PATH = APP_DIR / "templates" / "probable_pitchers.html"
PITCHER_PROFILE_TEMPLATE_PATH = APP_DIR / "templates" / "pitcher_profile.html"
STUFF_PLUS_CACHE_PATH = APP_DIR / "pitch_stuff_plus_cache.json"
REFRESH_SECONDS = 300
MLB_HEADSHOT_TEMPLATE = "https://midfield.mlbstatic.com/v1/people/{mlb_id}/silo/360?zoom=1.2"
VALID_BATS = {"R", "L", "S"}
POSITIONS = {
    "C",
    "1B",
    "2B",
    "3B",
    "SS",
    "LF",
    "CF",
    "RF",
    "DH",
    "INF",
    "OF",
    "UT",
    "UTIL",
    "1B/OF",
    "2B/SS",
    "3B/SS",
    "INF/OF",
    "OF/INF",
}
STARTING_LINEUP_LENGTH = 9
TEAM_OPTIONS = [
    {"slug": "diamondbacks", "code": "ARI", "name": "Arizona Diamondbacks", "league": "NL West"},
    {"slug": "braves", "code": "ATL", "name": "Atlanta Braves", "league": "NL East"},
    {"slug": "orioles", "code": "BAL", "name": "Baltimore Orioles", "league": "AL East"},
    {"slug": "red-sox", "code": "BOS", "name": "Boston Red Sox", "league": "AL East"},
    {"slug": "cubs", "code": "CHC", "name": "Chicago Cubs", "league": "NL Central"},
    {"slug": "white-sox", "code": "CHW", "name": "Chicago White Sox", "league": "AL Central"},
    {"slug": "reds", "code": "CIN", "name": "Cincinnati Reds", "league": "NL Central"},
    {"slug": "guardians", "code": "CLE", "name": "Cleveland Guardians", "league": "AL Central"},
    {"slug": "rockies", "code": "COL", "name": "Colorado Rockies", "league": "NL West"},
    {"slug": "tigers", "code": "DET", "name": "Detroit Tigers", "league": "AL Central"},
    {"slug": "astros", "code": "HOU", "name": "Houston Astros", "league": "AL West"},
    {"slug": "royals", "code": "KCR", "name": "Kansas City Royals", "league": "AL Central"},
    {"slug": "angels", "code": "LAA", "name": "Los Angeles Angels", "league": "AL West"},
    {"slug": "dodgers", "code": "LAD", "name": "Los Angeles Dodgers", "league": "NL West"},
    {"slug": "marlins", "code": "MIA", "name": "Miami Marlins", "league": "NL East"},
    {"slug": "brewers", "code": "MIL", "name": "Milwaukee Brewers", "league": "NL Central"},
    {"slug": "twins", "code": "MIN", "name": "Minnesota Twins", "league": "AL Central"},
    {"slug": "mets", "code": "NYM", "name": "New York Mets", "league": "NL East"},
    {"slug": "yankees", "code": "NYY", "name": "New York Yankees", "league": "AL East"},
    {"slug": "athletics", "code": "OAK", "name": "Athletics", "league": "AL West"},
    {"slug": "phillies", "code": "PHI", "name": "Philadelphia Phillies", "league": "NL East"},
    {"slug": "pirates", "code": "PIT", "name": "Pittsburgh Pirates", "league": "NL Central"},
    {"slug": "padres", "code": "SDP", "name": "San Diego Padres", "league": "NL West"},
    {"slug": "giants", "code": "SFG", "name": "San Francisco Giants", "league": "NL West"},
    {"slug": "mariners", "code": "SEA", "name": "Seattle Mariners", "league": "AL West"},
    {"slug": "cardinals", "code": "STL", "name": "St. Louis Cardinals", "league": "NL Central"},
    {"slug": "rays", "code": "TBR", "name": "Tampa Bay Rays", "league": "AL East"},
    {"slug": "rangers", "code": "TEX", "name": "Texas Rangers", "league": "AL West"},
    {"slug": "blue-jays", "code": "TOR", "name": "Toronto Blue Jays", "league": "AL East"},
    {"slug": "nationals", "code": "WSN", "name": "Washington Nationals", "league": "NL East"},
]
TEAM_MAP = {team["slug"]: team for team in TEAM_OPTIONS}
MLB_TEAM_NAME_TO_SLUG = {
    "arizona": "diamondbacks",
    "d-backs": "diamondbacks",
    "atlanta": "braves",
    "baltimore": "orioles",
    "boston": "red-sox",
    "chi cubs": "cubs",
    "chicago cubs": "cubs",
    "chi white sox": "white-sox",
    "chicago white sox": "white-sox",
    "cincinnati": "reds",
    "cleveland": "guardians",
    "colorado": "rockies",
    "detroit": "tigers",
    "houston": "astros",
    "kansas city": "royals",
    "la angels": "angels",
    "los angeles angels": "angels",
    "la dodgers": "dodgers",
    "los angeles dodgers": "dodgers",
    "miami": "marlins",
    "milwaukee": "brewers",
    "minnesota": "twins",
    "ny mets": "mets",
    "new york mets": "mets",
    "ny yankees": "yankees",
    "new york yankees": "yankees",
    "athletics": "athletics",
    "philadelphia": "phillies",
    "pittsburgh": "pirates",
    "san diego": "padres",
    "san francisco": "giants",
    "seattle": "mariners",
    "st. louis": "cardinals",
    "st louis": "cardinals",
    "tampa bay": "rays",
    "texas": "rangers",
    "toronto": "blue-jays",
    "washington": "nationals",
}
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "max-age=0",
    "Upgrade-Insecure-Requests": "1",
}
FANGRAPHS_HEADERS = {
    **DEFAULT_HEADERS,
    "Referer": "https://www.fangraphs.com/",
}
JSON_HEADERS = {
    "User-Agent": DEFAULT_HEADERS["User-Agent"],
    "Accept": "application/json",
}
FANGRAPHS_URL_CACHE: Dict[str, Optional[str]] = {}
PITCHER_PROFILE_CACHE: Dict[int, Dict[str, object]] = {}
DEPTH_CHART_CACHE: Dict[str, List[Dict[str, object]]] = {}
DEPTH_CHART_DATA_CACHE: Dict[str, Dict[str, object]] = {}
FANGRAPHS_PITCHER_ID_MAP_CACHE: Optional[Dict[str, int]] = None
PITCH_STUFF_CACHE: Dict[str, Dict[str, object]] = {}
SAVANT_PITCH_TYPES = [
    ("ff", "Four-Seam"),
    ("si", "Sinker"),
    ("fc", "Cutter"),
    ("fs", "Splitter"),
    ("fo", "Forkball"),
    ("sl", "Slider"),
    ("st", "Sweeper"),
    ("cu", "Curveball"),
    ("sv", "Slurve"),
    ("ch", "Changeup"),
    ("sc", "Screwball"),
    ("kn", "Knuckleball"),
]
STUFF_PLUS_FIELD_MAP = {
    "FF": "FF",
    "SI": "SI",
    "FC": "FC",
    "FS": "FS",
    "FO": "FO",
    "SL": "SL",
    "ST": "SL",
    "SV": "SL",
    "CU": "CU",
    "CH": "CH",
    "KC": "KC",
    "KN": None,
    "SC": None,
}
@dataclass
class PlayerRow:
    status: str
    position: str
    name: str
    bats: str
    stats: List[str]


@dataclass
class BullpenRow:
    name: str
    throws: str
    role: str
    usage: List[str]
    stats: List[str]


@dataclass
class BullpenInjuryRow:
    name: str
    throws: str
    status: str


class LineBreakHTMLParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: List[str] = []

    def handle_starttag(self, tag: str, attrs) -> None:
        if tag.lower() in {"h1", "h2", "h3", "div", "p", "br", "tr", "li", "section", "header"}:
            self.parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() in {"h1", "h2", "h3", "div", "p", "tr", "li", "section", "header"}:
            self.parts.append("\n")
        elif tag.lower() in {"td", "th", "a", "span"}:
            self.parts.append(" ")

    def handle_data(self, data: str) -> None:
        if data:
            self.parts.append(data)
            self.parts.append(" ")

    def get_text(self) -> str:
        return "".join(self.parts)


def fetch_url(url: str) -> bytes:
    req = Request(url, headers=DEFAULT_HEADERS)
    with urlopen(req, timeout=30) as resp:
        return resp.read()


def fetch_json(url: str) -> Dict[str, object]:
    req = Request(url, headers=JSON_HEADERS)
    with urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8", "ignore"))


def fetch_fangraphs_page(url: str) -> str:
    req = Request(url, headers=FANGRAPHS_HEADERS)
    with urlopen(req, timeout=30) as resp:
        return resp.read().decode("utf-8", "ignore")


def fetch_text_page(url: str) -> str:
    req = Request(url, headers=DEFAULT_HEADERS)
    with urlopen(req, timeout=30) as resp:
        return resp.read().decode("utf-8", "ignore")


def normalize_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (value or "").lower())


def resolve_team_slug(team_name: str) -> Optional[str]:
    return MLB_TEAM_NAME_TO_SLUG.get((team_name or "").strip().lower())


def next_mlb_date() -> date:
    return date.today() + timedelta(days=1)


def extract_lines(raw_html: str) -> List[str]:
    parser = LineBreakHTMLParser()
    parser.feed(raw_html)
    text = html.unescape(parser.get_text()).replace("\xa0", " ")
    lines = []
    for raw_line in text.splitlines():
        line = re.sub(r"\s+", " ", raw_line).strip()
        if line:
            lines.append(line)
    return lines


def looks_like_game_time(line: str) -> bool:
    return bool(re.match(r"^\d{1,2}:\d{2}\s+[AP]M ET$", line))


def looks_like_pitcher_line(line: str) -> bool:
    return " HP " in line or line == "TBD" or " | " in line


def looks_like_schedule_note(line: str) -> bool:
    normalized = (line or "").strip().lower()
    if not normalized:
        return False
    return normalized.startswith("makeup of ") or normalized.startswith("completion of ")


def clean_probable_pitcher(line: str) -> Dict[str, str]:
    cleaned = re.sub(r"\s+", " ", line).strip()
    if cleaned == "TBD":
        return {"name": "TBD", "hand": "", "record": "", "era": ""}

    match = re.match(
        r"^(?P<name>.+?)\s+(?P<hand>[RLS])\s+HP(?:\s+(?P<record>\d+\s*-\s*\d+))?(?:\s*\|\s*(?P<era>.+))?$",
        cleaned,
    )
    if match:
        return {
            "name": match.group("name").strip(),
            "hand": match.group("hand") or "",
            "record": (match.group("record") or "").strip(),
            "era": (match.group("era") or "").strip(),
        }

    return {"name": cleaned, "hand": "", "record": "", "era": ""}


def extract_pitcher_entries(lines: List[str]) -> List[Dict[str, str]]:
    entries: List[Dict[str, str]] = []
    pattern = re.compile(
        r"(?P<name>.+?)\s+(?P<hand>[RLS])\s+HP(?:\s+(?P<record>\d+\s*-\s*\d+))?(?:\s*\|\s*(?P<era>\d+(?:\.\d+)?\s+ERA))?"
    )
    hand_line_pattern = re.compile(r"^(?P<hand>[RLS])\s+HP$")
    stat_line_pattern = re.compile(r"^(?P<record>\d+\s*-\s*\d+)(?:\s*\|\s*(?P<era>\d+(?:\.\d+)?\s+ERA))?$")

    normalized_lines = [re.sub(r"\s+", " ", raw_line).strip() for raw_line in lines]
    idx = 0
    while idx < len(normalized_lines):
        line = normalized_lines[idx]
        if not line or line == "-":
            idx += 1
            continue
        if line == "TBD":
            entries.append({"name": "TBD", "hand": "", "record": "", "era": ""})
            idx += 1
            if idx < len(normalized_lines) and normalized_lines[idx] == "-":
                idx += 1
            continue

        if (
            idx + 2 < len(normalized_lines)
            and hand_line_pattern.match(normalized_lines[idx + 1])
            and stat_line_pattern.match(normalized_lines[idx + 2])
        ):
            hand_match = hand_line_pattern.match(normalized_lines[idx + 1])
            stat_match = stat_line_pattern.match(normalized_lines[idx + 2])
            entries.append(
                {
                    "name": line,
                    "hand": hand_match.group("hand"),
                    "record": stat_match.group("record").strip(),
                    "era": (stat_match.group("era") or "").strip(),
                }
            )
            idx += 3
            continue

        matches = list(pattern.finditer(line))
        if matches:
            for match in matches:
                entries.append(
                    {
                        "name": match.group("name").strip(),
                        "hand": match.group("hand") or "",
                        "record": (match.group("record") or "").strip(),
                        "era": (match.group("era") or "").strip(),
                    }
                )
            idx += 1
            continue

        if looks_like_pitcher_line(line):
            entries.append(clean_probable_pitcher(line))
        idx += 1

    return entries


def extract_mlb_pitcher_cards(raw_html: str) -> List[Dict[str, str]]:
    pattern = re.compile(
        r'<a href="/player/[^"]+-(?P<mlb_id>\d+)"[^>]*>.*?<div name="(?P<display_name>[^"]+)" class="playerMatchupstyle__PlayerMatchupWrapper.*?<img[^>]+src="(?P<headshot_url>[^"]+)"',
        re.DOTALL,
    )
    cards: List[Dict[str, str]] = []
    for match in pattern.finditer(raw_html):
        cards.append(
            {
                "mlb_id": match.group("mlb_id"),
                "display_name": html.unescape(match.group("display_name")),
                "headshot_url": html.unescape(match.group("headshot_url")),
            }
        )
    return cards


def attach_pitcher_card_details(games: List[Dict[str, object]], cards: List[Dict[str, str]]) -> None:
    card_index = 0
    for game in games:
        for side in ("away_pitcher", "home_pitcher"):
            pitcher = game[side]
            if pitcher.get("name") == "TBD":
                pitcher["mlb_id"] = None
                pitcher["headshot_url"] = ""
                continue
            if card_index >= len(cards):
                pitcher["mlb_id"] = None
                pitcher["headshot_url"] = ""
                continue

            card = cards[card_index]
            card_index += 1
            pitcher["mlb_id"] = int(card["mlb_id"])
            pitcher["headshot_url"] = card["headshot_url"]


def parse_probable_pitchers_page(target_date: date, raw_html: str) -> Dict[str, object]:
    lines = extract_lines(raw_html)
    games: List[Dict[str, object]] = []
    pitcher_cards = extract_mlb_pitcher_cards(raw_html)
    idx = 0

    while idx < len(lines):
        line = lines[idx]
        if not looks_like_game_time(line):
            idx += 1
            continue

        game_time = line
        search_end = min(idx + 40, len(lines))
        preview_index = None
        for look_ahead in range(idx + 1, search_end):
            if lines[look_ahead] == "Preview":
                preview_index = look_ahead
                break

        if preview_index is None:
            idx += 1
            continue

        window = lines[idx:preview_index]
        watch_index = next((i for i, value in enumerate(window) if value.startswith("Watch on:")), None)
        if watch_index is None or watch_index + 4 >= len(window):
            idx = preview_index + 1
            continue

        post_watch_lines = [value for value in window[watch_index + 1:] if value and not looks_like_schedule_note(value)]
        if len(post_watch_lines) < 2:
            idx = preview_index + 1
            continue

        away_team = post_watch_lines[0]
        home_team = post_watch_lines[1]
        pitcher_entries = extract_pitcher_entries(post_watch_lines[2:])
        if not pitcher_entries:
            idx = preview_index + 1
            continue

        if len(pitcher_entries) == 1:
            away_pitcher = pitcher_entries[0]
            home_pitcher = {"name": "TBD", "hand": "", "record": "", "era": ""}
        else:
            away_pitcher = pitcher_entries[0]
            home_pitcher = pitcher_entries[1]

        games.append(
            {
                "game_time": game_time,
                "away_team": away_team,
                "home_team": home_team,
                "away_pitcher": away_pitcher,
                "home_pitcher": home_pitcher,
            }
        )
        idx = preview_index + 1

    attach_pitcher_card_details(games, pitcher_cards)
    attach_fangraphs_ids(target_date, games)

    return {
        "date": target_date.isoformat(),
        "source_url": f"{MLB_SCORES_URL}/{target_date.isoformat()}",
        "games": games,
        "default_date": next_mlb_date().isoformat(),
    }


def fetch_probable_pitchers(target_date: date) -> Dict[str, object]:
    raw = fetch_url(f"{MLB_SCORES_URL}/{target_date.isoformat()}")
    html_text = raw.decode("utf-8", "ignore")
    return parse_probable_pitchers_page(target_date, html_text)


def fetch_depth_chart_data(team_slug: str) -> Dict[str, object]:
    if team_slug in DEPTH_CHART_DATA_CACHE:
        return DEPTH_CHART_DATA_CACHE[team_slug]

    page_html = fetch_fangraphs_page(f"https://www.fangraphs.com/roster-resource/depth-charts/{team_slug}")
    payload = extract_next_data(page_html)
    queries = payload.get("props", {}).get("pageProps", {}).get("dehydratedState", {}).get("queries", [])
    if not queries:
        DEPTH_CHART_DATA_CACHE[team_slug] = {}
        return {}

    data = queries[0].get("state", {}).get("data", {})
    DEPTH_CHART_DATA_CACHE[team_slug] = data if isinstance(data, dict) else {}
    return DEPTH_CHART_DATA_CACHE[team_slug]


def fetch_depth_chart_probables(team_slug: str) -> List[Dict[str, object]]:
    if team_slug in DEPTH_CHART_CACHE:
        return DEPTH_CHART_CACHE[team_slug]

    probable_data = fetch_depth_chart_data(team_slug).get("dataProbableStarters", {})
    game_list = probable_data.get("gameList", []) if isinstance(probable_data, dict) else []
    DEPTH_CHART_CACHE[team_slug] = game_list
    return game_list


def build_fangraphs_pitcher_id_map() -> Dict[str, int]:
    global FANGRAPHS_PITCHER_ID_MAP_CACHE
    if FANGRAPHS_PITCHER_ID_MAP_CACHE is not None:
        return FANGRAPHS_PITCHER_ID_MAP_CACHE

    pitcher_map: Dict[str, int] = {}
    for team in TEAM_OPTIONS:
        roster_rows = fetch_depth_chart_data(team["slug"]).get("dataRoster", [])
        if not isinstance(roster_rows, list):
            continue
        for row in roster_rows:
            position_label = str(row.get("position1") or row.get("position") or "")
            mlb_id = row.get("mlbamid")
            fangraphs_id = row.get("playerid") or row.get("playerid1") or row.get("playerid2")
            if "P" not in position_label or not mlb_id or not fangraphs_id:
                continue
            pitcher_map[str(mlb_id)] = int(fangraphs_id)

    FANGRAPHS_PITCHER_ID_MAP_CACHE = pitcher_map
    return pitcher_map


def attach_fangraphs_ids(target_date: date, games: List[Dict[str, object]]) -> None:
    target_label = f"{target_date.month}/{target_date.day}/{target_date.year}"
    for game in games:
        away_slug = resolve_team_slug(str(game.get("away_team", "")))
        if not away_slug:
            continue

        probable_rows = fetch_depth_chart_probables(away_slug)
        row = next((item for item in probable_rows if item.get("gameDate") == target_label), None)
        if not row:
            continue

        game["away_pitcher"]["fangraphs_player_id"] = int(row["playerId"]) if row.get("playerId") else None
        game["home_pitcher"]["fangraphs_player_id"] = int(row["oppPlayerId"]) if row.get("oppPlayerId") else None


def extract_next_data(page_html: str) -> Dict[str, object]:
    match = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', page_html)
    if not match:
        raise ValueError("Could not locate Fangraphs structured page data.")
    return json.loads(match.group(1))


def decode_duckduckgo_href(href: str) -> str:
    decoded_href = html.unescape(href)
    parsed = urlparse(decoded_href)
    if parsed.netloc.endswith("duckduckgo.com") and parsed.path == "/l/":
        uddg = parse_qs(parsed.query).get("uddg", [])
        if uddg:
            return unquote(uddg[0])
    if href.startswith("//"):
        return f"https:{decoded_href}"
    return decoded_href


def resolve_fangraphs_player_url(player_name: str) -> Optional[str]:
    cache_key = normalize_name(player_name)
    if cache_key in FANGRAPHS_URL_CACHE:
        return FANGRAPHS_URL_CACHE[cache_key]

    search_url = f"https://duckduckgo.com/html/?q={quote(player_name + ' site:fangraphs.com/players')}"
    req = Request(search_url, headers=DEFAULT_HEADERS)
    with urlopen(req, timeout=30) as resp:
        search_html = resp.read().decode("utf-8", "ignore")

    candidates = re.findall(r'href="([^"]+)"', search_html)
    for href in candidates:
        decoded = decode_duckduckgo_href(href)
        if "fangraphs.com/players/" not in decoded:
            continue
        player_match = re.search(r"https://www\.fangraphs\.com/players/([^/]+)/([^/?#]+)/", decoded)
        if not player_match:
            continue
        slug, player_id = player_match.groups()
        resolved = f"https://www.fangraphs.com/players/{slug}/{player_id}/stats/pitching"
        FANGRAPHS_URL_CACHE[cache_key] = resolved
        return resolved

    FANGRAPHS_URL_CACHE[cache_key] = None
    return None


def format_decimal(value: Optional[object], digits: int = 2) -> Optional[str]:
    if value is None or value == "":
        return None
    try:
        return f"{float(value):.{digits}f}"
    except (TypeError, ValueError):
        return str(value)


def format_percentage(value: Optional[object], digits: int = 1) -> Optional[str]:
    if value is None or value == "":
        return None
    try:
        return f"{float(value) * 100:.{digits}f}%"
    except (TypeError, ValueError):
        return str(value)


def find_current_pitching_row(data_common: Dict[str, object]) -> Optional[Dict[str, object]]:
    for row in data_common.get("data", []):
        if row.get("type") == 0 and row.get("AbbLevel") == "MLB":
            return row
    return None


def strip_tags(value: object) -> str:
    return re.sub(r"<[^>]+>", "", str(value or "")).strip()


def extract_season_int(value: object) -> Optional[int]:
    cleaned = strip_tags(value)
    match = re.search(r"\b(20\d{2})\b", cleaned)
    return int(match.group(1)) if match else None


def find_pitching_row_for_season(rows: List[Dict[str, object]], season: int) -> Optional[Dict[str, object]]:
    candidates = [
        row
        for row in rows
        if row.get("AbbLevel") == "MLB" and extract_season_int(row.get("Season")) == season
    ]
    if not candidates:
        return None

    def sort_key(row: Dict[str, object]) -> tuple[int, int]:
        team_label = strip_tags(row.get("Team"))
        is_total = 0 if team_label in {"- - -", "TOT"} else 1
        type_value = 0 if row.get("type") == 0 else 1
        return (is_total, type_value)

    return sorted(candidates, key=sort_key)[0]


def find_projection_row(rows: List[Dict[str, object]], team_label: str) -> Optional[Dict[str, object]]:
    for row in rows:
        if str(row.get("Team")) == team_label:
            return row
    return None


def build_pitcher_profile(mlb_player_id: int, fangraphs_player_id: Optional[int] = None) -> Dict[str, object]:
    if mlb_player_id in PITCHER_PROFILE_CACHE:
        return PITCHER_PROFILE_CACHE[mlb_player_id]

    mlb_data = fetch_json(f"https://statsapi.mlb.com/api/v1/people/{mlb_player_id}")
    people = mlb_data.get("people", [])
    if not people:
        raise ValueError("Could not find MLB player bio.")
    person = people[0]
    full_name = person.get("fullName", "")

    fangraphs_url = (
        f"https://www.fangraphs.com/statss.aspx?playerid={fangraphs_player_id}"
        if fangraphs_player_id
        else resolve_fangraphs_player_url(full_name)
    )
    if not fangraphs_url:
        raise ValueError("Could not resolve a Fangraphs player page for this pitcher.")

    fg_html = fetch_fangraphs_page(fangraphs_url)
    fg_payload = extract_next_data(fg_html)
    page_props = fg_payload.get("props", {}).get("pageProps", {})
    data_common = page_props.get("dataCommon", {})
    data_stats = page_props.get("dataStats", {})
    player_info = data_stats.get("playerInfo", {})
    team_info = data_stats.get("teamInfo", {})
    current_row = find_current_pitching_row(data_common)
    projection_rows = data_stats.get("data", [])

    if not current_row:
        raise ValueError("Could not find the current Fangraphs pitching row for this pitcher.")

    steamer_ros = find_projection_row(projection_rows, "Steamer (RoS)")
    the_bat_ros = find_projection_row(projection_rows, "THE BAT (RoS)")

    profile = {
        "player": {
            "mlb_id": mlb_player_id,
            "name": person.get("fullName") or player_info.get("firstLastName") or full_name,
            "team": team_info.get("MLB_FullName"),
            "headshot_url": MLB_HEADSHOT_TEMPLATE.format(mlb_id=mlb_player_id),
            "height": person.get("height") or player_info.get("HeightDisplay"),
            "weight": person.get("weight") or player_info.get("Weight"),
            "age": person.get("currentAge") or player_info.get("AgeToday"),
            "pitch_hand": (person.get("pitchHand") or {}).get("description", ""),
        },
        "sources": {
            "mlb_url": f"https://www.mlb.com/player/{person.get('nameSlug') or mlb_player_id}",
            "fangraphs_url": fangraphs_url,
        },
        "metrics": {
            "ERA": format_decimal(current_row.get("ERA")),
            "xERA": format_decimal(current_row.get("xERA")),
            "FIP": format_decimal(current_row.get("FIP")),
            "xFIP": format_decimal(current_row.get("xFIP")),
            "SIERA": format_decimal(current_row.get("SIERA")),
            "StuffPlus": format_decimal(current_row.get("sp_stuff"), 1),
            "LocationPlus": format_decimal(current_row.get("sp_location"), 1),
            "PitchingPlus": format_decimal(current_row.get("sp_pitching"), 1),
            "SteamerRosERA": format_decimal((steamer_ros or {}).get("ERA")),
            "TheBatRosERA": format_decimal((the_bat_ros or {}).get("ERA")),
            "KMinusBBPercent": format_percentage(current_row.get("K-BB%")),
        },
    }

    PITCHER_PROFILE_CACHE[mlb_player_id] = profile
    return profile


def find_section_indexes(lines: List[str]) -> Dict[str, int]:
    indexes: Dict[str, int] = {}
    for idx, line in enumerate(lines):
        compact = line.lower()
        if "starting lineup vsr" in compact and "vsR" not in indexes:
            indexes["vsR"] = idx
        elif "starting lineup vsl" in compact and "vsL" not in indexes:
            indexes["vsL"] = idx
        elif line.startswith("Updated:") and "updated" not in indexes:
            indexes["updated"] = idx
    return indexes


def parse_header_row(lines: List[str], start: int) -> List[str]:
    for idx in range(start, min(start + 4, len(lines))):
        if lines[idx].startswith("Order/Status"):
            header = lines[idx].replace("Order/Status", "Order/Status ")
            header = header.replace("wRC+", " wRC+")
            parts = [part for part in header.split() if part]
            return parts
    return ["Order/Status", "Position", "Name", "Bats", "PA", "HR", "wRC+"]


def locate_bats_index(tokens: List[str], position_index: int) -> Optional[int]:
    for idx in range(len(tokens) - 1, position_index, -1):
        if tokens[idx] in VALID_BATS:
            return idx
    return None


def parse_player_row(line: str) -> Optional[PlayerRow]:
    if line.startswith("Order/Status") or line.startswith("Updated:"):
        return None

    cleaned = re.sub(r"[▼▲â–¼â–²]", " ", line)
    cleaned = cleaned.replace("Ã¢â€“Â¼", " ").replace("Ã¢â€“Â²", " ")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    tokens = cleaned.split()
    if len(tokens) < 4:
        return None

    position_index = next((idx for idx, token in enumerate(tokens) if token in POSITIONS), None)
    if position_index is None or position_index == 0:
        return None

    bats_index = locate_bats_index(tokens, position_index)
    if bats_index is None or bats_index <= position_index + 1:
        return None

    status = " ".join(tokens[:position_index])
    position = tokens[position_index]
    name = " ".join(tokens[position_index + 1:bats_index])
    stats = tokens[bats_index + 1:]

    if not status or not position or not name:
        return None

    return PlayerRow(status=status, position=position, name=name, bats=tokens[bats_index], stats=stats)


def parse_section(lines: List[str], start: int, end: int) -> Dict[str, object]:
    title = lines[start]
    headers = parse_header_row(lines, start + 1)
    rows: List[PlayerRow] = []
    seen = set()

    for line in lines[start + 1:end]:
        row = parse_player_row(line)
        if row is None:
            continue
        row_key = (row.status, row.position, row.name, row.bats, tuple(row.stats))
        if row_key in seen:
            continue
        seen.add(row_key)
        rows.append(row)

    return {
        "title": title,
        "headers": headers,
        "rows": [
            {
                "status": row.status,
                "position": row.position,
                "name": row.name,
                "bats": row.bats,
                "stats": row.stats,
            }
            for row in rows
        ],
    }


def split_rows(rows: List[Dict[str, object]]) -> Dict[str, List[str]]:
    starters: List[str] = []
    bench: List[str] = []
    injured: List[str] = []

    for row in rows:
        status = str(row.get("status", "")).strip()
        name = str(row.get("name", "")).strip()
        if not name:
            continue
        lowered = status.lower()
        if lowered.startswith("bench"):
            bench.append(name)
        elif lowered.startswith("il"):
            injured.append(name)
        elif re.match(r"^\d+$", status):
            starters.append(name)

    if not starters:
        starters = [str(row.get("name", "")).strip() for row in rows[:STARTING_LINEUP_LENGTH] if row.get("name")]

    return {"starters": starters, "bench": bench, "il": injured}


def parse_lineup_page(team_slug: str, raw_html: str) -> Dict[str, object]:
    if team_slug not in TEAM_MAP:
        raise HTTPException(status_code=404, detail="Unknown team slug")

    lines = extract_lines(raw_html)
    indexes = find_section_indexes(lines)
    if "vsR" not in indexes or "vsL" not in indexes:
        raise ValueError("Could not find Fangraphs platoon lineup sections on the page.")

    updated_line = lines[indexes["updated"]] if "updated" in indexes else "Updated: unavailable"
    vsr_end = indexes["vsL"]
    vsl_end = indexes.get("updated", len(lines))
    vsr_section = parse_section(lines, indexes["vsR"], vsr_end)
    vsl_section = parse_section(lines, indexes["vsL"], vsl_end)

    return {
        "team": TEAM_MAP[team_slug],
        "source_url": f"{BASE_URL}/{team_slug}",
        "updated": updated_line,
        "refreshed_every_seconds": REFRESH_SECONDS,
        "sections": {
            "vsR": vsr_section,
            "vsL": vsl_section,
        },
        "excel_layout": {
            "vsR": split_rows(vsr_section["rows"]),
            "vsL": split_rows(vsl_section["rows"]),
        },
    }


def fetch_lineup_data(team_slug: str) -> Dict[str, object]:
    url = f"{BASE_URL}/{team_slug}"
    raw = fetch_url(url)
    html_text = raw.decode("utf-8", "ignore")
    return parse_lineup_page(team_slug, html_text)


TEAM_CODE_MAP = {team["code"]: team for team in TEAM_OPTIONS}
BULLPEN_ROLE_PREFIXES = [
    "Closer Committee",
    "Middle Reliever",
    "Long Reliever",
    "Setup Man",
    "Closer",
]


def strip_fangraphs_link_markup(line: str) -> str:
    cleaned = re.sub(r"【\d+†", " ", line)
    cleaned = cleaned.replace("】", " ")
    return re.sub(r"\s+", " ", cleaned).strip()


def parse_bullpen_row(line: str) -> Optional[Dict[str, object]]:
    cleaned = strip_fangraphs_link_markup(line)
    match = re.match(r"^(?P<team>[A-Z]{3})\s+(?P<body>.+)$", cleaned)
    if not match:
        return None

    team_code = match.group("team")
    if team_code not in TEAM_CODE_MAP:
        return None

    body = match.group("body")
    body_match = re.match(r"^(?P<name>.+?)\s+(?P<throws>[RL])\s+(?P<rest>.+)$", body)
    if not body_match:
        return None

    name = body_match.group("name").strip()
    throws = body_match.group("throws")
    rest = body_match.group("rest").strip()

    if rest.endswith("IL"):
        return {
            "kind": "injured",
            "team_code": team_code,
            "row": BullpenInjuryRow(name=name, throws=throws, status=rest),
        }

    role = next((prefix for prefix in BULLPEN_ROLE_PREFIXES if rest.startswith(prefix)), "")
    if not role:
        return None

    remainder = rest[len(role) :].strip()
    tokens = remainder.split()
    if len(tokens) < 9:
        return None

    usage = tokens[:-9]
    stats = tokens[-9:]
    return {
        "kind": "active",
        "team_code": team_code,
        "row": BullpenRow(name=name, throws=throws, role=role, usage=usage, stats=stats),
    }


def parse_bullpen_page(raw_html: str) -> Dict[str, object]:
    lines = extract_lines(raw_html)
    updated = next((line for line in reversed(lines) if line.startswith("Updated:")), "Updated: unavailable")
    teams: Dict[str, Dict[str, object]] = {
        team["slug"]: {
            "team": team,
            "active_headers": ["Name", "Thr", "Role", "Recent Usage", "vFA", "vSI", "G", "IP", "ERA", "Sv", "HLD", "K/9", "K%"],
            "active_rows": [],
            "injured_headers": ["Name", "Thr", "Status"],
            "injured_rows": [],
            "active_seen": set(),
            "injured_seen": set(),
        }
        for team in TEAM_OPTIONS
    }

    for line in lines:
        parsed = parse_bullpen_row(line)
        if not parsed:
            continue
        team = TEAM_CODE_MAP[parsed["team_code"]]
        bucket = teams[team["slug"]]
        if parsed["kind"] == "active":
            row = parsed["row"]
            row_key = (row.name, row.throws, row.role, tuple(row.usage), tuple(row.stats))
            if row_key in bucket["active_seen"]:
                continue
            bucket["active_seen"].add(row_key)
            bucket["active_rows"].append(
                {
                    "name": row.name,
                    "throws": row.throws,
                    "role": row.role,
                    "usage": " ".join(row.usage),
                    "stats": row.stats,
                }
            )
        else:
            row = parsed["row"]
            row_key = (row.name, row.throws, row.status)
            if row_key in bucket["injured_seen"]:
                continue
            bucket["injured_seen"].add(row_key)
            bucket["injured_rows"].append(
                {
                    "name": row.name,
                    "throws": row.throws,
                    "status": row.status,
                }
            )

    for team_payload in teams.values():
        team_payload.pop("active_seen", None)
        team_payload.pop("injured_seen", None)

    return {
        "source_url": BULLPEN_URL,
        "updated": updated,
        "refreshed_every_seconds": REFRESH_SECONDS,
        "teams": teams,
    }


def fetch_bullpen_data(team_slug: str) -> Dict[str, object]:
    if team_slug not in TEAM_MAP:
        raise HTTPException(status_code=404, detail="Unknown team slug")

    raw_html = fetch_fangraphs_page(BULLPEN_URL)
    payload = parse_bullpen_page(raw_html)
    team_payload = payload["teams"][team_slug]
    return {
        "team": team_payload["team"],
        "source_url": payload["source_url"],
        "updated": payload["updated"],
        "refreshed_every_seconds": payload["refreshed_every_seconds"],
        "sections": {
            "active": {
                "title": "Projected Bullpen",
                "headers": team_payload["active_headers"],
                "rows": team_payload["active_rows"],
            },
            "injured": {
                "title": "IL / Unavailable",
                "headers": team_payload["injured_headers"],
                "rows": team_payload["injured_rows"],
            },
        },
    }


def extract_js_array(raw_html: str, marker: str) -> str:
    marker_index = raw_html.find(marker)
    if marker_index == -1:
        raise ValueError("Could not find embedded Baseball Savant pitch arsenal data.")

    array_start = raw_html.find("[", marker_index)
    if array_start == -1:
        raise ValueError("Could not find start of Baseball Savant pitch arsenal data array.")

    depth = 0
    in_string = False
    escape = False

    for idx in range(array_start, len(raw_html)):
        char = raw_html[idx]

        if in_string:
            if escape:
                escape = False
            elif char == "\\":
                escape = True
            elif char == '"':
                in_string = False
            continue

        if char == '"':
            in_string = True
        elif char == "[":
            depth += 1
        elif char == "]":
            depth -= 1
            if depth == 0:
                return raw_html[array_start:idx + 1]

    raise ValueError("Could not find end of Baseball Savant pitch arsenal data array.")


def extract_savant_pitch_data(raw_html: str) -> List[Dict[str, object]]:
    return json.loads(extract_js_array(raw_html, "var data ="))


def eastern_now() -> datetime:
    try:
        return datetime.now(ZoneInfo("America/New_York"))
    except ZoneInfoNotFoundError:
        # On some Windows Python installs tzdata is missing; this app runs on the user's ET machine.
        return datetime.now()


def current_stuff_plus_refresh_key(now: Optional[datetime] = None) -> str:
    current_time = now if now else eastern_now()
    if current_time.tzinfo is not None:
        try:
            current_time = current_time.astimezone(ZoneInfo("America/New_York"))
        except ZoneInfoNotFoundError:
            pass
    effective_date = current_time.date() if current_time.hour >= 4 else current_time.date() - timedelta(days=1)
    return effective_date.isoformat()


def load_stuff_plus_cache_file() -> Dict[str, object]:
    if not STUFF_PLUS_CACHE_PATH.exists():
        return {}
    try:
        return json.loads(STUFF_PLUS_CACHE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def save_stuff_plus_cache_file(payload: Dict[str, object]) -> None:
    STUFF_PLUS_CACHE_PATH.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def extract_fangraphs_player_id_from_url(url: Optional[str]) -> Optional[int]:
    if not url:
        return None
    statss_match = re.search(r"[?&]playerid=(\d+)", url)
    if statss_match:
        return int(statss_match.group(1))
    path_match = re.search(r"/players/[^/]+/(\d+)/", url)
    if path_match:
        return int(path_match.group(1))
    return None


def build_velocity_pitcher_index(
    current_velocity_rows: List[Dict[str, object]],
    previous_velocity_rows: List[Dict[str, object]],
    probable_games: List[Dict[str, object]],
) -> Dict[str, Dict[str, object]]:
    pitcher_index: Dict[str, Dict[str, object]] = {}
    fangraphs_id_map = build_fangraphs_pitcher_id_map()

    for row in current_velocity_rows + previous_velocity_rows:
        pitcher_id = str(row.get("pitcher") or "").strip()
        if not pitcher_id:
            continue
        pitcher_entry = pitcher_index.setdefault(
            pitcher_id,
            {
                "pitcher_id": pitcher_id,
                "pitcher_name": str(row.get("name_display_first_last") or "").strip(),
                "fangraphs_player_id": fangraphs_id_map.get(pitcher_id),
            },
        )
        if not pitcher_entry["pitcher_name"]:
            pitcher_entry["pitcher_name"] = str(row.get("name_display_first_last") or "").strip()
        if not pitcher_entry.get("fangraphs_player_id") and fangraphs_id_map.get(pitcher_id):
            pitcher_entry["fangraphs_player_id"] = fangraphs_id_map[pitcher_id]

    for game in probable_games:
        for side in ("away_pitcher", "home_pitcher"):
            pitcher = game.get(side, {})
            pitcher_id = str(pitcher.get("mlb_id") or "").strip()
            if not pitcher_id:
                continue
            pitcher_entry = pitcher_index.setdefault(
                pitcher_id,
                {
                    "pitcher_id": pitcher_id,
                    "pitcher_name": pitcher.get("name", ""),
                    "fangraphs_player_id": None,
                },
            )
            if pitcher.get("name"):
                pitcher_entry["pitcher_name"] = pitcher["name"]
            if pitcher.get("fangraphs_player_id"):
                pitcher_entry["fangraphs_player_id"] = int(pitcher["fangraphs_player_id"])

    return pitcher_index


def build_pitch_stuff_plus_year_map(season_row: Optional[Dict[str, object]]) -> Dict[str, Optional[int]]:
    if not season_row:
        return {}
    return build_pitch_stuff_plus_map(season_row)


def fetch_pitch_stuff_plus_years(
    pitcher_name: str,
    seasons: List[int],
    fangraphs_player_id: Optional[int] = None,
) -> Dict[str, object]:
    fangraphs_url = (
        f"https://www.fangraphs.com/statss.aspx?playerid={fangraphs_player_id}"
        if fangraphs_player_id
        else resolve_fangraphs_player_url(pitcher_name)
    )
    resolved_fangraphs_player_id = fangraphs_player_id or extract_fangraphs_player_id_from_url(fangraphs_url)
    if not fangraphs_url:
        return {
            "pitcher_name": pitcher_name,
            "fangraphs_player_id": resolved_fangraphs_player_id,
            "years": {str(season): {} for season in seasons},
        }

    fg_html = fetch_fangraphs_page(fangraphs_url)
    fg_payload = extract_next_data(fg_html)
    data_stats = fg_payload.get("props", {}).get("pageProps", {}).get("dataStats", {})
    season_rows = data_stats.get("data", [])

    return {
        "pitcher_name": pitcher_name,
        "fangraphs_player_id": resolved_fangraphs_player_id,
        "years": {
            str(season): build_pitch_stuff_plus_year_map(find_pitching_row_for_season(season_rows, season))
            for season in seasons
        },
    }


def build_daily_stuff_plus_cache(
    pitcher_index: Dict[str, Dict[str, object]],
    seasons: List[int],
) -> Dict[str, object]:
    pitchers_payload: Dict[str, Dict[str, object]] = {}

    for pitcher_id, pitcher_info in pitcher_index.items():
        payload: Dict[str, object]
        try:
            payload = fetch_pitch_stuff_plus_years(
                pitcher_info.get("pitcher_name", ""),
                seasons=seasons,
                fangraphs_player_id=pitcher_info.get("fangraphs_player_id"),
            )
        except Exception:
            time.sleep(0.75)
            try:
                payload = fetch_pitch_stuff_plus_years(
                    pitcher_info.get("pitcher_name", ""),
                    seasons=seasons,
                    fangraphs_player_id=pitcher_info.get("fangraphs_player_id"),
                )
            except Exception:
                payload = {
                    "pitcher_name": pitcher_info.get("pitcher_name", ""),
                    "fangraphs_player_id": pitcher_info.get("fangraphs_player_id"),
                    "years": {str(season): {} for season in seasons},
                }
        pitchers_payload[pitcher_id] = {
            "pitcher_id": pitcher_id,
            "pitcher_name": payload.get("pitcher_name", pitcher_info.get("pitcher_name", "")),
            "fangraphs_player_id": payload.get("fangraphs_player_id"),
            "years": payload.get("years", {}),
        }
        time.sleep(0.2)

    return {
        "generated_at": eastern_now().isoformat(),
        "refresh_key": current_stuff_plus_refresh_key(),
        "seasons": seasons,
        "pitchers": pitchers_payload,
    }


def get_or_build_daily_stuff_plus_cache(
    pitcher_index: Dict[str, Dict[str, object]],
    seasons: List[int],
) -> Dict[str, object]:
    expected_refresh_key = current_stuff_plus_refresh_key()
    cached = load_stuff_plus_cache_file()
    cached_pitchers = cached.get("pitchers", {}) if isinstance(cached, dict) else {}
    cached_seasons = cached.get("seasons", []) if isinstance(cached, dict) else []

    cache_nonempty_count = sum(
        any(year_map for year_map in (pitcher_payload.get("years", {}) or {}).values())
        for pitcher_payload in cached_pitchers.values()
    )
    cache_has_values = cache_nonempty_count >= max(1, len(pitcher_index) // 2)

    if (
        cached.get("refresh_key") == expected_refresh_key
        and cached_seasons == seasons
        and cache_has_values
        and all(pitcher_id in cached_pitchers for pitcher_id in pitcher_index)
    ):
        return cached

    refreshed = build_daily_stuff_plus_cache(pitcher_index, seasons)
    save_stuff_plus_cache_file(refreshed)
    return refreshed


def attach_stuff_plus_from_cache(
    rows: List[Dict[str, object]],
    stuff_cache: Dict[str, object],
    current_year: int,
    previous_year: int,
) -> None:
    pitchers_payload = stuff_cache.get("pitchers", {}) if isinstance(stuff_cache, dict) else {}
    for row in rows:
        pitcher_payload = pitchers_payload.get(str(row.get("pitcher_id") or ""))
        years_payload = pitcher_payload.get("years", {}) if pitcher_payload else {}
        current_map = years_payload.get(str(current_year), {})
        previous_map = years_payload.get(str(previous_year), {})
        pitch_code = str(row.get("pitch_code", "")).upper()
        row["stuff_plus"] = current_map.get(pitch_code)
        row["previous_stuff_plus"] = previous_map.get(pitch_code)


def parse_float(value: object) -> Optional[float]:
    if value in (None, "", "null"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def build_velocity_comparison() -> Dict[str, object]:
    current_year = date.today().year
    previous_year = current_year - 1
    current_velocity_url = SAVANT_PITCH_ARSENALS_URL.format(year=current_year, minimum="q", metric_type="avg_speed")
    previous_velocity_url = SAVANT_PITCH_ARSENALS_URL.format(year=previous_year, minimum="100", metric_type="avg_speed")
    current_spin_url = SAVANT_PITCH_ARSENALS_URL.format(year=current_year, minimum="q", metric_type="avg_spin")
    previous_spin_url = SAVANT_PITCH_ARSENALS_URL.format(year=previous_year, minimum="100", metric_type="avg_spin")

    current_velocity_rows = extract_savant_pitch_data(fetch_text_page(current_velocity_url))
    previous_velocity_rows = extract_savant_pitch_data(fetch_text_page(previous_velocity_url))
    current_spin_rows = extract_savant_pitch_data(fetch_text_page(current_spin_url))
    previous_spin_rows = extract_savant_pitch_data(fetch_text_page(previous_spin_url))

    previous_velocity_by_pitcher = {str(row.get("pitcher")): row for row in previous_velocity_rows}
    current_spin_by_pitcher = {str(row.get("pitcher")): row for row in current_spin_rows}
    previous_spin_by_pitcher = {str(row.get("pitcher")): row for row in previous_spin_rows}
    comparison_rows: List[Dict[str, object]] = []
    team_labels = set()

    for current_velocity in current_velocity_rows:
        pitcher_id = str(current_velocity.get("pitcher") or "").strip()
        if not pitcher_id:
            continue
        previous_velocity = previous_velocity_by_pitcher.get(pitcher_id, {})
        current_spin = current_spin_by_pitcher.get(pitcher_id, {})
        previous_spin = previous_spin_by_pitcher.get(pitcher_id, {})
        team = str(current_velocity.get("name_abbrev") or current_velocity.get("name_display_club") or "").strip()
        if team:
            team_labels.add(team)

        for pitch_code, pitch_name in SAVANT_PITCH_TYPES:
            current_velocity_value = parse_float(current_velocity.get(f"{pitch_code}_avg_speed"))
            if current_velocity_value is None:
                continue

            previous_velocity_value = parse_float(previous_velocity.get(f"{pitch_code}_avg_speed"))
            velocity_delta = round(current_velocity_value - previous_velocity_value, 1) if previous_velocity_value is not None else None
            current_spin_value = parse_float(current_spin.get(f"{pitch_code}_avg_spin"))
            previous_spin_value = parse_float(previous_spin.get(f"{pitch_code}_avg_spin"))
            spin_delta = round(current_spin_value - previous_spin_value, 0) if previous_spin_value is not None and current_spin_value is not None else None
            comparison_rows.append(
                {
                    "pitcher_id": pitcher_id,
                    "pitcher_name": str(current_velocity.get("name_display_first_last") or "").strip(),
                    "team": team,
                    "throws": str(current_velocity.get("pitch_hand") or "").strip(),
                    "pitch_type": pitch_name,
                    "pitch_code": pitch_code.upper(),
                    "current_velocity": round(current_velocity_value, 1),
                    "previous_velocity": round(previous_velocity_value, 1) if previous_velocity_value is not None else None,
                    "delta": velocity_delta,
                    "current_spin": round(current_spin_value, 0) if current_spin_value is not None else None,
                    "previous_spin": round(previous_spin_value, 0) if previous_spin_value is not None else None,
                    "spin_delta": int(spin_delta) if spin_delta is not None else None,
                }
            )

    comparison_rows.sort(
        key=lambda row: (
            -(abs(row["delta"]) if row["delta"] is not None else -1),
            row["pitcher_name"],
            row["pitch_type"],
        )
    )

    rows_by_pitcher: Dict[str, List[Dict[str, object]]] = {}
    for row in comparison_rows:
        rows_by_pitcher.setdefault(row["pitcher_id"], []).append(row)

    probable_date = next_mlb_date()
    probable_payload = fetch_probable_pitchers(probable_date)
    pitcher_index = build_velocity_pitcher_index(current_velocity_rows, previous_velocity_rows, probable_payload.get("games", []))
    stuff_plus_cache = get_or_build_daily_stuff_plus_cache(pitcher_index, [previous_year, current_year])
    attach_stuff_plus_from_cache(comparison_rows, stuff_plus_cache, current_year, previous_year)
    tomorrow_games: List[Dict[str, object]] = []

    for game in probable_payload.get("games", []):
        starters = []
        for side, team_key in (("away_pitcher", "away_team"), ("home_pitcher", "home_team")):
            pitcher = game.get(side, {})
            mlb_id = pitcher.get("mlb_id")
            pitcher_rows = rows_by_pitcher.get(str(mlb_id), []) if mlb_id else []
            starters.append(
                {
                    "team": game.get(team_key, ""),
                    "pitcher_name": pitcher.get("name", "TBD"),
                    "hand": pitcher.get("hand", ""),
                    "record": pitcher.get("record", ""),
                    "era": pitcher.get("era", ""),
                    "mlb_id": mlb_id,
                    "fangraphs_player_id": pitcher.get("fangraphs_player_id"),
                    "rows": pitcher_rows,
                }
            )

        tomorrow_games.append(
            {
                "game_time": game.get("game_time", ""),
                "away_team": game.get("away_team", ""),
                "home_team": game.get("home_team", ""),
                "starters": starters,
            }
        )

    return {
        "current_year": current_year,
        "previous_year": previous_year,
        "source_urls": {
            "current_velocity": current_velocity_url,
            "previous_velocity": previous_velocity_url,
            "current_spin": current_spin_url,
            "previous_spin": previous_spin_url,
        },
        "stuff_plus_cache": {
            "path": str(STUFF_PLUS_CACHE_PATH),
            "generated_at": stuff_plus_cache.get("generated_at"),
            "refresh_key": stuff_plus_cache.get("refresh_key"),
        },
        "tomorrow": {
            "date": probable_payload.get("date", probable_date.isoformat()),
            "source_url": probable_payload.get("source_url", f"{MLB_SCORES_URL}/{probable_date.isoformat()}"),
            "games": tomorrow_games,
        },
        "refreshed_every_seconds": REFRESH_SECONDS,
        "teams": sorted(team_labels),
        "rows": comparison_rows,
    }


def build_pitch_stuff_plus_map(current_row: Dict[str, object]) -> Dict[str, Optional[int]]:
    pitch_map: Dict[str, Optional[int]] = {}
    for savant_code, fg_code in STUFF_PLUS_FIELD_MAP.items():
        if not fg_code:
            pitch_map[savant_code] = None
            continue
        value = parse_float(current_row.get(f"sp_s_{fg_code}"))
        pitch_map[savant_code] = int(round(value)) if value is not None else None
    return pitch_map


def fetch_pitch_stuff_plus(
    pitcher_name: str,
    mlb_id: Optional[int] = None,
    fangraphs_player_id: Optional[int] = None,
) -> Dict[str, object]:
    cache_key = f"{fangraphs_player_id or ''}|{mlb_id or ''}|{normalize_name(pitcher_name)}"
    if cache_key in PITCH_STUFF_CACHE:
        return PITCH_STUFF_CACHE[cache_key]

    fangraphs_url = (
        f"https://www.fangraphs.com/statss.aspx?playerid={fangraphs_player_id}"
        if fangraphs_player_id
        else resolve_fangraphs_player_url(pitcher_name)
    )
    if not fangraphs_url:
        payload = {"pitcher_name": pitcher_name, "stuff_by_pitch": {}}
        PITCH_STUFF_CACHE[cache_key] = payload
        return payload

    fg_html = fetch_fangraphs_page(fangraphs_url)
    fg_payload = extract_next_data(fg_html)
    data_common = fg_payload.get("props", {}).get("pageProps", {}).get("dataCommon", {})
    current_row = find_current_pitching_row(data_common)
    if not current_row:
        payload = {"pitcher_name": pitcher_name, "stuff_by_pitch": {}}
        PITCH_STUFF_CACHE[cache_key] = payload
        return payload

    payload = {
        "pitcher_name": pitcher_name,
        "stuff_by_pitch": build_pitch_stuff_plus_map(current_row),
    }
    PITCH_STUFF_CACHE[cache_key] = payload
    return payload


def attach_pitch_stuff_plus(rows: List[Dict[str, object]], pitcher_name: str, mlb_id: Optional[int] = None, fangraphs_player_id: Optional[int] = None) -> None:
    if not rows:
        return
    stuff_payload = fetch_pitch_stuff_plus(pitcher_name, mlb_id=mlb_id, fangraphs_player_id=fangraphs_player_id)
    stuff_by_pitch = stuff_payload.get("stuff_by_pitch", {})
    for row in rows:
        row["stuff_plus"] = stuff_by_pitch.get(str(row.get("pitch_code", "")).upper())


def build_bullpen_excel_workbook() -> bytes:
    wb = Workbook()
    ws = wb.active
    ws.title = "Bullpens"

    raw_html = fetch_fangraphs_page(BULLPEN_URL)
    payload = parse_bullpen_page(raw_html)

    for index, team in enumerate(TEAM_OPTIONS):
        start_col = 1 + index * 4
        team_payload = payload["teams"][team["slug"]]
        rows = team_payload["active_rows"]

        ws.cell(row=1, column=start_col, value=team["code"])
        ws.cell(row=2, column=start_col, value="Name")
        ws.cell(row=2, column=start_col + 1, value="Throws")
        ws.cell(row=2, column=start_col + 2, value="Role")

        for row_index, row in enumerate(rows, start=3):
            ws.cell(row=row_index, column=start_col, value=row["name"])
            ws.cell(row=row_index, column=start_col + 1, value=row["throws"])
            ws.cell(row=row_index, column=start_col + 2, value=row["role"])

        ws.column_dimensions[get_column_letter(start_col)].width = 24
        ws.column_dimensions[get_column_letter(start_col + 1)].width = 10
        ws.column_dimensions[get_column_letter(start_col + 2)].width = 22
        ws.column_dimensions[get_column_letter(start_col + 3)].width = 4

    ws.freeze_panes = "A1"

    output = BytesIO()
    wb.save(output)
    output.seek(0)
    return output.getvalue()


def team_block_height(payload: Dict[str, object]) -> int:
    layout = payload["excel_layout"]
    vsr = layout["vsR"]
    vsl = layout["vsL"]
    return max(
        1 + len(vsr["starters"]) + 2 + 1 + len(vsr["bench"]) + 2 + 1 + len(vsr["il"]),
        1 + len(vsl["starters"]),
    )


def write_team_block(ws, start_row: int, start_col: int, payload: Dict[str, object]) -> None:
    code = payload["team"]["code"]
    layout = payload["excel_layout"]
    vsr = layout["vsR"]
    vsl = layout["vsL"]

    ws.cell(row=start_row, column=start_col, value=code)
    ws.cell(row=start_row, column=start_col + 1, value=f"{code}L")

    current_row = start_row + 1
    for idx in range(max(len(vsr["starters"]), len(vsl["starters"]))):
        if idx < len(vsr["starters"]):
            ws.cell(row=current_row + idx, column=start_col, value=vsr["starters"][idx])
        if idx < len(vsl["starters"]):
            ws.cell(row=current_row + idx, column=start_col + 1, value=vsl["starters"][idx])

    current_row += max(len(vsr["starters"]), len(vsl["starters"]))
    current_row += 2

    ws.cell(row=current_row, column=start_col, value="Bench")
    current_row += 1
    for name in vsr["bench"]:
        ws.cell(row=current_row, column=start_col, value=name)
        current_row += 1

    current_row += 2
    ws.cell(row=current_row, column=start_col, value="IL")
    current_row += 1
    for name in vsr["il"]:
        ws.cell(row=current_row, column=start_col, value=name)
        current_row += 1


def build_excel_workbook() -> bytes:
    wb = Workbook()
    ws = wb.active
    ws.title = "Platoon Lineups"

    for index, team in enumerate(TEAM_OPTIONS):
        start_col = 1 + index * 3
        payload = fetch_lineup_data(team["slug"])
        write_team_block(ws, 1, start_col, payload)
        ws.column_dimensions[get_column_letter(start_col)].width = 24
        ws.column_dimensions[get_column_letter(start_col + 1)].width = 24
        ws.column_dimensions[get_column_letter(start_col + 2)].width = 4

    ws.freeze_panes = "A1"

    output = BytesIO()
    wb.save(output)
    output.seek(0)
    return output.getvalue()


app = FastAPI(title="MLB Pricing Tools", version="1.4.0")


@app.get("/", response_class=HTMLResponse)
def home() -> HTMLResponse:
    return HTMLResponse(HOME_TEMPLATE_PATH.read_text(encoding="utf-8"))


@app.get("/platoon-lineups", response_class=HTMLResponse)
def platoon_lineups_page() -> HTMLResponse:
    return HTMLResponse(LINEUP_TEMPLATE_PATH.read_text(encoding="utf-8"))


@app.get("/bullpens", response_class=HTMLResponse)
def bullpens_page() -> HTMLResponse:
    return HTMLResponse(BULLPEN_TEMPLATE_PATH.read_text(encoding="utf-8"))


@app.get("/velocity-comparison", response_class=HTMLResponse)
def velocity_comparison_page() -> HTMLResponse:
    return HTMLResponse(VELOCITY_TEMPLATE_PATH.read_text(encoding="utf-8"))


@app.get("/probable-pitchers", response_class=HTMLResponse)
def probable_pitchers_page() -> HTMLResponse:
    return HTMLResponse(PITCHERS_TEMPLATE_PATH.read_text(encoding="utf-8"))


@app.get("/pitchers/{mlb_player_id}", response_class=HTMLResponse)
def pitcher_profile_page(mlb_player_id: int) -> HTMLResponse:
    return HTMLResponse(PITCHER_PROFILE_TEMPLATE_PATH.read_text(encoding="utf-8"))


@app.get("/api/teams", response_class=JSONResponse)
def api_teams() -> JSONResponse:
    return JSONResponse({"teams": TEAM_OPTIONS, "default_team": "braves"})


@app.get("/api/lineups", response_class=JSONResponse)
def api_lineups(team: str = Query("braves", description="Fangraphs team slug")) -> JSONResponse:
    if team not in TEAM_MAP:
        raise HTTPException(status_code=404, detail="Unknown team slug")

    try:
        payload = fetch_lineup_data(team)
    except (HTTPError, URLError, TimeoutError) as exc:
        raise HTTPException(status_code=502, detail=f"Failed to fetch Fangraphs lineup page: {exc}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Unexpected parse error: {exc}") from exc

    return JSONResponse(
        payload,
        headers={
            "Cache-Control": "no-store, max-age=0",
            "Pragma": "no-cache",
        },
    )


@app.get("/api/bullpens", response_class=JSONResponse)
def api_bullpens(team: str = Query("braves", description="Fangraphs team slug")) -> JSONResponse:
    if team not in TEAM_MAP:
        raise HTTPException(status_code=404, detail="Unknown team slug")

    try:
        payload = fetch_bullpen_data(team)
    except (HTTPError, URLError, TimeoutError) as exc:
        raise HTTPException(status_code=502, detail=f"Failed to fetch Fangraphs bullpen page: {exc}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Unexpected bullpen parse error: {exc}") from exc

    return JSONResponse(
        payload,
        headers={
            "Cache-Control": "no-store, max-age=0",
            "Pragma": "no-cache",
        },
    )


@app.get("/api/velocity-comparison", response_class=JSONResponse)
def api_velocity_comparison() -> JSONResponse:
    try:
        payload = build_velocity_comparison()
    except (HTTPError, URLError, TimeoutError) as exc:
        raise HTTPException(status_code=502, detail=f"Failed to fetch Baseball Savant pitch-arsenals data: {exc}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Unexpected velocity comparison parse error: {exc}") from exc

    return JSONResponse(
        payload,
        headers={
            "Cache-Control": "no-store, max-age=0",
            "Pragma": "no-cache",
        },
    )


@app.get("/api/pitch-stuff-plus", response_class=JSONResponse)
def api_pitch_stuff_plus(
    name: str = Query(..., description="Pitcher name"),
    mlb_id: Optional[int] = Query(None, description="MLBAM pitcher id"),
    fg: Optional[int] = Query(None, description="Fangraphs pitcher id"),
) -> JSONResponse:
    try:
        payload = fetch_pitch_stuff_plus(name, mlb_id=mlb_id, fangraphs_player_id=fg)
    except (HTTPError, URLError, TimeoutError) as exc:
        raise HTTPException(status_code=502, detail=f"Failed to fetch Fangraphs pitch-level Stuff+ data: {exc}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Unexpected pitch Stuff+ parse error: {exc}") from exc

    return JSONResponse(
        payload,
        headers={
            "Cache-Control": "no-store, max-age=0",
            "Pragma": "no-cache",
        },
    )


@app.get("/api/probable-pitchers", response_class=JSONResponse)
def api_probable_pitchers(target_date: Optional[str] = Query(None, alias="date", description="Game date in YYYY-MM-DD")) -> JSONResponse:
    try:
        resolved_date = datetime.strptime(target_date, "%Y-%m-%d").date() if target_date else next_mlb_date()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid date. Use YYYY-MM-DD.") from exc

    try:
        payload = fetch_probable_pitchers(resolved_date)
    except (HTTPError, URLError, TimeoutError) as exc:
        raise HTTPException(status_code=502, detail=f"Failed to fetch MLB scores page: {exc}") from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Unexpected probable pitchers parse error: {exc}") from exc

    return JSONResponse(
        payload,
        headers={
            "Cache-Control": "no-store, max-age=0",
            "Pragma": "no-cache",
        },
    )


@app.get("/api/pitchers/{mlb_player_id}", response_class=JSONResponse)
def api_pitcher_profile(mlb_player_id: int, fg: Optional[int] = Query(None, description="Fangraphs player id")) -> JSONResponse:
    try:
        payload = build_pitcher_profile(mlb_player_id, fg)
    except (HTTPError, URLError, TimeoutError) as exc:
        raise HTTPException(status_code=502, detail=f"Failed to fetch pitcher profile data: {exc}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Unexpected pitcher profile error: {exc}") from exc

    return JSONResponse(
        payload,
        headers={
            "Cache-Control": "no-store, max-age=0",
            "Pragma": "no-cache",
        },
    )


@app.get("/api/export-lineups.xlsx")
def api_export_lineups() -> StreamingResponse:
    try:
        content = build_excel_workbook()
    except (HTTPError, URLError, TimeoutError) as exc:
        raise HTTPException(status_code=502, detail=f"Failed to fetch Fangraphs lineup data for export: {exc}") from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Unexpected export error: {exc}") from exc

    filename = f"mlb_platoon_lineups_{datetime.now():%Y%m%d_%H%M%S}.xlsx"
    return StreamingResponse(
        BytesIO(content),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/api/export-bullpens.xlsx")
def api_export_bullpens() -> StreamingResponse:
    try:
        content = build_bullpen_excel_workbook()
    except (HTTPError, URLError, TimeoutError) as exc:
        raise HTTPException(status_code=502, detail=f"Failed to fetch Fangraphs bullpen data for export: {exc}") from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Unexpected bullpen export error: {exc}") from exc

    filename = f"mlb_bullpens_{datetime.now():%Y%m%d_%H%M%S}.xlsx"
    return StreamingResponse(
        BytesIO(content),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/health", response_class=JSONResponse)
def health() -> JSONResponse:
    return JSONResponse({"ok": True})


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("platoon_viewer:app", host="127.0.0.1", port=8000, reload=True)
