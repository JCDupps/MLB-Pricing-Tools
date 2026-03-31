import html
import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from html.parser import HTMLParser
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, quote, unquote, urlparse
from urllib.request import Request, urlopen

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from openpyxl import Workbook
from openpyxl.utils import get_column_letter


BASE_URL = "https://www.fangraphs.com/roster-resource/platoon-lineups"
MLB_SCORES_URL = "https://www.mlb.com/scores"
APP_DIR = Path(__file__).resolve().parent
HOME_TEMPLATE_PATH = APP_DIR / "templates" / "index.html"
LINEUP_TEMPLATE_PATH = APP_DIR / "templates" / "platoon_lineups.html"
PITCHERS_TEMPLATE_PATH = APP_DIR / "templates" / "probable_pitchers.html"
PITCHER_PROFILE_TEMPLATE_PATH = APP_DIR / "templates" / "pitcher_profile.html"
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
    {"slug": "athletics", "code": "ATH", "name": "Athletics", "league": "AL West"},
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


@dataclass
class PlayerRow:
    status: str
    position: str
    name: str
    bats: str
    stats: List[str]


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
        search_end = min(idx + 24, len(lines))
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

        away_team = window[watch_index + 1]
        home_team = window[watch_index + 2]
        pitcher_entries = extract_pitcher_entries(window[watch_index + 3:])
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


def fetch_depth_chart_probables(team_slug: str) -> List[Dict[str, object]]:
    if team_slug in DEPTH_CHART_CACHE:
        return DEPTH_CHART_CACHE[team_slug]

    page_html = fetch_fangraphs_page(f"https://www.fangraphs.com/roster-resource/depth-charts/{team_slug}")
    payload = extract_next_data(page_html)
    queries = payload.get("props", {}).get("pageProps", {}).get("dehydratedState", {}).get("queries", [])
    if not queries:
        DEPTH_CHART_CACHE[team_slug] = []
        return []

    probable_data = queries[0].get("state", {}).get("data", {}).get("dataProbableStarters", {})
    game_list = probable_data.get("gameList", []) if isinstance(probable_data, dict) else []
    DEPTH_CHART_CACHE[team_slug] = game_list
    return game_list


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


app = FastAPI(title="MLB Pricing Tools", version="1.2.0")


@app.get("/", response_class=HTMLResponse)
def home() -> HTMLResponse:
    return HTMLResponse(HOME_TEMPLATE_PATH.read_text(encoding="utf-8"))


@app.get("/platoon-lineups", response_class=HTMLResponse)
def platoon_lineups_page() -> HTMLResponse:
    return HTMLResponse(LINEUP_TEMPLATE_PATH.read_text(encoding="utf-8"))


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


@app.get("/health", response_class=JSONResponse)
def health() -> JSONResponse:
    return JSONResponse({"ok": True})


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("platoon_viewer:app", host="127.0.0.1", port=8000, reload=True)
