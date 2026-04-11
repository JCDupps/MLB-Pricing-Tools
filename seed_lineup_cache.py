import json
from pathlib import Path

from platoon_viewer import LINEUP_CACHE_PATH, TEAM_OPTIONS, eastern_now, parse_lineup_page


APP_DIR = Path(__file__).resolve().parent
SEED_DIR = APP_DIR / "lineup_seed_html"


def load_existing_cache() -> dict:
    if not LINEUP_CACHE_PATH.exists():
        return {"teams": {}}
    try:
        payload = json.loads(LINEUP_CACHE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"teams": {}}
    if not isinstance(payload, dict):
        return {"teams": {}}
    if not isinstance(payload.get("teams"), dict):
        payload["teams"] = {}
    return payload


def main() -> None:
    SEED_DIR.mkdir(exist_ok=True)
    cache_payload = load_existing_cache()
    saved_count = 0
    missing = []
    failed = []

    for team in TEAM_OPTIONS:
        source_path = SEED_DIR / f"{team['slug']}.html"
        if not source_path.exists():
            missing.append(team["slug"])
            continue

        try:
            raw_html = source_path.read_text(encoding="utf-8", errors="ignore")
            payload = parse_lineup_page(team["slug"], raw_html)
            payload["cache_saved_at"] = eastern_now().isoformat()
            cache_payload["teams"][team["slug"]] = payload
            saved_count += 1
        except Exception as exc:
            failed.append(f"{team['slug']}: {exc}")

    LINEUP_CACHE_PATH.write_text(json.dumps(cache_payload, indent=2, sort_keys=True), encoding="utf-8")

    print(f"Saved {saved_count} lineup cache entries to {LINEUP_CACHE_PATH}")
    if missing:
        print("Missing HTML files:")
        for slug in missing:
            print(f"  - {slug}.html")
    if failed:
        print("Failed to parse:")
        for item in failed:
            print(f"  - {item}")


if __name__ == "__main__":
    main()
