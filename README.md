# MLB Pricing Tools

This starter app serves a live HTML dashboard for Fangraphs RosterResource platoon lineups.

## Files

- `platoon_viewer.py` FastAPI app and Fangraphs parser
- `templates/index.html` HTML dashboard

## Run

```powershell
uvicorn platoon_viewer:app --reload --host 127.0.0.1 --port 8000
```

Then open `http://127.0.0.1:8000/`.
