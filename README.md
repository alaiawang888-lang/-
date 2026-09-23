# Capitol Tracker Live

PWA + FastAPI backend for monitoring U.S. public financial disclosure updates.

## What works
- Mobile/PWA UI with filters for Trump, Pelosi, Congress, Executive, large purchases, repeat purchases.
- Backend API and SQLite persistence.
- 5-minute scheduler.
- Official House disclosure ZIP ingestion (year index + PTR PDF links).
- PDF transaction parsing when PDFs contain extractable text.
- OGE source watcher scaffold links to official disclosure search; OGE access rules may require Form 201/request flow, so it is not falsely treated as a fully scrapeable live feed.
- De-duplication, large-purchase flag, repeat-purchase flag.

## Run
1. Python 3.11+
2. `pip install -r requirements.txt`
3. `uvicorn app:app --host 0.0.0.0 --port 8000`
4. Open http://localhost:8000

The scheduler checks every 300 seconds by default. Change `POLL_SECONDS` env var if needed.

## Deploy
Works on Railway/Render/Fly.io with persistent disk. For production, use Postgres instead of SQLite and a persistent worker.

## Important
Official disclosures are delayed by law and are not real-time brokerage positions. OGE executive-branch reports have special public-access rules and are not all centrally hosted.
