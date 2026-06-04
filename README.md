# University Social Analytics — Backend

FastAPI backend for University Social Analytics Dashboard.

## Deploy to Railway

1. Create new project on [railway.app](https://railway.app)
2. Add **PostgreSQL** plugin
3. Connect this GitHub repo
4. Railway auto-sets `DATABASE_URL` — no manual config needed
5. Copy the public URL, add to Netlify frontend

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/register` | Register university, get credentials |
| GET | `/api/data/{id}` | Get cached social media data |
| POST | `/api/refresh/{id}` | Force refresh data now |
| GET | `/api/status/{id}` | Check last update times |

## Environment Variables (auto-set by Railway)

- `DATABASE_URL` — PostgreSQL connection string (set by Railway PostgreSQL plugin)
- `PORT` — Port to run on (set by Railway)

## How it works

1. University registers via `/api/register` with their Apify token
2. Backend immediately fetches their Instagram/TikTok/web data
3. Every night at 02:00 UTC — auto-refresh for ALL universities
4. Frontend calls `/api/data/{id}` to get fresh cached data
