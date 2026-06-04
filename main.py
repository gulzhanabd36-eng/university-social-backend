import os
import uuid
import secrets
import asyncio
import json
import logging
from datetime import datetime, timezone
from contextlib import asynccontextmanager

import asyncpg
import httpx
from fastapi import FastAPI, HTTPException, Header, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from apscheduler.schedulers.asyncio import AsyncIOScheduler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL", "")

db_pool = None
scheduler = AsyncIOScheduler()

# ─── DB Init ───────────────────────────────────────────────────────
async def create_tables(conn):
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS universities (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            name TEXT NOT NULL,
            instagram_handle TEXT DEFAULT '',
            tiktok_handle TEXT DEFAULT '',
            web_keywords TEXT DEFAULT '',
            apify_token TEXT NOT NULL,
            secret_token TEXT NOT NULL UNIQUE,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS data_cache (
            id SERIAL PRIMARY KEY,
            university_id UUID REFERENCES universities(id) ON DELETE CASCADE,
            platform TEXT NOT NULL,
            data JSONB,
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(university_id, platform)
        )
    """)
    logger.info("Tables ready")

# ─── Startup / Shutdown ────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool
    db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    async with db_pool.acquire() as conn:
        await create_tables(conn)
    scheduler.add_job(refresh_all_universities, 'cron', hour=2, minute=0, id='daily_refresh')
    scheduler.start()
    logger.info("Scheduler started — daily refresh at 02:00 UTC")
    yield
    scheduler.shutdown()
    await db_pool.close()

app = FastAPI(title="University Social Analytics API", version="1.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

# ─── Models ────────────────────────────────────────────────────────
class RegisterRequest(BaseModel):
    name: str
    instagram_handle: str = ""
    tiktok_handle: str = ""
    web_keywords: str = ""
    apify_token: str

class UpdateRequest(BaseModel):
    instagram_handle: str = ""
    tiktok_handle: str = ""
    web_keywords: str = ""
    apify_token: str = ""

# ─── Auth helper ───────────────────────────────────────────────────
async def get_university(university_id: str, authorization: str):
    token = authorization.replace("Bearer ", "").strip() if authorization else ""
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM universities WHERE id=$1 AND secret_token=$2",
            uuid.UUID(university_id), token
        )
    if not row:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return row

# ─── Apify helpers ─────────────────────────────────────────────────
async def apify_run_actor(apify_token: str, actor_id: str, input_data: dict) -> list:
    headers = {"Authorization": f"Bearer {apify_token}", "Content-Type": "application/json"}
    async with httpx.AsyncClient(timeout=180) as client:
        r = await client.post(
            f"https://api.apify.com/v2/acts/{actor_id}/runs",
            headers=headers, json=input_data
        )
        if r.status_code not in [200, 201]:
            logger.error(f"Apify start failed: {r.status_code} {r.text[:200]}")
            return []
        run_id = r.json()["data"]["id"]
        dataset_id = r.json()["data"]["defaultDatasetId"]

        for _ in range(60):
            await asyncio.sleep(5)
            s = await client.get(f"https://api.apify.com/v2/actor-runs/{run_id}", headers=headers)
            status = s.json()["data"]["status"]
            if status == "SUCCEEDED":
                break
            elif status in ["FAILED", "ABORTED", "TIMED-OUT"]:
                logger.error(f"Apify run {status}")
                return []

        items_r = await client.get(
            f"https://api.apify.com/v2/datasets/{dataset_id}/items",
            headers=headers, params={"limit": 50, "clean": True}
        )
        return items_r.json() if items_r.status_code == 200 else []

async def fetch_instagram(apify_token: str, handle: str) -> dict:
    handle = handle.lstrip("@")
    items = await apify_run_actor(
        apify_token,
        "apify~instagram-scraper",
        {"usernames": [handle], "resultsLimit": 20}
    )
    if not items:
        return {"error": "No data", "handle": handle}
    profile = items[0] if items else {}
    posts = [{"id": p.get("id"), "caption": p.get("caption", "")[:300], "likes": p.get("likesCount", 0), "comments": p.get("commentsCount", 0), "timestamp": p.get("timestamp", "")} for p in items[:20]]
    return {"handle": handle, "followers": profile.get("followersCount", 0), "posts_count": profile.get("postsCount", 0), "bio": profile.get("biography", ""), "posts": posts, "fetched_at": datetime.now(timezone.utc).isoformat()}

async def fetch_tiktok(apify_token: str, handle: str) -> dict:
    handle = handle.lstrip("@")
    items = await apify_run_actor(
        apify_token,
        "clockworks~tiktok-scraper",
        {"profiles": [handle], "resultsPerPage": 20}
    )
    if not items:
        return {"error": "No data", "handle": handle}
    profile = items[0] if items else {}
    videos = [{"id": v.get("id"), "desc": v.get("desc", "")[:300], "plays": v.get("playCount", 0), "likes": v.get("diggCount", 0), "shares": v.get("shareCount", 0), "createTime": v.get("createTimeISO", "")} for v in items[:20]]
    return {"handle": handle, "followers": profile.get("authorStats", {}).get("followerCount", 0), "videos": videos, "fetched_at": datetime.now(timezone.utc).isoformat()}

async def fetch_web(apify_token: str, keywords: str, university_name: str) -> dict:
    query = keywords or university_name
    items = await apify_run_actor(
        apify_token,
        "apify~google-search-scraper",
        {"queries": [query], "maxPagesPerQuery": 1, "resultsPerPage": 10}
    )
    results = [{"title": i.get("title", ""), "url": i.get("url", ""), "description": i.get("description", "")[:300)} for i in items[:20]]
    return {"query": query, "results": results, "fetched_at": datetime.now(timezone.utc).isoformat()}

async def refresh_university_data(uni: dict):
    uid = uni["id"]
    apify_token = uni["apify_token"]
    logger.info(f"Refreshing data for {uni['name']} ({uid})")
    tasks = []
    if uni["instagram_handle"]:
        tasks.append(("instagram", fetch_instagram(apify_token, uni["instagram_handle"])))
    if uni["tiktok_handle"]:
        tasks.append(("tiktok", fetch_tiktok(apify_token, uni["tiktok_handle"])))
    if uni["instagram_handle"] or uni["tiktok_handle"]:
        tasks.append(("web", fetch_web(apify_token, uni["web_keywords"], uni["name"])))

    results = await asyncio.gather(*[t[1] for t in tasks], return_exceptions=True)

    async with db_pool.acquire() as conn:
        for i, (platform, _) in enumerate(tasks):
            data = results[i] if not isinstance(results[i], Exception) else {"error": str(results[i])}
            await conn.execute("""
                INSERT INTO data_cache (university_id, platform, data, updated_at)
                VALUES ($1, $2, $3::jsonb, NOW())
                ON CONFLICT (university_id, platform) DO UPDATE
                SET data=$3::jsonb, updated_at=NOW()
            """, uid, platform, json.dumps(data))
    logger.info(f"Refreshed {uni['name']} — {len(tasks)} platforms")

async def refresh_all_universities():
    logger.info("Starting daily refresh for all universities...")
    async with db_pool.acquire() as conn:
        unis = await conn.fetch("SELECT * FROM universities")
    for uni in unis:
        try:
            await refresh_university_data(dict(uni))
        except Exception as e:
            logger.error(f"Error refreshing {uni['name']}: {e}")
    logger.info(f"Daily refresh complete — {len(unis)} universities")

# ─── Routes ────────────────────────────────────────────────────────
@app.get("/")
async def root():
    return {"status": "ok", "service": "University Social Analytics API", "version": "1.0"}

@app.post("/api/register")
async def register(req: RegisterRequest, background_tasks: BackgroundTasks):
    secret_token = secrets.token_hex(32)
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO universities (name, instagram_handle, tiktok_handle, web_keywords, apify_token, secret_token)
            VALUES ($1,$2,$3,$4,$5,$6) RETURNING id
        """, req.name, req.instagram_handle, req.tiktok_handle, req.web_keywords, req.apify_token, secret_token)
    uni_id = str(row["id"])
    uni = {"id": row["id"], "name": req.name, "instagram_handle": req.instagram_handle, "tiktok_handle": req.tiktok_handle, "web_keywords": req.web_keywords, "apify_token": req.apify_token}
    background_tasks.add_task(refresh_university_data, uni)
    return {"university_id": uni_id, "secret_token": secret_token, "message": "Registered! Initial data fetch started in background."}

@app.get("/api/data/{university_id}")
async def get_data(university_id: str, authorization: str = Header(None)):
    uni = await get_university(university_id, authorization or "")
    async with db_pool.acquire() as conn:
        cache = await conn.fetch("SELECT platform, data, updated_at FROM data_cache WHERE university_id=$1", uuid.UUID(university_id))
    return {
        "university": {"id": university_id, "name": uni["name"], "instagram": uni["instagram_handle"], "tiktok": uni["tiktok_handle"]},
        "data": {r["platform"]: {"data": r["data"], "updated_at": str(r["updated_at"])} for r in cache}
    }

@app.post("/api/refresh/{university_id}")
async def refresh(university_id: str, authorization: str = Header(None), background_tasks: BackgroundTasks = None):
    uni = await get_university(university_id, authorization or "")
    background_tasks.add_task(refresh_university_data, dict(uni))
    return {"message": "Refresh started in background"}

@app.get("/api/status/{university_id}")
async def status(university_id: str, authorization: str = Header(None)):
    uni = await get_university(university_id, authorization or "")
    async with db_pool.acquire() as conn:
        cache = await conn.fetch("SELECT platform, updated_at FROM data_cache WHERE university_id=$1", uuid.UUID(university_id))
    return {"university": uni["name"], "platforms": {r["platform"]: str(r["updated_at"]) for r in cache}}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
