"""
app/api.py — FastAPI trigger API for the scraper.

Start with:
    uvicorn app.api:app --host 0.0.0.0 --port $PORT
"""
import asyncio
import logging

from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse

from app import config
from app.database import (
    create_pool, close_pool, get_pool, get_running_job,
    acquire_advisory_lock, release_advisory_lock,
)

logger = logging.getLogger(__name__)

app = FastAPI(title="Wedding Scraper API", docs_url=None, redoc_url=None)

# Track the background task so we can check if it's still running in-process.
_active_task: asyncio.Task | None = None


@app.on_event("startup")
async def startup():
    logger.info("Connecting to PostgreSQL...")
    await create_pool()
    logger.info("Database connected.")


@app.on_event("shutdown")
async def shutdown():
    await close_pool()


# ── Auth helper ───────────────────────────────────────────────────────────────

def _check_token(authorization: str | None):
    token = config.SCRAPE_TRIGGER_TOKEN
    if not token:
        return  # token not configured → open (dev mode)
    if authorization != f"Bearer {token}":
        raise HTTPException(status_code=401, detail="Unauthorized")


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/status")
async def status():
    pool = get_pool()
    async with pool.acquire() as conn:
        job = await conn.fetchrow(
            "SELECT * FROM scrape_jobs WHERE status='running' ORDER BY id DESC LIMIT 1"
        )
        pending = await conn.fetchval(
            "SELECT COUNT(*) FROM scrape_locations WHERE status='pending' AND state=$1",
            config.SCRAPER_STATE,
        )
        completed = await conn.fetchval(
            "SELECT COUNT(*) FROM scrape_locations WHERE status='completed' AND state=$1",
            config.SCRAPER_STATE,
        )

    if job:
        return {
            "scraper_running": True,
            "job_id": job["id"],
            "category": job["category"],
            "state": job["state"],
            "completed": completed,
            "pending": pending,
        }
    return {
        "scraper_running": False,
        "job_id": None,
        "category": config.SCRAPER_CATEGORY,
        "state": config.SCRAPER_STATE,
        "completed": completed,
        "pending": pending,
    }


@app.post("/scrape/start")
async def scrape_start(authorization: str | None = Header(default=None)):
    _check_token(authorization)

    global _active_task

    # Check in-process task first (fast path within same process lifetime)
    if _active_task and not _active_task.done():
        running = await get_running_job()
        job_id = running["id"] if running else None
        return JSONResponse(
            status_code=409,
            content={"status": "already_running", "job_id": job_id},
        )

    # Check DB advisory lock (handles cross-restart / concurrent requests)
    pool = get_pool()
    probe_conn = await pool.acquire()
    try:
        locked = await acquire_advisory_lock(probe_conn)
        if not locked:
            running = await get_running_job()
            job_id = running["id"] if running else None
            return JSONResponse(
                status_code=409,
                content={"status": "already_running", "job_id": job_id},
            )
        # Release immediately — run_batch() will re-acquire it
        await release_advisory_lock(probe_conn)
    finally:
        await pool.release(probe_conn)

    # Check for pending work
    async with pool.acquire() as conn:
        pending = await conn.fetchval(
            "SELECT COUNT(*) FROM scrape_locations WHERE status='pending' AND state=$1",
            config.SCRAPER_STATE,
        )
    if not pending:
        return JSONResponse(
            status_code=200,
            content={"status": "completed", "message": "No pending locations."},
        )

    # Import here to avoid circular import at module load time
    from app.worker import run_batch

    async def _background():
        global _active_task
        try:
            await run_batch()
        except Exception as exc:
            logger.error(f"Background worker error: {exc}")

    _active_task = asyncio.create_task(_background())

    # Give the task a moment to acquire the lock and create the job
    await asyncio.sleep(0.3)

    running = await get_running_job()
    job_id = running["id"] if running else None
    logger.info(f"Trigger received. Job created: {job_id}")

    return JSONResponse(
        status_code=202,
        content={"status": "started", "job_id": job_id},
    )


@app.get("/scrape/status/{job_id}")
async def scrape_job_status(job_id: int):
    pool = get_pool()
    async with pool.acquire() as conn:
        job = await conn.fetchrow("SELECT * FROM scrape_jobs WHERE id=$1", job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return {
        "job_id": job["id"],
        "category": job["category"],
        "state": job["state"],
        "status": job["status"],
        "total_locations": job["total_locations"],
        "completed_locations": job["completed_locations"],
        "failed_locations": job["failed_locations"],
        "started_at": job["started_at"].isoformat() if job["started_at"] else None,
        "finished_at": job["finished_at"].isoformat() if job["finished_at"] else None,
    }
