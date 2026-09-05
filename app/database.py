"""app/database.py — asyncpg-based repository for all DB operations."""
import asyncpg
import logging
from datetime import datetime
from typing import List, Optional, Tuple

from app.config import DATABASE_URL, STALE_RUNNING_MINUTES, SCRAPE_LEASE_TIMEOUT_MINUTES

logger = logging.getLogger(__name__)

_pool: Optional[asyncpg.Pool] = None


async def create_pool() -> asyncpg.Pool:
    global _pool
    _pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=3)
    logger.info("Database pool created.")
    return _pool


async def close_pool():
    global _pool
    if _pool:
        await _pool.close()
        logger.info("Database pool closed.")


def get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("DB pool not initialised — call create_pool() first.")
    return _pool


# ── Advisory lock (prevents concurrent scraper workers) ──────────────────────

_ADVISORY_LOCK_KEY = 7_777_777  # arbitrary stable integer


async def acquire_advisory_lock(conn) -> bool:
    """Try to acquire a session-level advisory lock. Returns True if acquired."""
    return await conn.fetchval("SELECT pg_try_advisory_lock($1)", _ADVISORY_LOCK_KEY)


async def release_advisory_lock(conn):
    await conn.fetchval("SELECT pg_advisory_unlock($1)", _ADVISORY_LOCK_KEY)


# ── Job helpers ───────────────────────────────────────────────────────────────

async def get_running_job() -> Optional[asyncpg.Record]:
    """Return the currently running job row, or None."""
    pool = get_pool()
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT * FROM scrape_jobs WHERE status='running' ORDER BY id DESC LIMIT 1"
        )


async def get_or_create_job(category: str, state: str) -> int:
    """Return the id of an existing running job or create a new one."""
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id FROM scrape_jobs WHERE category=$1 AND state=$2 AND status='running' LIMIT 1",
            category, state,
        )
        if row:
            return row["id"]
        total = await conn.fetchval(
            "SELECT COUNT(*) FROM scrape_locations WHERE state=$1", state
        )
        job_id = await conn.fetchval(
            """INSERT INTO scrape_jobs (category, state, status, total_locations)
               VALUES ($1, $2, 'running', $3) RETURNING id""",
            category, state, total or 0,
        )
        logger.info(f"Created job id={job_id} category={category} state={state} total={total}")
        return job_id


async def update_job_progress(job_id: int):
    """Recount completed/failed from scrape_location_jobs and update the job row."""
    pool = get_pool()
    async with pool.acquire() as conn:
        completed = await conn.fetchval(
            "SELECT COUNT(*) FROM scrape_location_jobs WHERE job_id=$1 AND status='completed'",
            job_id,
        )
        failed = await conn.fetchval(
            "SELECT COUNT(*) FROM scrape_location_jobs WHERE job_id=$1 AND status IN ('failed','permanently_failed')",
            job_id,
        )
        total = await conn.fetchval(
            "SELECT total_locations FROM scrape_jobs WHERE id=$1", job_id
        )
        status = "completed" if (completed + failed) >= (total or 0) and total else "running"
        finished_at = datetime.utcnow() if status == "completed" else None
        await conn.execute(
            """UPDATE scrape_jobs
               SET completed_locations=$1, failed_locations=$2, status=$3, finished_at=$4
               WHERE id=$5""",
            completed, failed, status, finished_at, job_id,
        )


# ── Location claiming ─────────────────────────────────────────────────────────

async def recover_stale_locations():
    """Reset locations stuck in 'running' back to 'pending'."""
    pool = get_pool()
    async with pool.acquire() as conn:
        result = await conn.execute(
            f"""UPDATE scrape_locations
                SET status='pending', updated_at=NOW()
                WHERE status='running'
                  AND updated_at < NOW() - INTERVAL '{SCRAPE_LEASE_TIMEOUT_MINUTES} minutes'"""
        )
        # result is e.g. 'UPDATE 3'
        n = int(result.split()[-1]) if result else 0
        if n:
            logger.info(f"Recovered {n} stale-running location(s) → pending.")


async def claim_locations(job_id: int, batch_size: int) -> List[asyncpg.Record]:
    """
    Atomically claim up to batch_size pending locations for this job.
    Uses FOR UPDATE SKIP LOCKED so concurrent workers never double-claim.
    Returns list of scrape_locations rows.
    """
    pool = get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            rows = await conn.fetch(
                """SELECT sl.*
                   FROM scrape_locations sl
                   LEFT JOIN scrape_location_jobs slj
                     ON slj.location_id = sl.id AND slj.job_id = $1
                   WHERE sl.status = 'pending'
                     AND slj.id IS NULL
                   ORDER BY sl.id
                   FOR UPDATE OF sl SKIP LOCKED
                   LIMIT $2""",
                job_id, batch_size,
            )
            if not rows:
                return []

            ids = [r["id"] for r in rows]
            await conn.execute(
                "UPDATE scrape_locations SET status='running', updated_at=NOW() WHERE id=ANY($1)",
                ids,
            )
            # Create location_job rows
            await conn.executemany(
                """INSERT INTO scrape_location_jobs (job_id, location_id, status, started_at)
                   VALUES ($1, $2, 'running', NOW())
                   ON CONFLICT (job_id, location_id) DO UPDATE
                     SET status='running', started_at=NOW()""",
                [(job_id, loc_id) for loc_id in ids],
            )
        return rows


async def mark_location_completed(job_id: int, location_id: int):
    pool = get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """UPDATE scrape_locations
               SET status='completed', last_scraped_at=NOW(), updated_at=NOW()
               WHERE id=$1""",
            location_id,
        )
        await conn.execute(
            """UPDATE scrape_location_jobs
               SET status='completed', completed_at=NOW()
               WHERE job_id=$1 AND location_id=$2""",
            job_id, location_id,
        )


async def mark_location_failed(job_id: int, location_id: int, error: str, max_attempts: int):
    pool = get_pool()
    async with pool.acquire() as conn:
        attempts = await conn.fetchval(
            "SELECT attempts FROM scrape_locations WHERE id=$1", location_id
        ) or 0
        attempts += 1
        final_status = "permanently_failed" if attempts >= max_attempts else "failed"
        await conn.execute(
            """UPDATE scrape_locations
               SET status=$1, attempts=$2, last_error=$3, updated_at=NOW()
               WHERE id=$4""",
            final_status, attempts, error[:2000], location_id,
        )
        await conn.execute(
            """UPDATE scrape_location_jobs
               SET status=$1, attempts=$2, error_message=$3, completed_at=NOW()
               WHERE job_id=$4 AND location_id=$5""",
            final_status, attempts, error[:2000], job_id, location_id,
        )


# ── Business upsert ───────────────────────────────────────────────────────────

async def upsert_business(profile: dict, scraper_category: str) -> int:
    """
    Insert or update a business row.  Returns the business id.
    Uses ON CONFLICT (gmaps_url) so duplicates are never created.
    """
    pool = get_pool()
    scraped_at = profile.get("scraped_at")
    if isinstance(scraped_at, str):
        try:
            scraped_at = datetime.fromisoformat(scraped_at)
        except ValueError:
            scraped_at = datetime.utcnow()

    async with pool.acquire() as conn:
        business_id = await conn.fetchval(
            """INSERT INTO businesses (
                   scraper_category, name, address, phone, rating, review_count,
                   opening_hours, website,
                   gmaps_url, gmaps_name, gmaps_address, gmaps_phone,
                   gmaps_rating, gmaps_reviews_count, gmaps_opening_hours,
                   gmaps_category, gmaps_website, gmaps_about,
                   location_lat, location_lng, search_term, scraped_at, updated_at
               ) VALUES (
                   $1,$2,$3,$4,$5,$6,$7,$8,
                   $9,$10,$11,$12,$13,$14,$15,$16,$17,$18,
                   $19,$20,$21,$22,NOW()
               )
               ON CONFLICT (gmaps_url) DO UPDATE SET
                   gmaps_name            = EXCLUDED.gmaps_name,
                   gmaps_address         = EXCLUDED.gmaps_address,
                   gmaps_phone           = EXCLUDED.gmaps_phone,
                   gmaps_rating          = EXCLUDED.gmaps_rating,
                   gmaps_reviews_count   = EXCLUDED.gmaps_reviews_count,
                   gmaps_opening_hours   = EXCLUDED.gmaps_opening_hours,
                   gmaps_category        = EXCLUDED.gmaps_category,
                   gmaps_website         = EXCLUDED.gmaps_website,
                   gmaps_about           = EXCLUDED.gmaps_about,
                   search_term           = EXCLUDED.search_term,
                   scraped_at            = EXCLUDED.scraped_at,
                   updated_at            = NOW()
               RETURNING id""",
            scraper_category,
            profile.get("gmaps_name") or profile.get("name", ""),
            profile.get("gmaps_address") or profile.get("address", ""),
            profile.get("gmaps_phone") or profile.get("phone", ""),
            profile.get("gmaps_rating") or profile.get("rating", ""),
            profile.get("gmaps_reviews_count") or profile.get("review_count", ""),
            profile.get("gmaps_opening_hours") or profile.get("opening_hours", ""),
            profile.get("gmaps_website") or profile.get("website", ""),
            profile["gmaps_url"],
            profile.get("gmaps_name", ""),
            profile.get("gmaps_address", ""),
            profile.get("gmaps_phone", ""),
            profile.get("gmaps_rating", ""),
            profile.get("gmaps_reviews_count", ""),
            profile.get("gmaps_opening_hours", ""),
            profile.get("gmaps_category", ""),
            profile.get("gmaps_website", ""),
            profile.get("gmaps_about", ""),
            profile.get("location_lat"),
            profile.get("location_lng"),
            profile.get("search_term", ""),
            scraped_at,
        )
    return business_id


async def record_discovery(business_id: int, location_id: int, job_id: int, search_term: str):
    pool = get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO business_discoveries
                   (business_id, location_id, job_id, search_term)
               VALUES ($1,$2,$3,$4)
               ON CONFLICT DO NOTHING""",
            business_id, location_id, job_id, search_term,
        )
