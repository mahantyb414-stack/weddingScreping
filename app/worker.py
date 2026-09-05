"""
app/worker.py — bounded batch worker, safe to call from the API trigger.

Each call to run_batch():
  1. Acquires a PostgreSQL advisory lock (prevents concurrent workers).
  2. Recovers stale locations.
  3. Claims up to SCRAPER_BATCH_SIZE pending locations.
  4. Scrapes them one-by-one, saving + committing after each.
  5. Releases the lock and closes the browser.

The API calls run_batch() in a background asyncio task so the HTTP
response returns immediately.
"""
import asyncio
import logging
import os
import sys
import time
import traceback

import yaml
from playwright.async_api import async_playwright

from app import config
from app.database import (
    acquire_advisory_lock, release_advisory_lock,
    close_pool, create_pool, get_pool, claim_locations, get_or_create_job,
    mark_location_completed, mark_location_failed, record_discovery,
    recover_stale_locations, update_job_progress, upsert_business,
)
from app.scraper import GoogleMapsGeoScraper, get_location_settings, launch_browser, _build_context

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def load_category_config(category: str) -> dict:
    path = os.path.join(config.MATCHERS_DIR, f"{category}.yaml")
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    logger.warning(f"No matcher file for '{category}' at {path}. Using category name as search term.")
    return {"primary_search_term": category.replace("_", " ").title(), "search_terms": []}


async def process_location(loc, ctx, job_id: int, search_terms: list, category: str, idx: int, total: int):
    location_id = loc["id"]
    label = f"{loc.get('area','')} {loc.get('city','')} {loc.get('state','')}".strip()
    logger.info(f"[{idx}/{total}] {label}")

    settings = get_location_settings(dict(loc), search_terms)

    scraper = GoogleMapsGeoScraper(
        search_term=settings["search_terms"][0] if settings["search_terms"] else category,
        lat=loc["latitude"],
        lng=loc["longitude"],
        zoom=settings["zoom"],
        max_distance=settings["max_distance"],
        label=label,
    )

    saved = 0
    async for profile in scraper.scrape_all_async(ctx, settings["search_terms"]):
        try:
            biz_id = await upsert_business(profile, category)
            await record_discovery(biz_id, location_id, job_id, profile.get("search_term", ""))
            saved += 1
        except Exception as e:
            logger.error(f"  DB error saving business: {e}")

    logger.info(f"[{idx}/{total}] Saved {saved} businesses")
    return saved, scraper._collect_succeeded


async def run_batch():
    """
    Claim and scrape one bounded batch of locations.
    Acquires an advisory lock so only one batch runs at a time.
    Returns a dict with job_id and outcome for the API response.
    """
    logger.info("Worker started.")
    logger.info(f"Category: {config.SCRAPER_CATEGORY} | State: {config.SCRAPER_STATE}")
    logger.info(f"Batch size: {config.SCRAPER_BATCH_SIZE} | Max attempts: {config.MAX_LOCATION_ATTEMPTS}")

    pool = get_pool()
    lock_conn = await pool.acquire()
    lock_acquired = False

    try:
        lock_acquired = await acquire_advisory_lock(lock_conn)
        if not lock_acquired:
            logger.info("Advisory lock not acquired — another worker is running.")
            return {"status": "already_running"}

        logger.info("Lock acquired.")
        await recover_stale_locations()

        job_id = await get_or_create_job(config.SCRAPER_CATEGORY, config.SCRAPER_STATE)
        logger.info(f"Job id: {job_id}")

        locations = await claim_locations(job_id, config.SCRAPER_BATCH_SIZE)
        if not locations:
            logger.info("No pending locations.")
            return {"status": "no_pending", "job_id": job_id}

        logger.info(f"Claimed {len(locations)} location(s).")

        cat_cfg = load_category_config(config.SCRAPER_CATEGORY)
        primary = cat_cfg.get("primary_search_term", config.SCRAPER_CATEGORY.replace("_", " ").title())
        search_terms = [primary] + [t for t in cat_cfg.get("search_terms", []) if t != primary]

        deadline = time.monotonic() + config.MAX_RUNTIME_MINUTES * 60

        logger.info("Starting Chromium...")
        async with async_playwright() as pw:
            browser = await launch_browser(pw)
            ctx = await _build_context(browser)
            logger.info("Browser ready.")

            try:
                total = len(locations)
                for idx, loc in enumerate(locations, 1):
                    if time.monotonic() > deadline:
                        logger.warning("Approaching runtime limit — stopping early.")
                        break

                    location_id = loc["id"]
                    try:
                        saved, collect_ok = await process_location(
                            loc, ctx, job_id, search_terms,
                            config.SCRAPER_CATEGORY, idx, total,
                        )
                        if saved == 0 and collect_ok:
                            logger.warning(f"[{idx}/{total}] Zero results — possible block. Marking failed.")
                            await mark_location_failed(
                                job_id, location_id,
                                "zero results after successful collect — possible block",
                                config.MAX_LOCATION_ATTEMPTS,
                            )
                        else:
                            await mark_location_completed(job_id, location_id)
                            logger.info(f"[{idx}/{total}] Completed.")
                    except Exception as exc:
                        logger.error(f"[{idx}/{total}] Location failed: {exc}")
                        await mark_location_failed(
                            job_id, location_id, str(exc)[:2000], config.MAX_LOCATION_ATTEMPTS
                        )
            finally:
                logger.info("Closing browser...")
                try:
                    await ctx.close()
                except Exception:
                    pass
                try:
                    await browser.close()
                except Exception:
                    pass
                logger.info("Browser closed.")

        await update_job_progress(job_id)
        logger.info("Job counters updated.")
        logger.info("Batch completed.")
        return {"status": "completed", "job_id": job_id}

    except Exception as exc:
        logger.error(f"Worker error: {exc}\n{traceback.format_exc()}")
        return {"status": "error", "detail": str(exc)}
    finally:
        if lock_acquired:
            await release_advisory_lock(lock_conn)
            logger.info("Lock released.")
        await pool.release(lock_conn)


# ── CLI entry point (preserved for local use) ─────────────────────────────────

async def _run_cli():
    """CLI mode: run batches until all locations are done."""
    logger.info("Connecting to PostgreSQL...")
    await create_pool()
    logger.info("Connected.")
    try:
        while True:
            result = await run_batch()
            if result.get("status") in ("no_pending", "error"):
                break
            await asyncio.sleep(5)
    finally:
        await close_pool()
    logger.info("Worker finished.")


def main():
    asyncio.run(_run_cli())


if __name__ == "__main__":
    main()
