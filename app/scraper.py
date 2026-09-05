"""
app/scraper.py — Google Maps scraper (extraction logic preserved from
geo_scraper_jharkhand.py).  Storage replaced: businesses are yielded
one-by-one instead of accumulated in a list or written to JSON.
"""
import asyncio
import logging
import math
import random
import re
from datetime import datetime
from typing import AsyncIterator, List, Optional, Set
from urllib.parse import quote_plus

from playwright.async_api import Browser, BrowserContext, Page, async_playwright
from playwright.async_api import TimeoutError as PWTimeout

from app.config import CTX_POOL_SIZE, DETAIL_WORKERS, MAX_RESULTS

logger = logging.getLogger(__name__)

# ── Constants (unchanged from original) ──────────────────────────────────────

CITY_ZOOM    = 13
TOWN_ZOOM    = 14
RURAL_ZOOM   = 15
DEFAULT_ZOOM = 14
FALLBACK_ZOOMS = [13, 12, 11, 10]

MAX_RESULT_DISTANCE_CITY  = 30_000
MAX_RESULT_DISTANCE_TOWN  = 20_000
MAX_RESULT_DISTANCE_RURAL = 15_000

SCROLL_ITERS = 20
MAX_CTX_USES = 40

_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
]

_BLOCKED_TYPES = {"image", "media", "font", "stylesheet"}

_STEALTH_SCRIPT = """
    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
    Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
    Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
    window.chrome = { runtime: {} };
    const originalQuery = window.navigator.permissions.query;
    window.navigator.permissions.query = (parameters) => (
        parameters.name === 'notifications' ? Promise.resolve({ state: 'denied' }) : originalQuery(parameters)
    );
"""

BROWSER_ARGS = [
    "--no-sandbox",
    "--disable-dev-shm-usage",
    "--disable-gpu",
    "--disable-extensions",
    "--disable-background-networking",
    "--disable-default-apps",
    "--disable-sync",
    "--no-first-run",
    "--mute-audio",
    "--metrics-recording-only",
    "--disable-blink-features=AutomationControlled",
    "--js-flags=--max-old-space-size=256",
]

# ── Browser / context helpers ─────────────────────────────────────────────────

async def launch_browser(pw) -> Browser:
    return await pw.chromium.launch(headless=True, args=BROWSER_ARGS)


async def _build_context(browser: Browser) -> BrowserContext:
    ctx = await browser.new_context(
        user_agent=random.choice(_USER_AGENTS),
        viewport={"width": 1280, "height": 800},
        locale="en-US",
        timezone_id="Asia/Kolkata",
        java_script_enabled=True,
        extra_http_headers={
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Sec-CH-UA-Platform": '"Windows"',
            "Sec-CH-UA-Mobile": "?0",
        },
    )
    await ctx.add_init_script(_STEALTH_SCRIPT)

    async def _block(route):
        if route.request.resource_type in _BLOCKED_TYPES:
            await route.abort()
        else:
            await route.continue_()

    await ctx.route("**/*", _block)
    return ctx


# ── Location type helpers (unchanged) ────────────────────────────────────────

def get_location_type(loc: dict) -> str:
    area = loc.get("area", "").lower()
    city = loc.get("city", "").lower()
    for kw in ("city", "municipal", "corporation", "municipality", "nagar nigam"):
        if kw in area or kw in city:
            return "city"
    for kw in ("town", "nagar panchayat", "municipal council"):
        if kw in area or kw in city:
            return "town"
    return "rural"


def get_location_settings(loc: dict, search_terms: List[str]) -> dict:
    loc_type = get_location_type(loc)
    if loc_type == "city":
        return {"zoom": CITY_ZOOM,  "max_distance": MAX_RESULT_DISTANCE_CITY,  "search_terms": search_terms}
    if loc_type == "town":
        return {"zoom": TOWN_ZOOM,  "max_distance": MAX_RESULT_DISTANCE_TOWN,  "search_terms": search_terms}
    return {"zoom": RURAL_ZOOM, "max_distance": MAX_RESULT_DISTANCE_RURAL, "search_terms": search_terms}


# ── Core scraper class ────────────────────────────────────────────────────────

class GoogleMapsGeoScraper:
    """
    Coordinate-anchored Google Maps scraper.
    Businesses are yielded via scrape_all_async() instead of saved to JSON.
    """

    def __init__(self, search_term: str, lat: float, lng: float,
                 zoom: int = DEFAULT_ZOOM, max_distance: int = MAX_RESULT_DISTANCE_CITY,
                 label: str = ""):
        self.search_term   = search_term
        self.lat           = lat
        self.lng           = lng
        self.zoom          = zoom
        self.max_distance  = max_distance
        self.label         = label or search_term
        self.processed_urls: Set[str] = set()
        self._collect_succeeded = False

    # ── URL / distance helpers (unchanged) ───────────────────────────────────

    @staticmethod
    def _build_url(term: str, lat: float, lng: float, zoom: int) -> str:
        return f"https://www.google.com/maps/search/{quote_plus(term)}/@{lat},{lng},{zoom}z"

    @staticmethod
    def _haversine_m(lat1, lng1, lat2, lng2) -> float:
        R = 6_371_000
        lat1, lng1, lat2, lng2 = map(math.radians, [lat1, lng1, lat2, lng2])
        dlat, dlng = lat2 - lat1, lng2 - lng1
        a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlng / 2) ** 2
        return 2 * R * math.asin(math.sqrt(a))

    @staticmethod
    def _coords_from_url(url: str):
        m = re.search(r"!3d(-?[\d.]+)!4d(-?[\d.]+)", url)
        return (float(m.group(1)), float(m.group(2))) if m else None

    # ── Cookie consent (unchanged) ────────────────────────────────────────────

    async def _handle_cookie_consent(self, page: Page):
        for sel in [
            'button:has-text("Accept all")', 'button:has-text("Accept")',
            'button:has-text("Reject all")', 'form:nth-child(2) button',
            'button[aria-label*="Accept"]',
        ]:
            try:
                btn = await page.query_selector(sel)
                if btn and await btn.is_visible():
                    await btn.click(timeout=3000)
                    await asyncio.sleep(0.5)
                    break
            except Exception:
                pass

    # ── Place link extraction (unchanged) ─────────────────────────────────────

    async def _extract_place_links(self, page: Page) -> List[str]:
        try:
            return await page.eval_on_selector_all(
                "a[href*='/maps/place/']",
                "els => [...new Set(els.map(e => e.href).filter(h => h && h.includes('/maps/place/')))]",
            )
        except Exception:
            try:
                return await page.evaluate(
                    """() => {
                        const seen = new Set(); const results = [];
                        for (const el of document.querySelectorAll('a[href*="/maps/place/"]')) {
                            const href = el.href;
                            if (href && !seen.has(href)) { seen.add(href); results.push(href); }
                        }
                        return results;
                    }"""
                )
            except Exception:
                return []

    # ── URL collection (unchanged logic) ─────────────────────────────────────

    async def _collect_urls(self, page: Page, lat: float, lng: float,
                            zoom: int, term: str, retries: int = 2) -> List[str]:
        for attempt in range(1, retries + 1):
            try:
                url = self._build_url(term, lat, lng, zoom)
                logger.info(f"  [collect] attempt {attempt} zoom={zoom} term='{term}'")
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                await asyncio.sleep(random.uniform(0.5, 1.5))

                # Detect Google block / CAPTCHA / redirect away from Maps
                current = page.url
                if "google.com/maps" not in current and "maps/search" not in current and "/maps/place/" not in current:
                    logger.warning(f"  [collect] redirected away from Maps ({current[:80]}) — possible block")
                    await asyncio.sleep(5 * attempt)
                    continue

                await self._handle_cookie_consent(page)

                if "/maps/place/" in page.url:
                    return [page.url]

                try:
                    await page.wait_for_selector(
                        "div[role='feed'], a[href*='/maps/place/']", timeout=15000
                    )
                except PWTimeout:
                    logger.warning("  [collect] result container not found, retrying")
                    continue

                await asyncio.sleep(random.uniform(0.5, 1.0))

                feed = await page.query_selector("div[role='feed']")
                if feed:
                    for _ in range(SCROLL_ITERS):
                        await feed.evaluate("el => el.scrollTop = el.scrollHeight")
                        end = await page.query_selector(".HlvSq")
                        if end:
                            txt = (await end.inner_text()).strip().lower()
                            if "end of the list" in txt or "no results" in txt:
                                break
                        await asyncio.sleep(random.uniform(0.3, 0.7))
                        if len(await self._extract_place_links(page)) >= MAX_RESULTS:
                            break

                links = await self._extract_place_links(page)
                seen: Set[str] = set()
                urls: List[str] = []
                skipped = 0
                for lnk in links:
                    if lnk in self.processed_urls or lnk in seen:
                        continue
                    coords = self._coords_from_url(lnk)
                    if coords:
                        dist = self._haversine_m(lat, lng, coords[0], coords[1])
                        if dist > self.max_distance:
                            skipped += 1
                            continue
                    urls.append(lnk)
                    seen.add(lnk)

                if skipped:
                    logger.info(f"  [collect] skipped {skipped} outside radius")
                logger.info(f"  [collect] found {len(urls)} new URLs for '{term}'")
                self._collect_succeeded = True
                return urls[:MAX_RESULTS]

            except Exception as exc:
                logger.error(f"  [collect] attempt {attempt} error: {exc}")
                if attempt < retries:
                    await asyncio.sleep(2 * attempt)
        return []

    # ── Detail scraping (unchanged logic) ────────────────────────────────────

    async def _scrape_details(self, page: Page, url: str) -> Optional[dict]:
        for attempt in range(1, 3):
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                await page.wait_for_selector("h1.DUwDvf", timeout=10000)
                break
            except Exception as exc:
                if attempt == 2:
                    logger.error(f"  [detail] failed {url}: {exc}")
                    return None
                await asyncio.sleep(random.uniform(3, 6))

        try:
            await asyncio.sleep(random.uniform(0.5, 1.5))
            for _ in range(3):
                await page.evaluate("window.scrollBy(0, 300)")
                await asyncio.sleep(0.2)

            async def _text(sel: str) -> str:
                try:
                    el = await page.query_selector(sel)
                    return (await el.inner_text()).strip() if el else ""
                except Exception:
                    return ""

            async def _attr(sel: str, attr: str) -> str:
                try:
                    el = await page.query_selector(sel)
                    return (await el.get_attribute(attr) or "").strip() if el else ""
                except Exception:
                    return ""

            name = await _text("h1.DUwDvf")

            address = ""
            for sel in [
                "button[data-item-id='address'] .Io6YTe",
                "button[data-item-id='address']",
                "[data-item-id='address'] .fontBodyMedium",
            ]:
                t = await _text(sel)
                if t and len(t) > 5:
                    address = t
                    break

            phone = ""
            for sel in [
                "button[data-item-id*='phone'] .Io6YTe",
                "button[data-item-id*='phone']",
                "button[data-tooltip='Copy phone number']",
            ]:
                t = await _text(sel)
                if t and ("+" in t or any(c.isdigit() for c in t)):
                    phone = t
                    break

            website  = await _attr("a[data-item-id='authority']", "href")
            rating   = await _text(".F7nice span[aria-hidden='true']")
            reviews  = await _text(".F7nice span[aria-label*='reviews']")
            category = await _text("button.DkEaL")
            hours    = await _text(".ZDu9vd span")

            about = ""
            try:
                snippets = await page.eval_on_selector_all(
                    ".jftiEf .wiI7pd",
                    "els => els.slice(0,3).map(e => e.innerText.trim()).filter(t => t.length > 10)",
                )
                about = " | ".join(snippets)
            except Exception:
                pass

            return {
                "gmaps_url":            url,
                "gmaps_name":           name,
                "gmaps_address":        address,
                "gmaps_phone":          phone,
                "gmaps_rating":         rating,
                "gmaps_reviews_count":  reviews,
                "gmaps_opening_hours":  hours,
                "gmaps_category":       category,
                "gmaps_website":        website,
                "gmaps_about":          about,
                "location_lat":         self.lat,
                "location_lng":         self.lng,
                "search_term":          self.search_term,
                "scraped_at":           datetime.utcnow().isoformat(),
            }
        except Exception as exc:
            logger.error(f"  [detail] error {url}: {exc}")
            return None

    # ── Main scrape method — yields profiles instead of saving to JSON ────────

    async def scrape_all_async(
        self, ctx: BrowserContext, search_terms: List[str]
    ) -> AsyncIterator[dict]:
        """
        Scrape all businesses for this location.
        Yields each profile dict as soon as it is scraped.
        Uses a single context (low-memory mode).
        """
        logger.info(f"  Location: {self.label} ({self.lat},{self.lng}) zoom={self.zoom}")

        collect_page = await ctx.new_page()
        try:
            business_urls: List[str] = []
            for term in search_terms:
                for zoom_level in [self.zoom] + FALLBACK_ZOOMS:
                    urls = await self._collect_urls(
                        collect_page, self.lat, self.lng, zoom_level, term
                    )
                    if urls:
                        self.zoom = zoom_level
                        business_urls = urls
                        break
                if business_urls:
                    break
        finally:
            await collect_page.close()

        if not business_urls:
            logger.warning(f"  No businesses found for {self.label}")
            return

        logger.info(f"  Found {len(business_urls)} candidate URLs")

        detail_page = await ctx.new_page()
        try:
            for url in business_urls:
                if url in self.processed_urls:
                    continue
                await asyncio.sleep(random.uniform(0.5, 1.5))
                profile = await self._scrape_details(detail_page, url)
                if profile:
                    self.processed_urls.add(url)
                    yield profile
        finally:
            await detail_page.close()
