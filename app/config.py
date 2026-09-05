"""app/config.py — all configuration from environment variables."""
import os
from dotenv import load_dotenv
load_dotenv()

DATABASE_URL          = os.environ["DATABASE_URL"]

SCRAPER_BATCH_SIZE    = int(os.getenv("SCRAPER_BATCH_SIZE",    "5"))
MAX_LOCATION_ATTEMPTS = int(os.getenv("MAX_LOCATION_ATTEMPTS", "3"))
LOCATION_WORKERS      = int(os.getenv("LOCATION_WORKERS",      "1"))
CTX_POOL_SIZE         = int(os.getenv("CTX_POOL_SIZE",         "1"))
DETAIL_WORKERS        = int(os.getenv("DETAIL_WORKERS",        "1"))
MAX_RESULTS           = int(os.getenv("MAX_RESULTS",           "30"))

# Category / job config
SCRAPER_CATEGORY      = os.getenv("SCRAPER_CATEGORY", "mehendi_artist")
SCRAPER_STATE         = os.getenv("SCRAPER_STATE",    "Jharkhand")
MATCHERS_DIR          = os.path.join(os.path.dirname(os.path.dirname(__file__)), "matchers")

# Stale-running recovery
STALE_RUNNING_MINUTES        = int(os.getenv("STALE_RUNNING_MINUTES",        "30"))
SCRAPE_LEASE_TIMEOUT_MINUTES = int(os.getenv("SCRAPE_LEASE_TIMEOUT_MINUTES", "20"))

# API / server
PORT                  = int(os.getenv("PORT", "10000"))
SCRAPE_TRIGGER_TOKEN  = os.getenv("SCRAPE_TRIGGER_TOKEN", "")

# Runtime limit per triggered batch (minutes)
MAX_RUNTIME_MINUTES   = int(os.getenv("MAX_RUNTIME_MINUTES", "12"))
