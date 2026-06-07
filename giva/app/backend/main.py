"""
GIVA - FastAPI Backend
Provides semantic search, AI recommendations, product catalog APIs, image serving, and order management
"""
import os
import json
import base64
import logging
import random
import string
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional, List
from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel
import httpx

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="GIVA AI API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Lakebase (PostgreSQL) helpers — uses psycopg2 via asyncio thread executor
# ---------------------------------------------------------------------------
LAKEBASE_HOST = os.environ.get(
    "LAKEBASE_HOST",
    "",
)
LAKEBASE_PORT = 5432
LAKEBASE_DB = os.environ.get("LAKEBASE_DB", "giva")


def _pg_connect(token: str):
    """Open a synchronous psycopg2 connection.

    Lakebase OAuth auth: username = service-principal application ID
    (DATABRICKS_CLIENT_ID env var), password = OAuth access token.
    """
    import psycopg2
    pg_user = os.environ.get("DATABRICKS_CLIENT_ID", "").strip()
    if not pg_user:
        raise RuntimeError(
            "DATABRICKS_CLIENT_ID is not set — cannot determine Lakebase username"
        )
    return psycopg2.connect(
        host=LAKEBASE_HOST,
        port=LAKEBASE_PORT,
        dbname=LAKEBASE_DB,
        user=pg_user,
        password=token,
        sslmode="require",
        connect_timeout=15,
    )


async def _run_db(sync_fn):
    """Get a fresh token then run a synchronous DB function in a thread pool."""
    import asyncio
    token = await _get_oauth_token()
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, lambda: sync_fn(token))


_CREATE_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS brickjewels_orders (
        order_id      TEXT        PRIMARY KEY,
        customer_name TEXT        NOT NULL,
        customer_email TEXT,
        items         JSONB       NOT NULL,
        total_amount  BIGINT      NOT NULL,
        status        TEXT        DEFAULT 'confirmed',
        created_at    TIMESTAMPTZ DEFAULT NOW()
    )
"""

_CREATE_USERS_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS brickjewels_users (
        user_id        SERIAL      PRIMARY KEY,
        first_name     TEXT        NOT NULL,
        last_name      TEXT        NOT NULL,
        email          TEXT        UNIQUE NOT NULL,
        password_hash  TEXT        NOT NULL,
        country_code   TEXT        DEFAULT '+91',
        mobile         TEXT,
        created_at     TIMESTAMPTZ DEFAULT NOW()
    )
"""

_CREATE_USER_DATA_SQL = """
    CREATE TABLE IF NOT EXISTS brickjewels_user_data_scd2 (
        data_id         SERIAL      PRIMARY KEY,
        user_id         INTEGER     NOT NULL REFERENCES brickjewels_users(user_id),
        cart            JSONB       DEFAULT '[]'::jsonb,
        wishlist        JSONB       DEFAULT '[]'::jsonb,
        is_active       BOOLEAN     DEFAULT TRUE,
        effective_from  TIMESTAMPTZ DEFAULT NOW(),
        effective_to    TIMESTAMPTZ,
        updated_at      TIMESTAMPTZ DEFAULT NOW()
    )
"""

_CREATE_CHAT_SESSIONS_SQL = """
    CREATE TABLE IF NOT EXISTS brickjewels_chat_sessions (
        session_id     SERIAL      PRIMARY KEY,
        user_id        INTEGER     REFERENCES brickjewels_users(user_id),
        title          TEXT        DEFAULT 'New Conversation',
        messages       JSONB       DEFAULT '[]'::jsonb,
        created_at     TIMESTAMPTZ DEFAULT NOW(),
        updated_at     TIMESTAMPTZ DEFAULT NOW()
    )
"""

_CREATE_METAL_PRICES_SQL = """
    CREATE TABLE IF NOT EXISTS brickjewels_metal_prices (
        id             SERIAL      PRIMARY KEY,
        metal          TEXT        NOT NULL,
        currency       TEXT        NOT NULL DEFAULT 'INR',
        price_gram_24k NUMERIC,
        price_gram_22k NUMERIC,
        price_gram_21k NUMERIC,
        price_gram_20k NUMERIC,
        price_gram_18k NUMERIC,
        price_gram_16k NUMERIC,
        price_gram_14k NUMERIC,
        price_gram_10k NUMERIC,
        pct_change_24k NUMERIC    DEFAULT 0,
        pct_change_22k NUMERIC    DEFAULT 0,
        pct_change_21k NUMERIC    DEFAULT 0,
        pct_change_20k NUMERIC    DEFAULT 0,
        pct_change_18k NUMERIC    DEFAULT 0,
        pct_change_16k NUMERIC    DEFAULT 0,
        pct_change_14k NUMERIC    DEFAULT 0,
        pct_change_10k NUMERIC    DEFAULT 0,
        price_per_gram NUMERIC,
        price_per_oz   NUMERIC,
        is_active       BOOLEAN    DEFAULT TRUE,
        effective_from  TIMESTAMPTZ NOT NULL,
        effective_to    TIMESTAMPTZ,
        fetched_at      TIMESTAMPTZ NOT NULL
    )
"""

_MIGRATE_METAL_PRICES_SQL = """
    DO $$
    BEGIN
        ALTER TABLE brickjewels_metal_prices ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT TRUE;
        ALTER TABLE brickjewels_metal_prices ADD COLUMN IF NOT EXISTS effective_from TIMESTAMPTZ DEFAULT NOW();
        ALTER TABLE brickjewels_metal_prices ADD COLUMN IF NOT EXISTS effective_to TIMESTAMPTZ;
        ALTER TABLE brickjewels_metal_prices ADD COLUMN IF NOT EXISTS pct_change_24k NUMERIC DEFAULT 0;
        ALTER TABLE brickjewels_metal_prices ADD COLUMN IF NOT EXISTS pct_change_22k NUMERIC DEFAULT 0;
        ALTER TABLE brickjewels_metal_prices ADD COLUMN IF NOT EXISTS pct_change_21k NUMERIC DEFAULT 0;
        ALTER TABLE brickjewels_metal_prices ADD COLUMN IF NOT EXISTS pct_change_20k NUMERIC DEFAULT 0;
        ALTER TABLE brickjewels_metal_prices ADD COLUMN IF NOT EXISTS pct_change_18k NUMERIC DEFAULT 0;
        ALTER TABLE brickjewels_metal_prices ADD COLUMN IF NOT EXISTS pct_change_16k NUMERIC DEFAULT 0;
        ALTER TABLE brickjewels_metal_prices ADD COLUMN IF NOT EXISTS pct_change_14k NUMERIC DEFAULT 0;
        ALTER TABLE brickjewels_metal_prices ADD COLUMN IF NOT EXISTS pct_change_10k NUMERIC DEFAULT 0;
    EXCEPTION WHEN others THEN NULL;
    END $$;
"""

_KARAT_COLS = ["24k", "22k", "21k", "20k", "18k", "16k", "14k", "10k"]

# ---------------------------------------------------------------------------
# Product Price Computation — dynamic pricing based on live metal prices
# ---------------------------------------------------------------------------
MAKING_CHARGE_RATES = {
    "necklace": 0.40, "ring": 0.35, "earring": 0.35,
    "bangle": 0.30, "bracelet": 0.35, "pendant": 0.40,
}
METAL_GST_RATE = 0.03
MAKING_GST_RATE = 0.28

_CREATE_PRODUCT_PRICES_SQL = """
    CREATE TABLE IF NOT EXISTS brickjewels_product_prices (
        id                      SERIAL      PRIMARY KEY,
        product_id              TEXT        NOT NULL,
        karat                   TEXT,
        metal_type              TEXT,
        weight_grams            NUMERIC,
        category                TEXT,
        metal_rate_per_gram     NUMERIC     DEFAULT 0,
        metal_cost              NUMERIC     DEFAULT 0,
        diamond_cost            NUMERIC     DEFAULT 0,
        making_pct              NUMERIC     DEFAULT 0,
        making_cost             NUMERIC     DEFAULT 0,
        discount_pct            NUMERIC     DEFAULT 0,
        discount_value          NUMERIC     DEFAULT 0,
        gst_pct_metal_diamond   NUMERIC     DEFAULT 3,
        gst_cost_metal_diamond  NUMERIC     DEFAULT 0,
        gst_pct_making          NUMERIC     DEFAULT 28,
        gst_cost_making         NUMERIC     DEFAULT 0,
        total_before_gst        NUMERIC     DEFAULT 0,
        total_gst               NUMERIC     DEFAULT 0,
        final_price             NUMERIC     DEFAULT 0,
        is_active               BOOLEAN     DEFAULT TRUE,
        effective_from          TIMESTAMPTZ NOT NULL,
        effective_to            TIMESTAMPTZ,
        computed_at             TIMESTAMPTZ NOT NULL
    )
"""

# In-memory price cache: product_id → final_price (int)
_product_price_cache: dict = {}


def _parse_material(material: str):
    """Extract karat and metal type from the material string."""
    mat = (material or "").lower()
    karat = None
    metal = "gold"  # default

    # Extract karat from strings like "22K Gold", "18K Rose Gold & Diamond"
    import re
    karat_match = re.search(r'(\d+)\s*k', mat)
    if karat_match:
        karat = int(karat_match.group(1))

    if "silver" in mat:
        metal = "silver"
    elif "platinum" in mat:
        metal = "gold"  # proxy: use gold 24K for platinum
        karat = karat or 24
    elif "rose gold" in mat or "white gold" in mat:
        karat = karat or 18
    elif "gold" in mat:
        karat = karat or 22  # default gold karat

    if karat is None:
        karat = 18  # fallback

    has_diamond = "diamond" in mat or "solitaire" in mat
    return karat, metal, has_diamond


def _karat_to_col(karat: int) -> str:
    """Map karat int to the price column key."""
    valid = {24: "24k", 22: "22k", 21: "21k", 20: "20k", 18: "18k", 16: "16k", 14: "14k", 10: "10k"}
    return valid.get(karat, "18k")


def _diamond_price_for_product(product_id: str) -> float:
    """Deterministic random diamond price (60K-180K) seeded by product_id."""
    import hashlib
    seed = int(hashlib.md5(product_id.encode()).hexdigest()[:8], 16)
    rng = random.Random(seed)
    return rng.randint(60000, 180000)


async def _compute_product_prices():
    """Compute prices for all products using active metal prices from Lakebase."""
    from datetime import timezone as tz, timedelta
    ist = tz(timedelta(hours=5, minutes=30))
    now_ist = datetime.now(ist).isoformat()
    logger.info("Computing product prices from live metal rates...")

    # Step 1: Get active metal prices from Lakebase
    def _get_metal_prices(token):
        conn = _pg_connect(token)
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT metal, price_gram_24k, price_gram_22k, price_gram_21k,
                           price_gram_20k, price_gram_18k, price_gram_16k,
                           price_gram_14k, price_gram_10k
                    FROM brickjewels_metal_prices
                    WHERE is_active = TRUE
                """)
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, r)) for r in rows]
        finally:
            conn.close()

    try:
        metal_rows = await _run_db(_get_metal_prices)
    except Exception as exc:
        logger.error("Cannot read metal prices for product pricing: %s", exc)
        return

    if not metal_rows:
        logger.warning("No active metal prices found — skipping product price computation")
        return

    # Build lookup: metal_name → {karat_col → price_per_gram}
    metal_prices = {}
    for row in metal_rows:
        name = row["metal"].lower()  # "gold" or "silver"
        metal_prices[name] = {}
        for k in _KARAT_COLS:
            val = row.get(f"price_gram_{k}")
            metal_prices[name][k] = float(val) if val else 0

    # Step 2: Fetch all products from UC using run_sql helper
    try:
        sql = f"SELECT product_id, material, weight_grams, category FROM {PRODUCTS_TABLE} WHERE in_stock = true"
        products = await run_sql(sql)
    except Exception as exc:
        logger.error("Failed to fetch products from UC: %s", exc)
        return

    logger.info("Computing prices for %d products", len(products))

    # Step 3: Compute prices and store in Lakebase (SCD2)
    computed = []
    for p in products:
        pid = p["product_id"]
        material = p.get("material", "")
        weight = float(p.get("weight_grams", 0) or 0)
        category = (p.get("category", "") or "").lower()

        karat, metal, has_diamond = _parse_material(material)
        karat_col = _karat_to_col(karat)

        metal_rate = metal_prices.get(metal, metal_prices.get("gold", {})).get(karat_col, 0)
        metal_cost = round(weight * metal_rate, 2)

        diamond_cost = _diamond_price_for_product(pid) if has_diamond else 0

        making_pct = MAKING_CHARGE_RATES.get(category, 0.35) * 100  # store as percentage
        making_cost = round(metal_cost * (making_pct / 100), 2)

        discount_pct = 0  # standard 0% for now, column ready for future use
        discount_value = 0

        # GST: 3% on (metal + diamond), 28% on making
        gst_pct_md = METAL_GST_RATE * 100  # 3
        gst_cost_md = round((metal_cost + diamond_cost) * METAL_GST_RATE, 2)

        gst_pct_making = MAKING_GST_RATE * 100  # 28
        gst_cost_making = round(making_cost * MAKING_GST_RATE, 2)

        total_before_gst = round(metal_cost + diamond_cost + making_cost - discount_value, 2)
        total_gst = round(gst_cost_md + gst_cost_making, 2)
        final_price = round(total_before_gst + total_gst, 0)

        computed.append({
            "product_id": pid, "karat": f"{karat}K", "metal_type": metal,
            "weight_grams": weight, "category": category,
            "metal_rate_per_gram": metal_rate, "metal_cost": metal_cost,
            "diamond_cost": diamond_cost,
            "making_pct": making_pct, "making_cost": making_cost,
            "discount_pct": discount_pct, "discount_value": discount_value,
            "gst_pct_metal_diamond": gst_pct_md, "gst_cost_metal_diamond": gst_cost_md,
            "gst_pct_making": gst_pct_making, "gst_cost_making": gst_cost_making,
            "total_before_gst": total_before_gst, "total_gst": total_gst,
            "final_price": final_price,
        })

    def _store_prices(token):
        conn = _pg_connect(token)
        try:
            with conn.cursor() as cur:
                cur.execute(_CREATE_PRODUCT_PRICES_SQL)
                conn.commit()

                # SCD2: close all current active records
                cur.execute("""
                    UPDATE brickjewels_product_prices
                    SET is_active = FALSE, effective_to = %s
                    WHERE is_active = TRUE
                """, (now_ist,))

                # Insert new active records
                for c in computed:
                    cur.execute("""
                        INSERT INTO brickjewels_product_prices
                            (product_id, karat, metal_type, weight_grams, category,
                             metal_rate_per_gram, metal_cost, diamond_cost,
                             making_pct, making_cost,
                             discount_pct, discount_value,
                             gst_pct_metal_diamond, gst_cost_metal_diamond,
                             gst_pct_making, gst_cost_making,
                             total_before_gst, total_gst, final_price,
                             is_active, effective_from, effective_to, computed_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                                TRUE,%s,NULL,%s)
                    """, (
                        c["product_id"], c["karat"], c["metal_type"], c["weight_grams"],
                        c["category"], c["metal_rate_per_gram"], c["metal_cost"],
                        c["diamond_cost"], c["making_pct"], c["making_cost"],
                        c["discount_pct"], c["discount_value"],
                        c["gst_pct_metal_diamond"], c["gst_cost_metal_diamond"],
                        c["gst_pct_making"], c["gst_cost_making"],
                        c["total_before_gst"], c["total_gst"], c["final_price"],
                        now_ist, now_ist,
                    ))
            conn.commit()
        finally:
            conn.close()

    try:
        await _run_db(_store_prices)
    except Exception as exc:
        logger.error("Failed to store product prices: %s", exc)
        return

    # Step 4: Update in-memory cache
    _product_price_cache.clear()
    for c in computed:
        _product_price_cache[c["product_id"]] = int(c["final_price"])

    logger.info("Product prices computed and cached (%d products)", len(computed))


async def _load_product_price_cache():
    """Load active product prices from Lakebase into memory cache."""
    def _load(token):
        conn = _pg_connect(token)
        try:
            with conn.cursor() as cur:
                cur.execute(_CREATE_PRODUCT_PRICES_SQL)
                conn.commit()
                cur.execute("""
                    SELECT product_id, final_price
                    FROM brickjewels_product_prices
                    WHERE is_active = TRUE
                """)
                return cur.fetchall()
        finally:
            conn.close()

    try:
        rows = await _run_db(_load)
        _product_price_cache.clear()
        for pid, fp in rows:
            _product_price_cache[pid] = int(float(fp))
        logger.info("Loaded %d product prices into cache", len(_product_price_cache))
    except Exception as exc:
        logger.warning("Could not load product price cache: %s", exc)


def _apply_live_prices(products: list) -> list:
    """Override price_inr with computed live price from cache."""
    if not _product_price_cache:
        return products
    for p in products:
        pid = p.get("product_id")
        if pid and pid in _product_price_cache:
            p["price_inr"] = _product_price_cache[pid]
    return products


# ---------------------------------------------------------------------------
# In-memory catalog cache — 490 products fit comfortably in memory.
# Eliminates 1-3s per call for /api/products, /api/featured, /api/categories,
# /api/product/{id}, /api/recommend.
# ---------------------------------------------------------------------------
_catalog_cache: list[dict] = []
_categories_cache: list[dict] = []
_featured_cache: list[dict] = []
_catalog_by_id: dict = {}
_catalog_loaded_at: float = 0.0
CATALOG_CACHE_TTL_SECONDS = 600  # 10 min


async def _load_catalog_cache():
    """Pull the full product catalog into memory once."""
    global _catalog_cache, _categories_cache, _featured_cache, _catalog_by_id, _catalog_loaded_at
    import time as _time
    try:
        query = f"""
            SELECT product_id, name, description, category, subcategory, material,
                   occasion, style, collection, weight_grams, price_inr, image_url, tags,
                   llm_attributes, llm_description, in_stock
            FROM {PRODUCTS_TABLE}
        """
        rows = await run_sql(query)
        instock = [p for p in rows if p.get("in_stock") in (True, "true", 1, "1", "True")]

        from collections import defaultdict
        cats = defaultdict(lambda: {"count": 0, "min_price": float("inf"), "max_price": 0.0, "sum_price": 0.0})
        for p in instock:
            c = p.get("category")
            if not c:
                continue
            price = float(p.get("price_inr") or 0)
            cats[c]["count"] += 1
            cats[c]["min_price"] = min(cats[c]["min_price"], price)
            cats[c]["max_price"] = max(cats[c]["max_price"], price)
            cats[c]["sum_price"] += price
        categories = [
            {
                "category": k,
                "count": v["count"],
                "min_price": round(v["min_price"]) if v["min_price"] != float("inf") else 0,
                "max_price": round(v["max_price"]),
                "avg_price": round(v["sum_price"] / v["count"]) if v["count"] else 0,
            }
            for k, v in cats.items()
        ]
        categories.sort(key=lambda x: -x["count"])

        featured = [
            p for p in instock
            if any(s in (p.get("material") or "") for s in ("Diamond", "Solitaire", "22K Gold"))
        ]
        featured.sort(key=lambda p: -float(p.get("price_inr") or 0))

        _catalog_cache = instock
        _categories_cache = categories
        _featured_cache = featured[:8]
        _catalog_by_id = {p.get("product_id"): p for p in instock if p.get("product_id")}
        _catalog_loaded_at = _time.time()
        logger.info(
            "Catalog cache loaded: %d products, %d categories, %d featured",
            len(_catalog_cache), len(_categories_cache), len(_featured_cache),
        )
    except Exception as exc:
        logger.error("Failed to load catalog cache: %s", exc)


def _strip_internal(products: list[dict]) -> list[dict]:
    for p in products:
        p.pop("llm_attributes", None)
        p.pop("llm_description", None)
        p.pop("in_stock", None)
    return products


def _copy_products(products: list[dict]) -> list[dict]:
    return [dict(p) for p in products]


def _ensure_table_sync(conn):
    """Create all Lakebase tables within an already-open connection."""
    with conn.cursor() as cur:
        cur.execute(_CREATE_TABLE_SQL)
        cur.execute(_CREATE_USERS_TABLE_SQL)
        cur.execute(_CREATE_USER_DATA_SQL)
        cur.execute(_CREATE_CHAT_SESSIONS_SQL)
        cur.execute(_CREATE_METAL_PRICES_SQL)
        cur.execute(_MIGRATE_METAL_PRICES_SQL)
        cur.execute(_CREATE_PRODUCT_PRICES_SQL)
        cur.execute(_MIGRATE_USERS_MILESTONES_SQL)
        cur.execute(_CREATE_NUDGES_TABLE_SQL)
        cur.execute(_MIGRATE_NUDGES_CATEGORY_SQL)
        cur.execute(_CREATE_NUDGE_EMAILS_SQL)
        cur.execute(_CREATE_GENIE_HISTORY_SQL)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_nudges_user_active
            ON brickjewels_nudges (user_id, is_active) WHERE is_active = TRUE AND is_dismissed = FALSE
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_metal_prices_active
            ON brickjewels_metal_prices (metal, is_active) WHERE is_active = TRUE
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_product_prices_active
            ON brickjewels_product_prices (product_id, is_active) WHERE is_active = TRUE
        """)
        # Add user_id column to orders if not present
        cur.execute("""
            DO $$
            BEGIN
                ALTER TABLE brickjewels_orders ADD COLUMN IF NOT EXISTS user_id INTEGER;
            EXCEPTION WHEN others THEN NULL;
            END $$;
        """)
        # Index for fast active record lookup
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_user_data_scd2_active
            ON brickjewels_user_data_scd2 (user_id, is_active) WHERE is_active = TRUE
        """)
    conn.commit()


async def _ensure_orders_table():
    """Create the orders table if it doesn't exist (called at startup and lazily)."""
    def _create(token):
        conn = _pg_connect(token)
        try:
            _ensure_table_sync(conn)
        finally:
            conn.close()

    try:
        await _run_db(_create)
        logger.info("Orders table ready in Lakebase.")
    except Exception as exc:
        logger.warning("Lakebase startup setup failed (will retry on first order): %s", exc)


# ---------------------------------------------------------------------------
# Metal Prices (Gold & Silver) — MCX-equivalent via Yahoo Finance
# ---------------------------------------------------------------------------
# Karat purity ratios (fraction of pure 24K)
_KARAT_PURITY = {
    "24k": 1.0, "22k": 22/24, "21k": 21/24, "20k": 20/24,
    "18k": 18/24, "16k": 16/24, "14k": 14/24, "10k": 10/24,
}
# Yahoo Finance tickers: COMEX Gold & Silver futures + USD/INR FX rate
_YF_TICKERS = {"Gold": "GC=F", "Silver": "SI=F"}
_YF_FX_TICKER = "INR=X"  # USD to INR exchange rate
_TROY_OZ_TO_GRAMS = 31.1035
_INDIA_IMPORT_PREMIUM = 1.10  # ~10% (customs duty + GST) to approximate MCX prices


async def _fetch_yahoo_chart(ticker: str) -> float:
    """Fetch the latest price for a Yahoo Finance ticker using the chart API (async httpx)."""
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
    params = {"interval": "1d", "range": "1d"}
    headers_yf = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}

    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.get(url, params=params, headers=headers_yf)
        if resp.status_code != 200:
            raise RuntimeError(f"Yahoo Finance {ticker}: HTTP {resp.status_code}")
        data = resp.json()

    result = data.get("chart", {}).get("result", [])
    if not result:
        raise RuntimeError(f"Yahoo Finance {ticker}: empty result")

    price = result[0].get("meta", {}).get("regularMarketPrice", 0)
    if not price:
        raise RuntimeError(f"Yahoo Finance {ticker}: no regularMarketPrice")
    return float(price)


async def _fetch_and_store_metal_prices():
    """Fetch MCX-equivalent metal prices via Yahoo Finance chart API and store in Lakebase.

    Uses COMEX futures × USD/INR × India import premium (~10%) to derive
    MCX-equivalent prices. Karat prices derived from 24K using standard purity ratios.
    """
    from datetime import timezone as tz, timedelta
    ist = tz(timedelta(hours=5, minutes=30))
    now_ist = datetime.now(ist)
    logger.info("Fetching MCX-equivalent metal prices at %s IST", now_ist.strftime("%Y-%m-%d %H:%M:%S"))

    # Fetch USD/INR and metal prices concurrently (all async httpx, no yfinance needed)
    import asyncio
    try:
        usd_inr, gold_usd, silver_usd = await asyncio.gather(
            _fetch_yahoo_chart(_YF_FX_TICKER),
            _fetch_yahoo_chart(_YF_TICKERS["Gold"]),
            _fetch_yahoo_chart(_YF_TICKERS["Silver"]),
        )
    except Exception as exc:
        logger.error("Yahoo Finance fetch failed: %s", exc)
        raise RuntimeError(f"Yahoo Finance fetch failed: {exc}")

    logger.info("USD/INR=%.2f, Gold=$%.2f/oz, Silver=$%.2f/oz", usd_inr, gold_usd, silver_usd)

    rows = []
    for metal_name, price_usd_oz in [("Gold", gold_usd), ("Silver", silver_usd)]:
        price_inr_oz = price_usd_oz * usd_inr * _INDIA_IMPORT_PREMIUM
        price_inr_gram_24k = price_inr_oz / _TROY_OZ_TO_GRAMS

        row = {
            "metal": metal_name,
            "currency": "INR",
            "price_per_oz": round(price_inr_oz, 2),
            "price_per_gram": round(price_inr_gram_24k, 2),
            "fetched_at": now_ist.isoformat(),
        }
        for karat, purity in _KARAT_PURITY.items():
            row[f"price_gram_{karat}"] = round(price_inr_gram_24k * purity, 2)

        logger.info("MCX-equiv %s: $%.2f/oz × ₹%.2f × 1.10 → 24K=₹%.2f/g, 22K=₹%.2f/g",
                     metal_name, price_usd_oz, usd_inr, row["price_gram_24k"], row["price_gram_22k"])
        rows.append(row)

    if not rows:
        raise RuntimeError("Yahoo Finance returned no metal price data.")

    def _store(token):
        conn = _pg_connect(token)
        try:
            with conn.cursor() as cur:
                # Ensure table + migrations
                cur.execute(_CREATE_METAL_PRICES_SQL)
                cur.execute(_MIGRATE_METAL_PRICES_SQL)
                conn.commit()

                for r in rows:
                    # SCD2: fetch current active record for this metal
                    cur.execute("""
                        SELECT price_gram_24k, price_gram_22k, price_gram_21k,
                               price_gram_20k, price_gram_18k, price_gram_16k,
                               price_gram_14k, price_gram_10k
                        FROM brickjewels_metal_prices
                        WHERE metal = %s AND is_active = TRUE
                        ORDER BY effective_from DESC LIMIT 1
                    """, (r["metal"],))
                    prev = cur.fetchone()

                    # Calculate % change per karat
                    pct_changes = {}
                    for i, k in enumerate(_KARAT_COLS):
                        new_val = r.get(f"price_gram_{k}")
                        old_val = prev[i] if prev else None
                        if old_val and new_val and float(old_val) > 0:
                            pct_changes[k] = round(
                                ((float(new_val) - float(old_val)) / float(old_val)) * 100, 4
                            )
                        else:
                            pct_changes[k] = 0

                    # SCD2: close previous active record
                    cur.execute("""
                        UPDATE brickjewels_metal_prices
                        SET is_active = FALSE, effective_to = %s
                        WHERE metal = %s AND is_active = TRUE
                    """, (r["fetched_at"], r["metal"]))

                    # SCD2: insert new active record
                    cur.execute("""
                        INSERT INTO brickjewels_metal_prices
                            (metal, currency, price_gram_24k, price_gram_22k, price_gram_21k,
                             price_gram_20k, price_gram_18k, price_gram_16k, price_gram_14k,
                             price_gram_10k, pct_change_24k, pct_change_22k, pct_change_21k,
                             pct_change_20k, pct_change_18k, pct_change_16k, pct_change_14k,
                             pct_change_10k, price_per_gram, price_per_oz,
                             is_active, effective_from, effective_to, fetched_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                                TRUE, %s, NULL, %s)
                    """, (
                        r["metal"], r["currency"],
                        r["price_gram_24k"], r["price_gram_22k"], r["price_gram_21k"],
                        r["price_gram_20k"], r["price_gram_18k"], r["price_gram_16k"],
                        r["price_gram_14k"], r["price_gram_10k"],
                        pct_changes["24k"], pct_changes["22k"], pct_changes["21k"],
                        pct_changes["20k"], pct_changes["18k"], pct_changes["16k"],
                        pct_changes["14k"], pct_changes["10k"],
                        r["price_per_gram"], r["price_per_oz"],
                        r["fetched_at"], r["fetched_at"],
                    ))
                    logger.info(
                        "%s SCD2: closed prev record, inserted new (24K pct_change=%.2f%%)",
                        r["metal"], pct_changes["24k"],
                    )
            conn.commit()
            logger.info("Stored %d metal price rows (SCD2) in Lakebase", len(rows))
        finally:
            conn.close()

    try:
        await _run_db(_store)
    except Exception as exc:
        logger.error("Failed to store metal prices: %s", exc)
        return

    # After storing new metal prices, recompute all product prices
    try:
        await _compute_product_prices()
    except Exception as exc:
        logger.error("Product price computation failed: %s", exc)


def _scheduled_fetch_metal_prices():
    """Sync wrapper for APScheduler (runs async fetch in a new event loop)."""
    import asyncio
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.ensure_future(_fetch_and_store_metal_prices())
        else:
            loop.run_until_complete(_fetch_and_store_metal_prices())
    except RuntimeError:
        asyncio.run(_fetch_and_store_metal_prices())


@app.on_event("startup")
async def startup_event():
    await _ensure_orders_table()

    # Sync SP credentials to Databricks Secrets so the workflow notebook can use them
    try:
        host = get_host()
        token = await _get_oauth_token()
        sp_client_id = os.environ.get("DATABRICKS_CLIENT_ID", "")
        sp_client_secret = os.environ.get("DATABRICKS_CLIENT_SECRET", "")
        if token and sp_client_id and sp_client_secret:
            async with httpx.AsyncClient(timeout=15.0) as client:
                hdrs = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
                # Ensure scope exists
                await client.post(f"{host}/api/2.0/secrets/scopes/create",
                                  json={"scope": "giva"}, headers=hdrs)
                # Store SP creds
                await client.post(f"{host}/api/2.0/secrets/put",
                                  json={"scope": "giva", "key": "sp-client-id",
                                        "string_value": sp_client_id}, headers=hdrs)
                await client.post(f"{host}/api/2.0/secrets/put",
                                  json={"scope": "giva", "key": "sp-client-secret",
                                        "string_value": sp_client_secret}, headers=hdrs)
                await client.post(f"{host}/api/2.0/secrets/put",
                                  json={"scope": "giva", "key": "workspace-host",
                                        "string_value": host}, headers=hdrs)
                logger.info("SP credentials synced to Databricks Secrets (scope: giva)")
    except Exception as exc:
        logger.warning("Could not sync SP secrets (non-critical): %s", exc)

    # Load existing product price cache first (in case metal fetch fails)
    await _load_product_price_cache()

    # Load entire product catalog into memory — serves /api/products,
    # /api/featured, /api/categories, /api/product/{id} from RAM instead of
    # paying 1-3s per Statement Execution API round-trip.
    try:
        await _load_catalog_cache()
    except Exception as exc:
        logger.warning("Initial catalog cache load failed: %s", exc)

    # Fetch metal prices once on startup (triggers product price computation)
    try:
        await _fetch_and_store_metal_prices()
    except Exception as exc:
        logger.warning("Initial metal price fetch failed: %s", exc)

    # Schedule recurring fetches at 7:00 AM IST and 4:00 PM IST
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    from apscheduler.triggers.cron import CronTrigger

    scheduler = AsyncIOScheduler()
    # 9:01 AM IST = 3:31 AM UTC
    scheduler.add_job(
        _fetch_and_store_metal_prices,
        CronTrigger(hour=3, minute=31, timezone="UTC"),
        id="metal_prices_morning",
    )
    # 3:31 PM IST = 10:01 AM UTC
    scheduler.add_job(
        _fetch_and_store_metal_prices,
        CronTrigger(hour=10, minute=1, timezone="UTC"),
        id="metal_prices_afternoon",
    )
    # Daily nudge generation at 7:30 AM IST = 2:00 AM UTC
    scheduler.add_job(
        _generate_nudges,
        CronTrigger(hour=2, minute=0, timezone="UTC"),
        id="nudge_generation_daily",
    )
    scheduler.start()
    logger.info("Schedulers started (metal prices 7am/4pm IST, nudges 7:30am IST)")

    # Generate nudges on startup
    try:
        await _generate_nudges()
    except Exception as exc:
        logger.warning("Initial nudge generation failed: %s", exc)


def _generate_order_id() -> str:
    suffix = "".join(random.choices(string.ascii_uppercase + string.digits, k=6))
    ts = datetime.now(timezone.utc).strftime("%y%m%d")
    return f"BJ-ORD-{ts}-{suffix}"

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
# NOTE: catalog/schema/VS endpoint are env-driven so the `giva` installer can
# point the app at whatever workspace it provisioned. Defaults preserve the
# original reference build.
CATALOG = os.environ.get("GIVA_CATALOG", "main")
SCHEMA = os.environ.get("GIVA_SCHEMA", "giva_jewelry")   # products + embeddings + analytics
PRODUCTS_TABLE = os.environ.get("GIVA_PRODUCTS_TABLE", f"{CATALOG}.{SCHEMA}.enriched_jewelry_products")
VS_ENDPOINT = os.environ.get("GIVA_VS_ENDPOINT", "giva-vs")
VS_INDEX = os.environ.get("GIVA_VS_INDEX", f"{CATALOG}.{SCHEMA}.jewelry_embeddings_index")
EMBEDDING_MODEL = os.environ.get("GIVA_EMBEDDING_MODEL", "databricks-gte-large-en")
LLM_MODEL = os.environ.get("GIVA_LLM_MODEL", "databricks-claude-sonnet-4-6")
SARVAM_TRANSLATE_ENDPOINT = os.environ.get("GIVA_SARVAM_TRANSLATE", "sarvam-translate")
SARVAM_COMPLETE_ENDPOINT = os.environ.get("GIVA_SARVAM_COMPLETE", "sarvam-1")
IMAGE_VOLUME_PATH = os.environ.get("GIVA_IMAGE_VOLUME", f"/Volumes/{CATALOG}/{SCHEMA}/jewelry_images")


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------
_token_cache = {"token": None, "expires_at": 0}


def _get_workspace_host() -> str:
    host = os.environ.get("DATABRICKS_HOST", "")
    if not host:
        return ""
    if not host.startswith("http"):
        host = f"https://{host}"
    return host.rstrip("/")


async def _get_oauth_token() -> str:
    import time

    static_token = os.environ.get("DATABRICKS_TOKEN", "")
    if static_token:
        return static_token

    now = time.time()
    if _token_cache["token"] and _token_cache["expires_at"] > now + 60:
        return _token_cache["token"]

    host = _get_workspace_host()
    client_id = os.environ.get("DATABRICKS_CLIENT_ID", "")
    client_secret = os.environ.get("DATABRICKS_CLIENT_SECRET", "")

    if not all([host, client_id, client_secret]):
        logger.warning(
            "Missing Databricks credentials. "
            "DATABRICKS_HOST=%s, CLIENT_ID=%s, CLIENT_SECRET=%s",
            bool(host), bool(client_id), bool(client_secret),
        )
        return ""

    token_url = f"{host}/oidc/v1/token"
    payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "all-apis",
    }

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(
                token_url,
                data=payload,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
            if resp.status_code != 200:
                logger.error("OAuth token request failed (%s): %s", resp.status_code, resp.text)
                return ""
            data = resp.json()
            _token_cache["token"] = data["access_token"]
            _token_cache["expires_at"] = now + data.get("expires_in", 3600)
            logger.info("OAuth token acquired, expires in %s seconds", data.get("expires_in"))
            return _token_cache["token"]
    except Exception as exc:
        logger.error("OAuth token request exception: %s", exc)
        return ""


async def get_headers():
    token = await _get_oauth_token()
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def get_host():
    return _get_workspace_host()


# ---------------------------------------------------------------------------
# SQL helpers
# ---------------------------------------------------------------------------
# Persistent HTTP clients (created at startup, reused across requests).
# Pays TCP+TLS handshake cost ONCE instead of per-call.
_http_db: Optional[httpx.AsyncClient] = None
_http_files: Optional[httpx.AsyncClient] = None


def _get_http_db() -> httpx.AsyncClient:
    global _http_db
    if _http_db is None:
        _http_db = httpx.AsyncClient(
            timeout=60.0,
            limits=httpx.Limits(max_keepalive_connections=20, max_connections=40, keepalive_expiry=120),
        )
    return _http_db


def _get_http_files() -> httpx.AsyncClient:
    global _http_files
    if _http_files is None:
        _http_files = httpx.AsyncClient(
            timeout=30.0,
            limits=httpx.Limits(max_keepalive_connections=10, max_connections=20, keepalive_expiry=120),
        )
    return _http_files


async def run_sql(query: str) -> list[dict]:
    host = get_host()
    if not host:
        raise HTTPException(status_code=500, detail="DATABRICKS_HOST not set")

    url = f"{host}/api/2.0/sql/statements"
    headers = await get_headers()
    payload = {
        "statement": query,
        "warehouse_id": await get_warehouse_id(),
        "wait_timeout": "30s",
        "on_wait_timeout": "CONTINUE",
    }

    client = _get_http_db()
    resp = await client.post(url, json=payload, headers=headers)
    if resp.status_code != 200:
        raise HTTPException(status_code=500, detail=f"SQL error: {resp.text}")

    data = resp.json()
    statement_id = data.get("statement_id")
    status = data.get("status", {}).get("state")

    while status in ("PENDING", "RUNNING"):
        import asyncio
        await asyncio.sleep(0.2)
        poll_resp = await client.get(
            f"{host}/api/2.0/sql/statements/{statement_id}",
            headers=headers,
        )
        data = poll_resp.json()
        status = data.get("status", {}).get("state")

    if status != "SUCCEEDED":
        raise HTTPException(status_code=500, detail=f"SQL failed: {data.get('status')}")

    result = data.get("result", {})
    columns = [col["name"] for col in (data.get("manifest", {}).get("schema", {}).get("columns") or [])]
    rows = result.get("data_array", [])

    return [dict(zip(columns, row)) for row in rows]


_warehouse_id_cache = None


async def get_warehouse_id() -> str:
    global _warehouse_id_cache
    if _warehouse_id_cache:
        return _warehouse_id_cache

    env_wh = os.environ.get("DATABRICKS_WAREHOUSE_ID")
    if env_wh:
        _warehouse_id_cache = env_wh
        return _warehouse_id_cache

    host = get_host()
    headers = await get_headers()
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(
            f"{host}/api/2.0/sql/warehouses",
            headers=headers,
        )
        warehouses = resp.json().get("warehouses", [])
        for wh in warehouses:
            if wh.get("state") == "RUNNING":
                _warehouse_id_cache = wh["id"]
                return _warehouse_id_cache
        if warehouses:
            _warehouse_id_cache = warehouses[0]["id"]
            return _warehouse_id_cache

    raise HTTPException(status_code=500, detail="No SQL warehouse available")


# ---------------------------------------------------------------------------
# Embedding helper
# ---------------------------------------------------------------------------
async def get_embedding(text: str) -> list[float]:
    host = get_host()
    url = f"{host}/serving-endpoints/{EMBEDDING_MODEL}/invocations"
    headers = await get_headers()

    payload = {"input": [text]}
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(url, json=payload, headers=headers)
        if resp.status_code != 200:
            logger.warning(f"Embedding failed: {resp.text}")
            return []
        data = resp.json()
        return data["data"][0]["embedding"]


# ---------------------------------------------------------------------------
# Vector Search helper
# ---------------------------------------------------------------------------
async def vector_search(query_embedding: list[float], num_results: int = 20) -> list[dict]:
    host = get_host()
    url = f"{host}/api/2.0/vector-search/indexes/{VS_INDEX}/query"
    headers = await get_headers()

    payload = {
        "num_results": num_results,
        "query_vector": query_embedding,
        "columns": ["product_id"],
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(url, json=payload, headers=headers)
        if resp.status_code != 200:
            logger.warning(f"Vector search failed ({resp.status_code}): {resp.text}")
            return []

        data = resp.json()
        results = data.get("result", {}).get("data_array", [])
        columns = [c["name"] for c in data.get("manifest", {}).get("columns", [])]
        parsed = [dict(zip(columns, row)) for row in results]
        # Note: scores from this index are NOT cosine similarity (0-1).
        # Precision is handled by the attribute post-filter in the search endpoint.
        logger.info(f"Vector search: {len(parsed)} results returned")
        return parsed


# ---------------------------------------------------------------------------
# LLM helper
# ---------------------------------------------------------------------------
async def llm_recommend(query: str, products: list[dict]) -> str:
    host = get_host()
    url = f"{host}/serving-endpoints/{LLM_MODEL}/invocations"
    headers = await get_headers()

    product_list = "\n".join([
        f"- {p.get('name', 'Product')} ({p.get('category', '')}, {p.get('material', '')}, INR {int(float(p.get('price_inr', 0))):,})"
        for p in products[:5]
    ])

    system_prompt = """You are an expert GIVA jewelry consultant specializing in GIVA's diamond and gold jewelry.

FORMATTING RULES — follow these strictly:
- Write 3-4 sentences maximum. Be concise and elegant.
- Use **bold** only for product names and prices.
- Do NOT use markdown headers (##), blockquotes (>), bullet points, or emojis.
- Do NOT use section titles or headings.
- Write in flowing prose paragraphs, like a personal stylist speaking to a customer.
- End with one short styling tip if relevant."""

    user_message = f"""Customer query: "{query}"

Top matching pieces:
{product_list}

Recommend 2-3 best pieces from the list. Mention why they suit the customer, the metal, and a styling tip. Keep it short and elegant — no headers, no bullets, no emojis."""

    payload = {
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message},
        ],
        "max_tokens": 200,
        "temperature": 0.7,
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(url, json=payload, headers=headers)
        if resp.status_code != 200:
            return "Our curated selection matches your preferences beautifully. Each piece is crafted with GIVA's signature attention to purity and design."
        data = resp.json()
        return data["choices"][0]["message"]["content"]


async def llm_personalize_nudge(
    nudge_intent: dict,
    semaphore: "asyncio.Semaphore",
) -> dict:
    """Call the LLM to generate a personalized nudge message.

    On failure, returns the intent with the original template message intact.
    """
    import asyncio
    async with semaphore:
        host = get_host()
        url = f"{host}/serving-endpoints/{LLM_MODEL}/invocations"
        headers = await get_headers()

        payload = {
            "messages": [
                {"role": "system", "content": NUDGE_LLM_SYSTEM_PROMPT},
                {"role": "user", "content": _build_nudge_user_prompt(nudge_intent)},
            ],
            "max_tokens": 150,
            "temperature": 0.7,
        }

        try:
            async with httpx.AsyncClient(timeout=20.0) as client:
                resp = await client.post(url, json=payload, headers=headers)
                if resp.status_code == 200:
                    content = resp.json()["choices"][0]["message"]["content"].strip()
                    if content:
                        nudge_intent["message"] = content
                else:
                    logger.warning("LLM nudge call failed (HTTP %s) for user %s, using template",
                                   resp.status_code, nudge_intent["user_id"])
        except Exception as exc:
            logger.warning("LLM nudge call exception for user %s: %s, using template",
                           nudge_intent["user_id"], exc)

        return nudge_intent


# ---------------------------------------------------------------------------
# Request/Response Models
# ---------------------------------------------------------------------------
class SearchRequest(BaseModel):
    query: str
    category: Optional[str] = None
    occasion: Optional[str] = None
    material: Optional[str] = None
    min_price: Optional[int] = None
    max_price: Optional[int] = None
    sort: Optional[str] = None  # 'price_asc', 'price_desc'
    limit: int = 12


class ProductFilters(BaseModel):
    category: Optional[str] = None
    occasion: Optional[str] = None
    material: Optional[str] = None
    min_price: Optional[int] = None
    max_price: Optional[int] = None
    limit: int = 24
    offset: int = 0


class OrderItem(BaseModel):
    product_id: str
    name: str
    material: str
    price_inr: int
    quantity: int
    image_url: str


class CreateOrderRequest(BaseModel):
    customer_name: str
    customer_email: Optional[str] = None
    items: List[OrderItem]
    total_amount: int
    user_id: Optional[int] = None
    discount_code: Optional[str] = None
    discount_amount: Optional[int] = 0


# ---------------------------------------------------------------------------
# API Routes
# ---------------------------------------------------------------------------
@app.get("/api/metal-prices")
async def get_metal_prices():
    """Return the latest gold and silver prices from Lakebase."""
    def _query(token):
        conn = _pg_connect(token)
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT DISTINCT ON (metal)
                        metal, currency, price_gram_24k, price_gram_22k, price_gram_21k,
                        price_gram_20k, price_gram_18k, price_gram_16k, price_gram_14k,
                        price_gram_10k, pct_change_24k, pct_change_22k, pct_change_21k,
                        pct_change_20k, pct_change_18k, pct_change_16k, pct_change_14k,
                        pct_change_10k, price_per_oz, fetched_at
                    FROM brickjewels_metal_prices
                    WHERE is_active = TRUE
                    ORDER BY metal, fetched_at DESC
                """)
                cols = [d[0] for d in cur.description]
                rows = cur.fetchall()
                return [dict(zip(cols, r)) for r in rows]
        finally:
            conn.close()

    try:
        prices = await _run_db(_query)
        # Convert Decimal/datetime to JSON-safe types
        for p in prices:
            for k, v in p.items():
                if hasattr(v, "isoformat"):
                    p[k] = v.isoformat()
                elif hasattr(v, "as_integer_ratio"):  # Decimal
                    p[k] = float(v)
        return {"prices": prices}
    except Exception as exc:
        logger.error("Failed to fetch metal prices: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/product-price-breakdown/{product_id}")
async def get_product_price_breakdown(product_id: str):
    """Get full price breakdown for a product from brickjewels_product_prices."""
    safe_id = product_id.replace("'", "''")
    def _q(token):
        conn = _pg_connect(token)
        try:
            import psycopg2.extras
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT product_id, karat, metal_type, weight_grams, category,
                           metal_rate_per_gram, metal_cost, diamond_cost,
                           making_pct, making_cost, discount_pct, discount_value,
                           gst_pct_metal_diamond, gst_cost_metal_diamond,
                           gst_pct_making, gst_cost_making,
                           total_before_gst, total_gst, final_price, computed_at
                    FROM brickjewels_product_prices
                    WHERE product_id = %s AND is_active = TRUE
                    ORDER BY computed_at DESC LIMIT 1
                """, (product_id,))
                return cur.fetchone()
        finally:
            conn.close()

    row = await _run_db(_q)
    if not row:
        raise HTTPException(status_code=404, detail="Price breakdown not found")
    result = dict(row)
    for k, v in result.items():
        if hasattr(v, "isoformat"):
            result[k] = v.isoformat()
        elif hasattr(v, "as_integer_ratio"):
            result[k] = float(v)
    return result


@app.post("/api/refresh-all-prices")
async def refresh_all_prices():
    """Full chain: fetch metal prices from GoldAPI → store (SCD2) → recompute product prices → update cache.
    Called by the Databricks Workflow and can be triggered manually."""
    try:
        await _fetch_and_store_metal_prices()
        return {
            "status": "ok",
            "cached_products": len(_product_price_cache),
            "message": "Metal prices fetched, product prices recomputed, cache updated",
        }
    except Exception as exc:
        logger.error("Full price refresh failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/api/recompute-prices")
async def recompute_prices():
    """Manually trigger product price recomputation with diagnostics."""
    diag = {}
    try:
        # Check metal prices
        def _check_metals(token):
            conn = _pg_connect(token)
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT metal, price_gram_18k FROM brickjewels_metal_prices WHERE is_active = TRUE")
                    return cur.fetchall()
            finally:
                conn.close()
        metal_rows = await _run_db(_check_metals)
        diag["metal_prices_active"] = [{"metal": r[0], "price_18k": float(r[1]) if r[1] else None} for r in metal_rows]

        # Check UC query using run_sql
        try:
            wh_id = await get_warehouse_id()
            diag["warehouse_id"] = wh_id
            sql = f"SELECT product_id, material, weight_grams, category FROM {PRODUCTS_TABLE} WHERE in_stock = true LIMIT 3"
            uc_rows = await run_sql(sql)
            diag["uc_sample_rows"] = len(uc_rows)
            diag["uc_sample"] = uc_rows[:2] if uc_rows else []
        except Exception as uc_exc:
            diag["uc_error"] = str(uc_exc)

        await _compute_product_prices()
        diag["cached_products"] = len(_product_price_cache)
        return {"status": "ok", **diag}
    except Exception as exc:
        diag["error"] = str(exc)
        return {"status": "error", **diag}


@app.get("/api/price-cache-status")
async def price_cache_status():
    """Check in-memory product price cache status."""
    sample = dict(list(_product_price_cache.items())[:5])
    # Also check if breakdown query works
    breakdown_test = None
    if _product_price_cache:
        test_pid = list(_product_price_cache.keys())[0]
        try:
            def _test_bd(token):
                conn = _pg_connect(token)
                try:
                    import psycopg2.extras
                    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                        cur.execute("""SELECT product_id, karat, metal_cost, final_price
                                       FROM brickjewels_product_prices
                                       WHERE is_active = TRUE LIMIT 3""")
                        return [dict(r) for r in cur.fetchall()]
                finally:
                    conn.close()
            bd_rows = await _run_db(_test_bd)
            breakdown_test = {"rows_found": len(bd_rows), "sample": bd_rows}
            for r in (bd_rows or []):
                for k, v in r.items():
                    if hasattr(v, "as_integer_ratio"):
                        r[k] = float(v)
        except Exception as e:
            breakdown_test = {"error": str(e)}
    return {"cached_count": len(_product_price_cache), "sample": sample, "breakdown_db_test": breakdown_test}


@app.get("/health")
async def health():
    return {"status": "ok", "service": "giva-api"}


@app.get("/api/debug")
async def debug_connectivity():
    """Debug endpoint to test embedding and VS connectivity from app context."""
    host = get_host()
    token = await _get_oauth_token()
    results = {"host": host, "token_length": len(token)}

    # Test embedding
    try:
        emb = await get_embedding("gold necklace")
        results["embedding"] = {"status": "ok", "dim": len(emb), "nonzero": sum(1 for v in emb[:10] if v != 0)}
    except Exception as e:
        results["embedding"] = {"status": "error", "error": str(e)}

    # Test VS query - direct raw call
    try:
        emb2 = await get_embedding("gold necklace")
        vs_url = f"{host}/api/2.0/vector-search/indexes/{VS_INDEX}/query"
        vs_headers = await get_headers()
        vs_payload = {"num_results": 3, "query_vector": emb2, "columns": ["product_id"]}
        async with httpx.AsyncClient(timeout=30.0) as client:
            vs_resp = await client.post(vs_url, json=vs_payload, headers=vs_headers)
            vs_status = vs_resp.status_code
            vs_raw = vs_resp.json()
            data_array = vs_raw.get("result", {}).get("data_array", [])
            manifest_cols = vs_raw.get("manifest", {}).get("columns", [])
            results["vector_search"] = {
                "status": "ok" if vs_status == 200 else "error",
                "http_status": vs_status,
                "data_array_len": len(data_array),
                "manifest_columns": manifest_cols,
                "sample_row": data_array[0] if data_array else None,
                "error_msg": vs_raw.get("message", "") if vs_status != 200 else "",
            }
    except Exception as e:
        results["vector_search"] = {"status": "error", "error": str(e)}

    return results


@app.get("/api/images/{category}/{filename}")
async def serve_image(category: str, filename: str):
    """Proxy images from UC Volume to the frontend (with category subdirectory)."""
    import re
    valid_categories = ("ring", "earring", "necklace", "bracelet", "bangle", "pendant")
    if category.lower() not in valid_categories:
        raise HTTPException(status_code=400, detail="Invalid category")
    if not re.match(r'^[a-zA-Z0-9_\-]+\.(jpg|jpeg|png|webp)$', filename):
        raise HTTPException(status_code=400, detail="Invalid filename")

    host = get_host()
    if not host:
        raise HTTPException(status_code=500, detail="DATABRICKS_HOST not set")

    volume_path = f"{IMAGE_VOLUME_PATH}/{category.lower()}/{filename}"
    files_api_url = f"{host}/api/2.0/fs/files{volume_path}"

    token = await _get_oauth_token()
    headers = {"Authorization": f"Bearer {token}"}

    client = _get_http_files()
    resp = await client.get(files_api_url, headers=headers)
    if resp.status_code == 404:
        raise HTTPException(status_code=404, detail=f"Image not found: {filename}")
    if resp.status_code != 200:
        logger.error(f"Volume file fetch failed ({resp.status_code}): {resp.text[:200]}")
        raise HTTPException(status_code=502, detail="Failed to fetch image from volume")

    content_type = "image/jpeg"
    if filename.endswith(".png"):
        content_type = "image/png"
    elif filename.endswith(".webp"):
        content_type = "image/webp"

    return StreamingResponse(
        iter([resp.content]),
        media_type=content_type,
        headers={
            "Cache-Control": "public, max-age=86400, immutable",
            "Content-Length": str(len(resp.content)),
        },
    )


@app.get("/api/products")
async def get_products(
    category: Optional[str] = None,
    occasion: Optional[str] = None,
    material: Optional[str] = None,
    min_price: Optional[int] = None,
    max_price: Optional[int] = None,
    sort: Optional[str] = None,
    limit: int = 24,
    offset: int = 0,
):
    """Get product catalog with optional filters — served from in-memory cache."""
    if not _catalog_cache:
        await _load_catalog_cache()

    items = _catalog_cache
    if category:
        c_lower = category.lower()
        items = [p for p in items if (p.get("category") or "").lower() == c_lower]
    if occasion:
        items = [p for p in items if p.get("occasion") == occasion]
    if material:
        items = [p for p in items if material in (p.get("material") or "")]
    if min_price:
        items = [p for p in items if float(p.get("price_inr") or 0) >= min_price]
    if max_price:
        items = [p for p in items if float(p.get("price_inr") or 0) <= max_price]

    reverse = sort != "price_asc"
    items = sorted(items, key=lambda p: float(p.get("price_inr") or 0), reverse=reverse)

    total = len(items)
    page = _copy_products(items[offset:offset + limit])
    _apply_live_prices(page)
    _strip_internal(page)
    return {"products": page, "total": total, "offset": offset, "limit": limit}


def _extract_price_from_query(query: str, min_price: Optional[int], max_price: Optional[int]):
    """Parse price constraints from natural language queries."""
    import re
    q = query.lower().replace(",", "").replace("₹", "").replace("rs", "").replace("inr", "")
    # "under 50k", "below 50000", "less than 50k"
    m = re.search(r'(?:under|below|less than|within|upto|up to|max)\s*(\d+)\s*k?\b', q)
    if m and not max_price:
        val = int(m.group(1))
        max_price = val * 1000 if val < 1000 else val
    # "above 50k", "over 50000", "more than 50k", "minimum 50k"
    m = re.search(r'(?:above|over|more than|min|minimum|starting|from)\s*(\d+)\s*k?\b', q)
    if m and not min_price:
        val = int(m.group(1))
        min_price = val * 1000 if val < 1000 else val
    # "between 20k and 50k"
    m = re.search(r'between\s*(\d+)\s*k?\s*(?:and|to|-)\s*(\d+)\s*k?', q)
    if m and not min_price and not max_price:
        v1, v2 = int(m.group(1)), int(m.group(2))
        min_price = v1 * 1000 if v1 < 1000 else v1
        max_price = v2 * 1000 if v2 < 1000 else v2
    # "50k to 100k"
    m = re.search(r'(\d+)\s*k?\s*(?:to|-)\s*(\d+)\s*k?\b', q)
    if m and not min_price and not max_price:
        v1, v2 = int(m.group(1)), int(m.group(2))
        min_price = v1 * 1000 if v1 < 1000 else v1
        max_price = v2 * 1000 if v2 < 1000 else v2
    return min_price, max_price


@app.post("/api/search")
async def semantic_search(request: SearchRequest):
    """Semantic text search using vector similarity + strict attribute filtering."""
    import json as _json

    query = request.query.strip()
    if not query:
        raise HTTPException(status_code=400, detail="Query cannot be empty")

    # Extract price constraints from natural language
    request.min_price, request.max_price = _extract_price_from_query(
        query, request.min_price, request.max_price
    )
    if request.min_price or request.max_price:
        logger.info(f"Extracted price filter: min={request.min_price}, max={request.max_price}")

    # -----------------------------------------------------------------
    # Step 1: Extract query intent — product type + visual attributes
    # -----------------------------------------------------------------
    query_lower = query.lower()

    _product_keywords = [
        "mangalsutra", "solitaire", "choker", "tennis bracelet", "jhumka",
        "hoop", "stud", "chain", "bangle", "pendant", "bracelet",
        "necklace", "earring", "ring",
    ]
    _color_keywords = [
        "red", "blue", "green", "pink", "yellow", "white", "black",
        "purple", "orange", "multicolor", "colorless", "rose",
    ]
    _stone_keywords = [
        "ruby", "emerald", "sapphire", "pearl", "diamond", "gemstone",
        "stone", "crystal", "opal", "topaz", "garnet", "amethyst",
    ]
    _style_keywords = [
        "vintage", "modern", "classic", "minimalist", "floral",
        "geometric", "ornate", "art deco", "bohemian", "traditional",
    ]

    matched_types = [kw for kw in _product_keywords if kw in query_lower]
    matched_colors = [kw for kw in _color_keywords if kw in query_lower]
    matched_stones = [kw for kw in _stone_keywords if kw in query_lower]
    matched_styles = [kw for kw in _style_keywords if kw in query_lower]
    has_attribute_filter = matched_types or matched_colors or matched_stones or matched_styles

    logger.info(
        f"Query intent: types={matched_types}, colors={matched_colors}, "
        f"stones={matched_stones}, styles={matched_styles}"
    )

    # -----------------------------------------------------------------
    # Helper: check if a product matches ALL extracted attribute groups
    # Uses word-boundary matching so "ring" doesn't match "earring".
    # -----------------------------------------------------------------
    import re as _re_wb

    def _word_match(keyword: str, text: str) -> bool:
        """Check if keyword appears as a whole word in text."""
        return bool(_re_wb.search(r'\b' + _re_wb.escape(keyword) + r'\b', text))

    def product_matches_query(p: dict) -> bool:
        searchable = (
            f"{p.get('name', '')} {p.get('tags', '')} {p.get('subcategory', '')} "
            f"{p.get('category', '')} {p.get('material', '')} {p.get('description', '')} "
            f"{p.get('llm_description', '') or ''}"
        ).lower()

        llm_attrs = {}
        try:
            raw = p.get("llm_attributes") or ""
            if raw:
                llm_attrs = _json.loads(raw)
        except Exception:
            pass
        llm_text = " ".join(str(v) for v in llm_attrs.values()).lower()
        combined = f"{searchable} {llm_text}"

        if matched_types and not any(_word_match(kw, combined) for kw in matched_types):
            return False
        if matched_colors and not any(_word_match(kw, combined) for kw in matched_colors):
            return False
        if matched_stones and not any(_word_match(kw, combined) for kw in matched_stones):
            return False
        if matched_styles and not any(_word_match(kw, combined) for kw in matched_styles):
            return False
        return True

    # -----------------------------------------------------------------
    # Helper: strip internal columns before returning to frontend
    # -----------------------------------------------------------------
    def clean_products(prods: list[dict]) -> list[dict]:
        for p in prods:
            p.pop("llm_attributes", None)
            p.pop("llm_description", None)
        return prods

    # -----------------------------------------------------------------
    # Step 2: Vector search → fetch products → strict attribute filter
    # -----------------------------------------------------------------
    embedding = await get_embedding(query)

    product_ids = []
    if embedding:
        vs_results = await vector_search(embedding, num_results=30)
        product_ids = [r["product_id"] for r in vs_results if r.get("product_id")]
        logger.info(f"Vector search returned {len(product_ids)} product IDs for query: '{query}'")

    products = []
    if product_ids:
        ids_str = ", ".join([f"'{pid}'" for pid in product_ids])
        order_cases = " ".join([f"WHEN product_id = '{pid}' THEN {i}" for i, pid in enumerate(product_ids)])

        conditions = [f"product_id IN ({ids_str})", "in_stock = true"]
        if request.category:
            conditions.append(f"LOWER(category) = LOWER('{request.category}')")
        if request.occasion:
            conditions.append(f"occasion = '{request.occasion}'")
        if request.min_price:
            conditions.append(f"price_inr >= {request.min_price}")
        if request.max_price:
            conditions.append(f"price_inr <= {request.max_price}")

        if request.sort == "price_asc":
            order_clause = "ORDER BY price_inr ASC"
        elif request.sort == "price_desc":
            order_clause = "ORDER BY price_inr DESC"
        else:
            order_clause = f"ORDER BY CASE {order_cases} ELSE 999 END"

        query_sql = f"""
            SELECT product_id, name, description, category, subcategory, material,
                   occasion, style, collection, weight_grams, price_inr, image_url, tags,
                   llm_attributes, llm_description
            FROM {PRODUCTS_TABLE}
            WHERE {' AND '.join(conditions)}
            {order_clause}
            LIMIT {request.limit}
        """
        products = await run_sql(query_sql)
        logger.info(f"SQL returned {len(products)} products after DB filtering")

        # Apply strict attribute filter
        if has_attribute_filter:
            products = [p for p in products if product_matches_query(p)]
            logger.info(f"After attribute filter: {len(products)} products remain")

    # -----------------------------------------------------------------
    # Step 3: Fallback — broad SQL fetch + strict attribute filter
    # Fetches a wider candidate set, then applies the same attribute
    # filter that ensures only truly matching products are returned.
    # -----------------------------------------------------------------
    if not products:
        import re as _re
        _stop = {"the", "a", "an", "with", "and", "or", "for", "in", "on", "of", "to", "that", "this", "is", "its"}
        words = [w for w in _re.split(r'\s+', query.strip()) if len(w) > 2 and w.lower() not in _stop]
        if not words:
            words = [query.replace("'", "''")]

        # Use OR to get a broad candidate set — precision comes from attribute filter
        word_clauses = []
        for word in words[:5]:
            safe_word = word.replace("'", "''")
            word_clauses.append(
                f"(LOWER(name) LIKE LOWER('%{safe_word}%') "
                f"OR LOWER(tags) LIKE LOWER('%{safe_word}%') "
                f"OR LOWER(category) LIKE LOWER('%{safe_word}%') "
                f"OR LOWER(material) LIKE LOWER('%{safe_word}%') "
                f"OR LOWER(llm_description) LIKE LOWER('%{safe_word}%'))"
            )

        kw_conditions = ["in_stock = true", f"({' OR '.join(word_clauses)})"]
        if request.category:
            kw_conditions.append(f"LOWER(category) = LOWER('{request.category}')")
        if request.min_price:
            kw_conditions.append(f"price_inr >= {request.min_price}")
        if request.max_price:
            kw_conditions.append(f"price_inr <= {request.max_price}")

        fallback_sql = f"""
            SELECT product_id, name, description, category, subcategory, material,
                   occasion, style, collection, weight_grams, price_inr, image_url, tags,
                   llm_attributes, llm_description
            FROM {PRODUCTS_TABLE}
            WHERE {' AND '.join(kw_conditions)}
            ORDER BY price_inr DESC
            LIMIT 100
        """
        products = await run_sql(fallback_sql)
        logger.info(f"Keyword fallback returned {len(products)} candidates")

        # Apply strict attribute filter — this is where precision happens
        if has_attribute_filter:
            products = [p for p in products if product_matches_query(p)]
            logger.info(f"After attribute filter on fallback: {len(products)} products remain")

    # -----------------------------------------------------------------
    # Step 4: Clean up and return
    # -----------------------------------------------------------------
    products = clean_products(products)

    ai_recommendation = ""
    if products:
        ai_recommendation = await llm_recommend(query, products)
    else:
        ai_recommendation = (
            f"We couldn't find any products matching \"{query}\" in our current collection. "
            "Try broadening your search or browsing our categories to discover something you'll love."
        )

    _apply_live_prices(products)
    return {
        "query": query,
        "products": products[:request.limit],
        "total": len(products),
        "ai_recommendation": ai_recommendation,
        "search_type": "semantic" if embedding and product_ids else "keyword",
    }


@app.post("/api/image-search")
async def image_search(file: UploadFile = File(...)):
    """Image-based product search using LLM vision + semantic search."""
    image_data = await file.read()
    image_b64 = base64.b64encode(image_data).decode("utf-8")
    content_type = file.content_type or "image/jpeg"

    host = get_host()
    url = f"{host}/serving-endpoints/{LLM_MODEL}/invocations"
    headers = await get_headers()

    payload = {
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "image_url",
                        "image_url": {"url": f"data:{content_type};base64,{image_b64}"}
                    },
                    {
                        "type": "text",
                        "text": (
                            "Describe this jewelry piece in exactly 2-3 plain sentences. "
                            "Include: type (ring/earring/necklace/bracelet/bangle/pendant), "
                            "metal color and type, stone type if any, design style, and occasion. "
                            "Do NOT use markdown, headers, tables, bullet points, or emojis. "
                            "Write only flowing plain text, like a catalog description."
                        )
                    }
                ],
            }
        ],
        "max_tokens": 120,
    }

    description = "jewelry piece"
    async with httpx.AsyncClient(timeout=45.0) as client:
        resp = await client.post(url, json=payload, headers=headers)
        if resp.status_code == 200:
            description = resp.json()["choices"][0]["message"]["content"]

    search_req = SearchRequest(query=description, limit=12)
    results = await semantic_search(search_req)
    results["image_description"] = description
    results["search_type"] = "image"
    return results


@app.get("/api/product/{product_id}")
async def get_product(product_id: str):
    """Get a single product with related items — served from in-memory cache."""
    if not _catalog_cache:
        await _load_catalog_cache()

    base = _catalog_by_id.get(product_id)
    if not base:
        raise HTTPException(status_code=404, detail="Product not found")

    product = dict(base)
    _apply_live_prices([product])
    _strip_internal([product])

    category = (base.get("category") or "").lower()
    same_cat = [
        p for p in _catalog_cache
        if p.get("product_id") != product_id and (p.get("category") or "").lower() == category
    ]
    import random as _r
    related_src = _r.sample(same_cat, k=min(4, len(same_cat))) if same_cat else []
    related = _copy_products(related_src)
    _apply_live_prices(related)
    _strip_internal(related)

    return {"product": product, "related": related}


@app.get("/api/categories")
async def get_categories():
    """Get available categories with counts — served from in-memory cache."""
    if not _categories_cache:
        await _load_catalog_cache()
    return {"categories": _categories_cache}


@app.get("/api/featured")
async def get_featured():
    """Get featured products for homepage — served from in-memory cache."""
    if not _featured_cache:
        await _load_catalog_cache()
    products = _copy_products(_featured_cache)
    _apply_live_prices(products)
    _strip_internal(products)
    return {"products": products}


@app.post("/api/catalog/refresh")
async def refresh_catalog_cache():
    """Force-refresh the in-memory catalog cache (call after catalog table updates)."""
    await _load_catalog_cache()
    return {
        "products": len(_catalog_cache),
        "categories": len(_categories_cache),
        "featured": len(_featured_cache),
    }


@app.post("/api/recommend")
async def get_recommendations(request: SearchRequest):
    """Get LLM-powered personalized recommendations — pulls 6 random items from cache."""
    if not _catalog_cache:
        await _load_catalog_cache()

    items = _catalog_cache
    if request.occasion:
        items = [p for p in items if p.get("occasion") == request.occasion]
    if request.category:
        c_lower = request.category.lower()
        items = [p for p in items if (p.get("category") or "").lower() == c_lower]

    import random as _r
    sampled = _r.sample(items, k=min(6, len(items))) if items else []
    products = _copy_products(sampled)
    _apply_live_prices(products)
    _strip_internal(products)
    recommendation = await llm_recommend(request.query, products)

    return {"products": products, "ai_recommendation": recommendation, "query": request.query}


# ---------------------------------------------------------------------------
# Order endpoints (Lakebase / PostgreSQL)
# ---------------------------------------------------------------------------
@app.get("/api/db-status")
async def db_status():
    """Diagnostic endpoint: checks Lakebase connectivity and table state."""
    result = {
        "env": {
            "DATABRICKS_HOST": bool(os.environ.get("DATABRICKS_HOST")),
            "DATABRICKS_CLIENT_ID": bool(os.environ.get("DATABRICKS_CLIENT_ID")),
            "DATABRICKS_CLIENT_SECRET": bool(os.environ.get("DATABRICKS_CLIENT_SECRET")),
            "LAKEBASE_HOST": LAKEBASE_HOST,
        },
        "token": None,
        "connection": None,
        "table_exists": None,
        "error": None,
    }
    try:
        token = await _get_oauth_token()
        result["token"] = f"ok ({len(token)} chars)" if token else "empty"

        def _check(t):
            import psycopg2.extras
            conn = _pg_connect(t)
            try:
                conn.autocommit = True
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute("SELECT current_user, session_user")
                    row = dict(cur.fetchone())
                    cur.execute(
                        "SELECT COUNT(*) AS cnt FROM pg_tables WHERE tablename='brickjewels_orders'"
                    )
                    tbl = cur.fetchone()["cnt"]
                    return {"pg_user": row, "table_exists": tbl > 0}
            finally:
                conn.close()

        info = await _run_db(_check)
        result["connection"] = "ok"
        result["pg_user"] = info["pg_user"]
        result["table_exists"] = info["table_exists"]

        if not info["table_exists"]:
            def _create_inline(t):
                conn2 = _pg_connect(t)
                try:
                    _ensure_table_sync(conn2)
                    return "ok"
                except Exception as e:
                    return f"failed: {e}"
                finally:
                    conn2.close()
            create_result = await _run_db(_create_inline)
            result["table_created"] = create_result
    except Exception as exc:
        result["error"] = str(exc)

    return result


@app.post("/api/orders")
async def create_order(request: CreateOrderRequest):
    """Create a new order and persist it to Lakebase."""
    if not request.customer_name.strip():
        raise HTTPException(status_code=400, detail="Customer name is required")
    if not request.items:
        raise HTTPException(status_code=400, detail="Order must have at least one item")

    order_id = _generate_order_id()
    items_json = json.dumps([item.model_dump() for item in request.items])
    customer_name = request.customer_name.strip()
    customer_email = (request.customer_email or "").strip() or None
    total_amount = request.total_amount

    discount_code = (request.discount_code or "").strip() or None

    def _insert(token):
        conn = _pg_connect(token)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO brickjewels_orders
                        (order_id, customer_name, customer_email, items, total_amount, status, created_at, user_id)
                    VALUES (%s, %s, %s, %s::jsonb, %s, 'confirmed', NOW(), %s)
                    """,
                    (order_id, customer_name, customer_email, items_json, total_amount, request.user_id),
                )

                # Mark nudge as redeemed if a discount code was used
                if discount_code and request.user_id:
                    cur.execute("""UPDATE brickjewels_nudges
                                   SET is_redeemed = TRUE, redeemed_at = NOW()
                                   WHERE discount_code = %s AND user_id = %s
                                     AND is_active = TRUE AND is_redeemed = FALSE""",
                                (discount_code, request.user_id))

            conn.commit()
        finally:
            conn.close()

    try:
        await _run_db(_insert)
    except Exception as exc:
        logger.error("Failed to save order %s: %s", order_id, exc)
        raise HTTPException(status_code=500, detail=f"Failed to save order: {exc}")

    logger.info("Order created: %s for %s (%d items, discount_code=%s)",
                order_id, customer_name, len(request.items), discount_code)
    return {
        "order_id": order_id,
        "customer_name": customer_name,
        "total_amount": total_amount,
        "status": "confirmed",
        "message": "Thank you for shopping at GIVA!",
    }


@app.get("/api/admin/orders")
async def list_orders():
    """List all orders — for admin/validation use."""
    def _fetch_all(token):
        import psycopg2.extras
        conn = _pg_connect(token)
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT order_id, customer_name, customer_email,
                           total_amount, status, created_at,
                           jsonb_array_length(items) AS item_count
                    FROM brickjewels_orders
                    ORDER BY created_at DESC
                """)
                rows = cur.fetchall()
                return [
                    {
                        "order_id": r["order_id"],
                        "customer_name": r["customer_name"],
                        "customer_email": r["customer_email"],
                        "total_amount": int(r["total_amount"]),
                        "item_count": r["item_count"],
                        "status": r["status"],
                        "created_at": r["created_at"].strftime("%d %b %Y, %I:%M %p UTC") if r["created_at"] else "",
                    }
                    for r in rows
                ]
        finally:
            conn.close()

    try:
        orders = await _run_db(_fetch_all)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    return {"total": len(orders), "orders": orders}


@app.get("/api/orders/{order_id}")
async def get_order(order_id: str):
    """Look up an order by its ID."""
    safe_id = order_id.strip().upper()

    def _fetch(token):
        import psycopg2.extras
        conn = _pg_connect(token)
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT order_id, customer_name, customer_email,
                           items, total_amount, status, created_at
                    FROM brickjewels_orders
                    WHERE UPPER(order_id) = %s
                    """,
                    (safe_id,),
                )
                return cur.fetchone()
        finally:
            conn.close()

    try:
        row = await _run_db(_fetch)
    except Exception as exc:
        logger.error("Failed to fetch order %s: %s", order_id, exc)
        raise HTTPException(status_code=500, detail=f"Database error: {exc}")

    if not row:
        raise HTTPException(status_code=404, detail=f"Order '{order_id}' not found")

    items = row["items"] if isinstance(row["items"], list) else json.loads(row["items"])
    created_at = row["created_at"]
    created_str = created_at.strftime("%d %b %Y, %I:%M %p UTC") if created_at else ""

    return {
        "order_id": row["order_id"],
        "customer_name": row["customer_name"],
        "customer_email": row["customer_email"],
        "items": items,
        "total_amount": int(row["total_amount"]),
        "status": row["status"],
        "created_at": created_str,
    }


# ---------------------------------------------------------------------------
# Admin / Analyst endpoints
# ---------------------------------------------------------------------------

_ADMIN_CONFIG_PATH = Path(__file__).parent.parent / "admin_config.json"


def _load_admin_config():
    if _ADMIN_CONFIG_PATH.exists():
        with open(_ADMIN_CONFIG_PATH) as f:
            return json.load(f)
    return {"analysts": []}


class AdminLoginRequest(BaseModel):
    email: str
    password: str


@app.post("/api/admin/login")
async def admin_login(req: AdminLoginRequest):
    """Authenticate analyst from admin_config.json — no signup."""
    config = _load_admin_config()
    email = req.email.strip().lower()
    for analyst in config.get("analysts", []):
        if analyst["email"].lower() == email and analyst["password"] == req.password:
            return {"email": email, "name": analyst["name"], "role": analyst.get("role", "analyst")}
    raise HTTPException(status_code=401, detail="Invalid analyst credentials")


@app.get("/api/admin/users")
async def admin_list_users():
    """List all registered users."""
    def _fetch(token):
        import psycopg2.extras
        conn = _pg_connect(token)
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT user_id, first_name, last_name, email, country_code, mobile, created_at FROM brickjewels_users ORDER BY user_id")
                return cur.fetchall()
        finally:
            conn.close()

    try:
        rows = await _run_db(_fetch)
    except Exception as exc:
        logger.error("admin_list_users error: %s", exc)
        return {"users": [], "error": str(exc)}
    users = []
    for r in (rows or []):
        users.append({**dict(r), "created_at": r["created_at"].strftime("%d %b %Y, %I:%M %p") if r["created_at"] else ""})
    return {"users": users}


@app.get("/api/admin/all-orders")
async def admin_all_orders():
    """List all orders with user info."""
    def _fetch(token):
        import psycopg2.extras
        conn = _pg_connect(token)
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT o.order_id, o.customer_name, o.customer_email, o.items, o.total_amount, o.status, o.created_at, o.user_id
                    FROM brickjewels_orders o ORDER BY o.created_at DESC LIMIT 200
                """)
                return cur.fetchall()
        finally:
            conn.close()

    try:
        rows = await _run_db(_fetch)
    except Exception as exc:
        logger.error("admin_all_orders error: %s", exc)
        return {"orders": [], "error": str(exc)}
    orders = []
    for r in (rows or []):
        items = r["items"] if isinstance(r["items"], list) else json.loads(r["items"])
        orders.append({
            "order_id": r["order_id"], "customer_name": r["customer_name"],
            "customer_email": r["customer_email"], "items": items,
            "total_amount": int(r["total_amount"]), "status": r["status"],
            "user_id": r["user_id"],
            "created_at": r["created_at"].strftime("%d %b %Y, %I:%M %p") if r["created_at"] else "",
            "item_count": len(items),
        })
    return {"orders": orders}


@app.get("/api/admin/all-carts")
async def admin_all_carts():
    """List all user carts — only active SCD2 records with non-empty carts."""
    def _fetch(token):
        import psycopg2.extras
        conn = _pg_connect(token)
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT d.user_id, u.first_name, u.last_name, u.email, d.cart, d.updated_at
                    FROM brickjewels_user_data_scd2 d
                    JOIN brickjewels_users u ON d.user_id = u.user_id
                    WHERE d.is_active = TRUE AND d.cart != '[]'::jsonb
                    ORDER BY d.updated_at DESC
                """)
                return cur.fetchall()
        finally:
            conn.close()

    try:
        rows = await _run_db(_fetch)
    except Exception as exc:
        logger.error("admin_all_carts error: %s", exc)
        return {"carts": [], "error": str(exc)}
    carts = []
    for r in (rows or []):
        cart = r["cart"] if isinstance(r["cart"], list) else json.loads(r["cart"])
        total = sum(int(item.get("product", {}).get("price_inr", 0) or 0) * int(item.get("quantity", 1) or 1) for item in cart)
        carts.append({
            "user_id": r["user_id"], "name": f"{r['first_name']} {r['last_name']}",
            "email": r["email"], "items": cart,
            "item_count": sum(int(item.get("quantity", 1) or 1) for item in cart),
            "cart_value": total,
            "updated_at": r["updated_at"].strftime("%d %b %Y, %I:%M %p") if r["updated_at"] else "",
        })
    return {"carts": carts}


@app.get("/api/admin/all-wishlists")
async def admin_all_wishlists():
    """List all user wishlists — only active SCD2 records with non-empty wishlists."""
    def _fetch(token):
        import psycopg2.extras
        conn = _pg_connect(token)
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT d.user_id, u.first_name, u.last_name, u.email, d.wishlist, d.updated_at
                    FROM brickjewels_user_data_scd2 d
                    JOIN brickjewels_users u ON d.user_id = u.user_id
                    WHERE d.is_active = TRUE AND d.wishlist != '[]'::jsonb
                    ORDER BY d.updated_at DESC
                """)
                return cur.fetchall()
        finally:
            conn.close()

    try:
        rows = await _run_db(_fetch)
    except Exception as exc:
        logger.error("admin_all_wishlists error: %s", exc)
        return {"wishlists": [], "error": str(exc)}
    wishlists = []
    for r in (rows or []):
        wl = r["wishlist"] if isinstance(r["wishlist"], list) else json.loads(r["wishlist"])
        wishlists.append({
            "user_id": r["user_id"], "name": f"{r['first_name']} {r['last_name']}",
            "email": r["email"], "items": wl, "item_count": len(wl),
            "updated_at": r["updated_at"].strftime("%d %b %Y, %I:%M %p") if r["updated_at"] else "",
        })
    return {"wishlists": wishlists}


@app.get("/api/admin/all-chat-sessions")
async def admin_all_chat_sessions():
    """List all chat sessions with user info."""
    def _fetch(token):
        import psycopg2.extras
        conn = _pg_connect(token)
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT s.session_id, s.user_id, u.first_name, u.last_name, u.email,
                           s.title, s.messages, s.created_at, s.updated_at
                    FROM brickjewels_chat_sessions s
                    JOIN brickjewels_users u ON s.user_id = u.user_id
                    ORDER BY s.updated_at DESC LIMIT 200
                """)
                return cur.fetchall()
        finally:
            conn.close()

    try:
        rows = await _run_db(_fetch)
    except Exception as exc:
        logger.error("admin_all_chat_sessions error: %s", exc)
        return {"sessions": [], "error": str(exc)}
    sessions = []
    for r in (rows or []):
        msgs = r["messages"] if isinstance(r["messages"], list) else json.loads(r["messages"])
        sessions.append({
            "session_id": r["session_id"], "user_id": r["user_id"],
            "name": f"{r['first_name']} {r['last_name']}", "email": r["email"],
            "title": r["title"], "message_count": len(msgs),
            "messages": msgs,
            "created_at": r["created_at"].strftime("%d %b %Y, %I:%M %p") if r["created_at"] else "",
            "updated_at": r["updated_at"].strftime("%d %b %Y, %I:%M %p") if r["updated_at"] else "",
        })
    return {"sessions": sessions}


# Admin delete endpoints

@app.delete("/api/admin/users/{user_id}")
async def admin_delete_user(user_id: int):
    """Delete a user and all their related data."""
    def _delete(token):
        conn = _pg_connect(token)
        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM brickjewels_chat_sessions WHERE user_id = %s", (user_id,))
                cur.execute("DELETE FROM brickjewels_user_data_scd2 WHERE user_id = %s", (user_id,))
                cur.execute("UPDATE brickjewels_orders SET user_id = NULL WHERE user_id = %s", (user_id,))
                cur.execute("DELETE FROM brickjewels_users WHERE user_id = %s RETURNING user_id", (user_id,))
                deleted = cur.fetchone()
            conn.commit()
            return deleted is not None
        finally:
            conn.close()

    try:
        deleted = await _run_db(_delete)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    if not deleted:
        raise HTTPException(status_code=404, detail="User not found")
    return {"status": "deleted", "user_id": user_id}


@app.delete("/api/admin/orders/{order_id}")
async def admin_delete_order(order_id: str):
    """Delete an order."""
    def _delete(token):
        conn = _pg_connect(token)
        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM brickjewels_orders WHERE order_id = %s RETURNING order_id", (order_id,))
                deleted = cur.fetchone()
            conn.commit()
            return deleted is not None
        finally:
            conn.close()

    deleted = await _run_db(_delete)
    if not deleted:
        raise HTTPException(status_code=404, detail="Order not found")
    return {"status": "deleted", "order_id": order_id}


@app.delete("/api/admin/carts/{user_id}")
async def admin_clear_cart(user_id: int):
    """Clear a user's cart using SCD2 — expire active record, insert new with empty cart."""
    def _clear(token):
        conn = _pg_connect(token)
        try:
            with conn.cursor() as cur:
                # Get current wishlist to preserve it
                cur.execute("SELECT wishlist FROM brickjewels_user_data_scd2 WHERE user_id = %s AND is_active = TRUE", (user_id,))
                row = cur.fetchone()
                current_wishlist = row[0] if row else '[]'
                # Expire current active record
                cur.execute("UPDATE brickjewels_user_data_scd2 SET is_active = FALSE, effective_to = NOW() WHERE user_id = %s AND is_active = TRUE", (user_id,))
                # Insert new record with empty cart but same wishlist
                cur.execute("""
                    INSERT INTO brickjewels_user_data_scd2 (user_id, cart, wishlist, is_active, effective_from, updated_at)
                    VALUES (%s, '[]'::jsonb, %s::jsonb, TRUE, NOW(), NOW())
                """, (user_id, json.dumps(current_wishlist) if isinstance(current_wishlist, (list, dict)) else current_wishlist))
            conn.commit()
        finally:
            conn.close()

    await _run_db(_clear)
    return {"status": "cleared", "user_id": user_id}


@app.delete("/api/admin/wishlists/{user_id}")
async def admin_clear_wishlist(user_id: int):
    """Clear a user's wishlist using SCD2 — expire active record, insert new with empty wishlist."""
    def _clear(token):
        conn = _pg_connect(token)
        try:
            with conn.cursor() as cur:
                # Get current cart to preserve it
                cur.execute("SELECT cart FROM brickjewels_user_data_scd2 WHERE user_id = %s AND is_active = TRUE", (user_id,))
                row = cur.fetchone()
                current_cart = row[0] if row else '[]'
                # Expire current active record
                cur.execute("UPDATE brickjewels_user_data_scd2 SET is_active = FALSE, effective_to = NOW() WHERE user_id = %s AND is_active = TRUE", (user_id,))
                # Insert new record with same cart but empty wishlist
                cur.execute("""
                    INSERT INTO brickjewels_user_data_scd2 (user_id, cart, wishlist, is_active, effective_from, updated_at)
                    VALUES (%s, %s::jsonb, '[]'::jsonb, TRUE, NOW(), NOW())
                """, (user_id, json.dumps(current_cart) if isinstance(current_cart, (list, dict)) else current_cart))
            conn.commit()
        finally:
            conn.close()

    await _run_db(_clear)
    return {"status": "cleared", "user_id": user_id}


@app.delete("/api/admin/chat-sessions/{session_id}")
async def admin_delete_chat_session(session_id: int):
    """Delete a chat session."""
    def _delete(token):
        conn = _pg_connect(token)
        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM brickjewels_chat_sessions WHERE session_id = %s RETURNING session_id", (session_id,))
                deleted = cur.fetchone()
            conn.commit()
            return deleted is not None
        finally:
            conn.close()

    deleted = await _run_db(_delete)
    if not deleted:
        raise HTTPException(status_code=404, detail="Session not found")
    return {"status": "deleted", "session_id": session_id}


# ---------------------------------------------------------------------------
# Admin — Embed Config, Triggers, Milestone Editing
# ---------------------------------------------------------------------------
LAKEVIEW_DASHBOARD_ID = os.environ.get("LAKEVIEW_DASHBOARD_ID", "")
GENIE_SPACE_ID = os.environ.get("GENIE_SPACE_ID", "")


@app.get("/api/admin/embed-config")
async def admin_embed_config():
    """Return dashboard and Genie embed URLs for the internal UI."""
    host = get_host()
    return {
        "workspace_host": host,
        "dashboard_id": LAKEVIEW_DASHBOARD_ID,
        "dashboard_embed_url": f"{host}/embed/dashboardsv3/{LAKEVIEW_DASHBOARD_ID}" if LAKEVIEW_DASHBOARD_ID else "",
        "genie_space_id": GENIE_SPACE_ID,
        "genie_embed_url": f"{host}/embed/genie/rooms/{GENIE_SPACE_ID}" if GENIE_SPACE_ID else "",
    }


# Databricks Job IDs for admin triggers
PRICE_REFRESH_JOB_ID = os.environ.get("PRICE_REFRESH_JOB_ID", "")
NUDGE_EMAIL_JOB_ID = os.environ.get("NUDGE_EMAIL_JOB_ID", "")


async def _trigger_databricks_job(job_id: str) -> dict:
    """Trigger a Databricks Job run and return the run_id and job name."""
    host = get_host()
    headers = await get_headers()

    # Get job name
    async with httpx.AsyncClient(timeout=15.0) as client:
        job_resp = await client.get(f"{host}/api/2.0/jobs/get?job_id={job_id}", headers=headers)
        job_name = job_resp.json().get("settings", {}).get("name", f"Job {job_id}") if job_resp.status_code == 200 else f"Job {job_id}"

    # Trigger run
    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.post(f"{host}/api/2.0/jobs/run-now", headers=headers, json={"job_id": int(job_id)})
        if resp.status_code != 200:
            raise HTTPException(status_code=resp.status_code, detail=f"Failed to trigger job: {resp.text}")
        run_id = resp.json().get("run_id")

    return {"run_id": run_id, "job_id": job_id, "job_name": job_name}


@app.get("/api/admin/job-run-status/{run_id}")
async def admin_job_run_status(run_id: int):
    """Poll a Databricks Job run status. Returns state, result, and timing info."""
    host = get_host()
    headers = await get_headers()

    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.get(f"{host}/api/2.0/jobs/runs/get?run_id={run_id}", headers=headers)
        if resp.status_code != 200:
            raise HTTPException(status_code=resp.status_code, detail=f"Failed to get run status: {resp.text}")
        data = resp.json()

    state = data.get("state", {})
    start_time = data.get("start_time")
    end_time = data.get("end_time")
    duration_ms = (end_time - start_time) if start_time and end_time else None

    return {
        "run_id": run_id,
        "job_name": data.get("run_name", ""),
        "life_cycle_state": state.get("life_cycle_state", "UNKNOWN"),
        "result_state": state.get("result_state"),
        "state_message": state.get("state_message", ""),
        "start_time": datetime.fromtimestamp(start_time / 1000, tz=timezone(timedelta(hours=5, minutes=30))).isoformat() if start_time else None,
        "end_time": datetime.fromtimestamp(end_time / 1000, tz=timezone(timedelta(hours=5, minutes=30))).isoformat() if end_time else None,
        "duration_seconds": round(duration_ms / 1000, 1) if duration_ms else None,
    }


@app.post("/api/admin/trigger-price-refresh")
async def admin_trigger_price_refresh():
    """Admin action: trigger the Price Refresh Databricks Job. Returns run_id for polling."""
    try:
        result = await _trigger_databricks_job(PRICE_REFRESH_JOB_ID)
        return {"status": "triggered", **result}
    except Exception as exc:
        logger.error("Failed to trigger price refresh job: %s", exc)
        raise HTTPException(status_code=500, detail=f"Failed to trigger job: {str(exc)}")


@app.get("/api/admin/last-price-refresh")
async def admin_last_price_refresh():
    """Get the latest metal prices and when they were last refreshed."""
    def _q(token):
        conn = _pg_connect(token)
        try:
            import psycopg2.extras
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""SELECT metal, price_gram_24k::float as price_24k,
                                      price_gram_22k::float as price_22k,
                                      pct_change_24k::float as pct_change,
                                      fetched_at
                               FROM brickjewels_metal_prices
                               WHERE is_active = TRUE ORDER BY metal""")
                return [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()

    prices = await _run_db(_q)
    for p in prices:
        if p.get("fetched_at") and hasattr(p["fetched_at"], "isoformat"):
            p["fetched_at"] = p["fetched_at"].isoformat()
    return {"prices": prices}


@app.get("/api/admin/price-refresh-history")
async def admin_price_refresh_history(limit: int = 10):
    """Get recent Price Refresh job runs from Databricks Jobs API + prices for successful runs."""
    ist = timezone(timedelta(hours=5, minutes=30))

    # Step 1: Fetch recent runs from Databricks Jobs API
    host = get_host()
    headers = await get_headers()
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(
                f"{host}/api/2.0/jobs/runs/list",
                params={"job_id": int(PRICE_REFRESH_JOB_ID), "limit": limit},
                headers=headers,
            )
            if resp.status_code != 200:
                logger.warning("Price refresh history: Jobs API returned %s", resp.status_code)
                return {"runs": []}
            runs_data = resp.json().get("runs", [])
    except Exception as exc:
        logger.error("Price refresh history: Jobs API error: %s", exc)
        return {"runs": []}

    runs = []
    for r in runs_data:
        state = r.get("state", {})
        start_ms = r.get("start_time", 0)
        end_ms = r.get("end_time", 0)
        duration_ms = (end_ms - start_ms) if start_ms and end_ms else None

        runs.append({
            "run_id": r.get("run_id"),
            "start_time": datetime.fromtimestamp(start_ms / 1000, tz=ist).isoformat() if start_ms else None,
            "end_time": datetime.fromtimestamp(end_ms / 1000, tz=ist).isoformat() if end_ms else None,
            "duration_seconds": round(duration_ms / 1000, 1) if duration_ms else None,
            "life_cycle_state": state.get("life_cycle_state", "UNKNOWN"),
            "result_state": state.get("result_state"),
            "state_message": (state.get("state_message") or "")[:200],
            "prices": None,
        })

    # Step 2: For the most recent successful run, attach current prices from Lakebase
    try:
        def _get_recent_prices(token):
            conn = _pg_connect(token)
            try:
                import psycopg2.extras
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute("""
                        SELECT metal, price_gram_24k::float as price_24k,
                               price_gram_22k::float as price_22k,
                               price_gram_18k::float as price_18k,
                               pct_change_24k::float as pct_change,
                               fetched_at
                        FROM brickjewels_metal_prices
                        WHERE is_active = TRUE
                        ORDER BY metal
                    """)
                    return [dict(r) for r in cur.fetchall()]
            finally:
                conn.close()

        current_prices = await _run_db(_get_recent_prices)
        for p in current_prices:
            if p.get("fetched_at") and hasattr(p["fetched_at"], "isoformat"):
                p["fetched_at"] = p["fetched_at"].isoformat()

        # Attach prices to all successful runs (they represent the prices at that point)
        for run in runs:
            if run["result_state"] == "SUCCESS":
                run["prices"] = current_prices
    except Exception as exc:
        logger.warning("Price refresh history: Lakebase query failed: %s", exc)

    return {"runs": runs}


@app.get("/api/admin/nudge-user-ranking")
async def admin_nudge_user_ranking():
    """Rank all users by purchase potential for targeted nudge selection.

    Score (0-100) based on: recent orders, cart value, wishlist items,
    upcoming milestones, lifetime value, and recency.
    """
    def _rank(token):
        conn = _pg_connect(token)
        try:
            import psycopg2.extras
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                # Get all users with their data
                cur.execute("""
                    SELECT u.user_id, u.first_name, u.last_name, u.email,
                           u.date_of_birth, u.anniversary_date, u.milestone_label, u.milestone_date,
                           u.created_at
                    FROM brickjewels_users u
                    ORDER BY u.user_id
                """)
                users = [dict(r) for r in cur.fetchall()]
                user_ids = [u["user_id"] for u in users]
                if not user_ids:
                    return []

                # Order stats per user
                cur.execute("""
                    SELECT user_id,
                           COUNT(*) as total_orders,
                           COALESCE(SUM(total_amount), 0) as lifetime_value,
                           MAX(created_at) as last_order_at,
                           COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '30 days') as recent_orders_30d,
                           COALESCE(SUM(total_amount) FILTER (WHERE created_at >= NOW() - INTERVAL '30 days'), 0) as recent_value_30d
                    FROM brickjewels_orders
                    GROUP BY user_id
                """)
                order_stats = {r["user_id"]: dict(r) for r in cur.fetchall()}

                # Cart data
                cur.execute("""
                    SELECT d.user_id,
                           jsonb_array_length(COALESCE(d.cart, '[]'::jsonb)) as cart_items,
                           COALESCE((
                               SELECT SUM((item->'product'->>'price_inr')::bigint * COALESCE((item->>'quantity')::int, 1))
                               FROM jsonb_array_elements(d.cart) as item
                           ), 0) as cart_value,
                           jsonb_array_length(COALESCE(d.wishlist, '[]'::jsonb)) as wishlist_items
                    FROM brickjewels_user_data_scd2 d
                    WHERE d.is_active = TRUE
                """)
                user_data = {r["user_id"]: dict(r) for r in cur.fetchall()}

            # Score each user
            from datetime import date as date_type
            today = date_type.today()
            results = []

            for u in users:
                uid = u["user_id"]
                os_data = order_stats.get(uid, {})
                ud = user_data.get(uid, {})

                score = 0
                factors = {}
                insights = []

                # Factor 1: Recent orders (max 25)
                recent_30d = int(os_data.get("recent_orders_30d", 0))
                factors["recent_orders_30d"] = recent_30d
                if recent_30d >= 3:
                    score += 25; insights.append(f"{recent_30d} orders in last 30 days")
                elif recent_30d >= 1:
                    score += 15; insights.append(f"{recent_30d} order(s) in last 30 days")

                # Factor 2: Lifetime value (max 20)
                ltv = int(os_data.get("lifetime_value", 0))
                factors["lifetime_value"] = ltv
                if ltv >= 500000:
                    score += 20; insights.append(f"High LTV: ₹{ltv:,}")
                elif ltv >= 200000:
                    score += 15; insights.append(f"Good LTV: ₹{ltv:,}")
                elif ltv >= 50000:
                    score += 8; insights.append(f"LTV: ₹{ltv:,}")

                # Factor 3: Cart value (max 15)
                cart_val = int(ud.get("cart_value", 0))
                cart_items = int(ud.get("cart_items", 0))
                factors["cart_value"] = cart_val
                factors["cart_items"] = cart_items
                if cart_val >= 50000:
                    score += 15; insights.append(f"Cart: {cart_items} items worth ₹{cart_val:,}")
                elif cart_val > 0:
                    score += 8; insights.append(f"Cart: {cart_items} items (₹{cart_val:,})")

                # Factor 4: Wishlist (max 10)
                wl_items = int(ud.get("wishlist_items", 0))
                factors["wishlist_items"] = wl_items
                if wl_items >= 3:
                    score += 10; insights.append(f"{wl_items} wishlist items")
                elif wl_items >= 1:
                    score += 5; insights.append(f"{wl_items} wishlist item(s)")

                # Factor 5: Upcoming milestone within 14 days (max 20)
                milestone_info = None
                for label, d_val in [("Birthday", u.get("date_of_birth")),
                                      ("Anniversary", u.get("anniversary_date")),
                                      (u.get("milestone_label") or "Milestone", u.get("milestone_date"))]:
                    if not d_val:
                        continue
                    try:
                        this_year = date_type(today.year, d_val.month, d_val.day)
                        days_away = (this_year - today).days
                        if days_away < 0:
                            days_away = (date_type(today.year + 1, d_val.month, d_val.day) - today).days
                        if days_away <= 14:
                            score += 20
                            milestone_info = f"{label} in {days_away} day{'s' if days_away != 1 else ''}"
                            insights.append(milestone_info)
                            break
                        elif days_away <= 30:
                            score += 10
                            milestone_info = f"{label} in {days_away} days"
                            insights.append(milestone_info)
                            break
                    except ValueError:
                        continue
                factors["upcoming_milestone"] = milestone_info

                # Factor 6: Recency (max 10)
                last_order = os_data.get("last_order_at")
                if last_order:
                    days_ago = (datetime.now(last_order.tzinfo if last_order.tzinfo else None) - last_order).days if last_order else 999
                    factors["last_order_days_ago"] = days_ago
                    if days_ago <= 7:
                        score += 10; insights.append(f"Last order {days_ago}d ago")
                    elif days_ago <= 30:
                        score += 5
                else:
                    factors["last_order_days_ago"] = None

                # Potential label
                potential = "high" if score >= 50 else "medium" if score >= 25 else "low"

                results.append({
                    "user_id": uid,
                    "name": f"{u['first_name']} {u['last_name']}",
                    "email": u["email"],
                    "score": min(score, 100),
                    "potential": potential,
                    "insight": ". ".join(insights) if insights else "New user — no purchase activity yet.",
                    "factors": factors,
                })

            # Sort by score descending
            results.sort(key=lambda x: x["score"], reverse=True)
            for i, r in enumerate(results):
                r["rank"] = i + 1

            return results
        finally:
            conn.close()

    users = await _run_db(_rank)
    return {"users": users}


class NudgeTriggerRequest(BaseModel):
    user_ids: Optional[List[int]] = None


@app.post("/api/admin/trigger-nudges")
async def admin_trigger_nudges(req: NudgeTriggerRequest = NudgeTriggerRequest()):
    """Trigger nudge generation + email sending for selected users (or all if none specified).

    1. Generates nudges in-app for selected users
    2. Triggers the Nudge Email Sender Databricks Job with user_ids param
    """
    try:
        # Step 1: Generate nudges in-app (optionally filtered)
        if req.user_ids:
            # Filter: only generate for selected users
            logger.info("Generating nudges for %d selected users: %s", len(req.user_ids), req.user_ids)
        await _generate_nudges(user_ids_filter=req.user_ids)

        # Step 2: Trigger email sender job with user_ids as notebook param
        host = get_host()
        headers = await get_headers()
        notebook_params = {}
        if req.user_ids:
            notebook_params["user_ids"] = ",".join(str(uid) for uid in req.user_ids)

        async with httpx.AsyncClient(timeout=15.0) as client:
            # Get job name
            job_resp = await client.get(f"{host}/api/2.0/jobs/get?job_id={NUDGE_EMAIL_JOB_ID}", headers=headers)
            job_name = job_resp.json().get("settings", {}).get("name", f"Job {NUDGE_EMAIL_JOB_ID}") if job_resp.status_code == 200 else f"Job {NUDGE_EMAIL_JOB_ID}"

            # Trigger with params
            run_payload = {"job_id": int(NUDGE_EMAIL_JOB_ID)}
            if notebook_params:
                run_payload["notebook_params"] = notebook_params
            resp = await client.post(f"{host}/api/2.0/jobs/run-now", headers=headers, json=run_payload)
            if resp.status_code != 200:
                raise HTTPException(status_code=resp.status_code, detail=f"Failed to trigger job: {resp.text}")
            run_id = resp.json().get("run_id")

        return {
            "status": "triggered",
            "run_id": run_id,
            "job_id": NUDGE_EMAIL_JOB_ID,
            "job_name": job_name,
            "users_targeted": len(req.user_ids) if req.user_ids else "all",
        }
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to trigger nudge job: %s", exc)
        raise HTTPException(status_code=500, detail=f"Failed to trigger: {str(exc)}")


class MilestoneUpdateRequest(BaseModel):
    date_of_birth: Optional[str] = None
    anniversary_date: Optional[str] = None
    milestone_label: Optional[str] = None
    milestone_date: Optional[str] = None


@app.put("/api/admin/users/{user_id}/milestones")
async def admin_update_milestones(user_id: int, req: MilestoneUpdateRequest):
    """Admin action: edit user milestone dates for nudge targeting/testing."""
    def _update(token):
        conn = _pg_connect(token)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT user_id FROM brickjewels_users WHERE user_id = %s", (user_id,))
                if not cur.fetchone():
                    return False
                cur.execute("""
                    UPDATE brickjewels_users
                    SET date_of_birth = %s, anniversary_date = %s,
                        milestone_label = %s, milestone_date = %s
                    WHERE user_id = %s
                """, (req.date_of_birth or None, req.anniversary_date or None,
                      (req.milestone_label or "").strip() or None, req.milestone_date or None,
                      user_id))
            conn.commit()
            return True
        finally:
            conn.close()

    found = await _run_db(_update)
    if not found:
        raise HTTPException(status_code=404, detail="User not found")
    return {"status": "ok", "user_id": user_id}


# ---------------------------------------------------------------------------
# Admin — Analytics Data API (queries Lakebase for native charts)
# ---------------------------------------------------------------------------

@app.get("/api/admin/analytics/overview")
async def admin_analytics_overview(period: str = "daily"):
    """Sales overview: KPIs + revenue trend."""
    trunc = {"daily": "day", "weekly": "week", "monthly": "month"}.get(period, "day")

    def _q(token):
        conn = _pg_connect(token)
        try:
            import psycopg2.extras
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                # KPIs
                cur.execute("""
                    SELECT COUNT(DISTINCT order_id) as total_orders,
                           COALESCE(SUM(total_amount), 0) as total_revenue,
                           ROUND(COALESCE(AVG(total_amount), 0)) as avg_order_value,
                           COUNT(DISTINCT user_id) as unique_customers
                    FROM brickjewels_orders
                """)
                kpis = dict(cur.fetchone())

                # Revenue in last 30 days vs prior 30 days for trend
                cur.execute("""
                    SELECT COALESCE(SUM(CASE WHEN created_at >= NOW() - INTERVAL '30 days' THEN total_amount END), 0) as recent,
                           COALESCE(SUM(CASE WHEN created_at >= NOW() - INTERVAL '60 days'
                                              AND created_at < NOW() - INTERVAL '30 days' THEN total_amount END), 0) as prior
                    FROM brickjewels_orders
                """)
                trend = cur.fetchone()
                recent = float(trend["recent"] or 0)
                prior = float(trend["prior"] or 0)
                kpis["revenue_change_pct"] = round(((recent - prior) / prior * 100) if prior > 0 else 0, 1)

                # Revenue trend by period
                cur.execute(f"""
                    SELECT DATE_TRUNC('{trunc}', created_at) as period_date,
                           SUM(total_amount) as revenue,
                           COUNT(*) as orders
                    FROM brickjewels_orders
                    GROUP BY 1 ORDER BY 1
                """)
                trend_data = []
                for r in cur.fetchall():
                    trend_data.append({
                        "date": r["period_date"].isoformat() if r["period_date"] else None,
                        "revenue": int(r["revenue"] or 0),
                        "orders": int(r["orders"] or 0),
                    })

                # Orders by status
                cur.execute("""
                    SELECT status, COUNT(*) as count
                    FROM brickjewels_orders GROUP BY status
                """)
                status_dist = [{"status": r["status"], "count": int(r["count"])} for r in cur.fetchall()]

            return {"kpis": kpis, "trend": trend_data, "status_distribution": status_dist}
        finally:
            conn.close()

    return await _run_db(_q)


@app.get("/api/admin/analytics/categories")
async def admin_analytics_categories():
    """Category breakdown, top products, category trends."""
    def _q(token):
        conn = _pg_connect(token)
        try:
            import psycopg2.extras
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                # Category breakdown from order items (JSONB)
                cur.execute("""
                    SELECT item->>'category' as category,
                           SUM((item->>'price_inr')::bigint * COALESCE((item->>'quantity')::int, 1)) as revenue,
                           SUM(COALESCE((item->>'quantity')::int, 1)) as units,
                           COUNT(DISTINCT o.order_id) as orders
                    FROM brickjewels_orders o,
                         jsonb_array_elements(o.items) as item
                    WHERE item->>'category' IS NOT NULL AND item->>'category' != ''
                    GROUP BY 1 ORDER BY revenue DESC
                """)
                categories = [{"category": r["category"], "revenue": int(r["revenue"] or 0),
                               "units": int(r["units"] or 0), "orders": int(r["orders"] or 0)} for r in cur.fetchall()]

                # Top 12 products
                cur.execute("""
                    SELECT item->>'name' as product_name,
                           item->>'category' as category,
                           item->>'material' as material,
                           SUM((item->>'price_inr')::bigint * COALESCE((item->>'quantity')::int, 1)) as revenue,
                           SUM(COALESCE((item->>'quantity')::int, 1)) as units
                    FROM brickjewels_orders o,
                         jsonb_array_elements(o.items) as item
                    WHERE item->>'name' IS NOT NULL
                    GROUP BY 1, 2, 3 ORDER BY revenue DESC LIMIT 12
                """)
                top_products = [{"product_name": r["product_name"], "category": r["category"],
                                 "material": r["material"], "revenue": int(r["revenue"] or 0),
                                 "units": int(r["units"] or 0)} for r in cur.fetchall()]

                # Monthly category trend
                cur.execute("""
                    SELECT DATE_TRUNC('month', o.created_at) as month,
                           item->>'category' as category,
                           SUM((item->>'price_inr')::bigint * COALESCE((item->>'quantity')::int, 1)) as revenue
                    FROM brickjewels_orders o,
                         jsonb_array_elements(o.items) as item
                    WHERE item->>'category' IS NOT NULL AND item->>'category' != ''
                    GROUP BY 1, 2 ORDER BY 1
                """)
                cat_trend = [{"month": r["month"].isoformat() if r["month"] else None,
                              "category": r["category"], "revenue": int(r["revenue"] or 0)} for r in cur.fetchall()]

            return {"categories": categories, "top_products": top_products, "category_trend": cat_trend}
        finally:
            conn.close()

    return await _run_db(_q)


@app.get("/api/admin/analytics/metal-prices")
async def admin_analytics_metal_prices():
    """Gold price history + sales correlation."""
    def _q(token):
        conn = _pg_connect(token)
        try:
            import psycopg2.extras
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                # Gold price history (one per day, most recent record)
                cur.execute("""
                    SELECT DISTINCT ON (fetched_at::date)
                           fetched_at::date as price_date,
                           price_gram_24k::float as gold_24k,
                           price_gram_22k::float as gold_22k,
                           price_gram_18k::float as gold_18k,
                           pct_change_24k::float as pct_change
                    FROM brickjewels_metal_prices
                    WHERE metal = 'Gold'
                    ORDER BY fetched_at::date, fetched_at DESC
                """)
                gold_prices = [{"date": str(r["price_date"]), "gold_24k": round(r["gold_24k"] or 0, 2),
                                "gold_22k": round(r["gold_22k"] or 0, 2), "gold_18k": round(r["gold_18k"] or 0, 2),
                                "pct_change": round(r["pct_change"] or 0, 3)} for r in cur.fetchall()]

                # Daily revenue for overlay
                cur.execute("""
                    SELECT created_at::date as order_date,
                           SUM(total_amount) as revenue,
                           COUNT(*) as orders
                    FROM brickjewels_orders
                    GROUP BY 1 ORDER BY 1
                """)
                daily_rev = {str(r["order_date"]): {"revenue": int(r["revenue"]), "orders": int(r["orders"])} for r in cur.fetchall()}

                # Merge gold prices with revenue
                merged = []
                for g in gold_prices:
                    d = daily_rev.get(g["date"], {"revenue": 0, "orders": 0})
                    merged.append({**g, "revenue": d["revenue"], "orders": d["orders"]})

            return {"gold_prices": gold_prices, "gold_vs_sales": merged}
        finally:
            conn.close()

    return await _run_db(_q)


@app.get("/api/admin/analytics/customers")
async def admin_analytics_customers():
    """Top customers, nudge effectiveness, wishlist data."""
    def _q(token):
        conn = _pg_connect(token)
        try:
            import psycopg2.extras
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                # Top 15 customers by lifetime value
                cur.execute("""
                    SELECT u.first_name || ' ' || u.last_name as name,
                           u.email, COUNT(DISTINCT o.order_id) as orders,
                           COALESCE(SUM(o.total_amount), 0) as lifetime_value
                    FROM brickjewels_users u
                    JOIN brickjewels_orders o ON u.user_id = o.user_id
                    GROUP BY u.user_id, u.first_name, u.last_name, u.email
                    ORDER BY lifetime_value DESC LIMIT 15
                """)
                top_customers = [{"name": r["name"], "email": r["email"],
                                  "orders": int(r["orders"]), "lifetime_value": int(r["lifetime_value"])} for r in cur.fetchall()]

                # Nudge effectiveness
                cur.execute("""
                    SELECT nudge_type,
                           COUNT(*) as total,
                           SUM(CASE WHEN is_redeemed THEN 1 ELSE 0 END) as redeemed,
                           SUM(CASE WHEN is_dismissed THEN 1 ELSE 0 END) as dismissed
                    FROM brickjewels_nudges GROUP BY nudge_type
                """)
                nudges = [{"type": r["nudge_type"], "total": int(r["total"]),
                           "redeemed": int(r["redeemed"]), "dismissed": int(r["dismissed"])} for r in cur.fetchall()]

                # Wishlists by category
                cur.execute("""
                    SELECT item->>'category' as category,
                           COUNT(DISTINCT d.user_id) as users,
                           SUM((item->>'price_inr')::bigint) as total_value
                    FROM brickjewels_user_data_scd2 d,
                         jsonb_array_elements(d.wishlist) as item
                    WHERE d.is_active = TRUE AND item->>'category' IS NOT NULL AND item->>'category' != ''
                    GROUP BY 1 ORDER BY total_value DESC
                """)
                wishlists = [{"category": r["category"], "users": int(r["users"]),
                              "value": int(r["total_value"] or 0)} for r in cur.fetchall()]

                # Total users count
                cur.execute("SELECT COUNT(*) as cnt FROM brickjewels_users")
                total_users = int(cur.fetchone()["cnt"])

            return {"top_customers": top_customers, "nudge_effectiveness": nudges,
                    "wishlists_by_category": wishlists, "total_users": total_users}
        finally:
            conn.close()

    return await _run_db(_q)


# ---------------------------------------------------------------------------
# Admin — Genie Chat History (Lakebase)
# ---------------------------------------------------------------------------

_CREATE_GENIE_HISTORY_SQL = """
    CREATE TABLE IF NOT EXISTS brickjewels_genie_history (
        id              SERIAL      PRIMARY KEY,
        conversation_id TEXT        NOT NULL,
        title           TEXT        NOT NULL DEFAULT 'New Chat',
        messages        JSONB       DEFAULT '[]'::jsonb,
        admin_email     TEXT,
        created_at      TIMESTAMPTZ DEFAULT NOW(),
        updated_at      TIMESTAMPTZ DEFAULT NOW()
    )
"""


@app.get("/api/admin/genie/history")
async def genie_list_history():
    """List all Genie chat conversations from Lakebase."""
    def _q(token):
        conn = _pg_connect(token)
        try:
            import psycopg2.extras
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(_CREATE_GENIE_HISTORY_SQL)
                conn.commit()
                cur.execute("""SELECT id, conversation_id, title, messages, admin_email, created_at, updated_at
                               FROM brickjewels_genie_history ORDER BY updated_at DESC LIMIT 30""")
                rows = [dict(r) for r in cur.fetchall()]
                for r in rows:
                    for k in ("created_at", "updated_at"):
                        if r.get(k) and hasattr(r[k], "isoformat"):
                            r[k] = r[k].isoformat()
                    # Count messages for display
                    msgs = r.get("messages", [])
                    r["message_count"] = len([m for m in msgs if isinstance(m, dict) and m.get("role") == "user"]) if isinstance(msgs, list) else 0
                return rows
        finally:
            conn.close()

    return {"conversations": await _run_db(_q)}


class GenieSaveRequest(BaseModel):
    conversation_id: str
    title: str
    messages: list
    admin_email: Optional[str] = None


@app.post("/api/admin/genie/history")
async def genie_save_history(req: GenieSaveRequest):
    """Save or update a Genie chat conversation in Lakebase."""
    def _save(token):
        conn = _pg_connect(token)
        try:
            with conn.cursor() as cur:
                cur.execute(_CREATE_GENIE_HISTORY_SQL)
                conn.commit()
                # Upsert by conversation_id
                cur.execute("""SELECT id FROM brickjewels_genie_history WHERE conversation_id = %s""",
                            (req.conversation_id,))
                existing = cur.fetchone()
                if existing:
                    cur.execute("""UPDATE brickjewels_genie_history
                                   SET title = %s, messages = %s::jsonb, admin_email = %s, updated_at = NOW()
                                   WHERE conversation_id = %s""",
                                (req.title, json.dumps(req.messages), req.admin_email, req.conversation_id))
                else:
                    cur.execute("""INSERT INTO brickjewels_genie_history
                                   (conversation_id, title, messages, admin_email)
                                   VALUES (%s, %s, %s::jsonb, %s)""",
                                (req.conversation_id, req.title, json.dumps(req.messages), req.admin_email))
            conn.commit()
        finally:
            conn.close()

    await _run_db(_save)
    return {"status": "ok"}


@app.delete("/api/admin/genie/history/{conversation_id}")
async def genie_delete_history(conversation_id: str):
    """Delete a Genie chat conversation from Lakebase."""
    def _del(token):
        conn = _pg_connect(token)
        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM brickjewels_genie_history WHERE conversation_id = %s", (conversation_id,))
            conn.commit()
        finally:
            conn.close()

    await _run_db(_del)
    return {"status": "deleted"}


# ---------------------------------------------------------------------------
# Admin — Genie Proxy (native chat interface)
# ---------------------------------------------------------------------------

async def _genie_request(method: str, path: str, json_body=None, max_retries=3):
    """Make a Genie API request with retry logic for rate limiting (429)."""
    import asyncio
    host = get_host()
    space_id = GENIE_SPACE_ID
    if not space_id:
        raise HTTPException(status_code=400, detail="GENIE_SPACE_ID not configured")

    url = f"{host}/api/2.0/genie/spaces/{space_id}{path}"

    for attempt in range(max_retries):
        headers = await get_headers()
        async with httpx.AsyncClient(timeout=120.0) as client:
            if method == "GET":
                resp = await client.get(url, headers=headers)
            else:
                resp = await client.post(url, headers=headers, json=json_body or {})

        if resp.status_code == 429:
            wait = min(2 ** attempt * 3, 15)  # 3s, 6s, 12s
            logger.warning("Genie API rate limited (429), retrying in %ds (attempt %d/%d)", wait, attempt + 1, max_retries)
            await asyncio.sleep(wait)
            continue

        if resp.status_code != 200:
            raise HTTPException(status_code=resp.status_code, detail=resp.text)
        return resp.json()

    raise HTTPException(status_code=429, detail="Genie API rate limited. Please wait a moment and try again.")


class GenieMessageRequest(BaseModel):
    content: str


@app.post("/api/admin/genie/conversations")
async def genie_start_conversation(req: GenieMessageRequest):
    """Start a new Genie conversation with the user's actual question."""
    return await _genie_request("POST", "/start-conversation", {"content": req.content})



    content: str


@app.post("/api/admin/genie/conversations/{conversation_id}/messages")
async def genie_send_message(conversation_id: str, req: GenieMessageRequest):
    """Send a message to Genie with retry on rate limiting."""
    return await _genie_request("POST", f"/conversations/{conversation_id}/messages", {"content": req.content})


@app.get("/api/admin/genie/conversations/{conversation_id}/messages/{message_id}")
async def genie_get_message(conversation_id: str, message_id: str):
    """Poll for Genie message result with retry on rate limiting."""
    return await _genie_request("GET", f"/conversations/{conversation_id}/messages/{message_id}")


# ---------------------------------------------------------------------------
# Auth endpoints — Sign Up / Login with Lakebase
# ---------------------------------------------------------------------------
import hashlib


def hash_password(password: str) -> str:
    """Hash a password using SHA-256."""
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


def verify_password(password: str, stored_hash: str) -> bool:
    """Verify a password against a stored SHA-256 hash."""
    return hashlib.sha256(password.encode("utf-8")).hexdigest() == stored_hash


class SignUpRequest(BaseModel):
    first_name: str
    last_name: str
    email: str
    password: str
    country_code: str = "+91"
    mobile: str = ""
    date_of_birth: Optional[str] = None
    anniversary_date: Optional[str] = None
    milestone_label: Optional[str] = None
    milestone_date: Optional[str] = None


class LoginRequest(BaseModel):
    email: str
    password: str


@app.post("/api/auth/signup")
async def auth_signup(req: SignUpRequest):
    """Register a new user. Password is SHA-256 hashed before storage."""
    email = req.email.strip().lower()
    if not email or not req.password:
        raise HTTPException(status_code=400, detail="Email and password are required")
    if len(req.password) < 6:
        raise HTTPException(status_code=400, detail="Password must be at least 6 characters")

    password_hash = hash_password(req.password)

    def _signup(token):
        conn = _pg_connect(token)
        try:
            with conn.cursor() as cur:
                # Check if email already exists
                cur.execute("SELECT user_id FROM brickjewels_users WHERE email = %s", (email,))
                if cur.fetchone():
                    raise HTTPException(status_code=409, detail="An account with this email already exists")
                cur.execute(
                    """INSERT INTO brickjewels_users
                       (first_name, last_name, email, password_hash, country_code, mobile,
                        date_of_birth, anniversary_date, milestone_label, milestone_date)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING user_id""",
                    (req.first_name.strip(), req.last_name.strip(), email, password_hash,
                     req.country_code.strip(), req.mobile.strip(),
                     req.date_of_birth or None, req.anniversary_date or None,
                     (req.milestone_label or "").strip() or None, req.milestone_date or None),
                )
                user_id = cur.fetchone()[0]
            conn.commit()
            return user_id
        finally:
            conn.close()

    try:
        user_id = await _run_db(_signup)
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Signup DB error: %s", exc)
        raise HTTPException(status_code=500, detail=f"Registration failed: {exc}")

    return {
        "user_id": user_id,
        "first_name": req.first_name.strip(),
        "last_name": req.last_name.strip(),
        "email": email,
    }


@app.post("/api/auth/login")
async def auth_login(req: LoginRequest):
    """Authenticate user by email + SHA-256 password check."""
    email = req.email.strip().lower()
    if not email or not req.password:
        raise HTTPException(status_code=400, detail="Email and password are required")

    def _login(token):
        conn = _pg_connect(token)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT user_id, first_name, last_name, email, password_hash FROM brickjewels_users WHERE email = %s",
                    (email,),
                )
                row = cur.fetchone()
            return row
        finally:
            conn.close()

    try:
        row = await _run_db(_login)
    except Exception as exc:
        logger.error("Login DB error: %s", exc)
        raise HTTPException(status_code=500, detail=f"Login failed: {exc}")

    if not row:
        raise HTTPException(status_code=404, detail="No account found with this email address")

    user_id, first_name, last_name, db_email, password_hash = row

    if not verify_password(req.password, password_hash):
        raise HTTPException(status_code=401, detail="Incorrect password")

    return {
        "user_id": user_id,
        "first_name": first_name,
        "last_name": last_name,
        "email": db_email,
    }


class ProfileUpdateRequest(BaseModel):
    user_id: int
    first_name: str
    last_name: str
    email: str
    country_code: str = "+91"
    mobile: str = ""
    password: Optional[str] = None
    date_of_birth: Optional[str] = None
    anniversary_date: Optional[str] = None
    milestone_label: Optional[str] = None
    milestone_date: Optional[str] = None


@app.get("/api/auth/profile/{user_id}")
async def get_profile(user_id: int):
    """Get full user profile."""
    def _fetch(token):
        import psycopg2.extras
        conn = _pg_connect(token)
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT user_id, first_name, last_name, email, country_code, mobile, date_of_birth, anniversary_date, milestone_label, milestone_date FROM brickjewels_users WHERE user_id = %s", (user_id,))
                return cur.fetchone()
        finally:
            conn.close()

    row = await _run_db(_fetch)
    if not row:
        raise HTTPException(status_code=404, detail="User not found")
    result = dict(row)
    for k in ("date_of_birth", "anniversary_date", "milestone_date"):
        if result.get(k) and hasattr(result[k], "isoformat"):
            result[k] = result[k].isoformat()
    return result


@app.put("/api/auth/profile")
async def update_profile(req: ProfileUpdateRequest):
    """Update user profile. Email must remain unique."""
    email = req.email.strip().lower()

    updates = {"first_name": req.first_name.strip(), "last_name": req.last_name.strip(),
               "email": email, "country_code": req.country_code.strip(), "mobile": req.mobile.strip(),
               "date_of_birth": req.date_of_birth or None,
               "anniversary_date": req.anniversary_date or None,
               "milestone_label": (req.milestone_label or "").strip() or None,
               "milestone_date": req.milestone_date or None}
    if req.password and len(req.password) >= 6:
        updates["password_hash"] = hash_password(req.password)

    def _update(token):
        conn = _pg_connect(token)
        try:
            with conn.cursor() as cur:
                # Check email uniqueness (excluding self)
                cur.execute("SELECT user_id FROM brickjewels_users WHERE email = %s AND user_id != %s", (email, req.user_id))
                if cur.fetchone():
                    raise HTTPException(status_code=409, detail="This email is already used by another account")
                set_parts = []
                vals = []
                for k, v in updates.items():
                    set_parts.append(f"{k} = %s")
                    vals.append(v)
                vals.append(req.user_id)
                cur.execute(f"UPDATE brickjewels_users SET {', '.join(set_parts)} WHERE user_id = %s", vals)
            conn.commit()
        finally:
            conn.close()

    try:
        await _run_db(_update)
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Profile update error: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))

    return {"user_id": req.user_id, "first_name": updates["first_name"], "last_name": updates["last_name"], "email": email}


# ---------------------------------------------------------------------------
# User Data — Cart, Wishlist, Orders, Chat Sessions
# ---------------------------------------------------------------------------


class UserDataRequest(BaseModel):
    user_id: int
    cart: Optional[list] = None
    wishlist: Optional[list] = None


@app.get("/api/user/{user_id}/data")
async def get_user_data(user_id: int):
    """Fetch current (active) cart & wishlist for a user — SCD2."""
    def _fetch(token):
        import psycopg2.extras
        conn = _pg_connect(token)
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT cart, wishlist FROM brickjewels_user_data_scd2 WHERE user_id = %s AND is_active = TRUE",
                    (user_id,),
                )
                return cur.fetchone()
        finally:
            conn.close()

    try:
        row = await _run_db(_fetch)
    except Exception as exc:
        logger.error("Get user data error: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))

    if not row:
        return {"cart": [], "wishlist": []}

    cart = row["cart"] if isinstance(row["cart"], list) else json.loads(row["cart"]) if row["cart"] else []
    wishlist = row["wishlist"] if isinstance(row["wishlist"], list) else json.loads(row["wishlist"]) if row["wishlist"] else []
    return {"cart": cart, "wishlist": wishlist}


@app.put("/api/user/{user_id}/data")
async def save_user_data(user_id: int, req: UserDataRequest):
    """Save cart & wishlist using SCD2: expire current active row, insert new active row.
    Uses advisory lock to prevent race conditions from concurrent debounced saves."""
    def _save(token):
        conn = _pg_connect(token)
        try:
            with conn.cursor() as cur:
                # Advisory lock per user to serialize concurrent saves
                cur.execute("SELECT pg_advisory_xact_lock(%s)", (user_id,))
                # Expire ALL current active records (handles edge case of duplicates)
                cur.execute("""
                    UPDATE brickjewels_user_data_scd2
                    SET is_active = FALSE, effective_to = NOW()
                    WHERE user_id = %s AND is_active = TRUE
                """, (user_id,))
                # Insert new active record
                cur.execute("""
                    INSERT INTO brickjewels_user_data_scd2
                        (user_id, cart, wishlist, is_active, effective_from, updated_at)
                    VALUES (%s, %s::jsonb, %s::jsonb, TRUE, NOW(), NOW())
                """, (user_id, json.dumps(req.cart or []), json.dumps(req.wishlist or [])))
            conn.commit()
        finally:
            conn.close()

    try:
        await _run_db(_save)
    except Exception as exc:
        logger.error("Save user data error: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))
    return {"status": "saved"}


@app.get("/api/user/{user_id}/orders")
async def get_user_orders(user_id: int):
    """Get order history for a user."""
    def _fetch(token):
        import psycopg2.extras
        conn = _pg_connect(token)
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT order_id, customer_name, items, total_amount, status, created_at FROM brickjewels_orders WHERE user_id = %s ORDER BY created_at DESC LIMIT 50",
                    (user_id,),
                )
                return cur.fetchall()
        finally:
            conn.close()

    try:
        rows = await _run_db(_fetch)
    except Exception as exc:
        logger.error("Get user orders error: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))

    orders = []
    for row in (rows or []):
        items = row["items"] if isinstance(row["items"], list) else json.loads(row["items"])
        created_at = row["created_at"]
        orders.append({
            "order_id": row["order_id"],
            "customer_name": row["customer_name"],
            "items": items,
            "total_amount": int(row["total_amount"]),
            "status": row["status"],
            "created_at": created_at.strftime("%d %b %Y, %I:%M %p") if created_at else "",
        })
    return {"orders": orders}


# Chat sessions

class ChatSessionSave(BaseModel):
    user_id: int
    session_id: Optional[int] = None
    title: str = "New Conversation"
    messages: list = []


@app.get("/api/user/{user_id}/chat-sessions")
async def get_chat_sessions(user_id: int):
    """List all chat sessions for a user (titles + timestamps)."""
    def _fetch(token):
        import psycopg2.extras
        conn = _pg_connect(token)
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT session_id, title, created_at, updated_at FROM brickjewels_chat_sessions WHERE user_id = %s ORDER BY updated_at DESC LIMIT 50",
                    (user_id,),
                )
                return cur.fetchall()
        finally:
            conn.close()

    try:
        rows = await _run_db(_fetch)
    except Exception as exc:
        logger.error("Get chat sessions error: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))

    sessions = []
    for row in (rows or []):
        sessions.append({
            "session_id": row["session_id"],
            "title": row["title"],
            "created_at": row["created_at"].strftime("%d %b %Y, %I:%M %p") if row["created_at"] else "",
            "updated_at": row["updated_at"].strftime("%d %b %Y, %I:%M %p") if row["updated_at"] else "",
        })
    return {"sessions": sessions}


@app.get("/api/user/{user_id}/chat-sessions/{session_id}")
async def get_chat_session(user_id: int, session_id: int):
    """Get full messages for a specific chat session."""
    def _fetch(token):
        import psycopg2.extras
        conn = _pg_connect(token)
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT session_id, title, messages, created_at FROM brickjewels_chat_sessions WHERE session_id = %s AND user_id = %s",
                    (session_id, user_id),
                )
                return cur.fetchone()
        finally:
            conn.close()

    try:
        row = await _run_db(_fetch)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    if not row:
        raise HTTPException(status_code=404, detail="Chat session not found")

    messages = row["messages"] if isinstance(row["messages"], list) else json.loads(row["messages"])
    return {"session_id": row["session_id"], "title": row["title"], "messages": messages}


@app.post("/api/user/chat-sessions")
async def save_chat_session(req: ChatSessionSave):
    """Create or update a chat session."""
    def _save(token):
        conn = _pg_connect(token)
        try:
            with conn.cursor() as cur:
                if req.session_id:
                    cur.execute("""
                        UPDATE brickjewels_chat_sessions
                        SET messages = %s::jsonb, title = %s, updated_at = NOW()
                        WHERE session_id = %s AND user_id = %s
                        RETURNING session_id
                    """, (json.dumps(req.messages), req.title, req.session_id, req.user_id))
                    row = cur.fetchone()
                    if row:
                        conn.commit()
                        return row[0]
                # Create new session
                cur.execute("""
                    INSERT INTO brickjewels_chat_sessions (user_id, title, messages, created_at, updated_at)
                    VALUES (%s, %s, %s::jsonb, NOW(), NOW()) RETURNING session_id
                """, (req.user_id, req.title, json.dumps(req.messages)))
                session_id = cur.fetchone()[0]
            conn.commit()
            return session_id
        finally:
            conn.close()

    try:
        session_id = await _run_db(_save)
    except Exception as exc:
        logger.error("Save chat session error: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))

    return {"session_id": session_id}


# ---------------------------------------------------------------------------
# Chat endpoint — conversational AI shopping assistant
# ---------------------------------------------------------------------------

CHAT_SYSTEM_PROMPT = """You are GIVA's personal AI shopping assistant — a warm, knowledgeable jewelry consultant.

Your job is to help customers discover and choose the perfect jewelry through natural conversation. You have access to GIVA's full catalog of gold and diamond jewelry (necklaces and rings).

CONVERSATION RULES:
- Be warm, concise, and elegant. Sound like a personal stylist, not a search engine.
- Ask clarifying questions when the user's intent is vague (occasion? budget? style preference?).
- When you need to show products, include a JSON action block (explained below).
- Keep responses to 2-4 sentences. Never use markdown headers, bullet points, or emojis.
- Reference specific product details (material, weight, price) when discussing items.
- Suggest complementary pieces or styling tips naturally.

ACTION SYSTEM:
When the conversation requires showing products or performing cart actions, you MUST include exactly one action block in your response, formatted as:
```action
{"type": "search", "query": "your search query here"}
```

Action types:
- "search": Search the catalog. Set "query" to a descriptive search string (e.g., "diamond ring under 50000 for engagement minimalist style")
- "add_to_cart": Add product(s) to cart. Set "query" to describe the specific product to add (e.g., "gold necklace traditional style"). The system will search, find the best match, and add it to the cart. Use this when the user says things like "add that to my cart", "I'll take it", "add the gold necklace", etc. Use the conversation context to determine which product to add.
- "add_to_wishlist": Add product to the wishlist. Set "query" to describe the product. Use when the user says "add to wishlist", "save this for later", "bookmark this", "I like this one but not ready to buy", etc. Use conversation context to determine which product.
- "show_wishlist": Show all items currently in the user's wishlist. Use when the user says "show my wishlist", "what's in my wishlist", "pull up my saved items", etc. Set "query" to empty string.
- "wishlist_to_cart": Move a specific item from wishlist to cart. Set "query" to describe which wishlist item to move. Use when the user says "move X from wishlist to cart", "add the necklace from my wishlist to cart", "I want to buy that saved item", etc.
- "checkout": Open the checkout screen. Use when the user wants to proceed to checkout, pay, or complete their purchase (e.g., "yes proceed to checkout", "let me pay", "checkout", "place my order"). Set "query" to empty string.
- "chat": No products needed, just conversation (greeting, clarification, styling advice). Omit the action block entirely for pure chat.

When the user declines checkout (e.g., "no", "not yet", "I want to keep looking"), do NOT use any action block — just respond warmly and ask what else they'd like to explore.

If the user uploads an image, the image description will be appended to their message. Use it to search for similar pieces.

IMPORTANT: Only include the action block when you actually want to show products or perform an action. For greetings, follow-ups, or styling advice, just respond naturally without it."""


class ChatRequest(BaseModel):
    messages: list[dict]  # [{"role": "user"|"assistant", "content": str}]
    image_b64: Optional[str] = None
    image_type: Optional[str] = "image/jpeg"


@app.post("/api/chat")
async def chat(request: ChatRequest):
    """Conversational AI shopping assistant with search integration."""
    host = get_host()
    headers = await get_headers()

    user_message = request.messages[-1]["content"] if request.messages else ""
    image_description = None

    # If image provided, get description from Claude vision
    if request.image_b64:
        vision_url = f"{host}/serving-endpoints/{LLM_MODEL}/invocations"
        vision_payload = {
            "messages": [{
                "role": "user",
                "content": [
                    {"type": "image_url", "image_url": {"url": f"data:{request.image_type};base64,{request.image_b64}"}},
                    {"type": "text", "text": (
                        "Describe this jewelry piece in exactly 2-3 plain sentences. "
                        "Include: type (ring/earring/necklace/bracelet/bangle/pendant), "
                        "metal color and type, stone type if any, design style, and occasion."
                    )}
                ],
            }],
            "max_tokens": 120,
        }
        async with httpx.AsyncClient(timeout=45.0) as client:
            resp = await client.post(vision_url, json=vision_payload, headers=headers)
            if resp.status_code == 200:
                image_description = resp.json()["choices"][0]["message"]["content"]

    # Build conversation for Claude
    conv_messages = [{"role": "system", "content": CHAT_SYSTEM_PROMPT}]

    # Add conversation history (last 10 messages max)
    for msg in request.messages[-10:]:
        role = msg.get("role", "user")
        content = msg.get("content", "")
        if role in ("user", "assistant"):
            conv_messages.append({"role": role, "content": content})

    # Append image description to the last user message
    if image_description and conv_messages:
        last_msg = conv_messages[-1]
        if last_msg["role"] == "user":
            last_msg["content"] += f"\n\n[The user uploaded a jewelry image. Description: {image_description}]"

    # Call Claude for conversational response
    chat_url = f"{host}/serving-endpoints/{LLM_MODEL}/invocations"
    chat_payload = {
        "messages": conv_messages,
        "max_tokens": 350,
        "temperature": 0.7,
    }

    reply_text = "I'd love to help you find the perfect piece! Could you tell me more about what you're looking for?"
    action = "chat"
    products = []

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(chat_url, json=chat_payload, headers=headers)
            if resp.status_code == 200:
                raw_reply = resp.json()["choices"][0]["message"]["content"]

                # Parse action block if present
                import re
                action_match = re.search(r'```action\s*\n({.*?})\s*\n```', raw_reply, re.DOTALL)

                if action_match:
                    try:
                        action_data = json.loads(action_match.group(1))
                        action = action_data.get("type", "chat")
                        search_query = action_data.get("query", "")

                        # Remove action block from reply text
                        reply_text = raw_reply[:action_match.start()].strip()
                        trailing = raw_reply[action_match.end():].strip()
                        if trailing:
                            reply_text = reply_text + " " + trailing if reply_text else trailing

                        # Execute search if needed
                        if action in ("search", "add_to_cart", "add_to_wishlist", "wishlist_to_cart") and search_query:
                            limit = 1 if action in ("add_to_cart", "add_to_wishlist") else 6
                            search_req = SearchRequest(query=search_query, limit=limit)
                            search_results = await semantic_search(search_req)
                            products = search_results.get("products", [])[:limit]
                    except (json.JSONDecodeError, KeyError):
                        reply_text = raw_reply
                        action = "chat"
                else:
                    reply_text = raw_reply
                    action = "chat"
    except Exception as e:
        logger.error("Chat LLM error: %s", e)

    _apply_live_prices(products)
    return {
        "reply": reply_text,
        "products": products,
        "action": action,
        "image_description": image_description,
    }


# ---------------------------------------------------------------------------
# Nudge System — personalized offers based on milestones & cart analysis
# ---------------------------------------------------------------------------

_MIGRATE_USERS_MILESTONES_SQL = """
    DO $$ BEGIN
        ALTER TABLE brickjewels_users ADD COLUMN IF NOT EXISTS date_of_birth DATE;
        ALTER TABLE brickjewels_users ADD COLUMN IF NOT EXISTS anniversary_date DATE;
        ALTER TABLE brickjewels_users ADD COLUMN IF NOT EXISTS milestone_label TEXT;
        ALTER TABLE brickjewels_users ADD COLUMN IF NOT EXISTS milestone_date DATE;
    EXCEPTION WHEN others THEN NULL;
    END $$;
"""

_CREATE_NUDGES_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS brickjewels_nudges (
        nudge_id        SERIAL      PRIMARY KEY,
        user_id         INTEGER     NOT NULL REFERENCES brickjewels_users(user_id),
        nudge_type      TEXT        NOT NULL,
        title           TEXT        NOT NULL,
        message         TEXT        NOT NULL,
        discount_type   TEXT,
        discount_value  NUMERIC     DEFAULT 0,
        discount_code   TEXT,
        min_cart_value  NUMERIC     DEFAULT 0,
        valid_from      TIMESTAMPTZ NOT NULL,
        valid_to        TIMESTAMPTZ NOT NULL,
        is_active       BOOLEAN     DEFAULT TRUE,
        is_dismissed    BOOLEAN     DEFAULT FALSE,
        is_redeemed     BOOLEAN     DEFAULT FALSE,
        redeemed_at     TIMESTAMPTZ,
        created_at      TIMESTAMPTZ DEFAULT NOW()
    )
"""

_MIGRATE_NUDGES_CATEGORY_SQL = """
    DO $$ BEGIN
        ALTER TABLE brickjewels_nudges ADD COLUMN IF NOT EXISTS target_category TEXT;
    EXCEPTION WHEN others THEN NULL;
    END $$;
"""

_CREATE_NUDGE_EMAILS_SQL = """
    CREATE TABLE IF NOT EXISTS brickjewels_nudge_emails (
        email_id        SERIAL      PRIMARY KEY,
        user_id         INTEGER     NOT NULL REFERENCES brickjewels_users(user_id),
        nudge_id        INTEGER     REFERENCES brickjewels_nudges(nudge_id),
        recipient_email TEXT        NOT NULL,
        sender_email    TEXT        NOT NULL DEFAULT 'noreply@example.com',
        sender_name     TEXT        NOT NULL DEFAULT 'GIVA Offers',
        subject         TEXT        NOT NULL,
        body_html       TEXT,
        status          TEXT        DEFAULT 'pending',
        error_message   TEXT,
        sent_at         TIMESTAMPTZ,
        created_at      TIMESTAMPTZ DEFAULT NOW()
    )
"""

NUDGE_RULES = [
    {"type": "birthday", "window_before": 7, "window_after": 1,
     "discount_type": "making_pct", "discount_value": 50, "min_cart": 0,
     "title": "🎂 Birthday Special!", "msg": "Happy Birthday, {first_name}! Enjoy 50% off making charges on all jewelry."},
    {"type": "anniversary", "window_before": 7, "window_after": 1,
     "discount_type": "making_pct", "discount_value": 40, "min_cart": 0,
     "title": "💍 Anniversary Celebration", "msg": "Happy Anniversary, {first_name}! Get 40% off making charges this week."},
    {"type": "milestone", "window_before": 7, "window_after": 1,
     "discount_type": "cart_pct", "discount_value": 5, "min_cart": 25000,
     "title": "✨ Special Occasion Offer", "msg": "Celebrating your {milestone_label}? Enjoy 5% off your entire order!"},
    {"type": "cart_value", "discount_type": "cart_pct", "discount_value": 3, "min_cart": 100000,
     "title": "🛍️ Exclusive Cart Offer", "msg": "Your cart is worth ₹{cart_value}+! Get 3% off at checkout."},
    {"type": "cart_making", "discount_type": "making_pct", "discount_value": 25, "min_cart": 50000,
     "title": "💎 Making Charge Discount", "msg": "Premium pieces in your cart! Enjoy 25% off making charges."},
]

NUDGE_LLM_SYSTEM_PROMPT = """You are a personal jewelry stylist for GIVA, a premium Indian jewelry brand.

Write a short, warm, personalized nudge message (2-3 sentences max) to encourage a customer to use their discount offer.

RULES:
- Address the customer by first name.
- Reference specific items from their cart or wishlist by name when available.
- If they have wishlist items not yet in the cart, gently suggest them.
- If metal prices dropped today, mention it as a reason to buy now.
- If the customer has past orders, acknowledge them as a returning customer.
- Mention the discount naturally (e.g. "your 50% off making charges") but do NOT restate the discount code.
- If the discount targets a specific category, mention it naturally (e.g. "on your necklace picks").
- Keep the tone warm, personal, and elegant — like a trusted stylist, not a salesperson.
- Do NOT use markdown, headers, bullet points, or emojis.
- Do NOT exceed 3 sentences. Be concise."""


def _build_nudge_user_prompt(intent: dict) -> str:
    """Build the user-facing LLM prompt with all available context for one nudge."""
    parts = [f"Customer: {intent['first_name']}"]

    if intent.get("age"):
        parts.append(f"Age: {intent['age']}")

    parts.append(f"Occasion: {intent['nudge_type'].replace('_', ' ')}")
    parts.append(f"Discount offered: {intent['discount_description']}")

    target_cat = intent.get("target_category")
    if target_cat:
        parts.append(f"Discount applies to: {target_cat} category only")

    cart_items = intent.get("cart_items", [])
    if cart_items:
        cart_str = ", ".join(
            f"{it.get('name', 'item')} ({it.get('material', '')}, INR {int(float(it.get('price_inr', 0))):,})"
            for it in cart_items[:5]
        )
        parts.append(f"Items in cart: {cart_str}")

    wishlist_items = intent.get("wishlist_items", [])
    if wishlist_items:
        wl_str = ", ".join(
            f"{it.get('name', 'item')} ({it.get('category', '')})"
            for it in wishlist_items[:5]
        )
        parts.append(f"Wishlist: {wl_str}")

    order_count = intent.get("order_count", 0)
    if order_count > 0:
        parts.append(f"Previous orders: {order_count}")
        last_order = intent.get("last_order_summary", "")
        if last_order:
            parts.append(f"Most recent purchase: {last_order}")

    metal_trend = intent.get("metal_trend", "")
    if metal_trend:
        parts.append(f"Gold price trend: {metal_trend}")

    parts.append("\nWrite the personalized nudge message:")
    return "\n".join(parts)


def _gen_discount_code(nudge_type: str, user_id: int) -> str:
    prefix = {"birthday": "BDAY", "anniversary": "ANNV", "milestone": "MLST",
              "cart_value": "CART", "cart_making": "MAKE", "complete_profile": "PROF",
              "targeted_offer": "TGRT"}.get(nudge_type, "NUDG")
    suffix = "".join(random.choices(string.ascii_uppercase + string.digits, k=4))
    return f"{prefix}-{user_id}-{suffix}"


async def _generate_nudges(user_ids_filter=None):
    """Evaluate users and generate LLM-personalized nudge offers.

    Args:
        user_ids_filter: Optional list of user IDs to limit generation to.
                         If None, generates for all users.
    """
    from datetime import timezone as tz, timedelta, date
    import asyncio

    ist = tz(timedelta(hours=5, minutes=30))
    now = datetime.now(ist)
    today = now.date()
    logger.info("Generating nudges at %s IST", now.strftime("%Y-%m-%d %H:%M"))

    # ── Phase 1: Gather all data from Lakebase (sync) ──────────────────────
    def _gather(token):
        conn = _pg_connect(token)
        try:
            with conn.cursor() as cur:
                cur.execute(_MIGRATE_USERS_MILESTONES_SQL)
                cur.execute(_CREATE_NUDGES_TABLE_SQL)
                cur.execute(_MIGRATE_NUDGES_CATEGORY_SQL)
                conn.commit()

                # Users with milestone dates (optionally filtered)
                if user_ids_filter:
                    placeholders = ",".join(["%s"] * len(user_ids_filter))
                    cur.execute(f"""SELECT user_id, first_name, date_of_birth, anniversary_date,
                                          milestone_label, milestone_date FROM brickjewels_users
                                   WHERE user_id IN ({placeholders})""", user_ids_filter)
                else:
                    cur.execute("""SELECT user_id, first_name, date_of_birth, anniversary_date,
                                          milestone_label, milestone_date FROM brickjewels_users""")
                users = cur.fetchall()

                # Active carts + wishlists
                cur.execute("""SELECT DISTINCT ON (user_id) user_id, cart, wishlist
                               FROM brickjewels_user_data_scd2
                               WHERE is_active = TRUE ORDER BY user_id, effective_from DESC""")
                user_data = {}
                for r in cur.fetchall():
                    user_data[r[0]] = {"cart": r[1] if isinstance(r[1], list) else [],
                                       "wishlist": r[2] if isinstance(r[2], list) else []}

                # Order counts + last order items per user
                cur.execute("""SELECT user_id, COUNT(*) as cnt,
                               (array_agg(items ORDER BY created_at DESC))[1] as last_items
                               FROM brickjewels_orders GROUP BY user_id""")
                orders_info = {}
                for r in cur.fetchall():
                    orders_info[r[0]] = {"count": r[1], "last_items": r[2]}

                # Gold price trend
                cur.execute("""SELECT metal, pct_change_24k, pct_change_22k, pct_change_18k
                               FROM brickjewels_metal_prices WHERE is_active = TRUE""")
                gold_trend = ""
                for metal, pct24, pct22, pct18 in cur.fetchall():
                    if metal and metal.lower() in ("gold", "xau"):
                        pct = float(pct24 or pct22 or pct18 or 0)
                        if pct > 0:
                            gold_trend = f"up {pct:.1f}% today"
                        elif pct < 0:
                            gold_trend = f"down {abs(pct):.1f}% today"
                        break

                # Existing active nudges for deduplication (batched)
                cur.execute("""SELECT user_id, nudge_type FROM brickjewels_nudges
                               WHERE is_active = TRUE AND valid_to > %s""", (now.isoformat(),))
                existing = set()
                for uid, ntype in cur.fetchall():
                    existing.add((uid, ntype))

            return users, user_data, orders_info, gold_trend, existing
        finally:
            conn.close()

    try:
        users, user_data, orders_info, gold_trend, existing = await _run_db(_gather)
    except Exception as exc:
        logger.error("Nudge generation failed (gather phase): %s", exc)
        return

    # ── Build nudge intents with template messages as fallback ─────────────
    intents = []

    def _pick_target_category(cart_items, wishlist_items):
        """Pick the dominant category from cart (priority) then wishlist by total value."""
        from collections import Counter
        cat_value = Counter()
        for it in cart_items:
            cat = (it.get("category") or "").strip().lower()
            if cat:
                cat_value[cat] += int(float(it.get("price_inr", 0) or 0))
        if not cat_value:
            for it in wishlist_items:
                cat = (it.get("category") or "").strip().lower()
                if cat:
                    cat_value[cat] += int(float(it.get("price_inr", 0) or 0))
        if cat_value:
            return cat_value.most_common(1)[0][0].title()  # e.g. "Necklace"
        return None

    def _last_order_summary(uid):
        oi = orders_info.get(uid, {})
        last_items = oi.get("last_items")
        if not last_items:
            return ""
        if isinstance(last_items, str):
            try:
                last_items = json.loads(last_items)
            except Exception:
                return ""
        if not isinstance(last_items, list) or not last_items:
            return ""
        return ", ".join(
            it.get("product", {}).get("name", it.get("name", "item"))
            for it in last_items[:3]
        )

    for uid, fname, dob, anniv, ml_label, ml_date in users:
        ud = user_data.get(uid, {"cart": [], "wishlist": []})
        cart = ud.get("cart", []) or []
        wishlist = ud.get("wishlist", []) or []
        cart_items = [item.get("product", {}) for item in cart if isinstance(item, dict)][:5]
        oi = orders_info.get(uid, {})
        lo_summary = _last_order_summary(uid)

        # --- Date-based nudges ---
        date_checks = [
            ("birthday", dob, {}),
            ("anniversary", anniv, {}),
            ("milestone", ml_date, {"milestone_label": ml_label or "Special Occasion"}),
        ]
        for ntype, d, extra in date_checks:
            if not d:
                continue
            try:
                this_year = date(today.year, d.month, d.day)
            except ValueError:
                continue
            rule = next((r for r in NUDGE_RULES if r["type"] == ntype), None)
            if not rule:
                continue
            window_start = this_year - timedelta(days=rule.get("window_before", 7))
            window_end = this_year + timedelta(days=rule.get("window_after", 1))
            if not (window_start <= today <= window_end):
                continue
            if (uid, ntype) in existing:
                continue

            age = None
            if ntype == "birthday" and dob:
                age = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))

            target_cat = _pick_target_category(cart_items, wishlist[:5] if isinstance(wishlist, list) else [])
            disc_desc = f"{rule['discount_value']}% off {'making charges' if rule['discount_type'] == 'making_pct' else 'total cart'}"
            if target_cat:
                disc_desc += f" on {target_cat} items"
            template_msg = rule["msg"].format(first_name=fname, **extra)
            valid_to_dt = datetime(this_year.year, this_year.month, this_year.day,
                                   tzinfo=ist) + timedelta(days=rule.get("window_after", 1) + 1)

            intents.append({
                "user_id": uid, "first_name": fname,
                "nudge_type": ntype, "title": rule["title"],
                "message": template_msg,
                "discount_type": rule["discount_type"],
                "discount_value": rule["discount_value"],
                "discount_code": _gen_discount_code(ntype, uid),
                "min_cart": rule["min_cart"],
                "target_category": target_cat,
                "valid_from": now.isoformat(),
                "valid_to": valid_to_dt.isoformat(),
                # LLM context fields
                "discount_description": disc_desc, "age": age,
                "cart_items": cart_items,
                "wishlist_items": wishlist[:5] if isinstance(wishlist, list) else [],
                "order_count": oi.get("count", 0),
                "last_order_summary": lo_summary,
                "metal_trend": gold_trend,
            })

        # --- Cart-based nudges ---
        if cart and isinstance(cart, list):
            cart_total = sum(
                int(item.get("product", {}).get("price_inr", 0) or 0) * int(item.get("quantity", 1) or 1)
                for item in cart
            )
            for rule in NUDGE_RULES:
                if rule["type"] not in ("cart_value", "cart_making"):
                    continue
                if cart_total < rule["min_cart"]:
                    continue
                if (uid, rule["type"]) in existing:
                    continue

                cart_target_cat = _pick_target_category(cart_items, [])
                disc_desc = f"{rule['discount_value']}% off {'making charges' if rule['discount_type'] == 'making_pct' else 'total cart'}"
                if cart_target_cat:
                    disc_desc += f" on {cart_target_cat} items"
                template_msg = rule["msg"].format(first_name=fname, cart_value=f"{cart_total:,}")

                intents.append({
                    "user_id": uid, "first_name": fname,
                    "nudge_type": rule["type"], "title": rule["title"],
                    "message": template_msg,
                    "discount_type": rule["discount_type"],
                    "discount_value": rule["discount_value"],
                    "discount_code": _gen_discount_code(rule["type"], uid),
                    "min_cart": rule["min_cart"],
                    "target_category": cart_target_cat,
                    "valid_from": now.isoformat(),
                    "valid_to": (now + timedelta(days=3)).isoformat(),
                    # LLM context fields
                    "discount_description": disc_desc, "age": None,
                    "cart_items": cart_items,
                    "wishlist_items": wishlist[:5] if isinstance(wishlist, list) else [],
                    "order_count": oi.get("count", 0),
                    "last_order_summary": lo_summary,
                    "metal_trend": gold_trend,
                })

        # --- Complete profile nudge (no LLM needed) ---
        if not dob and not anniv and not ml_date and (uid, "complete_profile") not in existing:
            intents.append({
                "user_id": uid, "first_name": fname,
                "nudge_type": "complete_profile",
                "title": "🎁 Unlock Personalized Offers",
                "message": "Add your birthday and anniversary to receive exclusive discounts!",
                "discount_type": None, "discount_value": 0,
                "discount_code": None, "min_cart": 0,
                "valid_from": now.isoformat(),
                "valid_to": (now + timedelta(days=30)).isoformat(),
                "skip_llm": True,
            })

    # ── Admin-selected users: ensure at least one nudge per user ─────────
    if user_ids_filter:
        users_with_intents = {i["user_id"] for i in intents}
        for uid, fname, dob, anniv, ml_label, ml_date in users:
            if uid in users_with_intents:
                continue
            # This user was admin-selected but no standard rules triggered — create a targeted offer
            ud = user_data.get(uid, {"cart": [], "wishlist": []})
            cart = ud.get("cart", []) or []
            wishlist = ud.get("wishlist", []) or []
            cart_items = [item.get("product", {}) for item in cart if isinstance(item, dict)][:5]
            oi = orders_info.get(uid, {})
            lo_summary = _last_order_summary(uid)

            intents.append({
                "user_id": uid, "first_name": fname,
                "nudge_type": "targeted_offer",
                "title": "✨ Curated Just For You",
                "message": f"Hi {fname}, we have a special selection curated just for you! Explore our latest collection.",
                "discount_type": "cart_pct", "discount_value": 5,
                "discount_code": _gen_discount_code("targeted_offer", uid),
                "min_cart": 0,
                "target_category": _pick_target_category(cart_items, wishlist[:5] if isinstance(wishlist, list) else []),
                "valid_from": now.isoformat(),
                "valid_to": (now + timedelta(days=7)).isoformat(),
                # LLM context
                "discount_description": "5% off total cart — exclusive targeted offer",
                "age": None,
                "cart_items": cart_items,
                "wishlist_items": wishlist[:5] if isinstance(wishlist, list) else [],
                "order_count": oi.get("count", 0),
                "last_order_summary": lo_summary,
                "metal_trend": gold_trend,
            })
            logger.info("Created targeted_offer nudge for admin-selected user %d (%s)", uid, fname)

    if not intents:
        logger.info("No new nudges to create")
        return

    # ── Phase 2: LLM personalization (async) ───────────────────────────────
    sem = asyncio.Semaphore(5)
    llm_intents = [i for i in intents if not i.get("skip_llm")]
    static_intents = [i for i in intents if i.get("skip_llm")]

    if llm_intents:
        logger.info("Personalizing %d nudges via LLM", len(llm_intents))
        llm_results = await asyncio.gather(
            *[llm_personalize_nudge(i, sem) for i in llm_intents],
            return_exceptions=True,
        )
        # Replace failed coroutines with original intents (template fallback)
        finalized = []
        for i, result in enumerate(llm_results):
            if isinstance(result, Exception):
                logger.warning("LLM nudge task failed: %s, using template", result)
                finalized.append(llm_intents[i])
            else:
                finalized.append(result)
        intents = finalized + static_intents
    else:
        intents = static_intents

    # ── Phase 3: Insert all nudges into Lakebase (sync) ────────────────────
    def _insert(token):
        conn = _pg_connect(token)
        try:
            with conn.cursor() as cur:
                for it in intents:
                    cur.execute("""INSERT INTO brickjewels_nudges
                        (user_id, nudge_type, title, message, discount_type, discount_value,
                         discount_code, min_cart_value, target_category, valid_from, valid_to)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                        (it["user_id"], it["nudge_type"], it["title"], it["message"],
                         it.get("discount_type"), it.get("discount_value", 0),
                         it.get("discount_code"), it.get("min_cart", 0),
                         it.get("target_category"),
                         it["valid_from"], it["valid_to"]))
                conn.commit()
            return len(intents)
        finally:
            conn.close()

    try:
        count = await _run_db(_insert)
        logger.info("Nudge generation complete: %d nudges created (%d LLM-personalized)",
                     count, len(llm_intents))
    except Exception as exc:
        logger.error("Nudge generation failed (insert phase): %s", exc)


@app.get("/api/user/{user_id}/nudges")
async def get_user_nudges(user_id: int):
    """Get active nudges for a user."""
    def _q(token):
        conn = _pg_connect(token)
        try:
            import psycopg2.extras
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""SELECT nudge_id, nudge_type, title, message, discount_type,
                                      discount_value, discount_code, min_cart_value,
                                      target_category, valid_from, valid_to
                               FROM brickjewels_nudges
                               WHERE user_id=%s AND is_active=TRUE AND is_dismissed=FALSE
                                 AND is_redeemed=FALSE AND valid_to > NOW()
                               ORDER BY created_at DESC""", (user_id,))
                return [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()

    nudges = await _run_db(_q)
    for n in nudges:
        for k, v in n.items():
            if hasattr(v, "isoformat"):
                n[k] = v.isoformat()
            elif hasattr(v, "as_integer_ratio"):
                n[k] = float(v)
    return {"nudges": nudges}


@app.post("/api/user/{user_id}/nudges/{nudge_id}/dismiss")
async def dismiss_nudge(user_id: int, nudge_id: int):
    """Dismiss a nudge."""
    def _d(token):
        conn = _pg_connect(token)
        try:
            with conn.cursor() as cur:
                cur.execute("UPDATE brickjewels_nudges SET is_dismissed=TRUE WHERE nudge_id=%s AND user_id=%s", (nudge_id, user_id))
            conn.commit()
        finally:
            conn.close()
    await _run_db(_d)
    return {"status": "dismissed"}


@app.post("/api/user/{user_id}/nudges/{nudge_id}/apply")
async def apply_nudge(user_id: int, nudge_id: int, cart_total: int = 0):
    """Validate and compute discount for a nudge. Does NOT mark as redeemed —
    redemption happens only when the order is placed via POST /api/orders."""
    def _r(token):
        conn = _pg_connect(token)
        try:
            import psycopg2.extras
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""SELECT * FROM brickjewels_nudges
                               WHERE nudge_id=%s AND user_id=%s AND is_active=TRUE
                                 AND is_redeemed=FALSE AND valid_to > NOW()""", (nudge_id, user_id))
                nudge = cur.fetchone()
                if not nudge:
                    return None

                target_category = (nudge.get("target_category") or "").strip().lower()

                # Get user's cart
                cur.execute("""SELECT cart FROM brickjewels_user_data_scd2
                               WHERE user_id=%s AND is_active=TRUE
                               ORDER BY effective_from DESC LIMIT 1""", (user_id,))
                cart_row = cur.fetchone()
                cart = cart_row["cart"] if cart_row else []

                # Filter cart by target_category if set
                eligible_items = []
                for item in (cart or []):
                    prod = item.get("product", {})
                    if target_category:
                        item_cat = (prod.get("category") or "").strip().lower()
                        if item_cat != target_category:
                            continue
                    eligible_items.append(item)

                actual_cart_total = sum(
                    int(item.get("product", {}).get("price_inr", 0) or 0) * int(item.get("quantity", 1) or 1)
                    for item in (cart or [])
                )
                eligible_total = sum(
                    int(item.get("product", {}).get("price_inr", 0) or 0) * int(item.get("quantity", 1) or 1)
                    for item in eligible_items
                )

                if actual_cart_total < float(nudge["min_cart_value"] or 0):
                    return {"error": f"Minimum cart value of ₹{int(nudge['min_cart_value']):,} required"}

                if target_category and not eligible_items:
                    return {"error": f"No {nudge.get('target_category', '')} items in your cart. This offer applies to {nudge.get('target_category', '')} only."}

                discount_type = nudge["discount_type"]
                discount_value = float(nudge["discount_value"] or 0)
                discount_amount = 0

                if discount_type == "making_pct":
                    product_ids = [item.get("product", {}).get("product_id") for item in eligible_items if item.get("product", {}).get("product_id")]
                    quantities = {item.get("product", {}).get("product_id"): int(item.get("quantity", 1)) for item in eligible_items}
                    if product_ids:
                        placeholders = ",".join(["%s"] * len(product_ids))
                        cur.execute(f"""SELECT product_id, making_cost FROM brickjewels_product_prices
                                        WHERE product_id IN ({placeholders}) AND is_active=TRUE""", product_ids)
                        for row in cur.fetchall():
                            mc = float(row["making_cost"] or 0)
                            qty = quantities.get(row["product_id"], 1)
                            discount_amount += mc * qty * (discount_value / 100)
                elif discount_type == "cart_pct":
                    discount_amount = eligible_total * (discount_value / 100)
                elif discount_type == "flat_amount":
                    discount_amount = min(discount_value, eligible_total)

                discount_amount = round(discount_amount)

                return {
                    "nudge_id": nudge_id,
                    "discount_code": nudge["discount_code"],
                    "discount_type": discount_type,
                    "discount_value": discount_value,
                    "discount_amount": discount_amount,
                    "target_category": nudge.get("target_category"),
                    "cart_total": actual_cart_total,
                    "final_total": actual_cart_total - discount_amount,
                }
        finally:
            conn.close()

    result = await _run_db(_r)
    if result is None:
        raise HTTPException(status_code=404, detail="Nudge not found, expired, or already redeemed")
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result


@app.get("/api/nudge-email-history")
async def nudge_email_history():
    """Get nudge email sending history."""
    def _q(token):
        conn = _pg_connect(token)
        try:
            import psycopg2.extras
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(_CREATE_NUDGE_EMAILS_SQL)
                conn.commit()
                cur.execute("""SELECT email_id, user_id, recipient_email, subject, status,
                                      error_message, sent_at, created_at
                               FROM brickjewels_nudge_emails ORDER BY created_at DESC LIMIT 20""")
                rows = [dict(r) for r in cur.fetchall()]
                cur.execute("""SELECT COUNT(*) as total,
                                      COUNT(*) FILTER (WHERE status='sent') as sent,
                                      COUNT(*) FILTER (WHERE status='failed') as failed,
                                      COUNT(*) FILTER (WHERE status='pending') as pending
                               FROM brickjewels_nudge_emails""")
                stats = dict(cur.fetchone())
                return {"stats": stats, "recent": rows}
        finally:
            conn.close()

    result = await _run_db(_q)
    for r in result.get("recent", []):
        for k, v in r.items():
            if hasattr(v, "isoformat"):
                r[k] = v.isoformat()
    return result


@app.post("/api/nudges/generate")
async def trigger_nudge_generation():
    """Manually trigger nudge generation for all users."""
    await _generate_nudges()
    return {"status": "ok", "message": "Nudge generation triggered"}


# ---------------------------------------------------------------------------
# GIVA Concierge — Agentic Multi-Agent Supervisor
# A supervisor LLM (Claude) orchestrates specialized sub-agents via tool-calling:
#   • Discovery Agent      → semantic product search (vector_search)
#   • Pricing Agent        → live gold/silver-linked price breakdown
#   • Merchandising Agent  → catalog analytics (SQL, Genie-style)
#   • Vernacular Agent     → Sarvam translation for Indic languages
# Each tool call is recorded in a trace so the demo SHOWS the orchestration.
# ---------------------------------------------------------------------------

AGENT_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "discover_jewelry",
            "description": "Semantic search over GIVA's catalog for jewelry matching a natural-language description, occasion, style, metal or budget. Use whenever the shopper wants to find or browse pieces.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Natural-language description of what to find, e.g. 'minimal silver pendant for daily wear under 3000'"},
                    "max_results": {"type": "integer", "description": "How many pieces to return (default 6)"},
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "price_breakdown",
            "description": "Get the live, gold/silver-rate-linked price breakdown (metal cost, making charges, diamond, GST, final price) for a specific product_id. Use when a shopper asks why something costs what it does or about making charges.",
            "parameters": {
                "type": "object",
                "properties": {"product_id": {"type": "string"}},
                "required": ["product_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "catalog_analytics",
            "description": "Merchandising analytics over the GIVA catalog. metric is one of: 'price_by_category', 'material_mix', 'occasion_mix', 'top_collections'. Use for business/assortment questions like 'what's our average price by category' or 'which materials do we carry'.",
            "parameters": {
                "type": "object",
                "properties": {"metric": {"type": "string"}},
                "required": ["metric"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "translate",
            "description": "Translate text into an Indian language using Sarvam. language is e.g. 'Hindi', 'Tamil', 'Telugu', 'Bengali', 'Marathi', 'Kannada'. Use this to reply in the shopper's language when they write in a non-English Indic language.",
            "parameters": {
                "type": "object",
                "properties": {
                    "text": {"type": "string"},
                    "language": {"type": "string"},
                },
                "required": ["text", "language"],
            },
        },
    },
]

AGENT_SYSTEM_PROMPT = """You are the GIVA Concierge — the supervisor of a team of specialist AI agents for GIVA, an accessible-luxury Indian jewelry brand (925 silver, gold, lab-grown diamonds). You are powered by Databricks.

You orchestrate these sub-agents through tools:
- Discovery Agent (discover_jewelry): finds pieces by meaning, not keywords.
- Pricing Agent (price_breakdown): explains live gold/silver-rate-linked pricing & making charges.
- Merchandising Agent (catalog_analytics): answers assortment/business questions.
- Vernacular Agent (translate): replies in Indian languages.

Rules:
- Decide which agent(s) to call. You may call several in sequence (e.g. discover, then explain pricing).
- When recommending products, ALWAYS call discover_jewelry first — never invent products or prices.
- If the shopper writes in a non-English Indian language, compose your answer in English, then call translate to render it in their language and reply with the translation.
- Be warm, concise and elegant: 2-4 sentences. Use **bold** only for product names and prices (₹).
- You are a shopping concierge, not a coder — never mention SQL, tables, or internal IDs to the shopper."""


async def _agent_llm(messages: list, with_tools: bool = True) -> dict:
    """Call the Claude serving endpoint (OpenAI-style) and return the assistant message."""
    host = get_host()
    url = f"{host}/serving-endpoints/{LLM_MODEL}/invocations"
    headers = await get_headers()
    payload = {"messages": messages, "max_tokens": 700, "temperature": 0.3}
    if with_tools:
        payload["tools"] = AGENT_TOOLS
        payload["tool_choice"] = "auto"
    async with httpx.AsyncClient(timeout=60.0) as client:
        resp = await client.post(url, json=payload, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        return data["choices"][0]["message"]


async def _agent_tool_discover(args: dict) -> tuple[str, list]:
    query = args.get("query", "")
    n = int(args.get("max_results") or 6)
    try:
        emb = await get_embedding(query)
        hits = await vector_search(emb, num_results=max(n, 6))
        prods = []
        for h in hits[:n]:
            p = _catalog_by_id.get(h.get("product_id"))
            if p:
                prods.append(dict(p))
        prods = _apply_live_prices(prods)
        cards, lines = [], []
        for p in prods:
            price = int(float(p.get("price_inr") or 0))
            cards.append({
                "product_id": p.get("product_id"), "name": p.get("name"),
                "category": p.get("category"), "material": p.get("material"),
                "price_inr": price, "image_url": p.get("image_url"),
            })
            lines.append(f"{p.get('product_id')}: {p.get('name')} — {p.get('category')}, {p.get('material')}, ₹{price:,}")
        summary = "\n".join(lines) if lines else "No matching pieces found."
        return summary, cards
    except Exception as e:
        logger.warning("discover tool failed: %s", e)
        return f"Discovery failed: {e}", []


async def _agent_tool_price(args: dict) -> str:
    try:
        b = await get_product_price_breakdown(args.get("product_id", ""))
        if isinstance(b, dict):
            return json.dumps({k: b.get(k) for k in (
                "name", "metal_rate_per_gram", "metal_cost", "making_cost",
                "diamond_cost", "total_gst", "final_price") if k in b})
        return str(b)
    except Exception as e:
        return f"Price breakdown unavailable: {e}"


async def _agent_tool_analytics(args: dict) -> str:
    metric = (args.get("metric") or "price_by_category").lower()
    queries = {
        "price_by_category": f"SELECT category, COUNT(*) n, ROUND(AVG(price_inr)) avg_price, ROUND(MIN(price_inr)) min_price, ROUND(MAX(price_inr)) max_price FROM {PRODUCTS_TABLE} WHERE in_stock GROUP BY category ORDER BY n DESC",
        "material_mix": f"SELECT material, COUNT(*) n FROM {PRODUCTS_TABLE} WHERE in_stock GROUP BY material ORDER BY n DESC LIMIT 12",
        "occasion_mix": f"SELECT occasion, COUNT(*) n FROM {PRODUCTS_TABLE} WHERE in_stock GROUP BY occasion ORDER BY n DESC LIMIT 12",
        "top_collections": f"SELECT collection, COUNT(*) n, ROUND(AVG(price_inr)) avg_price FROM {PRODUCTS_TABLE} WHERE in_stock GROUP BY collection ORDER BY n DESC LIMIT 10",
    }
    sql = queries.get(next((k for k in queries if k in metric), "price_by_category"))
    try:
        rows = await run_sql(sql)
        return json.dumps(rows[:15], default=str)
    except Exception as e:
        return f"Analytics unavailable: {e}"


async def _agent_tool_translate(args: dict) -> str:
    host = get_host()
    url = f"{host}/serving-endpoints/{SARVAM_TRANSLATE_ENDPOINT}/invocations"
    headers = await get_headers()
    payload = {"messages": [
        {"role": "system", "content": f"Translate the text below to {args.get('language','Hindi')}."},
        {"role": "user", "content": args.get("text", "")},
    ], "max_tokens": 700, "temperature": 0.1}
    try:
        async with httpx.AsyncClient(timeout=90.0) as client:
            resp = await client.post(url, json=payload, headers=headers)
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, list):  # sarvam-translate returns a list-wrapped chat completion
                data = data[0]
            return data["choices"][0]["message"]["content"]
    except Exception as e:
        return f"[translation unavailable: {e}]"


_AGENT_LABELS = {
    "discover_jewelry": "Discovery Agent",
    "price_breakdown": "Pricing Agent",
    "catalog_analytics": "Merchandising Agent",
    "translate": "Vernacular Agent (Sarvam)",
}


class AgentChatRequest(BaseModel):
    message: str
    history: Optional[list] = None


@app.post("/api/agent/chat")
async def agent_chat(req: AgentChatRequest):
    """GIVA Concierge — agentic multi-agent supervisor endpoint."""
    messages = [{"role": "system", "content": AGENT_SYSTEM_PROMPT}]
    for h in (req.history or [])[-6:]:
        role = h.get("role")
        if role in ("user", "assistant") and h.get("content"):
            messages.append({"role": role, "content": h["content"]})
    messages.append({"role": "user", "content": req.message})

    trace, product_cards = [], []
    try:
        for _ in range(5):  # supervisor reasoning loop
            msg = await _agent_llm(messages, with_tools=True)
            tool_calls = msg.get("tool_calls") or []
            if not tool_calls:
                reply = (msg.get("content") or "").strip()
                return {"reply": reply, "trace": trace, "products": product_cards}
            messages.append(msg)
            for tc in tool_calls:
                fn = tc["function"]["name"]
                try:
                    args = json.loads(tc["function"].get("arguments") or "{}")
                except Exception:
                    args = {}
                if fn == "discover_jewelry":
                    result, cards = await _agent_tool_discover(args)
                    product_cards = cards or product_cards
                elif fn == "price_breakdown":
                    result = await _agent_tool_price(args)
                elif fn == "catalog_analytics":
                    result = await _agent_tool_analytics(args)
                elif fn == "translate":
                    result = await _agent_tool_translate(args)
                else:
                    result = "Unknown tool."
                trace.append({
                    "agent": _AGENT_LABELS.get(fn, fn),
                    "tool": fn,
                    "input": args,
                    "summary": (result[:240] if isinstance(result, str) else str(result)[:240]),
                })
                messages.append({"role": "tool", "tool_call_id": tc.get("id"), "content": result if isinstance(result, str) else json.dumps(result)})
        # Loop exhausted — ask for a final answer without tools
        final = await _agent_llm(messages, with_tools=False)
        return {"reply": (final.get("content") or "").strip(), "trace": trace, "products": product_cards}
    except Exception as e:
        logger.error("agent_chat failed: %s", e)
        return {"reply": "Sorry, the concierge had trouble just now. Please try again.", "trace": trace, "products": product_cards, "error": str(e)}


# ---------------------------------------------------------------------------
# Static file serving
# ---------------------------------------------------------------------------
_static_dir = Path(__file__).parent / "static"
if _static_dir.exists():
    app.mount("/assets", StaticFiles(directory=str(_static_dir / "assets")), name="assets")

    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str):
        index = _static_dir / "index.html"
        if index.exists():
            return FileResponse(str(index))
        return {"error": "Frontend not built"}
