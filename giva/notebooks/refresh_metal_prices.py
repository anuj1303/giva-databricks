# Databricks notebook source
# MAGIC %md
# MAGIC # GIVA — Metal Price Refresh & Product Price Recomputation
# MAGIC
# MAGIC Triggered by the **GIVA Price Refresh** workflow (9:01am + 3:31pm IST daily).
# MAGIC Can also be run manually. Uses SP credentials from Databricks Secrets.
# MAGIC
# MAGIC **Flow:** Yahoo Finance (COMEX × USD/INR × India premium) → MCX-equivalent prices
# MAGIC → Lakebase metal_prices (SCD2) → Compute product prices → Lakebase product_prices (SCD2)

# COMMAND ----------

# MAGIC %pip install psycopg2-binary

# COMMAND ----------

import requests, json, re, hashlib, random as rng_module
from datetime import datetime, timezone, timedelta

IST = timezone(timedelta(hours=5, minutes=30))
NOW_IST = datetime.now(IST)
now_ist_str = NOW_IST.isoformat()
print(f"Run started: {NOW_IST.strftime('%Y-%m-%d %H:%M:%S IST')}")

# Time-gate: Only run at 9:01 AM IST (3:31 UTC) or 3:31 PM IST (10:01 UTC)
# The cron fires 4 times (3:01, 3:31, 10:01, 10:31 UTC) — skip the 2 unwanted runs.
# Manual/admin triggers always run (no widget = not scheduled).
_is_scheduled = False
try:
    dbutils.widgets.get("__internal_trigger")  # will fail for scheduled runs too, but let's check
    _is_scheduled = False
except:
    _is_scheduled = True  # Assume scheduled unless admin-triggered

_now_utc = datetime.now(timezone.utc)
_ALLOWED_SLOTS = [(3, 31), (10, 1)]  # (hour, minute) in UTC
_in_window = any(abs(_now_utc.hour - h) == 0 and abs(_now_utc.minute - m) <= 5 for h, m in _ALLOWED_SLOTS)

# If this looks like a scheduled run at the wrong time, skip
if _is_scheduled and not _in_window:
    print(f"Skipping — current UTC time {_now_utc.strftime('%H:%M')} is not in allowed slots {_ALLOWED_SLOTS}")
    dbutils.notebook.exit("skipped — not a scheduled slot")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config & Auth

# COMMAND ----------

# Yahoo Finance tickers
YF_TICKERS = {"Gold": "GC=F", "Silver": "SI=F"}
YF_FX_TICKER = "INR=X"
TROY_OZ_GRAMS = 31.1035
INDIA_PREMIUM = 1.10  # ~10% customs + GST to approximate MCX

# Karat purity ratios
KARAT_PURITY = {
    "24k": 1.0, "22k": 22/24, "21k": 21/24, "20k": 20/24,
    "18k": 18/24, "16k": 16/24, "14k": 14/24, "10k": 10/24,
}
KARAT_COLS = list(KARAT_PURITY.keys())

LAKEBASE_HOST = "{{LAKEBASE_HOST}}"
LAKEBASE_DB = "giva"

CATALOG = "{{CATALOG}}"
SCHEMA = "{{SCHEMA}}"
PRODUCTS_TABLE = f"{CATALOG}.{SCHEMA}.products"

MAKING_RATES = {"necklace": 0.40, "ring": 0.35, "earring": 0.35, "bangle": 0.30, "bracelet": 0.35, "pendant": 0.40}
METAL_GST = 0.03
MAKING_GST = 0.28

SP_CLIENT_ID = dbutils.secrets.get("giva", "sp-client-id")
SP_CLIENT_SECRET = dbutils.secrets.get("giva", "sp-client-secret")
WS_HOST = dbutils.secrets.get("giva", "workspace-host")
print(f"SP Client ID: {SP_CLIENT_ID[:8]}...")

# COMMAND ----------

# Get SP OAuth token for Lakebase
token_resp = requests.post(
    f"{WS_HOST}/oidc/v1/token",
    data={"grant_type": "client_credentials", "client_id": SP_CLIENT_ID,
          "client_secret": SP_CLIENT_SECRET, "scope": "all-apis"},
    headers={"Content-Type": "application/x-www-form-urlencoded"}, timeout=15,
)
assert token_resp.status_code == 200, f"OAuth failed: {token_resp.text}"
sp_token = token_resp.json()["access_token"]
print(f"SP OAuth token acquired ({len(sp_token)} chars)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Fetch MCX-Equivalent Metal Prices via Yahoo Finance

# COMMAND ----------

def fetch_yahoo_price(ticker):
    """Fetch latest price from Yahoo Finance chart API (no yfinance library needed)."""
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}
    resp = requests.get(url, params={"interval": "1d", "range": "1d"}, headers=headers, timeout=15)
    assert resp.status_code == 200, f"Yahoo Finance {ticker}: HTTP {resp.status_code}"
    result = resp.json().get("chart", {}).get("result", [])
    assert result, f"Yahoo Finance {ticker}: empty result"
    price = result[0].get("meta", {}).get("regularMarketPrice", 0)
    assert price, f"Yahoo Finance {ticker}: no price returned"
    return float(price)

# Fetch USD/INR exchange rate
usd_inr = fetch_yahoo_price(YF_FX_TICKER)
print(f"USD/INR rate: ₹{usd_inr:.2f}")

# Fetch metals
fetched = {}
for metal, ticker in YF_TICKERS.items():
    price_usd_oz = fetch_yahoo_price(ticker)
    price_inr_oz = price_usd_oz * usd_inr * INDIA_PREMIUM
    price_inr_gram_24k = price_inr_oz / TROY_OZ_GRAMS

    data = {"price": price_inr_oz}
    for karat, purity in KARAT_PURITY.items():
        data[f"price_gram_{karat}"] = round(price_inr_gram_24k * purity, 2)

    fetched[metal] = data
    print(f"  {metal}: COMEX ${price_usd_oz:,.2f}/oz → MCX-equiv 24K=₹{data['price_gram_24k']:,.2f}/g, 22K=₹{data['price_gram_22k']:,.2f}/g")

print(f"\nMCX-equivalent Gold per 10g: ₹{fetched['Gold']['price_gram_24k'] * 10:,.0f}")
print(f"MCX-equivalent Silver per kg: ₹{fetched['Silver']['price_gram_24k'] * 1000:,.0f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Store Metal Prices (SCD2)

# COMMAND ----------

import psycopg2

conn = psycopg2.connect(host=LAKEBASE_HOST, port=5432, dbname=LAKEBASE_DB,
                         user=SP_CLIENT_ID, password=sp_token, sslmode="require", connect_timeout=15)
cur = conn.cursor()

for metal, data in fetched.items():
    cur.execute("""SELECT price_gram_24k, price_gram_22k, price_gram_21k, price_gram_20k,
                          price_gram_18k, price_gram_16k, price_gram_14k, price_gram_10k
                   FROM brickjewels_metal_prices WHERE metal=%s AND is_active=TRUE
                   ORDER BY effective_from DESC LIMIT 1""", (metal,))
    prev = cur.fetchone()

    pcts = {}
    for i, k in enumerate(KARAT_COLS):
        nv, ov = data.get(f"price_gram_{k}"), (prev[i] if prev else None)
        pcts[k] = round(((float(nv)-float(ov))/float(ov))*100, 4) if ov and nv and float(ov)>0 else 0

    cur.execute("UPDATE brickjewels_metal_prices SET is_active=FALSE, effective_to=%s WHERE metal=%s AND is_active=TRUE",
                (now_ist_str, metal))
    cur.execute("""INSERT INTO brickjewels_metal_prices
        (metal, currency, price_gram_24k, price_gram_22k, price_gram_21k, price_gram_20k,
         price_gram_18k, price_gram_16k, price_gram_14k, price_gram_10k,
         pct_change_24k, pct_change_22k, pct_change_21k, pct_change_20k,
         pct_change_18k, pct_change_16k, pct_change_14k, pct_change_10k,
         price_per_gram, price_per_oz, is_active, effective_from, effective_to, fetched_at)
        VALUES (%s,'INR',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,TRUE,%s,NULL,%s)""",
        (metal, *[data.get(f"price_gram_{k}") for k in KARAT_COLS],
         *[pcts[k] for k in KARAT_COLS], data.get("price_gram_24k"), data.get("price"), now_ist_str, now_ist_str))
    print(f"  {metal}: SCD2 updated (24K Δ={pcts['24k']:+.4f}%)")

conn.commit()
print("Metal prices stored ✓")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Compute & Store Product Prices (SCD2)

# COMMAND ----------

cur.execute("DROP TABLE IF EXISTS brickjewels_product_prices")
cur.execute("""CREATE TABLE IF NOT EXISTS brickjewels_product_prices (
    id SERIAL PRIMARY KEY, product_id TEXT NOT NULL, karat TEXT, metal_type TEXT,
    weight_grams NUMERIC, category TEXT, metal_rate_per_gram NUMERIC DEFAULT 0,
    metal_cost NUMERIC DEFAULT 0, diamond_cost NUMERIC DEFAULT 0,
    making_pct NUMERIC DEFAULT 0, making_cost NUMERIC DEFAULT 0,
    discount_pct NUMERIC DEFAULT 0, discount_value NUMERIC DEFAULT 0,
    gst_pct_metal_diamond NUMERIC DEFAULT 3, gst_cost_metal_diamond NUMERIC DEFAULT 0,
    gst_pct_making NUMERIC DEFAULT 28, gst_cost_making NUMERIC DEFAULT 0,
    total_before_gst NUMERIC DEFAULT 0, total_gst NUMERIC DEFAULT 0,
    final_price NUMERIC DEFAULT 0, is_active BOOLEAN DEFAULT TRUE,
    effective_from TIMESTAMPTZ NOT NULL, effective_to TIMESTAMPTZ, computed_at TIMESTAMPTZ NOT NULL)""")
cur.execute("CREATE INDEX IF NOT EXISTS idx_product_prices_active ON brickjewels_product_prices (product_id, is_active) WHERE is_active = TRUE")
conn.commit()
print("Product prices table schema ensured ✓")

products = [row.asDict() for row in spark.sql(f"SELECT product_id, material, weight_grams, category FROM {PRODUCTS_TABLE} WHERE in_stock=true").collect()]
print(f"Loaded {len(products)} products from UC")

metal_lookup = {name.lower(): {k: float(data.get(f"price_gram_{k}", 0)) for k in KARAT_COLS} for name, data in fetched.items()}

def parse_mat(m):
    m = (m or "").lower()
    km = re.search(r'(\d+)\s*k', m)
    karat = int(km.group(1)) if km else (24 if "platinum" in m else 18 if "rose gold" in m or "white gold" in m else 22 if "gold" in m else 18)
    metal = "silver" if "silver" in m else "gold"
    return karat, metal, "diamond" in m or "solitaire" in m

def k2c(k):
    return {24:"24k",22:"22k",21:"21k",20:"20k",18:"18k",16:"16k",14:"14k",10:"10k"}.get(k,"18k")

def dprice(pid):
    return rng_module.Random(int(hashlib.md5(pid.encode()).hexdigest()[:8],16)).randint(60000,180000)

cur.execute("UPDATE brickjewels_product_prices SET is_active=FALSE, effective_to=%s WHERE is_active=TRUE", (now_ist_str,))

count = 0
for p in products:
    pid, w = p["product_id"], float(p.get("weight_grams") or 0)
    cat = (p.get("category") or "").lower()
    karat, metal, has_d = parse_mat(p.get("material",""))
    rate = metal_lookup.get(metal, metal_lookup.get("gold",{})).get(k2c(karat), 0)
    mc = round(w * rate, 2)
    dc = dprice(pid) if has_d else 0
    mp = MAKING_RATES.get(cat, 0.35) * 100
    mk = round(mc * mp/100, 2)
    gmd = round((mc+dc)*METAL_GST, 2)
    gmk = round(mk*MAKING_GST, 2)
    tbg = round(mc+dc+mk, 2)
    tg = round(gmd+gmk, 2)
    fp = round(tbg+tg, 0)

    cur.execute("""INSERT INTO brickjewels_product_prices
        (product_id, karat, metal_type, weight_grams, category,
         metal_rate_per_gram, metal_cost, diamond_cost, making_pct, making_cost,
         discount_pct, discount_value, gst_pct_metal_diamond, gst_cost_metal_diamond,
         gst_pct_making, gst_cost_making, total_before_gst, total_gst, final_price,
         is_active, effective_from, effective_to, computed_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,0,0,%s,%s,%s,%s,%s,%s,%s,TRUE,%s,NULL,%s)""",
        (pid, f"{karat}K", metal, w, cat, rate, mc, dc, mp, mk,
         METAL_GST*100, gmd, MAKING_GST*100, gmk, tbg, tg, fp, now_ist_str, now_ist_str))
    count += 1

conn.commit()
print(f"Product prices computed & stored ✓ ({count} products)")

cur.execute("""SELECT product_id, karat, metal_cost, diamond_cost, making_cost,
                      gst_cost_metal_diamond+gst_cost_making as total_gst, final_price
               FROM brickjewels_product_prices WHERE is_active=TRUE ORDER BY final_price DESC LIMIT 5""")
print("\nTop 5 by price:")
for r in cur.fetchall():
    print(f"  {r[0]}: {r[1]} metal=₹{r[2]:,.0f} diamond=₹{r[3]:,.0f} making=₹{r[4]:,.0f} gst=₹{r[5]:,.0f} → ₹{r[6]:,.0f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Notify App to Refresh Price Cache

# COMMAND ----------

APP_URL = ""
try:
    r = requests.get(f"{APP_URL}/api/product-price-cache-status",
                     headers={"Authorization": f"Bearer {sp_token}"}, timeout=10)
    print(f"App cache status: {r.status_code} — {r.text[:200]}")
except Exception as e:
    print(f"App notification skipped (non-critical): {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

cur.close()
conn.close()

secs = (datetime.now(IST) - NOW_IST).total_seconds()
print(f"\n{'='*60}")
print(f"GIVA Price Refresh COMPLETE ({secs:.1f}s)")
print(f"  Source: Yahoo Finance (COMEX × USD/INR × India premium)")
print(f"  USD/INR: ₹{usd_inr:.2f}")
for metal, data in fetched.items():
    print(f"  {metal} 24K: ₹{data['price_gram_24k']:,.2f}/gram")
print(f"  Products repriced: {count}")
print(f"  Finished: {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S IST')}")
print(f"{'='*60}")
