# Databricks notebook source
# MAGIC %md
# MAGIC # BrickJewels — Continuous Data Generator
# MAGIC
# MAGIC Generates realistic transactional data every minute and writes to Lakebase.
# MAGIC Designed to run **indefinitely** on a minimum job cluster.
# MAGIC
# MAGIC **Generates per minute:**
# MAGIC - 1-3 new orders (referencing real products)
# MAGIC - ~20% chance of a new user signup
# MAGIC - ~30% chance of cart/wishlist updates
# MAGIC
# MAGIC **Source of truth:** Lakebase PostgreSQL → Lakeflow Connect CDC → Delta (dashboards/Genie)

# COMMAND ----------

import json
import random
import string
import time
import requests
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone, timedelta
from collections import defaultdict

IST = timezone(timedelta(hours=5, minutes=30))

# ── Config ─────────────────────────────────────────────────────────────────
LAKEBASE_HOST = "<your-lakebase-host>.database.cloud.databricks.com"
LAKEBASE_DB = "brickjewels"
LAKEBASE_PORT = 5432
WAREHOUSE_ID = "0f16ae8ffb7cdef3"

CATALOG = "main"
SCHEMA = "caratlane_jewelry"
PRODUCTS_TABLE = f"{CATALOG}.{SCHEMA}.enriched_jewelry_products"

SLEEP_SECONDS = 60  # 1 minute between cycles

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auth & Connection Setup

# COMMAND ----------

# SP credentials from Databricks Secrets
SP_CLIENT_ID = dbutils.secrets.get("brickjewels", "sp-client-id")
SP_CLIENT_SECRET = dbutils.secrets.get("brickjewels", "sp-client-secret")
WS_HOST = dbutils.secrets.get("brickjewels", "workspace-host")

def get_sp_token():
    """Get a fresh SP OAuth token."""
    resp = requests.post(
        f"{WS_HOST}/oidc/v1/token",
        data={"grant_type": "client_credentials", "client_id": SP_CLIENT_ID,
              "client_secret": SP_CLIENT_SECRET, "scope": "all-apis"},
        headers={"Content-Type": "application/x-www-form-urlencoded"}, timeout=15,
    )
    assert resp.status_code == 200, f"Token failed: {resp.text}"
    return resp.json()["access_token"]

def pg_connect(token):
    """Connect to Lakebase."""
    return psycopg2.connect(
        host=LAKEBASE_HOST, port=LAKEBASE_PORT, dbname=LAKEBASE_DB,
        user=SP_CLIENT_ID, password=token, sslmode="require", connect_timeout=15,
    )

print(f"SP Client ID: {SP_CLIENT_ID[:8]}...")
print(f"Workspace: {WS_HOST}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fetch Product Catalog (once at startup)

# COMMAND ----------

def fetch_products(token):
    """Fetch all products from UC via SQL Statement API."""
    url = f"{WS_HOST}/api/2.0/sql/statements"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {
        "warehouse_id": WAREHOUSE_ID,
        "statement": f"""SELECT product_id, name, category, subcategory, material,
                                occasion, CAST(price_inr AS INT) as price_inr, image_url
                         FROM {PRODUCTS_TABLE}""",
        "wait_timeout": "30s",
    }
    resp = requests.post(url, json=payload, headers=headers, timeout=60)
    data = resp.json()

    # Handle async polling
    if data.get("status", {}).get("state") == "PENDING":
        stmt_id = data["statement_id"]
        for _ in range(30):
            time.sleep(1)
            r = requests.get(f"{url}/{stmt_id}", headers=headers, timeout=15)
            data = r.json()
            if data.get("status", {}).get("state") != "PENDING":
                break

    columns = [c["name"] for c in data.get("manifest", {}).get("schema", {}).get("columns", [])]
    rows = data.get("result", {}).get("data_array", [])
    products = [dict(zip(columns, row)) for row in rows]
    print(f"Loaded {len(products)} products from catalog")
    return products

sp_token = get_sp_token()
PRODUCTS = fetch_products(sp_token)
assert len(PRODUCTS) > 0, "No products found!"

# Group products by category for smarter order generation
PRODUCTS_BY_CATEGORY = defaultdict(list)
for p in PRODUCTS:
    cat = p.get("category", "Unknown")
    PRODUCTS_BY_CATEGORY[cat].append(p)

CATEGORIES = list(PRODUCTS_BY_CATEGORY.keys())
print(f"Categories: {CATEGORIES}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Generation Helpers

# COMMAND ----------

FIRST_NAMES = [
    "Aarav", "Vivaan", "Aditya", "Vihaan", "Arjun", "Sai", "Reyansh", "Ayaan",
    "Krishna", "Ishaan", "Ananya", "Diya", "Myra", "Sara", "Aadhya", "Isha",
    "Kiara", "Anika", "Riya", "Nisha", "Priya", "Kavya", "Meera", "Zara",
    "Rohan", "Karan", "Neha", "Pooja", "Shreya", "Tanya", "Amit", "Rahul",
    "Sneha", "Divya", "Anjali", "Vikram", "Rajesh", "Sunita", "Manisha", "Deepak",
]
LAST_NAMES = [
    "Sharma", "Patel", "Singh", "Kumar", "Gupta", "Reddy", "Mehta", "Joshi",
    "Iyer", "Nair", "Chatterjee", "Desai", "Malhotra", "Kapoor", "Bose",
    "Agarwal", "Verma", "Rao", "Shah", "Thakur", "Pillai", "Menon", "Saxena",
]
OCCASIONS = ["Daily Wear", "Wedding Gift", "Anniversary", "Birthday", "Self Purchase", "Festive"]


def generate_order_id():
    now = datetime.now(IST)
    date_str = now.strftime("%y%m%d")
    rand = "".join(random.choices(string.ascii_uppercase + string.digits, k=6))
    return f"BJ-ORD-{date_str}-{rand}"


def generate_order(user_id, user_name, user_email):
    """Generate a single realistic order."""
    now = datetime.now(IST)

    # Pick 1-4 items, weighted toward 1-2
    num_items = random.choices([1, 2, 3, 4], weights=[40, 35, 15, 10])[0]

    # Prefer items from 1-2 categories (realistic shopping behavior)
    main_cat = random.choice(CATEGORIES)
    items = []
    total = 0

    for i in range(num_items):
        # 70% from main category, 30% from any
        if random.random() < 0.7 and PRODUCTS_BY_CATEGORY[main_cat]:
            product = random.choice(PRODUCTS_BY_CATEGORY[main_cat])
        else:
            product = random.choice(PRODUCTS)

        qty = random.choices([1, 2], weights=[85, 15])[0]
        price = int(float(product.get("price_inr", 5000) or 5000))

        items.append({
            "product_id": product["product_id"],
            "name": product["name"],
            "material": product.get("material", "Gold"),
            "price_inr": price,
            "quantity": qty,
            "image_url": product.get("image_url", ""),
            "category": product.get("category", ""),
        })
        total += price * qty

    statuses = ["confirmed"] * 60 + ["shipped"] * 30 + ["delivered"] * 10

    return {
        "order_id": generate_order_id(),
        "customer_name": user_name,
        "customer_email": user_email,
        "items": items,
        "total_amount": total,
        "status": random.choice(statuses),
        "created_at": now,
        "user_id": user_id,
    }


def generate_user():
    """Generate a single new user."""
    fn = random.choice(FIRST_NAMES)
    ln = random.choice(LAST_NAMES)
    email = f"{fn.lower()}.{ln.lower()}{random.randint(100, 9999)}@example.com"

    # 40% chance of DOB
    dob = None
    if random.random() < 0.4:
        from datetime import date
        dob = date(random.randint(1978, 2002), random.randint(1, 12), random.randint(1, 28))

    # 25% chance of anniversary
    anniv = None
    if random.random() < 0.25:
        from datetime import date
        anniv = date(random.randint(2015, 2025), random.randint(1, 12), random.randint(1, 28))

    return {
        "first_name": fn, "last_name": ln, "email": email,
        "password_hash": "demo_hash_" + "".join(random.choices(string.ascii_lowercase, k=8)),
        "country_code": "+91",
        "mobile": f"9{random.randint(100000000, 999999999)}",
        "date_of_birth": dob,
        "anniversary_date": anniv,
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Loop — Runs Forever

# COMMAND ----------

cycle = 0
total_orders = 0
total_users = 0
total_cart_updates = 0
token_refresh_interval = 50  # Refresh token every ~50 minutes

print(f"Starting continuous data generator at {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S IST')}")
print(f"Cycle interval: {SLEEP_SECONDS}s")
print("=" * 60)

while True:
    cycle += 1
    cycle_start = datetime.now(IST)

    try:
        # Refresh SP token periodically (tokens expire after ~1 hour)
        if cycle % token_refresh_interval == 1:
            sp_token = get_sp_token()
            print(f"[Cycle {cycle}] Token refreshed")

        conn = pg_connect(sp_token)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # ── Fetch existing users for order assignment ──
        cur.execute("SELECT user_id, first_name, last_name, email FROM brickjewels_users")
        existing_users = cur.fetchall()

        if not existing_users:
            print(f"[Cycle {cycle}] No users found, creating seed user...")
            new_u = generate_user()
            cur.execute("""INSERT INTO brickjewels_users
                (first_name, last_name, email, password_hash, country_code, mobile,
                 date_of_birth, anniversary_date)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING user_id""",
                (new_u["first_name"], new_u["last_name"], new_u["email"],
                 new_u["password_hash"], new_u["country_code"], new_u["mobile"],
                 new_u["date_of_birth"], new_u["anniversary_date"]))
            uid = cur.fetchone()["user_id"]
            conn.commit()
            existing_users = [{"user_id": uid, "first_name": new_u["first_name"],
                              "last_name": new_u["last_name"], "email": new_u["email"]}]
            total_users += 1

        actions = []

        # ── Generate 1-3 orders ──
        num_orders = random.choices([1, 2, 3], weights=[50, 35, 15])[0]
        for _ in range(num_orders):
            user = random.choice(existing_users)
            name = f"{user['first_name']} {user['last_name']}"
            order = generate_order(user["user_id"], name, user["email"])

            cur.execute("""INSERT INTO brickjewels_orders
                (order_id, customer_name, customer_email, items, total_amount, status, created_at, user_id)
                VALUES (%s,%s,%s,%s::jsonb,%s,%s,%s,%s)""",
                (order["order_id"], order["customer_name"], order["customer_email"],
                 json.dumps(order["items"]), order["total_amount"], order["status"],
                 order["created_at"], order["user_id"]))
            total_orders += 1
        actions.append(f"{num_orders} orders")

        # ── Occasionally create a new user (~20% chance) ──
        if random.random() < 0.20:
            new_u = generate_user()
            cur.execute("""INSERT INTO brickjewels_users
                (first_name, last_name, email, password_hash, country_code, mobile,
                 date_of_birth, anniversary_date)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (email) DO NOTHING RETURNING user_id""",
                (new_u["first_name"], new_u["last_name"], new_u["email"],
                 new_u["password_hash"], new_u["country_code"], new_u["mobile"],
                 new_u["date_of_birth"], new_u["anniversary_date"]))
            row = cur.fetchone()
            if row:
                total_users += 1
                actions.append("1 new user")

        # ── Occasionally update a cart (~30% chance) ──
        if random.random() < 0.30:
            user = random.choice(existing_users)
            uid = user["user_id"]
            num_cart_items = random.randint(1, 4)
            cart_products = random.sample(PRODUCTS, min(num_cart_items, len(PRODUCTS)))
            cart = [{"product": {
                "product_id": p["product_id"], "name": p["name"],
                "material": p.get("material", "Gold"),
                "price_inr": int(float(p.get("price_inr", 5000) or 5000)),
                "image_url": p.get("image_url", ""), "category": p.get("category", ""),
                "description": "",
            }, "quantity": random.choice([1, 2])} for p in cart_products]

            # Also maybe add wishlist items
            wishlist = []
            if random.random() < 0.5:
                wl_products = random.sample(PRODUCTS, min(random.randint(2, 5), len(PRODUCTS)))
                wishlist = [{"product_id": p["product_id"], "name": p["name"],
                            "material": p.get("material", "Gold"),
                            "price_inr": int(float(p.get("price_inr", 5000) or 5000)),
                            "image_url": p.get("image_url", ""), "category": p.get("category", "")}
                           for p in wl_products]

            # SCD2 update: expire old, insert new
            cur.execute("""UPDATE brickjewels_user_data_scd2
                SET is_active = FALSE, effective_to = NOW()
                WHERE user_id = %s AND is_active = TRUE""", (uid,))

            # Get existing wishlist if we're not replacing it
            if not wishlist:
                cur.execute("""SELECT wishlist FROM brickjewels_user_data_scd2
                    WHERE user_id = %s ORDER BY effective_from DESC LIMIT 1""", (uid,))
                row = cur.fetchone()
                if row and row.get("wishlist"):
                    wl = row["wishlist"]
                    wishlist = wl if isinstance(wl, list) else []

            cur.execute("""INSERT INTO brickjewels_user_data_scd2
                (user_id, cart, wishlist, is_active, effective_from, updated_at)
                VALUES (%s, %s::jsonb, %s::jsonb, TRUE, NOW(), NOW())""",
                (uid, json.dumps(cart), json.dumps(wishlist)))
            total_cart_updates += 1
            actions.append("1 cart update")

        conn.commit()
        cur.close()
        conn.close()

        elapsed = (datetime.now(IST) - cycle_start).total_seconds()
        print(f"[Cycle {cycle:>4}] {cycle_start.strftime('%H:%M:%S')} | {', '.join(actions):30s} | "
              f"Totals: {total_orders} orders, {total_users} users, {total_cart_updates} carts | {elapsed:.1f}s")

    except Exception as e:
        print(f"[Cycle {cycle:>4}] ERROR: {e}")
        # On connection errors, force token refresh next cycle
        if "authentication" in str(e).lower() or "connection" in str(e).lower():
            sp_token = get_sp_token()
            print(f"[Cycle {cycle}] Token force-refreshed after error")

    # Sleep until next cycle
    time.sleep(SLEEP_SECONDS)
