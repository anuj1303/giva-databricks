# Databricks notebook source
# MAGIC %md
# MAGIC # BrickJewels — Lakebase CDC Ingestion
# MAGIC
# MAGIC Delta Live Tables pipeline ingesting BrickJewels operational data from Lakebase PostgreSQL
# MAGIC into Unity Catalog streaming tables for analytics dashboards and Genie.
# MAGIC
# MAGIC **Source**: Lakebase PostgreSQL (`brickjewels` database)
# MAGIC **Target**: `main.brickjewels_analytics`

# COMMAND ----------

import dlt
import requests
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Lakebase connection
LB_HOST = "<your-lakebase-host>.database.cloud.databricks.com"
LB_DATABASE = "brickjewels"
LB_PORT = "5432"
JDBC_URL = f"jdbc:postgresql://{LB_HOST}:{LB_PORT}/{LB_DATABASE}?sslmode=require"

# SP credentials from secrets (client_id + client_secret are long-lived; tokens are not)
_SP_CLIENT_ID = spark.conf.get("spark.databricks.lakebase.user", "")
_WS_HOST = "<your-workspace-host>"

# Token cache (refreshed on each batch since tokens expire after ~1 hour)
_token_cache = {"token": None, "expires": 0}

def _get_fresh_token():
    """Generate a fresh SP OAuth token using client_credentials flow."""
    import time
    now = time.time()
    if _token_cache["token"] and now < _token_cache["expires"] - 60:
        return _token_cache["token"]

    sp_secret = dbutils.secrets.get("brickjewels", "sp-client-secret")
    resp = requests.post(
        f"{_WS_HOST}/oidc/v1/token",
        data={"grant_type": "client_credentials", "client_id": _SP_CLIENT_ID,
              "client_secret": sp_secret, "scope": "all-apis"},
        headers={"Content-Type": "application/x-www-form-urlencoded"}, timeout=15,
    )
    assert resp.status_code == 200, f"SP token failed: {resp.text}"
    token = resp.json()["access_token"]
    _token_cache["token"] = token
    _token_cache["expires"] = now + 3500  # ~58 minutes
    return token

def _read_lb(table_name):
    """Read a Lakebase table via JDBC with a fresh token."""
    token = _get_fresh_token()
    return (
        spark.read
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", table_name)
        .option("driver", "org.postgresql.Driver")
        .option("fetchsize", "10000")
        .option("user", _SP_CLIENT_ID)
        .option("password", token)
        .load()
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Users

# COMMAND ----------

@dlt.table(
    name="dim_users",
    comment="BrickJewels registered users with milestone dates for nudge targeting",
    table_properties={"quality": "silver", "source": "lakebase_postgresql"}
)
def dim_users():
    return (
        _read_lb("brickjewels_users")
        .select("user_id", "first_name", "last_name", "email",
                "country_code", "mobile",
                "date_of_birth", "anniversary_date",
                "milestone_label", "milestone_date", "created_at")
        .withColumn("_ingested_at", F.current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Orders

# COMMAND ----------

@dlt.table(
    name="fact_orders",
    comment="All BrickJewels orders with status and amount",
    table_properties={"quality": "silver", "source": "lakebase_postgresql"}
)
def fact_orders():
    return (
        _read_lb("brickjewels_orders")
        .select("order_id", "user_id", "customer_name", "customer_email",
                "items", "total_amount", "status", "created_at")
        .withColumn("order_date", F.to_date("created_at"))
        .withColumn("item_count",
                     F.size(F.from_json("items", ArrayType(
                         StructType([StructField("product_id", StringType())])
                     ))))
        .withColumn("_ingested_at", F.current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Order Items (denormalized from JSONB)

# COMMAND ----------

@dlt.table(
    name="fact_order_items",
    comment="Denormalized order items — one row per product per order, for category and product analytics",
    table_properties={"quality": "silver", "source": "lakebase_postgresql"}
)
def fact_order_items():
    item_schema = ArrayType(StructType([
        StructField("product_id", StringType()),
        StructField("name", StringType()),
        StructField("category", StringType()),
        StructField("material", StringType()),
        StructField("price_inr", LongType()),
        StructField("quantity", IntegerType()),
        StructField("image_url", StringType()),
    ]))
    return (
        _read_lb("brickjewels_orders")
        .select("order_id", "user_id", "created_at",
                F.from_json("items", item_schema).alias("parsed_items"))
        .withColumn("item", F.explode("parsed_items"))
        .select(
            "order_id", "user_id",
            F.to_date("created_at").alias("order_date"),
            F.col("item.product_id").alias("product_id"),
            F.col("item.name").alias("product_name"),
            F.col("item.category").alias("category"),
            F.col("item.material").alias("material"),
            F.col("item.quantity").alias("quantity"),
            F.col("item.price_inr").alias("unit_price"),
            (F.col("item.price_inr") * F.col("item.quantity")).alias("line_total"),
        )
        .withColumn("_ingested_at", F.current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metal Prices

# COMMAND ----------

@dlt.table(
    name="fact_metal_prices",
    comment="Historical gold and silver prices with daily % change",
    table_properties={"quality": "silver", "source": "lakebase_postgresql"}
)
def fact_metal_prices():
    return (
        _read_lb("brickjewels_metal_prices")
        .select("metal", "price_gram_24k", "price_gram_22k", "price_gram_18k",
                "price_gram_14k", "pct_change_24k", "is_active", "fetched_at")
        .withColumn("price_date", F.to_date("fetched_at"))
        .withColumn("_ingested_at", F.current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Nudges

# COMMAND ----------

@dlt.table(
    name="fact_nudges",
    comment="Personalized nudge offers — tracks generation, redemption, and dismissal",
    table_properties={"quality": "silver", "source": "lakebase_postgresql"}
)
def fact_nudges():
    return (
        _read_lb("brickjewels_nudges")
        .select("nudge_id", "user_id", "nudge_type", "discount_type",
                "discount_value", "target_category",
                "is_active", "is_redeemed", "is_dismissed",
                "created_at", "redeemed_at", "valid_from", "valid_to")
        .withColumn("_ingested_at", F.current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carts (Active Snapshots)

# COMMAND ----------

@dlt.table(
    name="fact_carts",
    comment="Active cart items per user — denormalized from SCD2 JSONB",
    table_properties={"quality": "silver", "source": "lakebase_postgresql"}
)
def fact_carts():
    item_schema = ArrayType(StructType([
        StructField("product", StructType([
            StructField("product_id", StringType()),
            StructField("name", StringType()),
            StructField("category", StringType()),
            StructField("material", StringType()),
            StructField("price_inr", LongType()),
        ])),
        StructField("quantity", IntegerType()),
    ]))
    return (
        _read_lb("brickjewels_user_data_scd2")
        .filter("is_active = TRUE")
        .select("user_id", "updated_at",
                F.from_json("cart", item_schema).alias("parsed_cart"))
        .withColumn("item", F.explode("parsed_cart"))
        .select(
            "user_id",
            F.to_date("updated_at").alias("snapshot_date"),
            F.col("item.product.product_id").alias("product_id"),
            F.col("item.product.name").alias("product_name"),
            F.col("item.product.category").alias("category"),
            F.col("item.product.material").alias("material"),
            F.col("item.quantity").alias("quantity"),
            F.col("item.product.price_inr").alias("price_inr"),
        )
        .filter("product_id IS NOT NULL")
        .withColumn("_ingested_at", F.current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wishlists (Active Snapshots)

# COMMAND ----------

@dlt.table(
    name="fact_wishlists",
    comment="Active wishlist items per user — denormalized from SCD2 JSONB",
    table_properties={"quality": "silver", "source": "lakebase_postgresql"}
)
def fact_wishlists():
    wl_schema = ArrayType(StructType([
        StructField("product_id", StringType()),
        StructField("name", StringType()),
        StructField("category", StringType()),
        StructField("material", StringType()),
        StructField("price_inr", LongType()),
    ]))
    return (
        _read_lb("brickjewels_user_data_scd2")
        .filter("is_active = TRUE")
        .select("user_id", "updated_at",
                F.from_json("wishlist", wl_schema).alias("parsed_wl"))
        .withColumn("item", F.explode("parsed_wl"))
        .select(
            "user_id",
            F.to_date("updated_at").alias("snapshot_date"),
            F.col("item.product_id").alias("product_id"),
            F.col("item.name").alias("product_name"),
            F.col("item.category").alias("category"),
            F.col("item.material").alias("material"),
            F.col("item.price_inr").alias("price_inr"),
        )
        .filter("product_id IS NOT NULL")
        .withColumn("_ingested_at", F.current_timestamp())
    )
