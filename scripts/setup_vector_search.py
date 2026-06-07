"""
BrickJewels Demo - Vector Search Setup
Rebuilds embeddings and Vector Search index from scratch
"""
import os
import time

CATALOG = "anuj_vm_workspace_catalog"
SCHEMA = "caratlane_jewelry"
PRODUCTS_TABLE = f"{CATALOG}.{SCHEMA}.enriched_jewelry_products"
EMBEDDINGS_TABLE = f"{CATALOG}.{SCHEMA}.jewelry_embeddings"
VS_ENDPOINT_NAME = "tanishq-jewelry-vs"  # reuse existing endpoint
VS_INDEX_NAME = f"{CATALOG}.{SCHEMA}.jewelry_embeddings_index"
EMBEDDING_MODEL = "databricks-gte-large-en"

# Initialize Spark
if os.environ.get('DATABRICKS_RUNTIME_VERSION'):
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    dbutils_available = True
else:
    from databricks.connect import DatabricksSession, DatabricksEnv
    env = DatabricksEnv().withDependencies("jmespath==1.0.1").withDependencies("httpx==0.28.1")
    spark = DatabricksSession.builder.profile("AnujLathi").serverless(True).withEnvironment(env).getOrCreate()
    dbutils_available = False

print(f"Spark version: {spark.version}")

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.vectorsearch import (
    EndpointType,
    VectorIndexType,
    DeltaSyncVectorIndexSpecRequest,
    EmbeddingVectorColumn,
    PipelineType,
)
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, FloatType
import httpx

w = WorkspaceClient(profile="AnujLathi")
host = w.config.host

# Get a fresh OAuth token via the Databricks CLI (handles browser/OAuth auth flows)
import subprocess, json
_token_data = json.loads(subprocess.check_output(
    ["databricks", "--profile", "AnujLathi", "auth", "token", "--output", "json"],
    stderr=subprocess.DEVNULL
))
token = _token_data["access_token"]

print(f"Connected to workspace: {host}")
print(f"Token acquired ({len(token)} chars)")


def get_embeddings_batch(texts: list[str]) -> list[list[float]]:
    """Get embeddings for a batch of texts using Foundation Model API."""
    url = f"{host}/serving-endpoints/{EMBEDDING_MODEL}/invocations"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    all_embeddings = []
    batch_size = 50

    for i in range(0, len(texts), batch_size):
        batch = texts[i:i + batch_size]
        payload = {"input": batch}
        print(f"  Embedding batch {i//batch_size + 1}/{(len(texts)+batch_size-1)//batch_size} ({len(batch)} texts)...")

        with httpx.Client(timeout=60.0) as client:
            resp = client.post(url, json=payload, headers=headers)
            if resp.status_code != 200:
                print(f"  Embedding error: {resp.text}")
                all_embeddings.extend([[0.0] * 1024] * len(batch))
                continue

            data = resp.json()
            embeddings = [item["embedding"] for item in sorted(data["data"], key=lambda x: x["index"])]
            all_embeddings.extend(embeddings)

        time.sleep(0.3)

    return all_embeddings


# ---------------------------------------------------------------------------
# Step 1: Drop old index if exists
# ---------------------------------------------------------------------------
print(f"\nStep 1: Dropping existing VS index '{VS_INDEX_NAME}' if it exists...")
try:
    w.vector_search_indexes.delete_index(VS_INDEX_NAME)
    print("  Old index deleted.")
    time.sleep(5)
except Exception as e:
    if "not found" in str(e).lower() or "does not exist" in str(e).lower():
        print("  No existing index found, proceeding.")
    else:
        print(f"  Warning: {e}")

# ---------------------------------------------------------------------------
# Step 2: Enable CDF on products table
# ---------------------------------------------------------------------------
print(f"\nStep 2: Enabling Change Data Feed on products table...")
spark.sql(f"""
    ALTER TABLE {PRODUCTS_TABLE}
    SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")
print("  CDF enabled.")

# ---------------------------------------------------------------------------
# Step 3: Generate embeddings for all products
# ---------------------------------------------------------------------------
print(f"\nStep 3: Generating embeddings from {PRODUCTS_TABLE}...")
products_df = spark.sql(f"SELECT product_id, embedding_text FROM {PRODUCTS_TABLE} ORDER BY product_id")
products = products_df.collect()
print(f"  Found {len(products)} products to embed")

texts = [row["embedding_text"] for row in products]
product_ids = [row["product_id"] for row in products]

print("  Calling Foundation Model API for embeddings...")
embeddings = get_embeddings_batch(texts)
print(f"  Got {len(embeddings)} embeddings, dimension: {len(embeddings[0]) if embeddings else 0}")

# ---------------------------------------------------------------------------
# Step 4: Save embeddings to Delta table (overwrite)
# ---------------------------------------------------------------------------
print(f"\nStep 4: Saving embeddings to {EMBEDDINGS_TABLE}...")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {EMBEDDINGS_TABLE} (
        product_id STRING NOT NULL,
        embedding ARRAY<FLOAT>
    )
    USING DELTA
    TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

rows = [(pid, [float(v) for v in emb]) for pid, emb in zip(product_ids, embeddings)]
schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("embedding", ArrayType(FloatType()), True),
])
embeddings_spark_df = spark.createDataFrame(rows, schema=schema)
embeddings_spark_df.write.mode("overwrite").saveAsTable(EMBEDDINGS_TABLE)
count = spark.sql(f"SELECT COUNT(*) as cnt FROM {EMBEDDINGS_TABLE}").collect()[0]["cnt"]
print(f"  Saved {count} embeddings to {EMBEDDINGS_TABLE}")

# ---------------------------------------------------------------------------
# Step 5: Wait for Vector Search Endpoint
# ---------------------------------------------------------------------------
print(f"\nStep 5: Waiting for Vector Search endpoint '{VS_ENDPOINT_NAME}'...")
for attempt in range(40):
    try:
        ep = w.vector_search_endpoints.get_endpoint(VS_ENDPOINT_NAME)
        state = str(ep.endpoint_status.state) if ep.endpoint_status else "UNKNOWN"
        print(f"  Attempt {attempt+1}: Endpoint state = {state}")
        if "ONLINE" in state.upper():
            print("  Endpoint is online!")
            break
        elif any(s in state.upper() for s in ["PROVISIONIN", "PENDING", "CREATING"]):
            time.sleep(30)
        else:
            print(f"  Unexpected state '{state}', waiting...")
            time.sleep(15)
    except Exception as e:
        if "not found" in str(e).lower():
            print("  Endpoint not found, creating...")
            w.vector_search_endpoints.create_endpoint(
                name=VS_ENDPOINT_NAME,
                endpoint_type=EndpointType.STANDARD,
            )
            time.sleep(30)
        else:
            raise

# ---------------------------------------------------------------------------
# Step 6: Create Vector Search Index
# ---------------------------------------------------------------------------
emb_dim = len(embeddings[0]) if embeddings else 1024
print(f"\nStep 6: Creating Vector Search index '{VS_INDEX_NAME}' (dim={emb_dim})...")
w.vector_search_indexes.create_index(
    name=VS_INDEX_NAME,
    endpoint_name=VS_ENDPOINT_NAME,
    primary_key="product_id",
    index_type=VectorIndexType.DELTA_SYNC,
    delta_sync_index_spec=DeltaSyncVectorIndexSpecRequest(
        source_table=EMBEDDINGS_TABLE,
        pipeline_type=PipelineType.TRIGGERED,
        embedding_vector_columns=[
            EmbeddingVectorColumn(
                name="embedding",
                embedding_dimension=emb_dim,
            )
        ],
    ),
)
print("  Index created. Waiting for initial sync...")

for attempt in range(60):
    time.sleep(10)
    idx = w.vector_search_indexes.get_index(VS_INDEX_NAME)
    status = idx.status
    detailed = str(getattr(status, 'detailed_state', status)) if status else "UNKNOWN"
    print(f"  Attempt {attempt+1}: Index state = {detailed}")
    if "ONLINE" in detailed.upper() or (status and getattr(status, 'ready', False)):
        print("  Index is ONLINE!")
        break

# ---------------------------------------------------------------------------
# Step 7: Grant permissions to app service principal
# ---------------------------------------------------------------------------
APP_SP = "b4f4efc6-f927-415e-b57e-810510097de8"
print(f"\nStep 7: Granting permissions to app service principal...")
grant_statements = [
    f"GRANT SELECT ON {VS_INDEX_NAME} TO `{APP_SP}`",
    f"GRANT SELECT ON TABLE {EMBEDDINGS_TABLE} TO `{APP_SP}`",
    f"GRANT SELECT ON TABLE {PRODUCTS_TABLE} TO `{APP_SP}`",
    f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `{APP_SP}`",
    f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.{SCHEMA} TO `{APP_SP}`",
    f"GRANT READ VOLUME ON VOLUME {CATALOG}.{SCHEMA}.jewelry_images TO `{APP_SP}`",
]
for stmt in grant_statements:
    try:
        spark.sql(stmt)
        print(f"  ✓ {stmt[:70]}...")
    except Exception as e:
        print(f"  ⚠ {stmt[:60]}: {e}")

print("\n✅ Vector Search setup complete!")
print(f"  Endpoint: {VS_ENDPOINT_NAME}")
print(f"  Index: {VS_INDEX_NAME}")
print(f"  Embeddings: {EMBEDDINGS_TABLE} ({count} rows)")
