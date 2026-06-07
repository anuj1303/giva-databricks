"""
BrickJewels Demo - CaratLane Data Enrichment Script
Reads raw CaratLane scraped data and enriches with material, price, occasion,
style, descriptions, and embedding_text for the BrickJewels app.
"""
import os
import random
import subprocess
import json
import time
import base64
import httpx
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType, IntegerType, BooleanType
)

CATALOG = "anuj_vm_workspace_catalog"
SCHEMA = "caratlane_jewelry"
SOURCE_TABLE = f"{CATALOG}.{SCHEMA}.jewelry_products"
TARGET_TABLE = f"{CATALOG}.{SCHEMA}.enriched_jewelry_products"
BRAND = "BrickJewels"

# LLM Image Analysis Configuration
LLM_MODEL = "databricks-claude-sonnet-4-6"
IMAGE_VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/jewelry_images"
ENABLE_LLM_ANALYSIS = True   # Set False to skip LLM step
LLM_BATCH_DELAY = 1.0        # Seconds between LLM calls (rate limiting)
HOST = "https://fe-vm-anuj-vm-workspace.cloud.databricks.com"

# Acquire Databricks auth token
_token_data = json.loads(subprocess.check_output(
    ["/opt/homebrew/bin/databricks", "--profile", "AnujLathi", "auth", "token", "--output", "json"],
    stderr=subprocess.DEVNULL,
))
TOKEN = _token_data["access_token"]


# ---------------------------------------------------------------------------
# LLM Image Analysis helpers
# ---------------------------------------------------------------------------

def download_image_base64(category: str, filename: str) -> str | None:
    """Download an image from UC Volume via Files API and return base64-encoded string."""
    volume_path = f"{IMAGE_VOLUME_PATH}/{category}/{filename}"
    url = f"{HOST}/api/2.0/fs/files{volume_path}"
    headers = {"Authorization": f"Bearer {TOKEN}"}
    try:
        with httpx.Client(timeout=30.0) as client:
            resp = client.get(url, headers=headers)
            if resp.status_code != 200:
                print(f"  WARNING: Failed to download {volume_path}: {resp.status_code}")
                return None
            return base64.b64encode(resp.content).decode("utf-8")
    except Exception as e:
        print(f"  WARNING: Error downloading {volume_path}: {e}")
        return None


LLM_ANALYSIS_PROMPT = """Analyze this jewelry image and extract structured attributes as JSON. Return ONLY valid JSON with these fields:

{
  "stone_type": "diamond/ruby/emerald/sapphire/pearl/cubic_zirconia/none/other",
  "stone_color": "colorless/red/green/blue/white/pink/yellow/multicolor/none",
  "stone_size": "large/medium/small/tiny/none",
  "stone_count": "single/few/many/cluster/none",
  "stone_shape": "round/oval/pear/marquise/princess/cushion/emerald_cut/none",
  "metal_type": "gold/white_gold/rose_gold/platinum/silver/mixed",
  "metal_color": "yellow/white/rose/silver/two_tone",
  "design_style": "classic/modern/vintage/bohemian/minimalist/ornate/floral/geometric/art_deco",
  "pattern": "solitaire/halo/cluster/pave/channel/bezel/prong/filigree/plain/mesh/rope/none",
  "finish": "polished/matte/brushed/hammered/textured/mixed",
  "visual_description": "One sentence describing what you see in the image"
}

Return ONLY the JSON object, no markdown formatting, no code blocks."""


def analyze_image_with_llm(image_b64: str, category: str, title: str) -> dict | None:
    """Call Claude Sonnet 4.6 vision to extract structured attributes from a jewelry image."""
    url = f"{HOST}/serving-endpoints/{LLM_MODEL}/invocations"
    headers = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}
    payload = {
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "image_url",
                        "image_url": {"url": f"data:image/jpeg;base64,{image_b64}"},
                    },
                    {
                        "type": "text",
                        "text": f"This is a {category} called '{title}'. {LLM_ANALYSIS_PROMPT}",
                    },
                ],
            }
        ],
        "max_tokens": 300,
        "temperature": 0.1,
    }
    try:
        with httpx.Client(timeout=60.0) as client:
            resp = client.post(url, json=payload, headers=headers)
            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", 5))
                print(f"  Rate limited, waiting {retry_after}s...")
                time.sleep(retry_after)
                resp = client.post(url, json=payload, headers=headers)
            if resp.status_code != 200:
                print(f"  WARNING: LLM call failed ({resp.status_code}): {resp.text[:200]}")
                return None
            content = resp.json()["choices"][0]["message"]["content"]
            content = content.strip()
            if content.startswith("```"):
                content = content.split("\n", 1)[1].rsplit("```", 1)[0]
            return json.loads(content)
    except (json.JSONDecodeError, KeyError, IndexError) as e:
        print(f"  WARNING: Failed to parse LLM response: {e}")
        return None
    except Exception as e:
        print(f"  WARNING: LLM analysis error: {e}")
        return None

# Initialize Spark
if os.environ.get('DATABRICKS_RUNTIME_VERSION'):
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
else:
    from databricks.connect import DatabricksSession, DatabricksEnv
    env = (DatabricksEnv()
           .withDependencies("jmespath==1.0.1")
           .withDependencies("httpx==0.28.1"))
    spark = DatabricksSession.builder.profile("AnujLathi").serverless(True).withEnvironment(env).getOrCreate()

print(f"Spark version: {spark.version}")

# ---------------------------------------------------------------------------
# Read raw CaratLane data
# ---------------------------------------------------------------------------
print(f"\nReading raw CaratLane data from {SOURCE_TABLE}...")
raw_df = spark.sql(f"SELECT * FROM {SOURCE_TABLE}")
raw_products = raw_df.collect()
print(f"  Total raw products: {len(raw_products)}")


# ---------------------------------------------------------------------------
# Enrichment functions
# ---------------------------------------------------------------------------

def infer_material(title: str) -> str:
    t = title.lower()
    if "22kt" in t or "22k" in t:
        return "22K Gold"
    if "solitaire" in t:
        return "Solitaire Diamond"
    if "gemstone" in t:
        return "Gold & Gemstone"
    if "pearl" in t:
        return "Gold & Pearl"
    if "platinum" in t:
        return "Platinum"
    if "rose gold" in t:
        return "Rose Gold"
    if "white gold" in t:
        return "White Gold"
    if "diamond" in t:
        return "18K Gold & Diamond"
    if "gold" in t:
        return "18K Gold"
    return "18K Gold & Diamond"


def infer_occasion(title: str, category: str) -> str:
    t = title.lower()
    if "mangalsutra" in t or "bridal" in t or "wedding" in t:
        return "Wedding"
    if "solitaire" in t or "engagement" in t:
        return "Engagement"
    if any(w in t for w in ["men", "men's", "boys"]):
        return "Daily"
    if any(w in t for w in ["kids", "kids'", "children"]):
        return "Daily"
    if any(w in t for w in ["cocktail", "statement", "sparkle", "cluster", "dazzling"]):
        return "Party"
    if "22kt" in t or "traditional" in t or "temple" in t:
        return "Festival"
    # Deterministic rotation based on title hash
    occasions = ["Daily", "Party", "Wedding", "Festival", "Daily"]
    return occasions[hash(title) % len(occasions)]


def infer_style(title: str) -> str:
    t = title.lower()
    if any(w in t for w in ["22kt", "temple", "traditional", "heritage"]):
        return "Heritage"
    if any(w in t for w in ["classic", "eternal", "timeless"]):
        return "Classic"
    if any(w in t for w in ["infinity", "modern", "linear", "contemporary", "sleek"]):
        return "Contemporary"
    if any(w in t for w in ["floral", "flora", "leaf", "leaves", "petal", "butterfly", "blossom"]):
        return "Floral"
    if any(w in t for w in ["mesh", "cluster", "geometric", "quad"]):
        return "Geometric"
    if any(w in t for w in ["hoop", "band", "minimalist", "dainty", "delicate"]):
        return "Minimalist"
    if any(w in t for w in ["heart", "love", "romantic"]):
        return "Romantic"
    return "Contemporary"


def infer_subcategory(title: str, category: str, material: str, style: str) -> str:
    t = title.lower()
    if category == "Ring":
        if "solitaire" in t:
            return "Solitaire Ring"
        if "band" in t or "eternity" in t:
            return "Band Ring"
        if "mangalsutra" in t or "vanki" in t:
            return "Traditional Ring"
        if "cocktail" in t or "cluster" in t:
            return "Cocktail Ring"
        return "Diamond Ring" if "Diamond" in material else "Gold Ring"
    elif category == "Earring":
        if "stud" in t:
            return "Stud Earrings"
        if "hoop" in t:
            return "Hoop Earrings"
        if "drop" in t or "jhumka" in t or "dangles" in t:
            return "Drop Earrings"
        return "Diamond Earrings" if "Diamond" in material else "Gold Earrings"
    elif category == "Necklace":
        if "chain" in t:
            return "Gold Chain"
        if "mangalsutra" in t:
            return "Mangalsutra"
        if "choker" in t:
            return "Choker"
        return "Diamond Necklace" if "Diamond" in material else "Gold Necklace"
    elif category == "Bracelet":
        if "tennis" in t:
            return "Tennis Bracelet"
        if "chain" in t or "charm" in t:
            return "Chain Bracelet"
        return "Diamond Bracelet" if "Diamond" in material else "Gold Bracelet"
    elif category == "Bangle":
        if "22k" in t.lower():
            return "Gold Bangle"
        return "Diamond Bangle" if "Diamond" in material else "Gold Bangle"
    elif category == "Pendant":
        if "solitaire" in t:
            return "Solitaire Pendant"
        return "Diamond Pendant" if "Diamond" in material else "Gold Pendant"
    return f"{category}"


PRICE_RANGES = {
    ("Ring", "18K Gold & Diamond"): (25000, 120000),
    ("Ring", "Solitaire Diamond"): (50000, 300000),
    ("Ring", "22K Gold"): (20000, 80000),
    ("Ring", "18K Gold"): (15000, 60000),
    ("Ring", "Rose Gold"): (18000, 70000),
    ("Ring", "Gold & Gemstone"): (22000, 90000),
    ("Ring", "Gold & Pearl"): (18000, 65000),
    ("Earring", "18K Gold & Diamond"): (20000, 95000),
    ("Earring", "Solitaire Diamond"): (40000, 200000),
    ("Earring", "22K Gold"): (18000, 75000),
    ("Earring", "18K Gold"): (12000, 50000),
    ("Earring", "Gold & Pearl"): (15000, 55000),
    ("Earring", "Gold & Gemstone"): (18000, 70000),
    ("Necklace", "18K Gold & Diamond"): (35000, 250000),
    ("Necklace", "22K Gold"): (30000, 200000),
    ("Necklace", "Gold & Gemstone"): (30000, 180000),
    ("Necklace", "Gold & Pearl"): (25000, 150000),
    ("Necklace", "18K Gold"): (20000, 120000),
    ("Bracelet", "18K Gold & Diamond"): (30000, 150000),
    ("Bracelet", "22K Gold"): (25000, 120000),
    ("Bracelet", "18K Gold"): (18000, 80000),
    ("Bangle", "22K Gold"): (40000, 250000),
    ("Bangle", "18K Gold & Diamond"): (35000, 180000),
    ("Bangle", "18K Gold"): (30000, 150000),
    ("Pendant", "18K Gold & Diamond"): (15000, 85000),
    ("Pendant", "Solitaire Diamond"): (30000, 150000),
    ("Pendant", "22K Gold"): (12000, 60000),
    ("Pendant", "18K Gold"): (10000, 45000),
    ("Pendant", "Gold & Gemstone"): (12000, 55000),
}

WEIGHT_RANGES = {
    "Ring": (1.5, 6.0),
    "Earring": (1.5, 5.5),
    "Necklace": (4.0, 25.0),
    "Bracelet": (5.0, 15.0),
    "Bangle": (8.0, 35.0),
    "Pendant": (1.5, 6.0),
}

COLLECTIONS = ["Celeste", "Signature Collection", "Heritage Collection", "Inara", "Lumière", "Éternelle"]


def generate_description(title: str, category: str, material: str, occasion: str, style: str) -> str:
    cat_lower = category.lower()
    occasion_phrase = {
        "Daily": "effortless everyday elegance",
        "Wedding": "bridal grandeur and timeless celebrations",
        "Party": "glamorous occasions and cocktail evenings",
        "Festival": "festive celebrations and traditional occasions",
        "Engagement": "life's most precious milestones",
    }.get(occasion, "any special occasion")
    return (
        f"The {title} is a beautifully crafted {cat_lower} in {material}, "
        f"featuring a {style.lower()} design aesthetic. "
        f"Perfect for {occasion_phrase}, this piece showcases "
        f"CaratLane's signature craftsmanship with BIS-hallmarked quality."
    )


# ---------------------------------------------------------------------------
# Enrich all products
# ---------------------------------------------------------------------------
print("\nEnriching product records...")
all_products = []

for row in raw_products:
    title = row["title"]
    category = row["category"]
    sku = row["sku"] or row["image_id"]
    image_id = row["image_id"]
    volume_path = row["volume_path"]
    source_url = row["source_url"] or ""

    material = infer_material(title)
    occasion = infer_occasion(title, category)
    style = infer_style(title)
    subcategory = infer_subcategory(title, category, material, style)
    collection = COLLECTIONS[hash(sku) % len(COLLECTIONS)]
    description = generate_description(title, category, material, occasion, style)

    # Deterministic price and weight based on SKU hash
    rng = random.Random(hash(sku) & 0xFFFFFFFF)
    price_key = (category, material)
    lo, hi = PRICE_RANGES.get(price_key, (20000, 150000))
    price = rng.randint(lo, hi)

    wt_lo, wt_hi = WEIGHT_RANGES.get(category, (2.0, 10.0))
    weight = round(rng.uniform(wt_lo, wt_hi), 1)

    # Build tags
    tags_parts = [category, subcategory, material, occasion, style, "CaratLane", BRAND, "hallmarked", "BIS"]
    if "Diamond" in material or "Solitaire" in material:
        tags_parts.append("diamond")
    if "Gold" in material:
        tags_parts.append("gold")
    if "Pearl" in material:
        tags_parts.append("pearl")
    if "Gemstone" in material:
        tags_parts.append("gemstone")
    if occasion == "Wedding":
        tags_parts.append("bridal")
    tags = ", ".join(tags_parts)

    # Build embedding text
    embedding_text = (
        f"{title}. {description} "
        f"Category: {category}. Subcategory: {subcategory}. "
        f"Material: {material}. Occasion: {occasion}. Style: {style}. "
        f"Collection: {collection}. Tags: {tags}"
    )

    # Product ID and image URL
    product_id = f"CL-{category.upper()[:4]}-{sku}"
    # Image URL with category subdirectory matching volume layout
    local_filename = volume_path.split("/")[-1] if volume_path else f"{category.lower()}_{sku}.jpg"
    image_url = f"/api/images/{category.lower()}/{local_filename}"

    all_products.append({
        "product_id": product_id,
        "name": title,
        "description": description,
        "category": category,
        "subcategory": subcategory,
        "material": material,
        "occasion": occasion,
        "style": style,
        "collection": collection,
        "weight_grams": float(weight),
        "price_inr": int(price),
        "image_url": image_url,
        "tags": tags,
        "embedding_text": embedding_text,
        "in_stock": True,
        "sku": sku,
        "source_url": source_url,
    })

print(f"Total enriched products: {len(all_products)}")

# ---------------------------------------------------------------------------
# LLM Image Analysis (optional)
# ---------------------------------------------------------------------------
if ENABLE_LLM_ANALYSIS:
    print("\nRunning LLM image analysis on product images...")
    success_count = 0
    fail_count = 0

    for i, product in enumerate(all_products):
        parts = product["image_url"].split("/")
        category_dir = parts[-2]   # e.g. "necklace"
        filename = parts[-1]       # e.g. "some_image.jpg"

        print(f"  [{i+1}/{len(all_products)}] Analyzing: {product['name'][:50]}...")

        image_b64 = download_image_base64(category_dir, filename)
        if not image_b64:
            product["llm_attributes"] = None
            product["llm_description"] = None
            fail_count += 1
            continue

        attributes = analyze_image_with_llm(image_b64, product["category"], product["name"])
        if attributes:
            product["llm_attributes"] = json.dumps(attributes)
            visual_desc = attributes.get("visual_description", "")
            attr_parts = []
            if attributes.get("stone_type") and attributes["stone_type"] != "none":
                stone_desc = attributes["stone_type"]
                if attributes.get("stone_color") and attributes["stone_color"] != "none":
                    stone_desc = f"{attributes['stone_color']} {stone_desc}"
                if attributes.get("stone_size") and attributes["stone_size"] != "none":
                    stone_desc = f"{attributes['stone_size']} {stone_desc}"
                attr_parts.append(f"Stone: {stone_desc}")
            if attributes.get("metal_color") and attributes["metal_color"] not in ("none", ""):
                attr_parts.append(f"Metal: {attributes['metal_color']} {attributes.get('metal_type', '')}")
            if attributes.get("design_style"):
                attr_parts.append(f"Design: {attributes['design_style']}")
            if attributes.get("pattern") and attributes["pattern"] != "none":
                attr_parts.append(f"Setting: {attributes['pattern']}")
            product["llm_description"] = f"{visual_desc} {'. '.join(attr_parts)}."
            success_count += 1
        else:
            product["llm_attributes"] = None
            product["llm_description"] = None
            fail_count += 1

        time.sleep(LLM_BATCH_DELAY)

    print(f"  LLM analysis complete: {success_count} succeeded, {fail_count} failed")

    # Rebuild embedding_text with LLM descriptions
    print("\nUpdating embedding_text with LLM visual descriptions...")
    for product in all_products:
        llm_part = product.get("llm_description") or ""
        if llm_part:
            product["embedding_text"] = (
                f"{product['name']}. {product['description']} "
                f"Category: {product['category']}. Subcategory: {product['subcategory']}. "
                f"Material: {product['material']}. Occasion: {product['occasion']}. Style: {product['style']}. "
                f"Collection: {product['collection']}. Tags: {product['tags']}. "
                f"Visual attributes: {llm_part}"
            )
else:
    print("\nSkipping LLM image analysis (ENABLE_LLM_ANALYSIS=False)")
    for product in all_products:
        product["llm_attributes"] = None
        product["llm_description"] = None

# ---------------------------------------------------------------------------
# Write to Delta table
# ---------------------------------------------------------------------------
schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("description", StringType(), True),
    StructField("category", StringType(), True),
    StructField("subcategory", StringType(), True),
    StructField("material", StringType(), True),
    StructField("occasion", StringType(), True),
    StructField("style", StringType(), True),
    StructField("collection", StringType(), True),
    StructField("weight_grams", FloatType(), True),
    StructField("price_inr", IntegerType(), True),
    StructField("image_url", StringType(), True),
    StructField("tags", StringType(), True),
    StructField("embedding_text", StringType(), True),
    StructField("in_stock", BooleanType(), True),
    StructField("sku", StringType(), True),
    StructField("source_url", StringType(), True),
    StructField("llm_attributes", StringType(), True),
    StructField("llm_description", StringType(), True),
])

df = spark.createDataFrame(all_products, schema=schema)
df = df.withColumn("created_at", F.current_timestamp())

print(f"\nWriting {len(all_products)} products to {TARGET_TABLE}...")
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TARGET_TABLE)

# Enable CDF for vector search sync
spark.sql(f"ALTER TABLE {TARGET_TABLE} SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")

count = spark.sql(f"SELECT COUNT(*) as cnt FROM {TARGET_TABLE}").collect()[0]["cnt"]
print(f"Written {count} products to {TARGET_TABLE}")

print("\nCategory distribution:")
spark.sql(f"""
    SELECT category, COUNT(*) as count, ROUND(AVG(price_inr)) as avg_price,
           ROUND(MIN(price_inr)) as min_price, ROUND(MAX(price_inr)) as max_price
    FROM {TARGET_TABLE}
    GROUP BY category
    ORDER BY count DESC
""").show()

print("\nMaterial distribution:")
spark.sql(f"""
    SELECT material, COUNT(*) as count
    FROM {TARGET_TABLE}
    GROUP BY material
    ORDER BY count DESC
""").show()

print("\nSample products:")
spark.sql(f"""
    SELECT product_id, name, category, material, price_inr, occasion, image_url
    FROM {TARGET_TABLE}
    LIMIT 8
""").show(truncate=60)

print("Data enrichment complete!")
