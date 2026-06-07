-- BrickJewels — Unity Catalog data layer
-- Loaded by brickjewels.installer after the parquet files are uploaded to the volume.
-- {{CATALOG}} / {{SCHEMA}} / {{VOLUME}} are templated by the installer.
-- Statements are split on ';' — keep one statement per ';'.

-- 1. Enriched product catalog (~236 LLM-enriched jewelry products)
CREATE OR REPLACE TABLE {{CATALOG}}.{{SCHEMA}}.enriched_jewelry_products
AS SELECT * FROM read_files(
  '/Volumes/{{CATALOG}}/{{SCHEMA}}/{{VOLUME}}/_load/enriched_jewelry_products.parquet',
  format => 'parquet'
);

-- 2. Embeddings (product_id + 1024-dim ARRAY<FLOAT>) — source for Vector Search.
--    Cast is required: Vector Search DELTA_SYNC only accepts ARRAY<FLOAT>, and the
--    packaged parquet stores the vector as ARRAY<DOUBLE>.
CREATE OR REPLACE TABLE {{CATALOG}}.{{SCHEMA}}.jewelry_embeddings
AS SELECT product_id, CAST(embedding AS ARRAY<FLOAT>) AS embedding FROM read_files(
  '/Volumes/{{CATALOG}}/{{SCHEMA}}/{{VOLUME}}/_load/jewelry_embeddings.parquet',
  format => 'parquet'
);

-- 3. Vector Search DELTA_SYNC requires Change Data Feed on the source table
ALTER TABLE {{CATALOG}}.{{SCHEMA}}.jewelry_embeddings
  SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- 4. Analytics star-schema — powers the AI/BI dashboard (CDC-synced from Lakebase in prod)
CREATE OR REPLACE TABLE {{CATALOG}}.{{SCHEMA}}.dim_users
AS SELECT * FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/{{VOLUME}}/_load/dim_users.parquet', format => 'parquet');

CREATE OR REPLACE TABLE {{CATALOG}}.{{SCHEMA}}.fact_orders
AS SELECT * FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/{{VOLUME}}/_load/fact_orders.parquet', format => 'parquet');

CREATE OR REPLACE TABLE {{CATALOG}}.{{SCHEMA}}.fact_order_items
AS SELECT * FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/{{VOLUME}}/_load/fact_order_items.parquet', format => 'parquet');

CREATE OR REPLACE TABLE {{CATALOG}}.{{SCHEMA}}.fact_metal_prices
AS SELECT * FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/{{VOLUME}}/_load/fact_metal_prices.parquet', format => 'parquet');

CREATE OR REPLACE TABLE {{CATALOG}}.{{SCHEMA}}.fact_nudges
AS SELECT * FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/{{VOLUME}}/_load/fact_nudges.parquet', format => 'parquet');

CREATE OR REPLACE TABLE {{CATALOG}}.{{SCHEMA}}.fact_wishlists
AS SELECT * FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/{{VOLUME}}/_load/fact_wishlists.parquet', format => 'parquet')
