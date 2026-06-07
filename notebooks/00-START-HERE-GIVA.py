# Databricks notebook source
# MAGIC %md
# MAGIC # 💎 GIVA — one-line demo installer
# MAGIC
# MAGIC This notebook installs the complete **GIVA** AI jewelry-commerce demo into
# MAGIC **this** workspace: Unity Catalog data + Vector Search + Genie + AI/BI dashboard +
# MAGIC Lakebase (with seed data) + a React/FastAPI app + two **PAUSED** background jobs.
# MAGIC
# MAGIC Everything is provisioned from pre-baked real data — no scraping, no LLM vision step.

# COMMAND ----------

# MAGIC %pip install giva-databricks
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import giva
giva.help()
giva.list_demos()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install
# MAGIC On Free Edition the default catalog `main`/`workspace` works; on other workspaces
# MAGIC pass a catalog you can write to. The install takes ~10–15 min (Lakebase + Vector
# MAGIC Search + app compute are the slow parts).

# COMMAND ----------

giva.install('giva', catalog='main')

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⏸ A note on the two pipelines
# MAGIC The **Metals Refresh** and **Nudge Emails** jobs are created **PAUSED** — they will
# MAGIC not run on a schedule. The storefront and admin analytics work immediately because
# MAGIC metal/product prices are **seeded**. Trigger a live refresh from the admin app's
# MAGIC *Run now* buttons, or unpause the jobs in the Jobs UI when you're ready.
# MAGIC
# MAGIC The install summary above prints the Genie, dashboard, Lakebase, job and app links.
