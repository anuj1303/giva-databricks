# 💎 GIVA — one-line Databricks demo installer

`giva` packages the entire **GIVA** AI jewelry-commerce demo into a
single `pip`-installable package, à la [dbdemos](https://www.dbdemos.ai/). One call
provisions the whole stack into your workspace from pre-baked real data.

```python
%pip install giva-databricks
dbutils.library.restartPython()

import giva
giva.install('giva', catalog='my_catalog')
```

Locally with a CLI profile:

```bash
pip install giva-databricks
python -c "import giva; giva.install('giva', profile='AnujLathi', catalog='anuj_vm_workspace_catalog')"
```

## What gets installed

| # | Asset | Detail |
|---|-------|--------|
| 1 | **UC data layer** | schema + volume, `enriched_jewelry_products` (~236 LLM-enriched products) + `jewelry_embeddings` + ~236 product images |
| 2 | **Vector Search** | `giva-vs` endpoint + `jewelry_embeddings_index` (DELTA_SYNC, 1024-dim) for semantic & image search |
| 3 | **Genie Space** | natural-language analytics over the product catalog |
| 4 | **AI/BI dashboard** | catalog & sales analytics, published |
| 5 | **Lakebase (Postgres)** | `giva-orders` instance + `giva` db: orders, users, nudges, live metal prices, dynamic product prices — **seeded with data** |
| 6 | **App** | React + FastAPI storefront + admin analytics (Databricks App) |
| 7 | **Background jobs** | **Metals Refresh** + **Nudge Emails** — created **PAUSED** (stopped, not triggered) |

### ⏸ The two pipelines are installed PAUSED

Per design, the **Metals Refresh Pipeline** and **Nudge Emails Pipeline** jobs are
created with their schedule `pause_status = PAUSED`. They will **not** run on a
trigger. The demo works out of the box because metal prices and product prices are
**seeded** into Lakebase. When you want a live refresh, either:

- trigger them manually from the **admin app** (Run now), or
- unpause the schedule in the Jobs UI.

> The two job notebooks read Service-Principal credentials and (for emails) Gmail
> OAuth from a Databricks secret scope `giva`. These secrets are **not**
> created by the installer — set them before unpausing if you want the jobs to run.

## Options

```python
giva.install(
    'giva',
    catalog='my_catalog',          # required on non-Free-Edition workspaces
    schema='giva',          # default
    warehouse_id=None,             # default: first running/available
    install_app=True,
    install_lakebase=True,         # provision Lakebase + seed
    install_jobs=True,             # create the two PAUSED jobs
    install_vector_search=True,
    profile='AnujLathi',           # only when running locally
)
```

## Rebuilding the package data (maintainers)

The packaged `data/`, `dashboards/` and Lakebase schema are exported from the live
reference build. To refresh them:

```bash
python scripts/export_live_assets.py --profile AnujLathi \
    --catalog anuj_vm_workspace_catalog --schema caratlane_jewelry
python -m build           # produces dist/giva-*.whl
```

## Installing the package

Until it's on PyPI, install the wheel straight from a GitHub Release:

```bash
pip install https://github.com/anuj1303/giva-demo/releases/download/v0.1.0/giva-0.1.0-py3-none-any.whl
```

or build-and-install from source:

```bash
pip install "git+https://github.com/anuj1303/giva-demo.git"
```

## Publishing to PyPI (maintainers)

The `Publish giva-databricks to PyPI` GitHub Action publishes via **trusted publishing**
(OIDC — no API tokens). One-time setup, then a one-click publish:

1. On https://pypi.org → your account → **Publishing** → **Add a pending publisher**:
   - PyPI Project Name: `giva`
   - Owner: `anuj1303`
   - Repository name: `giva-demo`
   - Workflow name: `publish.yml`
   - Environment: *(leave blank)*
2. In the repo: **Actions → Publish giva-databricks to PyPI → Run workflow**.

> Note: the wheel is ~88 MB (the 11.5M-row `nudge_emails` seed is most of it).
> That's under PyPI's 100 MB per-file limit but large — the GitHub Release wheel
> above is the recommended distribution for most users.

## Reference build

- Workspace: AWS FE VM (`fe-vm-anuj-vm-workspace`)
- Source catalog/schema: `anuj_vm_workspace_catalog.caratlane_jewelry`
- App: `tanishq-jewelry-demo` (UI name **GIVA**)
