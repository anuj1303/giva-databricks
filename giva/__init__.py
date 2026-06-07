"""
giva — one-line installer for the GIVA Databricks demo.

GIVA is an end-to-end AI jewelry-commerce demo. One call provisions:

  1. Unity Catalog data layer   (catalog/schema/volume, ~236 enriched products + images)
  2. Vector Search index        (semantic + image search, 1024-dim)
  3. Genie Space                (NL -> SQL over the product catalog)
  4. AI/BI dashboard            (sales / catalog analytics, published)
  5. Lakebase (Postgres)        (orders, users, nudges, live metal prices, dynamic product prices) + seed data
  6. FastAPI + React app        (storefront + admin analytics)
  7. Two background jobs        (Metals Refresh + Nudge Emails) — installed PAUSED (stopped, not triggered)

Usage (inside a Databricks notebook):

    %pip install giva-databricks
    dbutils.library.restartPython()

    import giva
    giva.help()
    giva.install('giva', catalog='my_catalog')

Locally (with a Databricks CLI profile):

    giva.install('giva', profile='DEFAULT', catalog='main')
"""
import json
from importlib import resources

from .installer import install, Installer  # noqa: F401

__version__ = "0.1.1"
__all__ = ["install", "list_demos", "help", "Installer", "__version__"]


def list_demos():
    """Print the demos bundled in this package."""
    conf_dir = resources.files("giva").joinpath("conf")
    print("\n💎  Available GIVA demos:\n")
    for entry in conf_dir.iterdir():
        if entry.name.endswith(".json"):
            c = json.loads(entry.read_text(encoding="utf-8"))
            print(f"   • {c['name']:<14} {c['title']}")
            print(f"     {c['description'][:120]}…\n")
    print("Install with:  giva.install('giva', catalog='<your_catalog>')\n")


def help():
    """Print quick-start help."""
    print(__doc__)
    print("Options for install():")
    print("  demo            demo name (default 'giva')")
    print("  catalog         target UC catalog   (default 'main')")
    print("  schema          target UC schema    (default 'giva')")
    print("  warehouse_id    SQL warehouse to use (default: first running/available)")
    print("  install_app     also deploy the FastAPI app          (default True)")
    print("  install_lakebase provision Lakebase + seed data       (default True)")
    print("  install_jobs    create the two PAUSED background jobs (default True)")
    print("  profile         Databricks CLI profile (only needed when running locally)")
    print("  overwrite       reinstall over existing assets        (default False)\n")
    print("Note: the Metals Refresh and Nudge Emails jobs are always created PAUSED.\n")
