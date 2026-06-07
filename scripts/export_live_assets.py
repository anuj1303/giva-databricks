#!/usr/bin/env python3
"""
export_live_assets.py
=====================
Populate the `brickjewels` package with REAL data exported from the live
reference workspace. Run this ONCE (with a valid CLI profile) before building
the wheel. It writes into ../brickjewels/{data,dashboards,genie,sql,jobs}.

    python scripts/export_live_assets.py --profile AnujLathi \
        --catalog anuj_vm_workspace_catalog --schema caratlane_jewelry

What it exports:
  • enriched_jewelry_products + jewelry_embeddings  -> data/*.parquet
  • ~236 product images from the UC volume           -> data/jewelry_images.tar.gz
  • Lakeview dashboard (serialized)                  -> dashboards/brickjewels.lvdash.json  (re-templated)
  • Genie space instructions (if present live)       -> genie/instructions.md               (re-templated)
  • the two job schedules (for reference)            -> jobs/_live_*.json
  • Lakebase schema + seed rows                      -> sql/lakebase_schema.sql, data/lakebase/*.csv.gz

Everything that embeds the source catalog.schema is rewritten to the
{{CATALOG}}/{{SCHEMA}} placeholders the installer expects.
"""
from __future__ import annotations
import argparse, gzip, io, json, os, re, sys, tarfile, time
from pathlib import Path

PKG = Path(__file__).resolve().parents[1] / "brickjewels"
DATA = PKG / "data"; LB = DATA / "lakebase"
for d in (DATA, LB, PKG / "dashboards", PKG / "genie", PKG / "sql", PKG / "jobs"):
    d.mkdir(parents=True, exist_ok=True)

# reference build identifiers (overridable via flags)
DASHBOARD_ID = "01f134d2465d1b01b0a5aeea4d1eda1a"
GENIE_SPACE_ID = "01f134d284ec10e283632e4394823c94"
METALS_JOB_ID = "812450569282600"
NUDGE_JOB_ID = "659485880453977"
LAKEBASE_HOST = "instance-cd970b9c-afcf-4477-9463-17d29bf00a93.database.cloud.databricks.com"
LAKEBASE_DB = "brickjewels"
# the AI/BI dashboard reads from a separate analytics star-schema (CDC-synced from Lakebase)
ANALYTICS_SCHEMA = "brickjewels_analytics"
ANALYTICS_TABLES = ["dim_users", "fact_orders", "fact_order_items",
                    "fact_metal_prices", "fact_nudges", "fact_wishlists"]
LB_TABLES = ["brickjewels_users", "brickjewels_orders", "brickjewels_user_data_scd2",
             "brickjewels_metal_prices", "brickjewels_product_prices", "brickjewels_nudges",
             "brickjewels_nudge_emails", "brickjewels_chat_sessions", "brickjewels_genie_history"]


def log(s): print(f"  [export] {s}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", default="AnujLathi")
    ap.add_argument("--catalog", default="anuj_vm_workspace_catalog")
    ap.add_argument("--schema", default="caratlane_jewelry")
    ap.add_argument("--volume", default="jewelry_images")
    ap.add_argument("--warehouse-id", default=None)
    ap.add_argument("--skip-lakebase", action="store_true")
    ap.add_argument("--skip-images", action="store_true")
    ap.add_argument("--only-lakebase", action="store_true", help="export only Lakebase (skip UC/images/dashboard/genie/jobs)")
    ap.add_argument("--only-analytics", action="store_true", help="export only the analytics tables + dashboard")
    ap.add_argument("--force-lakebase", action="store_true", help="re-dump tables even if a csv.gz already exists")
    a = ap.parse_args()

    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient(profile=a.profile)
    api = w.api_client
    host = w.config.host.rstrip("/")
    src = f"{a.catalog}.{a.schema}"
    log(f"workspace {host}  source {src}")

    wid = a.warehouse_id or _first_warehouse(w)
    log(f"warehouse {wid}")

    if a.only_analytics:
        adir = DATA / "analytics"; adir.mkdir(exist_ok=True)
        for t in ANALYTICS_TABLES:
            export_table(w, wid, f"{a.catalog}.{ANALYTICS_SCHEMA}.{t}", adir / f"{t}.parquet")
        export_dashboard(api, DASHBOARD_ID, src, PKG / "dashboards" / "brickjewels.lvdash.json")
        log("done (analytics + dashboard).")
        return

    if not a.only_lakebase:
        # ---- 1. UC tables -> parquet (local reconstruction; 236 rows is tiny) ----
        export_table(w, wid, f"{src}.enriched_jewelry_products", DATA / "enriched_jewelry_products.parquet")
        export_table(w, wid, f"{src}.jewelry_embeddings", DATA / "jewelry_embeddings.parquet",
                     array_cols={"embedding"})

        # ---- 1b. analytics star-schema (powers the AI/BI dashboard) ----
        adir = DATA / "analytics"; adir.mkdir(exist_ok=True)
        for t in ANALYTICS_TABLES:
            export_table(w, wid, f"{a.catalog}.{ANALYTICS_SCHEMA}.{t}", adir / f"{t}.parquet")

        # ---- 2. images -> tar.gz ----
        if not a.skip_images:
            export_images(w, f"/Volumes/{a.catalog}/{a.schema}/{a.volume}", DATA / "jewelry_images.tar.gz")

        # ---- 3. dashboard ----
        export_dashboard(api, DASHBOARD_ID, src, PKG / "dashboards" / "brickjewels.lvdash.json")

        # ---- 4. genie: the live space has only a one-line instruction; we keep the
        #         curated brickjewels/genie/instructions.md instead. (export skipped on purpose)

        # ---- 5. job schedules (reference only; authored job JSONs guarantee PAUSED) ----
        for jid, name in ((METALS_JOB_ID, "_live_metals"), (NUDGE_JOB_ID, "_live_nudge")):
            try:
                j = api.do("GET", "/api/2.1/jobs/get", query={"job_id": jid})
                (PKG / "jobs" / f"{name}.json").write_text(json.dumps(j.get("settings", {}), indent=2))
                sch = (j.get("settings", {}) or {}).get("schedule", {})
                log(f"job {jid} schedule: {sch.get('quartz_cron_expression')} ({sch.get('pause_status')})")
            except Exception as e:
                log(f"job {jid} fetch skipped: {e}")

    # ---- 6. lakebase schema + seed ----
    if not a.skip_lakebase:
        try:
            export_lakebase(w, host, a.profile, force=a.force_lakebase)
        except Exception as e:
            log(f"⚠️  lakebase export failed (run with --skip-lakebase to bypass): {e}")

    log("done. Review brickjewels/data, dashboards, sql, then build the wheel.")


def _first_warehouse(w):
    whs = list(w.warehouses.list())
    running = [x for x in whs if getattr(x.state, "value", str(x.state)) == "RUNNING"]
    return (running or whs)[0].id


def _sql(w, wid, stmt, external=False):
    from databricks.sdk.service.sql import Disposition, Format
    se = w.statement_execution
    kw = {}
    if external:
        kw = {"disposition": Disposition.EXTERNAL_LINKS, "format": Format.JSON_ARRAY}
    r = se.execute_statement(warehouse_id=wid, statement=stmt, wait_timeout="50s", **kw)
    for _ in range(300):
        st = r.status.state.value if r.status and r.status.state else None
        if st in ("PENDING", "RUNNING"):
            time.sleep(2); r = se.get_statement(r.statement_id); continue
        break
    if r.status.state.value != "SUCCEEDED":
        raise RuntimeError(r.status.error.message if r.status.error else "sql failed")
    return r


def _fetch_rows(w, wid, fqn):
    """Return (cols, rows) for SELECT * FROM fqn, transparently using EXTERNAL_LINKS
    (chunked, downloaded via urllib) when the inline 25MB cap would be exceeded."""
    import urllib.request
    try:
        r = _sql(w, wid, f"SELECT * FROM {fqn}")
        cols = [c.name for c in r.manifest.schema.columns]
        rows = (r.result.data_array if r.result else None) or []
        nxt = r.result.next_chunk_index if r.result else None
        while nxt is not None:
            ch = w.statement_execution.get_statement_result_chunk_n(r.statement_id, nxt)
            rows.extend(ch.data_array or []); nxt = ch.next_chunk_index
        return cols, rows
    except RuntimeError as e:
        if "byte limit" not in str(e).lower():
            raise
    # large result → external links; iterate every chunk by index (deterministic)
    r = _sql(w, wid, f"SELECT * FROM {fqn}", external=True)
    cols = [c.name for c in r.manifest.schema.columns]
    rows = []
    se = w.statement_execution
    total = r.manifest.total_chunk_count or 1
    for ci in range(total):
        chunk = r.result if ci == 0 else se.get_statement_result_chunk_n(r.statement_id, ci)
        for link in (chunk.external_links or []):
            with urllib.request.urlopen(link.external_link) as resp:
                rows.extend(json.loads(resp.read().decode()))
    return cols, rows


def export_table(w, wid, fqn, out_path, array_cols=frozenset()):
    """Fetch all rows via SQL and write a parquet file locally."""
    import pandas as pd
    log(f"exporting {fqn} -> {out_path.name}")
    cols, rows = _fetch_rows(w, wid, fqn)
    df = pd.DataFrame(rows, columns=cols)
    # cast array columns (returned as JSON strings) back to python lists of float
    for ac in array_cols:
        if ac in df.columns:
            df[ac] = df[ac].apply(lambda v: json.loads(v) if isinstance(v, str) else v)
    df.to_parquet(out_path, index=False)
    log(f"  {len(df)} rows, {out_path.stat().st_size//1024} KB")


def export_images(w, vol_dir, out_tgz):
    log(f"exporting images from {vol_dir}")
    files = []
    def walk(p):
        for e in w.files.list_directory_contents(p):
            if e.is_directory:
                walk(e.path)
            else:
                files.append(e.path)
    walk(vol_dir)
    log(f"  {len(files)} files; downloading…")
    with tarfile.open(out_tgz, "w:gz") as tar:
        for i, fp in enumerate(files, 1):
            rel = fp[len(vol_dir):].lstrip("/")
            resp = w.files.download(fp)
            blob = resp.contents.read()
            ti = tarfile.TarInfo(name=rel); ti.size = len(blob)
            tar.addfile(ti, io.BytesIO(blob))
            if i % 50 == 0:
                log(f"  …{i}/{len(files)}")
    log(f"  wrote {out_tgz.name} ({out_tgz.stat().st_size//1024} KB)")


def _retemplate(text, src):
    """Rewrite the source product schema AND the analytics schema to placeholders.
    Both are consolidated into the single install {{SCHEMA}}."""
    cat, sch = src.split(".")
    text = text.replace(f"{cat}.{sch}", "{{CATALOG}}.{{SCHEMA}}")
    text = text.replace(f"{cat}.{ANALYTICS_SCHEMA}", "{{CATALOG}}.{{SCHEMA}}")
    return text


def export_dashboard(api, did, src, out):
    try:
        d = api.do("GET", f"/api/2.0/lakeview/dashboards/{did}")
        ser = d.get("serialized_dashboard") or "{}"
        out.write_text(_retemplate(ser, src))
        log(f"dashboard -> {out.name} ({len(ser)//1024} KB)")
    except Exception as e:
        log(f"⚠️  dashboard export failed: {e}")


def export_genie(api, sid, src, out):
    try:
        g = api.do("GET", f"/api/2.0/genie/spaces/{sid}",
                   query={"include_serialized_space": "true"})
        base = json.loads(g.get("serialized_space") or "{}")
        instr = (((base.get("instructions") or {}).get("text_instructions") or [{}])[0]
                 .get("content") or [None])[0]
        if instr:
            out.write_text(_retemplate(instr, src))
            log(f"genie instructions -> {out.name} ({len(instr)} chars)")
        else:
            log("genie: no live text instructions; keeping reconstructed instructions.md")
    except Exception as e:
        log(f"genie export skipped ({e}); keeping reconstructed instructions.md")


def _lb_token(w, profile):
    import subprocess
    out = subprocess.run(["databricks", "auth", "token", "--profile", profile, "--output", "json"],
                         capture_output=True, text=True).stdout
    return json.loads(out)["access_token"]


def _lb_connect(w, profile):
    """Fresh Lakebase connection as the current user (OAuth token)."""
    import psycopg2
    return psycopg2.connect(host=LAKEBASE_HOST, port=5432, dbname=LAKEBASE_DB,
                            user=w.current_user.me().user_name, password=_lb_token(w, profile),
                            sslmode="require", connect_timeout=30)


def export_lakebase(w, host, profile, force=False):
    """Dump schema + seed CSVs. One fresh connection per table; resilient to per-table failures."""
    log(f"lakebase connect {LAKEBASE_HOST}/{LAKEBASE_DB} as {w.current_user.me().user_name}")
    schema_lines = ["-- BrickJewels Lakebase schema (exported from live)\n"]
    ok, failed = [], []
    for t in LB_TABLES:
        try:
            conn = _lb_connect(w, profile); conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("""
                  SELECT column_name, data_type, column_default, is_nullable
                  FROM information_schema.columns WHERE table_name=%s ORDER BY ordinal_position""", (t,))
                colrows = cur.fetchall()
                if not colrows:
                    log(f"  {t}: not found, skipping"); conn.close(); continue
                defs = []
                for name, dtype, default, nullable in colrows:
                    d = f'    {name} {dtype.upper()}'
                    if default and "nextval" not in str(default):
                        d += f" DEFAULT {default}"
                    if nullable == "NO":
                        d += " NOT NULL"
                    defs.append(d)
                schema_lines.append(f"CREATE TABLE IF NOT EXISTS {t} (\n" + ",\n".join(defs) + "\n);\n")

                out = LB / f"{t}.csv.gz"
                if out.exists() and not force:
                    log(f"  {t}: csv.gz already present, schema refreshed, data kept"); conn.close(); ok.append(t); continue
                buf = io.StringIO()
                cur.copy_expert(f"COPY {t} TO STDOUT WITH CSV HEADER", buf)
                text = buf.getvalue()
                out.write_bytes(gzip.compress(text.encode()))
                nrows = max(text.count("\n") - 1, 0)
                log(f"  {t}: {nrows} rows -> {t}.csv.gz")
            conn.close(); ok.append(t)
        except Exception as e:
            failed.append(t); log(f"  ⚠️ {t}: {str(e)[:80]}")
            try: conn.close()
            except Exception: pass
    (PKG / "sql" / "lakebase_schema.sql").write_text("\n".join(schema_lines))
    log(f"lakebase schema -> sql/lakebase_schema.sql  (ok={len(ok)} failed={failed})")


if __name__ == "__main__":
    main()
