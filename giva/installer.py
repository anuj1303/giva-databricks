"""
giva.installer
======================
One-call installer for the **GIVA** AI jewelry-commerce demo — modelled
on dbdemos. `giva.install('giva', catalog=...)` provisions, into
the current Databricks workspace:

  1. UC data layer       (schema + volume + enriched products + embeddings + images)
  2. Vector Search       (DELTA_SYNC index, 1024-dim, semantic + image search)
  3. Genie Space         (NL -> SQL over the catalog)
  4. AI/BI dashboard     (catalog / sales analytics, published)
  5. Lakebase (Postgres) (orders/users/nudges/metal & product prices) + seed data
  6. FastAPI + React app (storefront + admin analytics)
  7. Two background jobs (Metals Refresh + Nudge Emails) — created PAUSED

Run from a Databricks notebook (auth automatic) or locally with a CLI profile:
    giva.install('giva', profile='DEFAULT', catalog='my_catalog')
"""
from __future__ import annotations

import gzip
import io
import json
import tarfile
import time
import uuid
from importlib import resources


# ───────────────────────────── small helpers ──────────────────────────────
def _pkg_path(rel: str):
    return resources.files("giva").joinpath(rel)


def _pkg_text(rel: str) -> str:
    return _pkg_path(rel).read_text(encoding="utf-8")


def _pkg_bytes(rel: str) -> bytes:
    return _pkg_path(rel).read_bytes()


def _exists(rel: str) -> bool:
    try:
        return _pkg_path(rel).is_file()
    except Exception:
        return False


def _load_conf(demo: str) -> dict:
    return json.loads(_pkg_text(f"conf/{demo}.json"))


def _render(text: str, ctx: dict) -> str:
    out = text
    for k, v in ctx.items():
        out = out.replace("{{" + k.upper() + "}}", str(v))
    return out


def _log(step: str, msg: str = ""):
    print(f"  [giva] {step:<13} {msg}")


# ───────────────────────────── the installer ──────────────────────────────
class Installer:
    def __init__(self, demo: str, *, profile: str | None = None,
                 catalog: str | None = None, schema: str | None = None,
                 warehouse_id: str | None = None, overwrite: bool = False,
                 install_app: bool = True, install_lakebase: bool = True,
                 install_jobs: bool = True, install_vector_search: bool = True,
                 workspace_path: str | None = None, app_name: str | None = None,
                 genie_title: str | None = None, dashboard_title: str | None = None,
                 vs_endpoint: str | None = None, lakebase_instance: str | None = None):
        from databricks.sdk import WorkspaceClient

        self.demo = demo
        self.conf = _load_conf(demo)
        d = self.conf["defaults"]
        self.overwrite = overwrite
        self.install_app = install_app
        self.install_lakebase = install_lakebase
        self.install_jobs = install_jobs
        self.install_vs = install_vector_search

        self.w = WorkspaceClient(profile=profile) if profile else WorkspaceClient()
        self.api = self.w.api_client
        self.me = self.w.current_user.me()
        self.user = self.me.user_name
        self.host = self.w.config.host.rstrip("/")

        self.catalog = catalog or d["catalog"]
        self.schema = schema or d["schema"]
        self.volume = d["volume"]
        self.vs_endpoint = vs_endpoint or d["vs_endpoint"]
        self.lakebase_instance = lakebase_instance or d["lakebase_instance"]
        self.lakebase_db = d["lakebase_database"]
        self.app_name = app_name or d["app_name"]
        self.genie_title = genie_title or d["genie_title"]
        self.dashboard_title = dashboard_title or d["dashboard_title"]
        self.warehouse_id = warehouse_id
        self.workspace_path = workspace_path or f"/Workspace/Users/{self.user}/giva_{self.schema}"

        # results
        self.genie_space_id = None
        self.dashboard_id = None
        self.app_url = None
        self.lakebase_host = None
        self.job_ids = {}  # env_var -> job_id

    def ctx(self) -> dict:
        return {
            "catalog": self.catalog, "schema": self.schema, "volume": self.volume,
            "vs_endpoint": self.vs_endpoint, "current_user": self.user,
            "lakebase_instance": self.lakebase_instance,
            "lakebase_database": self.lakebase_db,
            "lakebase_host": self.lakebase_host or "",
            "dashboard_id": self.dashboard_id or "",
            "genie_space_id": self.genie_space_id or "",
            "warehouse_id": self.warehouse_id or "",
            "price_refresh_job_id": self.job_ids.get("PRICE_REFRESH_JOB_ID", ""),
            "nudge_email_job_id": self.job_ids.get("NUDGE_EMAIL_JOB_ID", ""),
            "today": time.strftime("%Y-%m-%d"), "demo_name": self.demo,
        }

    # -- SQL (polls; tolerates cold serverless warehouse) ------------------
    def sql(self, statement: str):
        se = self.w.statement_execution
        resp = se.execute_statement(warehouse_id=self.warehouse_id,
                                    statement=statement, wait_timeout="50s")
        for _ in range(150):
            state = resp.status.state.value if resp.status and resp.status.state else None
            if state in ("PENDING", "RUNNING"):
                time.sleep(2); resp = se.get_statement(resp.statement_id); continue
            break
        state = resp.status.state.value if resp.status and resp.status.state else None
        if state != "SUCCEEDED":
            err = resp.status.error.message if (resp.status and resp.status.error) else ""
            raise RuntimeError(f"SQL failed (state={state}): {err}\n--- {statement[:200]}")
        cols = [c.name for c in resp.manifest.schema.columns] if resp.manifest and resp.manifest.schema else []
        rows = (resp.result.data_array if resp.result else None) or []
        return cols, rows

    # =====================================================================
    def run(self):
        print(f"\n💎  Installing demo '{self.demo}' → {self.catalog}.{self.schema}")
        print(f"    workspace : {self.host}")
        print(f"    user      : {self.user}\n")

        self.validate_catalog()
        self.resolve_warehouse()
        self.load_data()
        if self.install_vs:
            self._safe("vector-search", self.create_vector_search)
        self._safe("genie", self.create_genie)
        self._safe("dashboard", self.create_dashboard)
        if self.install_lakebase:
            self._safe("lakebase", self.provision_lakebase)
        if self.install_jobs:
            self._safe("jobs", self.create_jobs)
        if self.install_app:
            self._safe("app", self.deploy_app)
        self.summary()

    def _safe(self, name, fn):
        try:
            fn()
        except Exception as e:
            _log(name, f"⚠️  skipped/failed: {e}")

    # -- 0a. catalog -------------------------------------------------------
    def validate_catalog(self):
        try:
            names = [c.name for c in self.w.catalogs.list()]
        except Exception:
            return
        if self.catalog in names:
            _log("catalog", f"{self.catalog} ✓"); return
        hidden = {"system", "samples", "__databricks_internal"}
        usable = [n for n in names if n not in hidden]
        raise RuntimeError(
            f"Catalog '{self.catalog}' not found. Re-run with catalog=<one you can use>.\n"
            f"   Available: {', '.join(usable[:15]) or '(none visible)'}")

    # -- 0b. warehouse -----------------------------------------------------
    def resolve_warehouse(self):
        if self.warehouse_id:
            _log("warehouse", f"using {self.warehouse_id}"); return
        whs = list(self.w.warehouses.list())
        if not whs:
            raise RuntimeError("No SQL warehouse found. Create one or pass warehouse_id=...")
        running = [x for x in whs if getattr(x.state, "value", str(x.state)) == "RUNNING"]
        chosen = (running or whs)[0]
        self.warehouse_id = chosen.id
        if not running:
            _log("warehouse", f"starting {chosen.name}")
            try: self.w.warehouses.start(chosen.id).result(timeout=600)
            except Exception: pass
        _log("warehouse", f"using {chosen.name} ({chosen.id})")

    # -- 1. data layer -----------------------------------------------------
    def load_data(self):
        dconf = self.conf["data"]
        self.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.schema}")
        self.sql(f"CREATE VOLUME IF NOT EXISTS {self.catalog}.{self.schema}.{self.volume}")
        _log("schema", f"{self.catalog}.{self.schema} (+ volume {self.volume})")

        vol = f"/Volumes/{self.catalog}/{self.schema}/{self.volume}"
        # parquet tables -> volume/_load
        for t in dconf["tables"]:
            blob = _pkg_bytes(t["parquet"])
            dest = f"{vol}/_load/{t['name']}.parquet"
            self.w.files.upload(dest, io.BytesIO(blob), overwrite=True)
            _log("upload", f"{t['name']}.parquet ({len(blob)//1024} KB)")
        # images -> volume/<category>/<file>
        self._upload_images(dconf.get("image_archive"), vol)

        # run setup.sql
        sql_text = _render(_pkg_text(dconf["setup_sql"]), self.ctx())
        for i, st in enumerate([s.strip() for s in self._split_sql(sql_text) if s.strip()], 1):
            self.sql(st)
            _log("sql", f"[{i}] {st.splitlines()[0][:55]}…")
        _, rows = self.sql(f"SELECT COUNT(*) FROM {self.catalog}.{self.schema}.enriched_jewelry_products")
        _log("verify", f"enriched_jewelry_products = {rows[0][0]} rows")

    def _upload_images(self, archive_rel, vol):
        if not archive_rel or not _exists(archive_rel):
            _log("images", "no image archive packaged — skipping"); return
        raw = _pkg_bytes(archive_rel)
        n = 0
        with tarfile.open(fileobj=io.BytesIO(raw), mode="r:gz") as tar:
            for m in tar.getmembers():
                if not m.isfile():
                    continue
                f = tar.extractfile(m)
                if f is None:
                    continue
                self.w.files.upload(f"{vol}/{m.name}", io.BytesIO(f.read()), overwrite=True)
                n += 1
                if n % 50 == 0:
                    _log("images", f"…{n} uploaded")
        _log("images", f"{n} product images → {vol}")

    @staticmethod
    def _split_sql(text):
        clean = "\n".join(l for l in text.splitlines() if not l.lstrip().startswith("--"))
        return clean.split(";")

    # -- 2. vector search --------------------------------------------------
    def create_vector_search(self):
        vc = self.conf["vector_search"]
        idx = f"{self.catalog}.{self.schema}.{vc['index']}"
        src = f"{self.catalog}.{self.schema}.{vc['source_table']}"
        # endpoint (idempotent)
        try:
            self.api.do("POST", "/api/2.0/vector-search/endpoints",
                        body={"name": self.vs_endpoint, "endpoint_type": vc["endpoint_type"]})
            _log("vs", f"creating endpoint {self.vs_endpoint}")
        except Exception:
            _log("vs", f"endpoint {self.vs_endpoint} exists")
        self._wait_vs_endpoint()
        # index (idempotent)
        try:
            self.api.do("POST", "/api/2.0/vector-search/indexes", body={
                "name": idx, "endpoint_name": self.vs_endpoint,
                "primary_key": vc["primary_key"], "index_type": "DELTA_SYNC",
                "delta_sync_index_spec": {
                    "source_table": src, "pipeline_type": vc["sync_type"],
                    "embedding_vector_columns": [{
                        "name": vc["embedding_column"],
                        "embedding_dimension": vc["embedding_dimension"]}]}})
            _log("vs", f"creating index {vc['index']}")
        except Exception as e:
            _log("vs", f"index exists or create skipped ({str(e)[:60]})")
        # trigger a sync
        try:
            self.api.do("POST", f"/api/2.0/vector-search/indexes/{idx}/sync")
        except Exception:
            pass
        _log("vs", "index sync triggered (continues in background)")

    def _wait_vs_endpoint(self, timeout=900):
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                e = self.api.do("GET", f"/api/2.0/vector-search/endpoints/{self.vs_endpoint}")
                state = (e.get("endpoint_status") or {}).get("state")
                if state == "ONLINE":
                    _log("vs", "endpoint ONLINE"); return
            except Exception:
                pass
            time.sleep(15)
        _log("vs", "endpoint still provisioning (continuing)")

    # -- 3. genie ----------------------------------------------------------
    def create_genie(self):
        gconf = self.conf["genie"]; ctx = self.ctx()

        def col_cfg(identifier, entity):
            ident = _render(identifier, ctx)
            _, rows = self.sql(f"DESCRIBE {ident}")
            names = [r[0] for r in rows if r[0] and not r[0].startswith("#") and r[0].strip()]
            cfgs = []
            for c in sorted(set(names)):
                cf = {"column_name": c, "enable_format_assistance": True}
                if c in entity:
                    cf["enable_entity_matching"] = True
                cfgs.append(cf)
            return {"identifier": ident, "column_configs": cfgs}

        tables = [col_cfg(t["identifier"], set(t.get("entity_columns", []))) for t in gconf["tables"]]
        tables.sort(key=lambda t: t["identifier"])
        space = {"version": 2, "data_sources": {"tables": tables}}
        created = self.api.do("POST", "/api/2.0/genie/spaces", body={
            "warehouse_id": self.warehouse_id, "serialized_space": json.dumps(space),
            "title": self.genie_title, "description": self.conf["description"][:900]})
        self.genie_space_id = created["space_id"]
        _log("genie", f"created '{self.genie_title}' ({self.genie_space_id})")

        instr = _render(_pkg_text(gconf["instructions"]), ctx)
        got = self.api.do("GET", f"/api/2.0/genie/spaces/{self.genie_space_id}",
                          query={"include_serialized_space": "true"})
        base = json.loads(got["serialized_space"])
        base["instructions"] = {"text_instructions": [{"id": uuid.uuid4().hex, "content": [instr]}]}
        self.api.do("PATCH", f"/api/2.0/genie/spaces/{self.genie_space_id}",
                    body={"serialized_space": json.dumps(base)})
        _log("genie", "instructions applied")

    # -- 4. dashboard ------------------------------------------------------
    def create_dashboard(self):
        dconf = self.conf["dashboards"][0]
        if not _exists(dconf["definition"]):
            _log("dashboard", "no dashboard definition packaged — skipping"); return
        serialized = _render(_pkg_text(dconf["definition"]), self.ctx())
        self._mkdir(self.workspace_path)
        created = self.api.do("POST", "/api/2.0/lakeview/dashboards", body={
            "display_name": self.dashboard_title, "warehouse_id": self.warehouse_id,
            "serialized_dashboard": serialized, "parent_path": self.workspace_path})
        self.dashboard_id = created["dashboard_id"]
        _log("dashboard", f"created '{self.dashboard_title}' ({self.dashboard_id})")
        if dconf.get("publish"):
            self.api.do("POST", f"/api/2.0/lakeview/dashboards/{self.dashboard_id}/published",
                        body={"warehouse_id": self.warehouse_id})
            _log("dashboard", "published")

    # -- 5. lakebase -------------------------------------------------------
    def provision_lakebase(self):
        lc = self.conf["lakebase"]
        # 5a. instance (idempotent)
        inst = self._get_lakebase_instance()
        if inst is None:
            self.api.do("POST", "/api/2.0/database/instances",
                        body={"name": self.lakebase_instance, "capacity": lc["capacity"]})
            _log("lakebase", f"creating instance '{self.lakebase_instance}' (~3-5 min)")
        inst = self._wait_lakebase_instance()
        self.lakebase_host = inst.get("read_write_dns")
        _log("lakebase", f"instance available @ {self.lakebase_host}")

        # 5b. connect (as current user, OAuth token) and create the database
        token = self._bearer()
        self._pg_create_database(self.lakebase_host, token)

        # 5c. schema + seed (into the giva database)
        if _exists(lc["schema_sql"]):
            self._pg_run_script(self.lakebase_host, token, self.lakebase_db,
                                _pkg_text(lc["schema_sql"]))
            _log("lakebase", "schema created")
        self._seed_lakebase(lc, token)

    def _seed_lakebase(self, lc, token):
        import psycopg2
        seed_dir = lc.get("seed_dir")
        conn = self._pg(self.lakebase_host, token, self.lakebase_db)
        try:
            for table in lc.get("seed_order", []):
                rel = f"{seed_dir}/{table}.csv.gz"
                if not _exists(rel):
                    continue
                try:
                    with conn.cursor() as cur:
                        cur.execute(f"SELECT COUNT(*) FROM {table}")
                        if cur.fetchone()[0] > 0:
                            _log("lakebase", f"{table}: already populated, skip"); continue
                        # stream-decompress straight into COPY (avoids loading huge tables into memory)
                        reader = gzip.open(io.BytesIO(_pkg_bytes(rel)), "rt", encoding="utf-8")
                        # Map columns by the CSV header (not table position) so a seed exported
                        # in a different column order than lakebase_schema.sql still loads correctly.
                        header = reader.readline().strip()
                        cols = ", ".join('"' + c.strip().strip('"') + '"' for c in header.split(","))
                        cur.copy_expert(f"COPY {table} ({cols}) FROM STDIN WITH CSV", reader)
                    conn.commit()
                    _log("lakebase", f"seeded {table}")
                except Exception as e:
                    conn.rollback()
                    _log("lakebase", f"⚠️  {table} skipped: {str(e).splitlines()[0][:90]}")
        finally:
            conn.close()

    def _get_lakebase_instance(self):
        try:
            return self.api.do("GET", f"/api/2.0/database/instances/{self.lakebase_instance}")
        except Exception:
            return None

    def _wait_lakebase_instance(self, timeout=900):
        deadline = time.time() + timeout
        while time.time() < deadline:
            inst = self._get_lakebase_instance() or {}
            if inst.get("state") == "AVAILABLE":
                return inst
            time.sleep(15)
        raise RuntimeError("timed out waiting for Lakebase instance")

    def _bearer(self) -> str:
        hdr = self.w.config.authenticate() or {}
        auth = hdr.get("Authorization", "")
        return auth.split(" ", 1)[1] if " " in auth else auth

    def _pg(self, host, token, dbname):
        import psycopg2
        return psycopg2.connect(host=host, port=5432, dbname=dbname,
                                user=self.user, password=token, sslmode="require")

    def _pg_create_database(self, host, token):
        conn = self._pg(host, token, "databricks_postgres")
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM pg_database WHERE datname=%s", (self.lakebase_db,))
                if not cur.fetchone():
                    cur.execute(f'CREATE DATABASE "{self.lakebase_db}"')
                    _log("lakebase", f"database '{self.lakebase_db}' created")
                else:
                    _log("lakebase", f"database '{self.lakebase_db}' exists")
        finally:
            conn.close()

    def _pg_run_script(self, host, token, dbname, script):
        conn = self._pg(host, token, dbname); conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(script)
        finally:
            conn.close()

    def _grant_lakebase_sp(self, sp_client_id, sp_numeric_id):
        """Give the app service principal a Postgres login + schema rights."""
        if not self.lakebase_host:
            return
        token = self._bearer()
        conn = self._pg(self.lakebase_host, token, self.lakebase_db); conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(f'DO $$ BEGIN CREATE ROLE "{sp_client_id}" LOGIN; '
                            f'EXCEPTION WHEN duplicate_object THEN NULL; END $$;')
                if sp_numeric_id:
                    cur.execute(f'SECURITY LABEL FOR databricks_auth ON ROLE "{sp_client_id}" '
                                f"IS 'id={sp_numeric_id},type=SERVICE_PRINCIPAL'")
                cur.execute(f'GRANT USAGE, CREATE ON SCHEMA public TO "{sp_client_id}"')
                cur.execute(f'GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO "{sp_client_id}"')
                cur.execute(f'GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO "{sp_client_id}"')
                cur.execute(f'ALTER DEFAULT PRIVILEGES IN SCHEMA public '
                            f'GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO "{sp_client_id}"')
            _log("lakebase", f"granted Postgres access to app SP {sp_client_id}")
        finally:
            conn.close()

    # -- 6. jobs (created PAUSED) ------------------------------------------
    def create_jobs(self):
        from databricks.sdk.service.workspace import ImportFormat, Language
        nb_root = f"{self.workspace_path}/notebooks"
        self._mkdir(nb_root)
        for j in self.conf["jobs"]:
            # upload the notebook (render {{CATALOG}}/{{SCHEMA}}/{{LAKEBASE_HOST}}… so it
            # points at what was just provisioned — keeps the packaged source generic)
            nb_src = _render(_pkg_text(j["notebook"]), self.ctx())
            nb_ws = f"{self.workspace_path}/{j['workspace_subpath']}"
            self.w.workspace.upload(nb_ws, nb_src.encode("utf-8"),
                                    format=ImportFormat.SOURCE, language=Language.PYTHON, overwrite=True)
            # render + create the job
            spec = json.loads(_render(_pkg_text(j["definition"]), {**self.ctx(), "notebook_path": nb_ws}))
            # belt-and-suspenders: force PAUSED
            if j.get("paused") and isinstance(spec.get("schedule"), dict):
                spec["schedule"]["pause_status"] = "PAUSED"
            created = self.api.do("POST", "/api/2.1/jobs/create", body=spec)
            jid = str(created["job_id"])
            self.job_ids[j["env_var"]] = jid
            sch = spec.get("schedule", {})
            _log("jobs", f"{j['key']} → job {jid}  schedule={sch.get('pause_status','PAUSED')} ✓ (not triggered)")

    # -- 7. app ------------------------------------------------------------
    def deploy_app(self):
        from databricks.sdk.service.workspace import ImportFormat
        aconf = self.conf["app"]; ctx = self.ctx()
        app_ws = f"{self.workspace_path}/app"

        existing = self._get_app()
        if existing is None:
            self.api.do("POST", "/api/2.0/apps",
                        body={"name": self.app_name, "description": "GIVA demo app"})
            _log("app", f"creating '{self.app_name}' (provisioning compute, ~2-3 min)")
        else:
            _log("app", f"reusing existing '{self.app_name}'")
        app = self._wait_app_compute()
        sp = app.get("service_principal_client_id")
        sp_num = app.get("service_principal_id")
        _log("app", f"service principal {sp}")

        # grant app SP access to the data (UC)
        for g in aconf["grants"]:
            try:
                self.sql(_render(g, {**ctx, "app_sp": sp}))
            except Exception as e:
                _log("app", f"grant skipped: {str(e)[:60]}")
        _log("app", "granted SP catalog/schema/select/volume/index")

        # grant app SP access to Lakebase (Postgres role + label + schema grants)
        if self.install_lakebase and self.lakebase_host:
            try:
                self._grant_lakebase_sp(sp, sp_num)
            except Exception as e:
                _log("app", f"lakebase SP grant skipped: {str(e)[:80]}")

        # upload source (render app.yaml with all resolved ids)
        self._mkdir(app_ws); self._mkdir(f"{app_ws}/backend"); self._mkdir(f"{app_ws}/backend/static")
        self._mkdir(f"{app_ws}/backend/static/assets")
        text_files = {
            "app.yaml": _render_appyaml(_pkg_text("app/app.yaml"), ctx),
            "requirements.txt": _pkg_text("app/requirements.txt"),
            "admin_config.json": _pkg_text("app/admin_config.json") if _exists("app/admin_config.json") else "{}",
            "backend/__init__.py": "",
            "backend/main.py": _pkg_text("app/backend/main.py"),
            "backend/static/index.html": _pkg_text("app/backend/static/index.html"),
        }
        for rel, content in text_files.items():
            self.w.workspace.upload(f"{app_ws}/{rel}", content.encode("utf-8"),
                                    format=ImportFormat.AUTO, overwrite=True)
        # static assets (binary)
        for ap in _pkg_path("app/backend/static/assets").iterdir():
            self.w.workspace.upload(f"{app_ws}/backend/static/assets/{ap.name}",
                                    ap.read_bytes(), format=ImportFormat.AUTO, overwrite=True)
        _log("app", f"uploaded source → {app_ws}")

        # attach resources
        resources_body = []
        for r in aconf["resources"]:
            if r["type"] == "sql_warehouse":
                resources_body.append({"name": r["name"],
                    "sql_warehouse": {"id": self.warehouse_id, "permission": r["permission"]}})
            elif r["type"] == "database" and self.lakebase_host:
                resources_body.append({"name": r["name"], "database": {
                    "instance_name": self.lakebase_instance,
                    "database_name": self.lakebase_db, "permission": r["permission"]}})
        try:
            self.api.do("PATCH", f"/api/2.0/apps/{self.app_name}",
                        body={"name": self.app_name, "resources": resources_body})
            _log("app", "attached resources")
        except Exception as e:
            _log("app", f"resource attach warning: {str(e)[:60]}")

        # deploy
        dep = self.api.do("POST", f"/api/2.0/apps/{self.app_name}/deployments",
                          body={"source_code_path": app_ws})
        self._wait_app_deploy(dep.get("deployment_id"))
        self.app_url = (self._get_app() or {}).get("url")
        _log("app", f"deployed → {self.app_url}")

    def _get_app(self):
        try: return self.api.do("GET", f"/api/2.0/apps/{self.app_name}")
        except Exception: return None

    def _wait_app_compute(self, timeout=600):
        deadline = time.time() + timeout
        while time.time() < deadline:
            app = self._get_app() or {}
            cs = (app.get("compute_status") or {}).get("state")
            if cs in ("ACTIVE", "STOPPED"): return app
            if cs == "ERROR": raise RuntimeError(f"app compute ERROR: {app.get('compute_status')}")
            time.sleep(8)
        raise RuntimeError("timed out waiting for app compute")

    def _wait_app_deploy(self, deployment_id, timeout=600):
        if not deployment_id: return
        deadline = time.time() + timeout
        while time.time() < deadline:
            dep = self.api.do("GET", f"/api/2.0/apps/{self.app_name}/deployments/{deployment_id}")
            state = (dep.get("status") or {}).get("state")
            if state == "SUCCEEDED": return
            if state in ("FAILED", "STOPPED", "CANCELLED"):
                raise RuntimeError(f"deployment {state}: {dep.get('status')}")
            time.sleep(8)
        _log("app", "deploy still in progress (timeout) — check the app page")

    def _mkdir(self, path):
        try: self.w.workspace.mkdirs(path)
        except Exception: pass

    # -- summary -----------------------------------------------------------
    def summary(self):
        print("\n✅  GIVA installed.\n")
        line = lambda k, v: print(f"   • {k:<12} {v}")
        line("Data", f"{self.catalog}.{self.schema} (enriched_jewelry_products, jewelry_embeddings)")
        if self.install_vs:
            line("Vector Srch", f"{self.vs_endpoint} / jewelry_embeddings_index")
        if self.genie_space_id:
            line("Genie", f"{self.host}/genie/rooms/{self.genie_space_id}")
        if self.dashboard_id:
            line("Dashboard", f"{self.host}/dashboardsv3/{self.dashboard_id}")
        if self.lakebase_host:
            line("Lakebase", f"{self.lakebase_instance} / {self.lakebase_db}")
        if self.job_ids:
            for k, v in self.job_ids.items():
                line("Job (PAUSED)", f"{k} = {v}")
        if self.app_url:
            line("App", self.app_url)
        print("\n   ⏸  Metals Refresh & Nudge Emails jobs are PAUSED — trigger them")
        print("      manually from the admin app, or unpause when you're ready.")
        print("   💎  Try in Genie:  “What are the top categories by total list value?”\n")


def _render_appyaml(yaml_text: str, ctx: dict) -> str:
    return _render(yaml_text, ctx)


# ───────────────────────────── public API ─────────────────────────────────
def install(demo: str = "giva", *, profile: str | None = None,
            catalog: str | None = None, schema: str | None = None,
            warehouse_id: str | None = None, overwrite: bool = False,
            install_app: bool = True, install_lakebase: bool = True,
            install_jobs: bool = True, install_vector_search: bool = True,
            workspace_path: str | None = None, app_name: str | None = None,
            genie_title: str | None = None, dashboard_title: str | None = None,
            vs_endpoint: str | None = None, lakebase_instance: str | None = None):
    """Install the GIVA demo into the current workspace. See module docstring."""
    Installer(demo, profile=profile, catalog=catalog, schema=schema,
              warehouse_id=warehouse_id, overwrite=overwrite, install_app=install_app,
              install_lakebase=install_lakebase, install_jobs=install_jobs,
              install_vector_search=install_vector_search, workspace_path=workspace_path,
              app_name=app_name, genie_title=genie_title, dashboard_title=dashboard_title,
              vs_endpoint=vs_endpoint, lakebase_instance=lakebase_instance).run()
