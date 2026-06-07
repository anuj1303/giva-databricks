import sys
sys.path.insert(0, "/Users/anuj.lathi/Desktop/brickjewels-demo")
from brickjewels.installer import Installer

S = "anuj_vm_workspace_catalog.brickjewels_demo_test"
VOL = "/Volumes/anuj_vm_workspace_catalog/brickjewels_demo_test/jewelry_images/_load/jewelry_embeddings.parquet"

inst = Installer("brickjewels", profile="AnujLathi", catalog="anuj_vm_workspace_catalog",
                 schema="brickjewels_demo_test", warehouse_id="0f16ae8ffb7cdef3",
                 vs_endpoint="brickjewels-test-vs", lakebase_instance="brickjewels-test",
                 app_name="brickjewels-test")

print(">>> FIX 2: recast embeddings to ARRAY<FLOAT> + CDF")
inst.sql(f"CREATE OR REPLACE TABLE {S}.jewelry_embeddings AS "
         f"SELECT product_id, CAST(embedding AS ARRAY<FLOAT>) AS embedding "
         f"FROM read_files('{VOL}', format => 'parquet')")
inst.sql(f"ALTER TABLE {S}.jewelry_embeddings SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
print("    recast done")

print(">>> create vector search index (was failing before)")
inst.create_vector_search()

print(">>> FIX 1: create the two PAUSED jobs")
inst.create_jobs()
print("    job_ids:", inst.job_ids)

print(">>> VERIFY jobs are PAUSED")
for env_var, jid in inst.job_ids.items():
    j = inst.api.do("GET", "/api/2.1/jobs/get", query={"job_id": jid})
    s = j.get("settings", {})
    sch = s.get("schedule", {})
    print(f"    {s.get('name')}  (job {jid})  pause_status={sch.get('pause_status')}  cron={sch.get('quartz_cron_expression')}")
