# Databricks notebook source
# MAGIC %md
# MAGIC # GIVA — Nudge Email Sender
# MAGIC
# MAGIC Sends personalized offer emails to users with active nudges.
# MAGIC Runs daily at 7 PM IST or triggered manually from the Databricks Jobs UI.
# MAGIC
# MAGIC **Flow:**
# MAGIC 1. Generate any new nudges (calls the nudge engine)
# MAGIC 2. Fetch all active, non-emailed nudges per user
# MAGIC 3. Send personalized HTML emails via Gmail API
# MAGIC 4. Log every email in `brickjewels_nudge_emails` table

# COMMAND ----------

import json, requests, base64, psycopg2, psycopg2.extras
from datetime import datetime, timezone, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

IST = timezone(timedelta(hours=5, minutes=30))
NOW_IST = datetime.now(IST)
print(f"Run started: {NOW_IST.strftime('%Y-%m-%d %H:%M:%S IST')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config & Auth

# COMMAND ----------

LAKEBASE_HOST = "{{LAKEBASE_HOST}}"
LAKEBASE_DB = "giva"
SENDER_EMAIL = "noreply@example.com"
SENDER_NAME = "GIVA Offers"
APP_URL = "https://tanishq-jewelry-demo-4203758776894418.aws.databricksapps.com"

# SP creds for Lakebase
SP_CLIENT_ID = dbutils.secrets.get("giva", "sp-client-id")
SP_CLIENT_SECRET = dbutils.secrets.get("giva", "sp-client-secret")
WS_HOST = dbutils.secrets.get("giva", "workspace-host")

# Gmail OAuth creds
gmail_creds = json.loads(dbutils.secrets.get("giva", "gmail-oauth-creds"))
GMAIL_CLIENT_ID = gmail_creds["client_id"]
GMAIL_CLIENT_SECRET = gmail_creds["client_secret"]
GMAIL_REFRESH_TOKEN = gmail_creds["refresh_token"]

print(f"SP Client ID: {SP_CLIENT_ID[:8]}...")
print(f"Gmail Client ID: {GMAIL_CLIENT_ID[:20]}...")

# COMMAND ----------

# Get SP OAuth token for Lakebase
sp_token_resp = requests.post(
    f"{WS_HOST}/oidc/v1/token",
    data={"grant_type": "client_credentials", "client_id": SP_CLIENT_ID,
          "client_secret": SP_CLIENT_SECRET, "scope": "all-apis"},
    headers={"Content-Type": "application/x-www-form-urlencoded"}, timeout=15,
)
assert sp_token_resp.status_code == 200, f"SP OAuth failed: {sp_token_resp.text}"
sp_token = sp_token_resp.json()["access_token"]

# Get Gmail access token
gmail_token_resp = requests.post(
    "https://oauth2.googleapis.com/token",
    data={"grant_type": "refresh_token", "client_id": GMAIL_CLIENT_ID,
          "client_secret": GMAIL_CLIENT_SECRET, "refresh_token": GMAIL_REFRESH_TOKEN},
    timeout=15,
)
assert gmail_token_resp.status_code == 200, f"Gmail OAuth failed: {gmail_token_resp.text}"
gmail_token = gmail_token_resp.json()["access_token"]
print("Both tokens acquired ✓")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Generate Nudges (ensure latest)

# COMMAND ----------

# Read optional user_ids parameter (comma-separated, passed from admin trigger)
try:
    user_ids_param = dbutils.widgets.get("user_ids")
except:
    user_ids_param = ""

TARGET_USER_IDS = [int(x.strip()) for x in user_ids_param.split(",") if x.strip()] if user_ids_param else []
if TARGET_USER_IDS:
    print(f"Targeted mode: sending nudges to {len(TARGET_USER_IDS)} users: {TARGET_USER_IDS}")
else:
    print("Broadcast mode: sending nudges to ALL users with active nudges")

# Skip app-level nudge generation — admin trigger already generated nudges for selected users
# Only call generate if running in broadcast (non-targeted) mode
if not TARGET_USER_IDS:
    try:
        r = requests.post(f"{APP_URL}/api/nudges/generate",
                          headers={"Authorization": f"Bearer {sp_token}"}, timeout=60)
        print(f"Nudge generation: {r.status_code} - {r.text[:200] if r.status_code == 200 else r.text[:500]}")
    except Exception as e:
        print(f"Nudge generation via app failed (non-critical, will use existing nudges): {e}")
else:
    print("Skipping broadcast nudge generation (targeted users already have nudges generated)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Fetch Users with Active Nudges

# COMMAND ----------

conn = psycopg2.connect(host=LAKEBASE_HOST, port=5432, dbname=LAKEBASE_DB,
                         user=SP_CLIENT_ID, password=sp_token, sslmode="require", connect_timeout=15)
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

# Ensure email history table exists
cur.execute("""CREATE TABLE IF NOT EXISTS brickjewels_nudge_emails (
    email_id SERIAL PRIMARY KEY, user_id INTEGER NOT NULL,
    nudge_id INTEGER, recipient_email TEXT NOT NULL,
    sender_email TEXT NOT NULL DEFAULT 'noreply@example.com',
    sender_name TEXT NOT NULL DEFAULT 'GIVA Offers',
    subject TEXT NOT NULL, body_html TEXT,
    status TEXT DEFAULT 'pending', error_message TEXT,
    sent_at TIMESTAMPTZ, created_at TIMESTAMPTZ DEFAULT NOW())""")
conn.commit()

# Clear previously failed emails so those nudges get retried
cur.execute("DELETE FROM brickjewels_nudge_emails WHERE status = 'failed'")
conn.commit()

# Get active nudges that haven't been emailed yet (filtered if targeted)
if TARGET_USER_IDS:
    placeholders = ",".join(["%s"] * len(TARGET_USER_IDS))
    cur.execute(f"""
        SELECT n.nudge_id, n.user_id, n.nudge_type, n.title, n.message,
               n.discount_type, n.discount_value, n.discount_code,
               n.target_category, n.valid_from, n.valid_to,
               u.first_name, u.last_name, u.email as user_email
        FROM brickjewels_nudges n
        JOIN brickjewels_users u ON n.user_id = u.user_id
        WHERE n.is_active = TRUE AND n.is_dismissed = FALSE AND n.is_redeemed = FALSE
          AND n.valid_to > NOW()
          AND n.user_id IN ({placeholders})
          AND n.nudge_id NOT IN (
              SELECT COALESCE(nudge_id, -1) FROM brickjewels_nudge_emails WHERE status = 'sent'
          )
        ORDER BY n.user_id, n.created_at DESC
    """, TARGET_USER_IDS)
else:
    cur.execute("""
        SELECT n.nudge_id, n.user_id, n.nudge_type, n.title, n.message,
               n.discount_type, n.discount_value, n.discount_code,
               n.target_category, n.valid_from, n.valid_to,
               u.first_name, u.last_name, u.email as user_email
        FROM brickjewels_nudges n
        JOIN brickjewels_users u ON n.user_id = u.user_id
        WHERE n.is_active = TRUE AND n.is_dismissed = FALSE AND n.is_redeemed = FALSE
          AND n.valid_to > NOW()
          AND n.nudge_id NOT IN (
              SELECT COALESCE(nudge_id, -1) FROM brickjewels_nudge_emails WHERE status = 'sent'
          )
        ORDER BY n.user_id, n.created_at DESC
    """)
pending_nudges = cur.fetchall()
print(f"Found {len(pending_nudges)} nudges to email" + (f" (targeted to {len(TARGET_USER_IDS)} users)" if TARGET_USER_IDS else " (all users)"))

# Group by user
from collections import defaultdict
user_nudges = defaultdict(list)
for n in pending_nudges:
    user_nudges[(n["user_id"], n["first_name"], n["user_email"])].append(n)

print(f"Across {len(user_nudges)} users")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Build & Send Emails

# COMMAND ----------

def build_nudge_email_html(first_name, nudges_list):
    """Build a GIVA-styled HTML email with all active nudges for a user."""
    nudge_blocks = []
    for n in nudges_list:
        valid_to = datetime.fromisoformat(str(n["valid_to"])).strftime("%d %b %Y") if n["valid_to"] else ""
        discount_info = ""
        cat_label = f' on {n["target_category"]}' if n.get("target_category") else ""
        if n["discount_type"] == "making_pct":
            discount_info = f'<span style="color:#061e58;font-size:24px;font-weight:800;">{int(n["discount_value"])}% OFF</span><br><span style="color:#6b6f76;font-size:12px;">on Making Charges{cat_label}</span>'
        elif n["discount_type"] == "cart_pct":
            discount_info = f'<span style="color:#061e58;font-size:24px;font-weight:800;">{int(n["discount_value"])}% OFF</span><br><span style="color:#6b6f76;font-size:12px;">on Cart Value{cat_label}</span>'
        elif n["discount_type"] == "flat_amount":
            cat_html = '<br><span style="color:#6b6f76;font-size:12px;">' + cat_label.strip() + '</span>' if cat_label else ''
            discount_info = f'<span style="color:#061e58;font-size:24px;font-weight:800;">₹{int(n["discount_value"]):,} OFF</span>{cat_html}'

        code_block = ""
        if n["discount_code"]:
            code_block = f'''
            <div style="margin-top:14px;padding:10px 18px;background:#fff2df;border:1px dashed #C9A84C;border-radius:8px;display:inline-block;">
                <span style="color:#6b6f76;font-size:10px;letter-spacing:1px;">USE CODE</span><br>
                <span style="color:#061e58;font-size:16px;font-weight:bold;letter-spacing:2px;">{n["discount_code"]}</span>
            </div>'''

        nudge_blocks.append(f'''
        <div style="background:#ffffff;border:1px solid #e9e3d8;border-left:4px solid #e9718b;border-radius:12px;padding:20px;margin-bottom:16px;">
            <div style="font-size:16px;color:#061e58;font-weight:700;margin-bottom:8px;">{n["title"]}</div>
            <div style="font-size:13px;color:#3a3a3a;line-height:1.6;margin-bottom:12px;">{n["message"]}</div>
            {f'<div style="margin-bottom:10px;">{discount_info}</div>' if discount_info else ''}
            {code_block}
            <div style="margin-top:12px;font-size:11px;color:#9a9a9a;">Valid until {valid_to}</div>
        </div>''')

    return f'''<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1.0"></head>
<body style="margin:0;padding:0;background:#f3f0fb;font-family:'Helvetica Neue',Arial,sans-serif;">
<div style="max-width:600px;margin:0 auto;background:#fcfaf6;border-radius:0 0 16px 16px;overflow:hidden;">
    <!-- Header: GIVA navy → lavender -->
    <div style="background:linear-gradient(135deg,#061e58 0%,#0c2f74 55%,#928bf1 100%);padding:28px 32px;text-align:center;">
        <div style="font-size:26px;font-weight:800;color:#ffffff;letter-spacing:6px;">GIVA</div>
        <div style="font-size:10px;color:#ffffff;opacity:0.75;margin-top:6px;letter-spacing:2px;text-transform:uppercase;">Powered by Databricks</div>
    </div>

    <!-- Accent strip -->
    <div style="height:4px;background:linear-gradient(90deg,#e9718b,#928bf1,#C9A84C);"></div>

    <!-- Body -->
    <div style="padding:32px 24px;">
        <div style="font-size:15px;color:#121212;margin-bottom:24px;">
            Dear {first_name},<br><br>
            We've curated a few special offers just for you ✨
        </div>

        {''.join(nudge_blocks)}

        <div style="text-align:center;margin-top:28px;">
            <a href="{APP_URL}" style="display:inline-block;background:linear-gradient(135deg,#061e58,#0c2f74);color:#ffffff;text-decoration:none;padding:13px 34px;border-radius:30px;font-weight:600;font-size:14px;">
                Shop GIVA →
            </a>
        </div>

        <div style="margin-top:32px;padding-top:20px;border-top:1px solid #e9e3d8;font-size:11px;color:#9a9a9a;text-align:center;line-height:1.6;">
            You're receiving this because you have an account at GIVA.<br>
            This is a demo application — no real transactions are processed.<br>
            <span style="color:#928bf1;">◆</span> GIVA · Powered by Databricks
        </div>
    </div>
</div>
</body></html>'''


def send_gmail(to_email, subject, html_body, gmail_token):
    """Send an email via Gmail REST API."""
    msg = MIMEMultipart("alternative")
    msg["From"] = f"{SENDER_NAME} <{SENDER_EMAIL}>"
    msg["To"] = to_email
    msg["Subject"] = subject
    msg["Reply-To"] = "noreply@giva-demo.example"
    msg.attach(MIMEText(html_body, "html"))

    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")
    resp = requests.post(
        "https://gmail.googleapis.com/gmail/v1/users/me/messages/send",
        headers={"Authorization": f"Bearer {gmail_token}", "Content-Type": "application/json",
                 "x-goog-user-project": "gcp-sandbox-field-eng"},
        json={"raw": raw}, timeout=30,
    )
    return resp.status_code, resp.json()

# COMMAND ----------

sent_count = 0
failed_count = 0

for (uid, fname, user_email), nudges_list in user_nudges.items():
    if not user_email:
        print(f"  Skipping user {uid} ({fname}) — no email address")
        continue

    # Build email
    subject = f"✨ {nudges_list[0]['title']} — Exclusive GIVA Offer"
    html = build_nudge_email_html(fname, nudges_list)

    # Send
    try:
        status_code, resp_data = send_gmail(user_email, subject, html, gmail_token)
        if status_code == 200:
            # Log success for each nudge
            for n in nudges_list:
                cur.execute("""INSERT INTO brickjewels_nudge_emails
                    (user_id, nudge_id, recipient_email, sender_email, sender_name, subject, body_html, status, sent_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, 'sent', NOW())""",
                    (uid, n["nudge_id"], user_email, SENDER_EMAIL, SENDER_NAME, subject, html))
            conn.commit()
            sent_count += 1
            print(f"  ✓ Sent to {user_email} ({fname}) — {len(nudges_list)} nudges")
        else:
            error_msg = resp_data.get("error", {}).get("message", str(resp_data))[:200]
            for n in nudges_list:
                cur.execute("""INSERT INTO brickjewels_nudge_emails
                    (user_id, nudge_id, recipient_email, sender_email, sender_name, subject, status, error_message)
                    VALUES (%s, %s, %s, %s, %s, %s, 'failed', %s)""",
                    (uid, n["nudge_id"], user_email, SENDER_EMAIL, SENDER_NAME, subject, error_msg))
            conn.commit()
            failed_count += 1
            print(f"  ✗ Failed for {user_email}: HTTP {status_code} — {error_msg}")
    except Exception as e:
        for n in nudges_list:
            cur.execute("""INSERT INTO brickjewels_nudge_emails
                (user_id, nudge_id, recipient_email, sender_email, sender_name, subject, status, error_message)
                VALUES (%s, %s, %s, %s, %s, %s, 'failed', %s)""",
                (uid, n["nudge_id"], user_email, SENDER_EMAIL, SENDER_NAME, subject, str(e)[:200]))
        conn.commit()
        failed_count += 1
        print(f"  ✗ Exception for {user_email}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

cur.close()
conn.close()

secs = (datetime.now(IST) - NOW_IST).total_seconds()
print(f"\n{'='*60}")
print(f"GIVA Nudge Email Sender COMPLETE ({secs:.1f}s)")
print(f"  Users processed: {len(user_nudges)}")
print(f"  Emails sent: {sent_count}")
print(f"  Emails failed: {failed_count}")
print(f"  Nudges covered: {len(pending_nudges)}")
print(f"  Finished: {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S IST')}")
print(f"{'='*60}")
