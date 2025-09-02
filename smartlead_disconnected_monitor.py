import os
import sys
import time
import logging
import datetime as dt
from typing import List, Dict, Any, Set

import requests
import psycopg2
from psycopg2.extras import Json, execute_values
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from dotenv import load_dotenv

# ============ ENV CONFIG ============
load_dotenv()

SMARTLEAD_BEARER = os.getenv("SMARTLEAD_BEARER_TOKEN", "").strip()
SUPABASE_DSN = os.getenv("SUPABASE_DSN", "").strip()
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN", "").strip()
SLACK_CHANNEL_ID = os.getenv("SLACK_CHANNEL_ID", "").strip()
SMARTLEAD_BASE = os.getenv("SMARTLEAD_BASE", "https://server.smartlead.ai").rstrip("/")

TABLE = "public.disconnected_accounts_duplicate"

# API endpoint
DISCONNECTED_ENDPOINT = f"{SMARTLEAD_BASE}/api/email-account/get-total-email-accounts"
REQUEST_LIMIT_PER_PAGE = 5000
PAUSE_SEC = 0.25

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# ============ DB HELPERS ============
def db_conn():
    return psycopg2.connect(SUPABASE_DSN)

# ============ API FETCH ============
def fetch_disconnected(session: requests.Session) -> List[Dict[str, Any]]:
    def fetch_with_filter(params):
        out, offset, limit = [], 0, REQUEST_LIMIT_PER_PAGE
        headers = {"Authorization": f"Bearer {SMARTLEAD_BEARER}", "Accept": "application/json"}
        while True:
            q = {"offset": offset, "limit": limit, **params}
            resp = session.get(DISCONNECTED_ENDPOINT, headers=headers, params=q, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            items = data.get("data", {}).get("email_accounts", []) or []
            out.extend(items)
            logging.info(f"Fetched {len(items)} accounts at offset {offset} with {params}")
            if len(items) < limit:
                break
            offset += limit
            time.sleep(PAUSE_SEC)
        return out

    # Fetch SMTP-only, IMAP-only, and both
    smtp = fetch_with_filter({"isSmtpSuccess": "false"})
    imap = fetch_with_filter({"isImapSuccess": "false"})
    both = [acc for acc in smtp if acc in imap]  # intersection if needed

    # De-duplicate by id
    merged = {int(acc["id"]): acc for acc in smtp + imap}.values()
    return list(merged)



def normalize(item: Dict[str, Any]) -> Dict[str, Any]:
    tags = []
    for m in (item.get("email_account_tag_mappings") or []):
        t = m.get("tag") or {}
        if t.get("name"):
            tags.append(t["name"])

    # Determine disconnection type
    smtp_ok = item.get("is_smtp_success", True)
    imap_ok = item.get("is_imap_success", True)
    if not smtp_ok and not imap_ok:
        disconnection_type = "SMTP + IMAP"
    elif not smtp_ok:
        disconnection_type = "SMTP"
    elif not imap_ok:
        disconnection_type = "IMAP"
    else:
        disconnection_type = None

    return {
        "email_account_id": int(item["id"]),
        "account_id": int(item["id"]),  # mirror to satisfy NOT NULL
        "from_email": item.get("from_email") or "",
        "from_name": item.get("from_name") or "",
        "account_type": item.get("type") or "",
        "tags": ",".join(tags),  # store as comma-separated string
        "disconnection_type": disconnection_type,
        "payload": item,
    }


# ===================== GROUP CONFIG =====================
# Map group names to Slack channel IDs
CHANNEL_MAP = {
    "VOLTIC": "C09BJDZBU9E",
    "ENDY": "C01XXXXXXX2",
    "SCALEDMAIL": "C01XXXXXXX3",
    "CHEAPINBOXES": "C01XXXXXXX4",
    "DEFAULT": "C029111B6DT",    
    # Add more as needed
}

def classify_group(account: Dict[str, Any]) -> str:
    """
    Classify account into groups based on tags.
    If a tag *contains* a keyword, map it to the group.
    """
    tags = [t.upper() for t in account.get("tags", [])]

    # VOLTIC
    if any("VO" in t for t in tags):
        return "VOLTIC"
    # ENDY
    elif any("ENDY" in t for t in tags):
        return "ENDY"
    # SCALEDMAIL
    elif any("SCALED" in t for t in tags):
        return "SCALEDMAIL"
    # CHEAPINBOXES
    elif any("CHEAP" in t for t in tags):
        return "CHEAPINBOXES"
    else:
        return "DEFAULT"



# ============ DB LOGIC ============
def load_prev_ids() -> Set[int]:
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT email_account_id FROM {TABLE} WHERE currently_disconnected = TRUE;")
        return {int(r[0]) for r in cur.fetchall()}

def upsert_current(rows: List[Dict[str, Any]]):
    now = dt.datetime.utcnow()
    values = []
    for r in rows:
        values.append((
            r["email_account_id"],
            r["account_id"],
            r["from_email"],
            r["from_name"],
            r["account_type"],
            r["tags"],
            r["disconnection_type"],
            now,
            now,
            True,
            1,
            Json(r["payload"])
        ))

    sql = f"""
    INSERT INTO {TABLE} (
      email_account_id, account_id, from_email, from_name, account_type, tags, disconnection_type,
      first_disconnected_at, last_seen_disconnected_at, currently_disconnected,
      disconnect_count, last_payload
    )
    VALUES %s
    ON CONFLICT (email_account_id) DO UPDATE SET
      from_email = EXCLUDED.from_email,
      from_name = EXCLUDED.from_name,
      account_type = EXCLUDED.account_type,
      tags = EXCLUDED.tags,
      disconnection_type = EXCLUDED.disconnection_type,
      last_seen_disconnected_at = EXCLUDED.last_seen_disconnected_at,
      account_id = EXCLUDED.account_id,
      currently_disconnected = TRUE,
      disconnect_count = CASE
        WHEN {TABLE}.currently_disconnected = FALSE THEN {TABLE}.disconnect_count + 1
        ELSE {TABLE}.disconnect_count
      END,
      last_payload = EXCLUDED.last_payload;
    """

    with db_conn() as conn, conn.cursor() as cur:
        execute_values(cur, sql, values, page_size=500)
        conn.commit()
    logging.info(f"Upserted {len(rows)} rows in bulk")


def mark_reconnected(prev_ids: Set[int], curr_ids: Set[int]):
    diff = list(prev_ids - curr_ids)
    if not diff:
        return
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(f"UPDATE {TABLE} SET currently_disconnected = FALSE WHERE email_account_id = ANY(%s);", (diff,))
        conn.commit()
    logging.info(f"Marked {len(diff)} accounts as reconnected")

# ============ SLACK ============
def post_slack_grouped(new_rows: List[Dict[str, Any]], total_curr: int, total_prev: int):
    if not SLACK_BOT_TOKEN:
        logging.warning("Slack not configured.")
        return
    client = WebClient(token=SLACK_BOT_TOKEN)

    grouped = {}
    for r in new_rows:
        group = classify_group(r)
        grouped.setdefault(group, []).append(r)

    for group, rows in grouped.items():
        channel_id = CHANNEL_MAP.get(group)
        if not channel_id:
            logging.warning(f"No Slack channel mapping for group {group}, skipping")
            continue

        count = len(rows)
        emails_list = "\n".join([f"- {r['from_email']}" for r in rows])

        blocks = [
            {"type": "header", "text": {"type": "plain_text", "text": f"[{group}] {count} newly disconnected"}},
            {"type": "section", "text": {"type": "mrkdwn", "text": emails_list}},
            {"type": "context", "elements": [
                {"type": "mrkdwn", "text": f"Current disconnected: {total_curr} | Previously: {total_prev}"}
            ]}
        ]

        try:
            client.chat_postMessage(channel=channel_id, blocks=blocks, text=f"{group} new disconnections")
            logging.info(f"Posted {count} new disconnects to {group} ({channel_id})")
        except SlackApiError as e:
            logging.error(f"Slack post failed for {group}: {e}")



# ============ MAIN ============
def main():
    start = time.time()
    if not SMARTLEAD_BEARER:
        logging.error("SMARTLEAD_BEARER_TOKEN missing")
        sys.exit(1)
    session = requests.Session()
    raw = fetch_disconnected(session)
    current = [normalize(x) for x in raw]
    curr_ids = {r["email_account_id"] for r in current}
    logging.info(f"Current disconnected count: {len(curr_ids)}")
    prev_ids = load_prev_ids()
    logging.info(f"Previously disconnected count: {len(prev_ids)}")
    new_ids = curr_ids - prev_ids
    id_to_row = {r["email_account_id"]: r for r in current}
    newly = [id_to_row[i] for i in sorted(new_ids)]
    if current:
        upsert_current(current)
    mark_reconnected(prev_ids, curr_ids)
    if newly:
        logging.info(f"New disconnections detected: {len(newly)}")
        post_slack_grouped(newly, total_curr=len(curr_ids), total_prev=len(prev_ids))
    else:
        logging.info("No new disconnections detected")

    elapsed = round(time.time() - start, 2)
    logging.info(f"Execution completed in {elapsed} seconds")
    print(f"# New disconnects: {len(newly)} | Execution time: {elapsed}s")

if __name__ == "__main__":
    main()
