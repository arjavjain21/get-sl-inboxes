#!/usr/bin/env python3
"""
Smartlead accounts export with smart auth detection and fast fail.

- Paste your token into SMARTLEAD_TOKEN below.
- The script probes both Authorization header styles:
    1) Authorization: <TOKEN>
    2) Authorization: Bearer <TOKEN>
  It picks the one that works, then proceeds.
- 10k page size by default.
"""

import argparse
import csv
import json
import logging
import sys
import time
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple

import requests

API_URL = "https://server.smartlead.ai/api/email-account/get-total-email-accounts"
DEFAULT_LIMIT = 10000
REQUEST_TIMEOUT = 30
TOKEN_PROBE_TIMEOUT = 12
MAX_429_RETRIES = 4

# Paste token only. Do not include the word "Bearer".
SMARTLEAD_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyIjp7InRlYW1fbWVtYmVyX2VtYWlsIjoiYXJqYXZAZWFnbGVpbmZvc2VydmljZS5jb20iLCJ0ZWFtX21lbWJlcl9pZCI6OTc0NDE0LCJ0ZWFtX21lbWJlcl9uYW1lIjoiQXJqYXYgSmFpbiIsInRlYW1fbWVtYmVyX3V1aWQiOiI5ZjBkMjJiMS0yNjA4LTQzNmQtOTY4NS1kNWEzYmQ3MTQxNWQiLCJ0ZWFtX21lbWJlcl91c2VyX2lkIjoyMzY4LCJ0ZWFtX21lbWJlcl9yb2xlIjoiRlVMTF9NRU1CRVJfQUNDRVNTIiwiaWQiOjIzNjgsInV1aWQiOiIyZmJmNGY3ZC00NGFmLTRmZjEtOGUyNS01NjU1ZjU0ODNmZDAiLCJlbWFpbCI6ImF0aXNoYXlAZWFnbGVpbmZvc2VydmljZXMuY29tIiwibmFtZSI6IkF0aXNoYXkgSmFpbiIsInRva2VuX3ZlcnNpb24iOjJ9LCJodHRwczovL2hhc3VyYS5pby9qd3QvY2xhaW1zIjp7IngtaGFzdXJhLWFsbG93ZWQtcm9sZXMiOlsidGVhbV9tZW1iZXJzIl0sIngtaGFzdXJhLWRlZmF1bHQtcm9sZSI6InRlYW1fbWVtYmVycyIsIngtaGFzdXJhLXRlYW0tbWVtYmVyLWlkIjoiOTc0NDE0IiwieC1oYXN1cmEtdGVhbS1tZW1iZXItdXVpZCI6IjlmMGQyMmIxLTI2MDgtNDM2ZC05Njg1LWQ1YTNiZDcxNDE1ZCIsIngtaGFzdXJhLXRlYW0tbWVtYmVyLW5hbWUiOiJBcmphdiBKYWluIiwieC1oYXN1cmEtdGVhbS1tZW1iZXItZW1haWwiOiJhcmphdkBlYWdsZWluZm9zZXJ2aWNlLmNvbSIsIngtaGFzdXJhLXRlYW0tbWVtYmVyLXJvbGUiOiJGVUxMX01FTUJFUl9BQ0NFU1MiLCJ4LWhhc3VyYS11c2VyLWlkIjoiMjM2OCIsIngtaGFzdXJhLXVzZXItdXVpZCI6IjJmYmY0ZjdkLTQ0YWYtNGZmMS04ZTI1LTU2NTVmNTQ4M2ZkMCIsIngtaGFzdXJhLXVzZXItZW1haWwiOiJhdGlzaGF5QGVhZ2xlaW5mb3NlcnZpY2VzLmNvbSIsIngtaGFzdXJhLXVzZXItbmFtZSI6IkF0aXNoYXkgSmFpbiIsIngtaGFzdXJhLXRva2VuLXZlcnNpb24iOiIyIn0sImlhdCI6MTc1NTg4OTE2MX0.xV1FvWTeBJu7cJBAV_1cShWktmQZxmJ_afCOWoGvYKY"

CSV_COLUMNS = [
    "id",
    "time_to_wait_in_mins",
    "from_name",
    "from_email",
    "__typename",
    "type",
    "smtp_host",
    "is_smtp_success",
    "is_imap_success",
    "message_per_day",
    "daily_sent_count",
    "smart_sender_flag",
    "client_id",
    "client",

    # dns_validation_status flattened
    "isSPFVerified",
    "isDKIMVerified",
    "isDMARCVerified",
    "lastVerifiedTime",

    # email_warmup_details flattened
    "warmup_status",
    "warmup_reputation",
    "is_warmup_blocked",

    # email_account_tag_mappings flattened
    "tag_id",
    "tag_name",
    "tag_color",
    "email_account_tag_mappings_count",

    # campaign mappings
    "email_campaign_account_mappings_count",
]


class TokenInvalidError(Exception):
    pass

class NonRecoverableHTTPError(Exception):
    pass

def make_header_variants(token: str):
    token = token.strip()
    # If user pasted with "Bearer " already, normalize to raw
    if token.lower().startswith("bearer "):
        token = token.split(" ", 1)[1].strip()
    raw = {"Accept": "application/json", "Authorization": token}
    bearer = {"Accept": "application/json", "Authorization": f"Bearer {token}"}
    return token, [raw, bearer]

def http_get(url: str, headers: Dict[str, str], params: Dict[str, int], timeout: int) -> requests.Response:
    attempt = 0
    backoff = 1.5
    while True:
        t0 = time.perf_counter()
        resp = requests.get(url, headers=headers, params=params, timeout=timeout)
        dt_ms = int((time.perf_counter() - t0) * 1000)

        body_lc = (resp.text or "").lower()

        # Treat classic auth failures as fatal
        if resp.status_code in (401, 403) or "invalid token" in body_lc or "jwt malformed" in body_lc:
            logging.warning("Auth failure %s in %sms. Body: %s", resp.status_code, dt_ms, safe_slice(resp.text))
            raise TokenInvalidError("Auth rejected")

        # Handle rate limit
        if resp.status_code == 429:
            attempt += 1
            if attempt > MAX_429_RETRIES:
                raise NonRecoverableHTTPError("Too many 429 responses")
            retry_after = resp.headers.get("Retry-After")
            try:
                sleep_s = float(retry_after) if retry_after else backoff * attempt
            except Exception:
                sleep_s = backoff * attempt
            logging.info("429 received, sleeping %.1fs then retrying", sleep_s)
            time.sleep(sleep_s)
            continue

        # Other server errors: surface and stop
        if 500 <= resp.status_code < 600:
            logging.error("HTTP %s in %sms. Body: %s", resp.status_code, dt_ms, safe_slice(resp.text))
            raise NonRecoverableHTTPError(f"Server error {resp.status_code}")

        return resp

def select_auth_headers(token: str) -> Dict[str, str]:
    token_norm, variants = make_header_variants(token)
    # Try raw first, then bearer
    for idx, h in enumerate(variants):
        style = "RAW" if idx == 0 else "BEARER"
        try:
            logging.info("Probing auth style: %s", style)
            resp = http_get(API_URL, h, {"offset": 0, "limit": 1}, TOKEN_PROBE_TIMEOUT)
            data = resp.json()
            if data.get("ok", False):
                logging.info("Auth style %s works.", style)
                return h
            # ok=false with message often means invalid token
            msg = data.get("message", "")
            logging.warning("Probe ok=false on %s: %s", style, msg)
        except TokenInvalidError:
            # Try next style
            continue
        except NonRecoverableHTTPError as e:
            # If this was a 500 with jwt malformed, try next style
            continue
        except Exception as e:
            logging.warning("Probe error on %s: %s", style, e)
            continue
    raise TokenInvalidError("Neither RAW nor BEARER Authorization formats worked")

def fetch_page(headers: Dict[str, str], offset: int, limit: int) -> Tuple[List[Dict], Optional[int]]:
    resp = http_get(API_URL, headers, {"offset": offset, "limit": limit}, REQUEST_TIMEOUT)
    if not resp.ok:
        raise NonRecoverableHTTPError(f"HTTP {resp.status_code}")
    try:
        payload = resp.json()
    except Exception as e:
        logging.error("Invalid JSON at offset=%s limit=%s: %s", offset, limit, e)
        raise
    accounts = payload.get("data", {}).get("email_accounts", []) or []
    last_seen = payload.get("data", {}).get("lastSeenId")
    logging.info("Fetched %s accounts at offset=%s limit=%s", len(accounts), offset, limit)
    return accounts, last_seen

def stream_accounts(headers: Dict[str, str], page_limit: int) -> Iterable[Dict]:
    offset = 0
    while True:
        accounts, _ = fetch_page(headers, offset, page_limit)
        if not accounts:
            logging.info("No more accounts at offset=%s. Stopping.", offset)
            break
        for acc in accounts:
            yield acc
        offset += len(accounts)

def flatten_account(acc: Dict) -> Dict:
    row = {
        "id": acc.get("id"),
        "time_to_wait_in_mins": acc.get("time_to_wait_in_mins"),
        "from_name": acc.get("from_name"),
        "from_email": acc.get("from_email"),
        "__typename": acc.get("__typename"),
        "type": acc.get("type"),
        "smtp_host": acc.get("smtp_host"),
        "is_smtp_success": acc.get("is_smtp_success"),
        "is_imap_success": acc.get("is_imap_success"),
        "message_per_day": acc.get("message_per_day"),
        "daily_sent_count": acc.get("daily_sent_count"),
        "smart_sender_flag": acc.get("smart_sender_flag"),
        "client_id": acc.get("client_id"),
        "client": json.dumps(acc.get("client"), ensure_ascii=False),
    }

    # dns_validation_status
    dns = acc.get("dns_validation_status") or {}
    row["isSPFVerified"] = dns.get("isSPFVerified")
    row["isDKIMVerified"] = dns.get("isDKIMVerified")
    row["isDMARCVerified"] = dns.get("isDMARCVerified")
    row["lastVerifiedTime"] = dns.get("lastVerifiedTime")

    # email_warmup_details
    warm = acc.get("email_warmup_details") or {}
    row["warmup_status"] = warm.get("status")
    row["warmup_reputation"] = warm.get("warmup_reputation")
    row["is_warmup_blocked"] = warm.get("is_warmup_blocked")

    # email_account_tag_mappings
    tags = acc.get("email_account_tag_mappings", [])
    tag_ids, tag_names, tag_colors = [], [], []
    if isinstance(tags, list) and tags:
        for t in tags:
            if isinstance(t, dict):
                tag_obj = t.get("tag") if "tag" in t else t
                if tag_obj:
                    tag_ids.append(str(tag_obj.get("id") or ""))
                    tag_names.append(tag_obj.get("name") or "")
                    tag_colors.append(tag_obj.get("color") or "")
    row["tag_id"] = ", ".join(tag_ids)
    row["tag_name"] = ", ".join(tag_names)
    row["tag_color"] = ", ".join(tag_colors)
    row["email_account_tag_mappings_count"] = len(tags) if tags else 0

    # email_campaign_account_mappings
    try:
        row["email_campaign_account_mappings_count"] = (
            acc.get("email_campaign_account_mappings_aggregate", {})
               .get("aggregate", {})
               .get("count")
        )
    except Exception:
        row["email_campaign_account_mappings_count"] = None

    return row


def write_csv(rows: Iterable[Dict], out_path: str) -> int:
    t0 = time.perf_counter()
    count = 0
    with open(out_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for raw in rows:
            writer.writerow(flatten_account(raw))
            count += 1
            if count % 50000 == 0:
                logging.info("Wrote %s rows so far", count)
    dt = time.perf_counter() - t0
    logging.info("Wrote %s rows to %s in %.2fs", count, out_path, dt)
    return count

def safe_slice(s: str, n: int = 400) -> str:
    if s is None:
        return ""
    return s[:n] + ("..." if len(s) > n else "")

def main():
    parser = argparse.ArgumentParser(description="Export Smartlead email accounts to CSV")
    parser.add_argument("--token", help="JWT token, not the API key. Do not include the word Bearer.")
    parser.add_argument("--out", default=f"smartlead_email_accounts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        help="Output CSV path")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="Page size per request")
    args = parser.parse_args()

    token = (args.token or SMARTLEAD_TOKEN or "").strip()
    if not token or token == "PASTE_JWT_HERE":
        print("Paste your JWT into SMARTLEAD_TOKEN or pass --token", file=sys.stderr)
        sys.exit(2)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", force=True)
    logging.info("Run started. Target CSV: %s", args.out)
    logging.info("Configured page size: %s", args.limit)

    # Probe both header styles and select
    try:
        headers = select_auth_headers(token)
    except TokenInvalidError:
        logging.error("Token rejected in both RAW and BEARER formats. Check copy, spacing, or expiry.")
        sys.exit(1)
    except Exception as e:
        logging.error("Auth probe failed: %s", e)
        sys.exit(1)

    t0 = time.perf_counter()
    try:
        total = write_csv(stream_accounts(headers, args.limit), args.out)
    except TokenInvalidError as e:
        logging.error("Stopped due to token invalidation mid run: %s", e)
        sys.exit(1)
    except NonRecoverableHTTPError as e:
        logging.error("Stopped due to server error: %s", e)
        sys.exit(1)
    except requests.RequestException as e:
        logging.error("Network error: %s", e)
        sys.exit(1)
    except Exception as e:
        logging.exception("Unexpected error: %s", e)
        sys.exit(1)

    elapsed = time.perf_counter() - t0
    logging.info("Completed. Rows: %s. Elapsed: %.2fs", total, elapsed)

if __name__ == "__main__":
    main()
