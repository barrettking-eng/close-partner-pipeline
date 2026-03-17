#!/usr/bin/env python3
"""
Close Partner Overlap Report
Fetches all PartnerStack customers, cross-references against Close.com leads,
and produces CSV reports showing partner influence per Close.com account.

Outputs (pushed to close-partner-pipeline/reports/):
  overlap_report.csv      — one row per Close.com lead, partner count + details
  unmatched_customers.csv — PartnerStack customers with no Close.com match
  overlap_summary.json    — headline stats for dashboard/monitoring use
"""

import json, os, base64, urllib.request, urllib.parse, time, csv, io
from datetime import datetime, timezone
from collections import defaultdict

# ── CONFIG ────────────────────────────────────────────────────
PS_PUBLIC_KEY  = os.environ["PS_PUBLIC_KEY"]
PS_SECRET_KEY  = os.environ["PS_SECRET_KEY"]
CLOSE_API_KEY  = os.environ["CLOSE_API_KEY"]
GH_TOKEN       = os.environ["GH_PAT"]
REPORT_REPO    = "barrettking-eng/close-partner-pipeline"
PS_API_BASE    = "https://api.partnerstack.com/api/v2"
CLOSE_API_BASE = "https://api.close.com/api/v1"
BATCH_SIZE     = 20    # emails per Close.com OR query
CLOSE_DELAY    = 0.02  # 50 req/sec (well under 60/s limit)

AFFILIATE_SLUGS = {
    "affiliatepartner",
    "affiliatepartnersponsorship",
    "affiliatepartnerdiscountpartnershipsplitcommission",
    "affiliatepartnerdiscountpartnershipnocommission",
}
SOLUTIONS_SLUGS = {
    "solutionpartnertier1",
    "solutionpartnertier2",
    "solutionpartnertier3",
}

# ── PARTNERSTACK ───────────────────────────────────────────────
def _ps_get(path, retries=4):
    creds = base64.b64encode(f"{PS_PUBLIC_KEY}:{PS_SECRET_KEY}".encode()).decode()
    for attempt in range(retries):
        try:
            req = urllib.request.Request(
                f"{PS_API_BASE}{path}",
                headers={"Authorization": f"Basic {creds}", "Accept": "application/json",
                         "User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.loads(r.read())["data"]
        except (urllib.error.HTTPError, urllib.error.URLError, Exception) as e:
            wait = 2 ** attempt
            print(f"  PS retry {attempt+1}/{retries} after {wait}s ({e})")
            time.sleep(wait)
    raise RuntimeError(f"PS API failed after {retries} retries: {path}")

def fetch_ps_customers():
    """Paginate all PS customers → {email: [partnership_ref_dicts]}"""
    print("Fetching PartnerStack customers…")
    by_email = defaultdict(list)
    cursor, page, total = None, 0, 0
    while True:
        qs = "?limit=100" + (f"&starting_after={urllib.parse.quote(cursor)}" if cursor else "")
        data = _ps_get(f"/customers{qs}")
        for item in data["items"]:
            email = (item.get("email") or "").strip().lower()
            if email:
                by_email[email].append({
                    "partnership_key": item.get("partnership_key"),
                    "partner_key":     item.get("partner_key"),
                    "customer_key":    item.get("customer_key"),
                    "source_type":     item.get("source_type"),
                })
        total += len(data["items"])
        page  += 1
        if page % 20 == 0:
            print(f"  page {page}: {total} records, {len(by_email)} unique emails")
        if not data["has_more"] or not data["items"]:
            break
        cursor = data["items"][-1]["key"]
    print(f"  Done: {total} records → {len(by_email)} unique emails")
    return by_email

def fetch_ps_partnerships():
    """Paginate all PS partnerships → {partnership_key: {name, email, type, slug}}"""
    print("Fetching PartnerStack partnerships…")
    by_key = {}
    cursor, page = None, 0
    while True:
        qs = "?limit=100" + (f"&starting_after={urllib.parse.quote(cursor)}" if cursor else "")
        data = _ps_get(f"/partnerships{qs}")
        for item in data["items"]:
            slug = item.get("group", {}).get("slug", "")
            tags = item.get("tags", [])
            by_key[item["key"]] = {
                "name":  f"{item.get('first_name','')} {item.get('last_name','')}".strip(),
                "email": item.get("email") or "",
                "type":  tags[0] if tags else slug,
                "slug":  slug,
            }
        page += 1
        if not data["has_more"] or not data["items"]:
            break
        cursor = data["items"][-1]["key"]
    print(f"  Done: {len(by_key)} partnerships loaded")
    return by_key

# ── CLOSE.COM ──────────────────────────────────────────────────
def _close_get(path):
    creds = base64.b64encode(f"{CLOSE_API_KEY}:".encode()).decode()
    req = urllib.request.Request(
        f"{CLOSE_API_BASE}{path}",
        headers={"Authorization": f"Basic {creds}", "Accept": "application/json"})
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read())

def _lookup_batch(emails):
    """
    Look up a batch of emails via a single OR query.
    Returns {email: {"id", "display_name", "status_label"}}
    """
    if not emails:
        return {}
    query = " OR ".join(f'email:"{e}"' for e in emails)
    time.sleep(CLOSE_DELAY)
    try:
        path = (
            "/lead/?_limit=200"
            "&_fields=id,display_name,status_label,contacts"
            "&query=" + urllib.parse.quote(query)
        )
        leads = _close_get(path).get("data", [])
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        print(f"  Warning: Close batch failed ({e.code}): {body[:120]}")
        return {}
    except Exception as e:
        print(f"  Warning: Close batch error: {e}")
        return {}

    email_set = {e.lower() for e in emails}
    result = {}
    for lead in leads:
        lead_summary = {
            "id":           lead["id"],
            "display_name": lead.get("display_name"),
            "status_label": lead.get("status_label"),
        }
        for contact in lead.get("contacts", []):
            for em_obj in contact.get("emails", []):
                ce = em_obj.get("email", "").lower()
                if ce in email_set and ce not in result:
                    result[ce] = lead_summary
    return result

def lookup_all_emails(emails):
    """Batch all emails against Close.com, return {email: lead_dict}."""
    print(f"Querying Close.com for {len(emails)} unique emails "
          f"({(len(emails) + BATCH_SIZE - 1) // BATCH_SIZE} batches)…")
    emails = list(emails)
    result = {}
    for i in range(0, len(emails), BATCH_SIZE):
        batch = emails[i:i + BATCH_SIZE]
        result.update(_lookup_batch(batch))
        if (i // BATCH_SIZE) % 50 == 49:
            matched = sum(1 for e in emails[:i+BATCH_SIZE] if e in result)
            print(f"  {i+BATCH_SIZE}/{len(emails)} emails processed, {matched} matched so far")
    print(f"  Done: {len(result)} emails matched to Close.com leads")
    return result

# ── CROSS-REFERENCE ────────────────────────────────────────────
def build_overlap(ps_by_email, ps_partnerships, close_by_email):
    """
    Join PS customers → Close.com leads.
    Returns:
      by_lead: {lead_id: {display_name, status, contact_emails, partnerships: {pk: info}}}
      unmatched: [{customer_email, customer_key, partner_name, partner_email, partner_type}]
    """
    by_lead    = defaultdict(lambda: {
        "display_name": None, "status_label": None,
        "contact_emails": set(), "partnerships": {}
    })
    unmatched = []

    for email, refs in ps_by_email.items():
        lead = close_by_email.get(email)
        if lead:
            lid = lead["id"]
            by_lead[lid]["display_name"] = lead["display_name"]
            by_lead[lid]["status_label"] = lead["status_label"]
            by_lead[lid]["contact_emails"].add(email)
            for ref in refs:
                pk = ref["partnership_key"]
                if pk and pk not in by_lead[lid]["partnerships"]:
                    info = ps_partnerships.get(pk, {})
                    by_lead[lid]["partnerships"][pk] = {
                        "name":         info.get("name", "Unknown"),
                        "email":        info.get("email", ""),
                        "type":         info.get("type", ""),
                        "slug":         info.get("slug", ""),
                        "customer_key": ref.get("customer_key", ""),
                    }
        else:
            for ref in refs:
                info = ps_partnerships.get(ref.get("partnership_key", ""), {})
                unmatched.append({
                    "customer_email": email,
                    "customer_key":   ref.get("customer_key", ""),
                    "partner_name":   info.get("name", ""),
                    "partner_email":  info.get("email", ""),
                    "partner_type":   info.get("type", ""),
                    "partner_slug":   info.get("slug", ""),
                })

    return dict(by_lead), unmatched

# ── REPORT FORMATTING ──────────────────────────────────────────
def _flag(n):
    if n == 0: return "no_partner"
    if n == 1: return "single_partner"
    return "multi_partner"

def format_overlap_rows(by_lead):
    rows = []
    for lid, d in by_lead.items():
        partners = list(d["partnerships"].values())
        rows.append({
            "close_lead_id":  lid,
            "company_name":   d["display_name"],
            "lead_status":    d["status_label"],
            "contact_emails": " | ".join(sorted(d["contact_emails"])),
            "partner_count":  len(partners),
            "flag":           _flag(len(partners)),
            "partner_names":  " | ".join(p["name"]  for p in partners),
            "partner_emails": " | ".join(p["email"] for p in partners),
            "partner_types":  " | ".join(p["type"]  for p in partners),
            "customer_keys":  " | ".join(p["customer_key"] or "" for p in partners),
        })
    rows.sort(key=lambda r: -r["partner_count"])
    return rows

def to_csv(rows, fields):
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=fields, extrasaction="ignore")
    w.writeheader()
    w.writerows(rows)
    return buf.getvalue()

# ── GITHUB PUSH ───────────────────────────────────────────────
def push_file(path, content_str):
    encoded = base64.b64encode(content_str.encode()).decode()
    api_url  = f"https://api.github.com/repos/{REPORT_REPO}/contents/{path}"
    headers  = {
        "Authorization": f"token {GH_TOKEN}",
        "Accept":        "application/vnd.github.v3+json",
        "Content-Type":  "application/json",
    }
    sha = None
    try:
        with urllib.request.urlopen(urllib.request.Request(api_url, headers=headers)) as r:
            sha = json.loads(r.read())["sha"]
    except urllib.error.HTTPError:
        pass
    body = {
        "message": f"Overlap report — {datetime.now(timezone.utc).strftime('%Y-%m-%d')}",
        "content": encoded,
    }
    if sha:
        body["sha"] = sha
    req = urllib.request.Request(api_url, data=json.dumps(body).encode(),
                                  headers=headers, method="PUT")
    with urllib.request.urlopen(req) as r:
        json.loads(r.read())
    print(f"  ✓ Pushed {path}")

# ── MAIN ──────────────────────────────────────────────────────
if __name__ == "__main__":
    start = datetime.now(timezone.utc)

    # 1. Fetch PartnerStack data
    ps_by_email    = fetch_ps_customers()
    ps_partnerships = fetch_ps_partnerships()

    # 2. Look up all PS customer emails in Close.com
    close_by_email = lookup_all_emails(ps_by_email.keys())

    # 3. Build overlap
    by_lead, unmatched = build_overlap(ps_by_email, ps_partnerships, close_by_email)

    # 4. Stats
    multi  = sum(1 for d in by_lead.values() if len(d["partnerships"]) > 1)
    single = sum(1 for d in by_lead.values() if len(d["partnerships"]) == 1)
    print(f"\n── Overlap Summary ──────────────────────────")
    print(f"  Matched Close.com leads:  {len(by_lead):,}")
    print(f"  Multi-partner accounts:   {multi:,}")
    print(f"  Single-partner accounts:  {single:,}")
    print(f"  Unmatched PS customers:   {len(unmatched):,}")
    elapsed = (datetime.now(timezone.utc) - start).seconds
    print(f"  Runtime: {elapsed}s")

    # 5. Format rows
    overlap_rows  = format_overlap_rows(by_lead)
    overlap_fields = [
        "close_lead_id", "company_name", "lead_status", "contact_emails",
        "partner_count", "flag", "partner_names", "partner_emails",
        "partner_types", "customer_keys",
    ]
    unmatched_fields = [
        "customer_email", "customer_key", "partner_name",
        "partner_email", "partner_type", "partner_slug",
    ]

    # 6. Summary JSON (top 25 multi-partner, headline counts)
    summary = {
        "generatedAt":           datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "matched_close_leads":   len(by_lead),
        "multi_partner":         multi,
        "single_partner":        single,
        "unmatched_ps_customers": len(unmatched),
        "runtime_seconds":       elapsed,
        "top_multi_partner": [
            {
                "company":      r["company_name"],
                "lead_id":      r["close_lead_id"],
                "status":       r["lead_status"],
                "partner_count": r["partner_count"],
                "partners":     r["partner_names"],
            }
            for r in overlap_rows if r["partner_count"] > 1
        ][:25],
    }

    # 7. Push to GitHub
    print("\nPushing reports…")
    push_file("reports/overlap_report.csv",       to_csv(overlap_rows,  overlap_fields))
    push_file("reports/unmatched_customers.csv",  to_csv(unmatched,     unmatched_fields))
    push_file("reports/overlap_summary.json",     json.dumps(summary, indent=2))

    print("\nDone.")
