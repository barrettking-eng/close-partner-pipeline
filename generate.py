#!/usr/bin/env python3
"""
Close Partner Dashboard — Daily Data Generator
Fetches PartnerStack data from Google Sheets, builds data.json for
the Affiliate and Solutions Consultant dashboards, and pushes to GitHub.
"""

import csv, json, os, base64, urllib.request
from datetime import datetime, timezone
from collections import defaultdict

# ── CONFIG ────────────────────────────────────────────────────
SHEET_URL = (
    "https://docs.google.com/spreadsheets/d/"
    "1rRssJYUGlnE3jICG5LSFSDJanPfvqVNBFdA-aS5mx04"
    "/export?format=csv&gid=953589945"
)
AFFILIATE_REPO  = "barrettking-eng/close-affiliate-analysis"
SOLUTIONS_REPO  = "barrettking-eng/close-solutions-analysis"
GH_TOKEN        = os.environ["GH_PAT"]

# Column indices
COL_DATE       = 0
COL_ID         = 1
COL_NAME       = 2
COL_EMAIL      = 3
COL_TYPE       = 4
COL_CUSTOMERS  = 5
COL_REVENUE    = 7
COL_CLICKS     = 8
COL_COMMISSION = 9
COL_APPROVED   = 10

# ── FETCH ─────────────────────────────────────────────────────
def fetch_rows():
    print("Fetching sheet…")
    req = urllib.request.Request(SHEET_URL, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req) as r:
        text = r.read().decode("utf-8")
    rows = list(csv.reader(text.splitlines()))
    print(f"  {len(rows)} total rows")
    return rows

def latest_per_partner(rows):
    """Keep only the most recent snapshot row per partner ID."""
    best = {}
    for row in rows:
        if len(row) < 11:
            continue
        pid  = row[COL_ID]
        date = row[COL_DATE]
        if pid not in best or date > best[pid][COL_DATE]:
            best[pid] = row
    result = list(best.values())
    print(f"  {len(result)} unique partners (latest snapshot)")
    return result

# ── PARSERS ───────────────────────────────────────────────────
def num(v, cast=int):
    try:
        return cast(float(v)) if v else 0
    except (ValueError, TypeError):
        return 0

def parse(row):
    return {
        "name":       row[COL_NAME].strip(),
        "email":      row[COL_EMAIL].strip(),
        "type":       row[COL_TYPE].strip(),
        "customers":  num(row[COL_CUSTOMERS]),
        "revenue":    num(row[COL_REVENUE], float),
        "clicks":     num(row[COL_CLICKS]),
        "commission": num(row[COL_COMMISSION], float),
        "approved":   row[COL_APPROVED].strip().lower() == "yes",
    }

def tier_of(row):
    t = row["type"]
    if "Tier 3" in t: return "Tier 3"
    if "Tier 2" in t: return "Tier 2"
    return "Tier 1"

# ── AFFILIATE DATASET ─────────────────────────────────────────
def build_affiliate(rows):
    print("Building affiliate dataset…")
    partners = [parse(r) for r in rows if "Affiliate" in r[COL_TYPE]]

    approved = sum(1 for p in partners if p["approved"])
    active   = [p for p in partners if p["customers"] > 0]
    warm     = [p for p in partners if p["customers"] == 0 and p["clicks"] > 0]
    dormant  = [p for p in partners if p["customers"] == 0 and p["clicks"] == 0]

    top_customers = sorted(partners, key=lambda p: p["customers"], reverse=True)[:20]
    top_revenue   = sorted(partners, key=lambda p: p["revenue"],   reverse=True)[:10]
    warm_leads    = sorted(warm,     key=lambda p: p["clicks"],     reverse=True)[:10]

    keys_c = ["name", "email", "customers", "revenue", "clicks"]
    keys_r = ["name", "customers", "revenue", "clicks"]
    keys_w = ["name", "email", "clicks"]

    return {
        "lastUpdated": datetime.now(timezone.utc).strftime("%B %d, %Y"),
        "totals": {
            "partners":   len(partners),
            "customers":  sum(p["customers"]  for p in partners),
            "revenue":    round(sum(p["revenue"]    for p in partners), 2),
            "commission": round(sum(p["commission"] for p in partners), 2),
            "clicks":     sum(p["clicks"]     for p in partners),
            "active":     len(active),
            "approved":   approved,
            "declined":   len(partners) - approved,
        },
        "activity": {
            "active":  len(active),
            "warm":    len(warm),
            "dormant": len(dormant),
        },
        "topCustomers": [{k: p[k] for k in keys_c} for p in top_customers],
        "topRevenue":   [{k: p[k] for k in keys_r} for p in top_revenue],
        "warmLeads":    [{k: p[k] for k in keys_w} for p in warm_leads],
    }

# ── SOLUTIONS DATASET ─────────────────────────────────────────
def build_solutions(rows):
    print("Building solutions dataset…")
    partners = []
    for r in rows:
        if "Solution Partner" in r[COL_TYPE]:
            p = parse(r)
            p["tier"] = tier_of(p)
            partners.append(p)

    def tier_stats(tier_name):
        t = [p for p in partners if p["tier"] == tier_name]
        active  = [p for p in t if p["customers"] > 0]
        warm    = [p for p in t if p["customers"] == 0 and p["clicks"] > 0]
        dormant = [p for p in t if p["customers"] == 0 and p["clicks"] == 0]
        return {
            "count":    len(t),
            "active":   len(active),
            "warm":     len(warm),
            "dormant":  len(dormant),
            "customers": sum(p["customers"] for p in t),
            "revenue":  round(sum(p["revenue"] for p in t), 2),
        }

    active  = [p for p in partners if p["customers"] > 0]
    warm    = [p for p in partners if p["customers"] == 0 and p["clicks"] > 0]
    dormant = [p for p in partners if p["customers"] == 0 and p["clicks"] == 0]

    top_customers = sorted(partners, key=lambda p: p["customers"], reverse=True)[:20]
    top_revenue   = sorted(partners, key=lambda p: p["revenue"],   reverse=True)[:10]
    warm_leads    = sorted(warm,     key=lambda p: p["clicks"],     reverse=True)[:10]

    keys_c = ["name", "email", "tier", "customers", "revenue", "clicks"]
    keys_r = ["name", "tier", "customers", "revenue", "clicks"]
    keys_w = ["name", "tier", "email", "clicks"]

    return {
        "lastUpdated": datetime.now(timezone.utc).strftime("%B %d, %Y"),
        "totals": {
            "partners":   len(partners),
            "customers":  sum(p["customers"]  for p in partners),
            "revenue":    round(sum(p["revenue"]    for p in partners), 2),
            "commission": round(sum(p["commission"] for p in partners), 2),
            "clicks":     sum(p["clicks"]     for p in partners),
            "active":     len(active),
        },
        "activity": {
            "active":  len(active),
            "warm":    len(warm),
            "dormant": len(dormant),
        },
        "tiers": {
            "t3": tier_stats("Tier 3"),
            "t2": tier_stats("Tier 2"),
            "t1": tier_stats("Tier 1"),
        },
        "topCustomers": [{k: p[k] for k in keys_c} for p in top_customers],
        "topRevenue":   [{k: p[k] for k in keys_r} for p in top_revenue],
        "warmLeads":    [{k: p[k] for k in keys_w} for p in warm_leads],
    }

# ── GITHUB PUSH ───────────────────────────────────────────────
def push_file(repo, path, content_str, token):
    encoded = base64.b64encode(content_str.encode()).decode()
    api_url = f"https://api.github.com/repos/{repo}/contents/{path}"
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json",
        "Content-Type": "application/json",
    }

    # Get current SHA if file already exists
    sha = None
    req = urllib.request.Request(api_url, headers=headers)
    try:
        with urllib.request.urlopen(req) as r:
            sha = json.loads(r.read())["sha"]
    except urllib.error.HTTPError:
        pass  # file doesn't exist yet

    body = {
        "message": f"Auto-update {path} — {datetime.now(timezone.utc).strftime('%Y-%m-%d')}",
        "content": encoded,
    }
    if sha:
        body["sha"] = sha

    req = urllib.request.Request(
        api_url,
        data=json.dumps(body).encode(),
        headers=headers,
        method="PUT",
    )
    with urllib.request.urlopen(req) as r:
        result = json.loads(r.read())
    print(f"  ✓ Pushed {path} → {repo}")

# ── MAIN ──────────────────────────────────────────────────────
if __name__ == "__main__":
    rows   = fetch_rows()
    latest = latest_per_partner(rows)

    aff_data = build_affiliate(latest)
    push_file(AFFILIATE_REPO, "data.json", json.dumps(aff_data, indent=2), GH_TOKEN)
    print(f"  Affiliates: {aff_data['totals']['partners']} partners, "
          f"${aff_data['totals']['revenue']:,.0f} revenue")

    sol_data = build_solutions(latest)
    push_file(SOLUTIONS_REPO, "data.json", json.dumps(sol_data, indent=2), GH_TOKEN)
    print(f"  Solutions:  {sol_data['totals']['partners']} partners, "
          f"${sol_data['totals']['revenue']:,.0f} revenue")

    print("\nDone.")
