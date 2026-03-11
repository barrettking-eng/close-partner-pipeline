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
MAX_DAYS        = 30   # keep last N days of snapshots

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

# ── HELPERS ───────────────────────────────────────────────────
def num(v, cast=int):
    try:
        return cast(float(v)) if v else 0
    except (ValueError, TypeError):
        return 0

def parse(row):
    return {
        "id":         row[COL_ID].strip(),
        "name":       row[COL_NAME].strip(),
        "email":      row[COL_EMAIL].strip(),
        "type":       row[COL_TYPE].strip(),
        "customers":  num(row[COL_CUSTOMERS]),
        "revenue":    num(row[COL_REVENUE], float),
        "clicks":     num(row[COL_CLICKS]),
        "commission": num(row[COL_COMMISSION], float),
        "approved":   row[COL_APPROVED].strip().lower() == "yes",
    }

def tier_of(ptype):
    if "Tier 3" in ptype: return "Tier 3"
    if "Tier 2" in ptype: return "Tier 2"
    return "Tier 1"

# ── GROUP ROWS BY DATE ─────────────────────────────────────────
def group_by_date(rows, type_test):
    """Returns {date: {partner_id: parsed_row}} for rows matching type_test."""
    by_date = defaultdict(dict)
    for row in rows:
        if len(row) < 11:
            continue
        if not type_test(row[COL_TYPE]):
            continue
        date = row[COL_DATE]
        pid  = row[COL_ID]
        # For duplicate partner+date keep the row (should be unique already)
        if pid not in by_date[date]:
            by_date[date][pid] = parse(row)
    return by_date

# ── SNAPSHOT BUILDER ──────────────────────────────────────────
def build_snapshot(partners, include_tier=False):
    """Build a single-date snapshot from a list of parsed partner dicts."""
    approved = sum(1 for p in partners if p["approved"])
    active   = [p for p in partners if p["customers"] > 0]
    warm     = [p for p in partners if p["customers"] == 0 and p["clicks"] > 0]
    dormant  = [p for p in partners if p["customers"] == 0 and p["clicks"] == 0]

    top_customers = sorted(partners, key=lambda p: p["customers"], reverse=True)[:20]
    top_revenue   = sorted(partners, key=lambda p: p["revenue"],   reverse=True)[:10]
    warm_leads    = sorted(warm,     key=lambda p: p["clicks"],     reverse=True)[:10]

    # Compact byId lookup for delta calculations: {id: {c, r, k}}
    by_id = {p["id"]: {"c": p["customers"], "r": round(p["revenue"], 2), "k": p["clicks"]}
             for p in partners}

    snap = {
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
        "topCustomers": [
            {k: p[k] for k in (["name","email","id","tier","customers","revenue","clicks"]
                                if include_tier else ["name","email","id","customers","revenue","clicks"])}
            for p in top_customers
        ],
        "topRevenue": [
            {k: p[k] for k in (["name","id","tier","customers","revenue","clicks"]
                                if include_tier else ["name","id","customers","revenue","clicks"])}
            for p in top_revenue
        ],
        "warmLeads": [
            {k: p[k] for k in (["name","email","id","tier","clicks"]
                                if include_tier else ["name","email","id","clicks"])}
            for p in warm_leads
        ],
        "byId": by_id,
    }

    if include_tier:
        def tier_stats(tier_name):
            t = [p for p in partners if p.get("tier") == tier_name]
            act = [p for p in t if p["customers"] > 0]
            wrm = [p for p in t if p["customers"] == 0 and p["clicks"] > 0]
            drm = [p for p in t if p["customers"] == 0 and p["clicks"] == 0]
            return {
                "count":     len(t),
                "active":    len(act),
                "warm":      len(wrm),
                "dormant":   len(drm),
                "customers": sum(p["customers"] for p in t),
                "revenue":   round(sum(p["revenue"] for p in t), 2),
            }
        snap["tiers"] = {
            "t3": tier_stats("Tier 3"),
            "t2": tier_stats("Tier 2"),
            "t1": tier_stats("Tier 1"),
        }

    return snap

# ── FULL DATASET BUILDER ──────────────────────────────────────
def build_dataset(rows, type_test, include_tier=False):
    by_date = group_by_date(rows, type_test)
    sorted_dates = sorted(by_date.keys())[-MAX_DAYS:]  # keep last MAX_DAYS

    snapshots = {}
    for date in sorted_dates:
        partners = list(by_date[date].values())
        if include_tier:
            for p in partners:
                p["tier"] = tier_of(p["type"])
        snapshots[date] = build_snapshot(partners, include_tier=include_tier)
        print(f"  {date}: {len(partners)} partners")

    return {
        "generatedAt":   datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "availableDates": sorted_dates,
        "latest":        sorted_dates[-1] if sorted_dates else None,
        "snapshots":     snapshots,
    }

# ── GITHUB PUSH ───────────────────────────────────────────────
def push_file(repo, path, content_str, token):
    encoded = base64.b64encode(content_str.encode()).decode()
    api_url = f"https://api.github.com/repos/{repo}/contents/{path}"
    headers = {
        "Authorization": f"token {token}",
        "Accept":        "application/vnd.github.v3+json",
        "Content-Type":  "application/json",
    }
    sha = None
    req = urllib.request.Request(api_url, headers=headers)
    try:
        with urllib.request.urlopen(req) as r:
            sha = json.loads(r.read())["sha"]
    except urllib.error.HTTPError:
        pass

    body = {
        "message": f"Auto-update {path} — {datetime.now(timezone.utc).strftime('%Y-%m-%d')}",
        "content": encoded,
    }
    if sha:
        body["sha"] = sha

    req = urllib.request.Request(api_url, data=json.dumps(body).encode(),
                                  headers=headers, method="PUT")
    with urllib.request.urlopen(req) as r:
        json.loads(r.read())
    print(f"  ✓ Pushed {path} → {repo}")

# ── MAIN ──────────────────────────────────────────────────────
if __name__ == "__main__":
    rows = fetch_rows()

    print("\nBuilding affiliate dataset…")
    aff_data = build_dataset(rows, lambda t: "Affiliate" in t, include_tier=False)
    push_file(AFFILIATE_REPO, "data.json",
              json.dumps(aff_data, separators=(',', ':')), GH_TOKEN)

    print("\nBuilding solutions dataset…")
    sol_data = build_dataset(rows, lambda t: "Solution Partner" in t, include_tier=True)
    push_file(SOLUTIONS_REPO, "data.json",
              json.dumps(sol_data, separators=(',', ':')), GH_TOKEN)

    print("\nDone.")
