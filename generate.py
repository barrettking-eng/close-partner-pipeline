#!/usr/bin/env python3
"""
Close Partner Dashboard — Daily Data Generator
Fetches live data from PartnerStack API, builds data.json for
the Affiliate and Solutions Consultant dashboards, and pushes to GitHub.
"""

import json, os, base64, urllib.request, urllib.parse
from datetime import datetime, timezone

# ── CONFIG ────────────────────────────────────────────────────
PS_PUBLIC_KEY   = os.environ["PS_PUBLIC_KEY"]
PS_SECRET_KEY   = os.environ["PS_SECRET_KEY"]
AFFILIATE_REPO  = "barrettking-eng/close-affiliate-analysis"
SOLUTIONS_REPO  = "barrettking-eng/close-solutions-analysis"
GH_TOKEN        = os.environ["GH_PAT"]
MAX_DAYS        = 30   # keep last N days of snapshots
PS_API_BASE     = "https://api.partnerstack.com/api/v2"

# Group slugs that belong to each dashboard
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

# ── PARTNERSTACK API ───────────────────────────────────────────
def _ps_get(path):
    """Make an authenticated GET request to the PartnerStack API."""
    credentials = base64.b64encode(
        f"{PS_PUBLIC_KEY}:{PS_SECRET_KEY}".encode()
    ).decode()
    req = urllib.request.Request(
        f"{PS_API_BASE}{path}",
        headers={
            "Authorization": f"Basic {credentials}",
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (compatible; close-partner-pipeline/1.0)",
        }
    )
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read())

def fetch_all_partnerships():
    """Paginate through all partnerships, return list of parsed partner dicts."""
    partners = []
    cursor = None
    page = 0
    while True:
        qs = "?limit=100"
        if cursor:
            qs += f"&starting_after={urllib.parse.quote(cursor)}"
        data = _ps_get(f"/partnerships{qs}")["data"]
        items = data["items"]
        for item in items:
            p = _parse_partnership(item)
            if p:
                partners.append(p)
        page += 1
        print(f"  page {page}: {len(items)} fetched, {len(partners)} matched so far")
        if not data["has_more"] or not items:
            break
        cursor = items[-1]["key"]
    return partners

def _parse_partnership(item):
    slug = item.get("group", {}).get("slug", "")
    if slug not in AFFILIATE_SLUGS and slug not in SOLUTIONS_SLUGS:
        return None  # skip integration partners, internal, etc.

    stats = item.get("stats", {})
    tags  = item.get("tags", [])
    ptype = tags[0] if tags else slug

    return {
        "id":         item["key"],
        "name":       f"{item.get('first_name','')} {item.get('last_name','')}".strip(),
        "email":      item.get("email") or "",
        "type":       ptype,
        "slug":       slug,
        "customers":  int(stats.get("CUSTOMER_COUNT", 0) or 0),
        "revenue":    float(stats.get("REVENUE", 0) or 0),
        "clicks":     int(stats.get("LINK_CLICKS", 0) or 0),
        "commission": float(stats.get("COMMISSION_EARNED", 0) or 0),
        "approved":   item.get("approved_status") == "approved",
    }

# ── HELPERS ───────────────────────────────────────────────────
def tier_of(slug):
    if "tier3" in slug: return "Tier 3"
    if "tier2" in slug: return "Tier 2"
    return "Tier 1"

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

# ── INCREMENTAL DATASET UPDATE ────────────────────────────────
def fetch_existing_data(repo, path, token):
    """Fetch existing data.json from GitHub repo, return parsed dict or None."""
    api_url = f"https://api.github.com/repos/{repo}/contents/{path}"
    headers = {
        "Authorization": f"token {token}",
        "Accept":        "application/vnd.github.v3+json",
    }
    req = urllib.request.Request(api_url, headers=headers)
    try:
        with urllib.request.urlopen(req) as r:
            file_data = json.loads(r.read())
            content = base64.b64decode(file_data["content"]).decode("utf-8")
            return json.loads(content)
    except Exception:
        return None

def build_updated_dataset(existing, new_snap, today):
    """Merge today's snapshot into existing data, trim to MAX_DAYS."""
    if existing and "snapshots" in existing:
        snapshots = existing["snapshots"]
        dates = existing.get("availableDates", [])
    else:
        snapshots = {}
        dates = []

    snapshots[today] = new_snap
    if today not in dates:
        dates.append(today)
    dates = sorted(dates)[-MAX_DAYS:]
    # prune snapshots not in trimmed dates
    snapshots = {d: snapshots[d] for d in dates if d in snapshots}

    return {
        "generatedAt":    datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "availableDates": dates,
        "latest":         dates[-1] if dates else None,
        "snapshots":      snapshots,
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
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    print(f"Fetching all partnerships from PartnerStack API…")
    all_partners = fetch_all_partnerships()
    print(f"  Total matched partners: {len(all_partners)}")

    affiliates = [p for p in all_partners if p["slug"] in AFFILIATE_SLUGS]
    solutions  = [p for p in all_partners if p["slug"] in SOLUTIONS_SLUGS]
    for p in solutions:
        p["tier"] = tier_of(p["slug"])

    print(f"\nBuilding affiliate snapshot ({len(affiliates)} partners)…")
    aff_snap = build_snapshot(affiliates, include_tier=False)
    existing_aff = fetch_existing_data(AFFILIATE_REPO, "data.json", GH_TOKEN)
    aff_data = build_updated_dataset(existing_aff, aff_snap, today)
    push_file(AFFILIATE_REPO, "data.json",
              json.dumps(aff_data, separators=(',', ':')), GH_TOKEN)

    print(f"\nBuilding solutions snapshot ({len(solutions)} partners)…")
    sol_snap = build_snapshot(solutions, include_tier=True)
    existing_sol = fetch_existing_data(SOLUTIONS_REPO, "data.json", GH_TOKEN)
    sol_data = build_updated_dataset(existing_sol, sol_snap, today)
    push_file(SOLUTIONS_REPO, "data.json",
              json.dumps(sol_data, separators=(',', ':')), GH_TOKEN)

    print("\nDone.")
