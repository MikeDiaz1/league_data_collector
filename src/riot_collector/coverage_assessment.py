"""Explain aggregate sampling results without changing collection or budgets.

Thresholds are transparent operational heuristics, not statistical tests or
claims about historical player ranks or population representativeness.
"""
from datetime import datetime

from .coverage import TARGET_TIERS


MIN_HOURS = 24
MIN_MATCHES_PER_REGION = 250
TIER_SHARE_LOW = .15
TIER_SHARE_HIGH = .25


def assess_balance(report):
    """Assess recent seed-attributed downloads only; preserve missing strata."""
    window = report["window"]
    try:
        hours = (datetime.fromisoformat(window["until_utc"].replace("Z", "+00:00"))
                 - datetime.fromisoformat(window["since_utc"].replace("Z", "+00:00"))).total_seconds() / 3600
    except (KeyError, ValueError, TypeError):
        hours = 0
    all_rows = report.get("group_yield", [])
    configured = report.get("configured_budgets", {}).get("platforms", [])
    regions = sorted(set(configured) or {r["region"] for r in all_rows})
    recent = {(r["region"], r["seed_tier"]): r for r in all_rows if r["stream"] == "recent"}
    attribution = report.get("metric_availability", {}).get("sampling_attribution", True)
    # Unattributed downloads can hide missing recent-tier work. Do not award a
    # balance verdict when the window includes such data.
    unknown = sum(r.get("new_matches", 0) for r in all_rows
                  if r["stream"] == "UNKNOWN" or (r["stream"] == "recent" and r["seed_tier"] not in TARGET_TIERS))
    totals = {region: sum(recent.get((region, tier), {}).get("new_matches", 0)
                          for tier in TARGET_TIERS) for region in regions}
    rows, diagnostics = [], []
    for region in regions:
        ready = attribution and not unknown and hours >= MIN_HOURS and totals[region] >= MIN_MATCHES_PER_REGION
        for tier in TARGET_TIERS:
            row = recent.get((region, tier), {})
            count = row.get("new_matches", 0)
            share = count / totals[region] if totals[region] else None
            status = ("Attribution unavailable" if not attribution or unknown else
                      "Too early to judge" if not ready else
                      "Below target" if share < TIER_SHARE_LOW else
                      "Above target" if share > TIER_SHARE_HIGH else "Within target range")
            rows.append({"region": region, "seed_tier": tier, "stream": "recent",
                         "new_matches": count, "share": share, "status": status})
            prefix = f"{region} / {tier.title()}"
            histories = row.get("completed_recent_history_jobs")
            missing = row.get("unknown_recent_history_outcomes", 0)
            if histories and histories >= 20 and missing == 0:
                for field, label, advice in (
                    ("empty_recent_history_jobs", "empty history checks",
                     "Broaden or rotate the seed cohort before increasing repeat checks of the same players."),
                    ("capped_recent_history_jobs", "history checks reached the match limit",
                     "Review the per-player match limit against player diversity; reaching the cap does not prove more games existed."),
                ):
                    value = row.get(field)
                    if value is not None and value / histories >= .5:
                        diagnostics.append(f"{prefix}: {value:,}/{histories:,} {label} ({value / histories:.0%}). {advice}")
            routes, stored = row.get("discovery_routes"), row.get("already_stored_routes")
            if routes and routes >= 100 and stored is not None and stored / routes >= .5:
                diagnostics.append(f"{prefix}: {stored:,}/{routes:,} discoveries ({stored / routes:.0%}) led to already saved matches. Broaden seed coverage before increasing this group's discovery budget.")
            pending = row.get("pending_discovered_matches")
            if pending and pending >= 100:
                diagnostics.append(f"{prefix}: {pending:,} discovered matches still await download (current status). Check download progress before allocating more discovery work; a single snapshot cannot show whether the queue is growing.")
            failed = row.get("failed_discovered_matches")
            if failed:
                diagnostics.append(f"{prefix}: {failed:,} discovered matches have failed download jobs. Inspect failures before attributing low yield to the sampling budget.")
    if not attribution or unknown:
        verdict = "Attribution incomplete"
        advice = "Some downloads cannot be assigned to the target sampling groups. Choose a window with sampling telemetry before judging balance."
    elif not rows or any(r["status"] == "Too early to judge" for r in rows):
        verdict = "Too early to judge overall balance"
        advice = "Wait for at least 24 hours and 250 recent seed-attributed matches per region. Uneven early counts alone do not justify changing budgets."
    elif any(r["status"] in ("Below target", "Above target") for r in rows):
        verdict = "Tier coverage needs review"
        advice = "Some tier shares are outside the working target range. Check overlap, empty histories and pending downloads below; if the gap persists in comparable windows, review seed coverage and tier scheduling. The 8:1:1 weights control streams, not individual tiers."
    else:
        verdict = "Tier coverage is within the working range"
        advice = "No tier-budget adjustment is indicated by these counts alone. Continue checking comparable windows, player overlap and patch coverage."
    total = sum(totals.values())
    region_rows = []
    ready = bool(regions) and attribution and not unknown and hours >= MIN_HOURS and total >= MIN_MATCHES_PER_REGION * len(regions)
    for region in regions:
        share = totals[region] / total if total else None
        target = 1 / len(regions)
        status = ("Attribution unavailable" if not attribution or unknown else
                  "Too early to judge" if not ready else
                  "Below target" if share < target * .75 else
                  "Above target" if share > target * 1.25 else "Within target range")
        region_rows.append({"region": region, "new_matches": totals[region], "share": share,
                            "target": target, "status": status})
    if any(r["status"] in ("Below target", "Above target") for r in region_rows):
        if verdict == "Tier coverage is within the working range":
            verdict = "Regional coverage needs review"
        advice += " Regional game totals also fall outside the equal-coverage reference range; inspect regional queues and availability before changing allocations."
    return {"verdict": verdict, "advice": advice, "hours": hours, "rows": rows,
            "region_rows": region_rows, "diagnostics": diagnostics, "unknown_matches": unknown}
