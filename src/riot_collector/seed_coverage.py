"""Read-only ladder pagination and division-level recent-history diagnostics."""
from collections import Counter, defaultdict
from datetime import datetime, timezone
import json
import math


SEED_TIERS = ("EMERALD", "DIAMOND")
DIVISIONS = ("I", "II", "III", "IV")
MIN_DIVISION_CHECKS = 20


def _object(raw):
    try:
        value = json.loads(raw)
        return value if isinstance(value, dict) else {}
    except (ValueError, TypeError):
        return {}


def _number(value):
    return type(value) in (int, float) and math.isfinite(value)


def _utc(value):
    return datetime.fromtimestamp(value, timezone.utc).isoformat().replace("+00:00", "Z") if value is not None else None


def read_seed_evidence(conn, tables, job_columns):
    """Use the caller's read snapshot; no State, locks, migrations or HTTP."""
    created = "created_at" if "created_at" in job_columns else "NULL AS created_at"
    seeds = [dict(r) for r in conn.execute(
        f"SELECT platform,payload,status,updated_at,{created} FROM jobs WHERE kind='seed'")]
    histories, observations = [], {}
    has_histories = {"sampling_tier", "sampling_stream"}.issubset(job_columns)
    if has_histories:
        histories = [dict(r) for r in conn.execute(
            f"SELECT platform,sampling_tier,payload,status,updated_at,{created} FROM jobs "
            "WHERE kind='recent_history' AND sampling_stream='recent' "
            "AND sampling_tier IN ('EMERALD','DIAMOND')")]
        for row in histories:
            row["payload"] = _object(row["payload"])
        ids = sorted({row["payload"].get("seed_rank_observation_id") for row in histories
                      if type(row["payload"].get("seed_rank_observation_id")) is int})
        if "rank_observations" in tables:
            for start in range(0, len(ids), 400):
                batch = ids[start:start + 400]
                marks = ",".join("?" for _ in batch)
                observations.update({r["id"]: dict(r) for r in conn.execute(
                    "SELECT id,puuid,platform,tier,division FROM rank_observations "
                    f"WHERE id IN ({marks})", batch)})
    return {"seeds": seeds, "histories": histories, "observations": observations,
            "histories_available": has_histories,
            "selection_times_available": "created_at" in job_columns,
            "division_observations_available": "rank_observations" in tables}


def summarize_seed_coverage(evidence, regions, since, until):
    """Separate current ladder state from windowed outcomes and selection time.

    End-of-ladder is inferred: all recorded pages 1..N are done and no successor
    exists. The collector atomically enqueues a successor for nonempty pages.
    This is traversal completion, not a known percentage of the live population.
    """
    chains = defaultdict(list)
    regions = set(regions)
    for row in evidence["seeds"]:
        payload = _object(row["payload"])
        tier, division, bucket, page = (payload.get(k) for k in ("tier", "division", "bucket", "page"))
        if tier not in SEED_TIERS:
            continue
        regions.add(row["platform"])
        if division not in DIVISIONS or type(bucket) is not int:
            continue
        chains[(row["platform"], tier, bucket, division)].append(dict(row, page=page))
    for row in evidence["histories"]:
        regions.add(row["platform"])

    scans = defaultdict(dict)
    progress = []
    scan_ids = {(r, t, b) for r, t, b, _ in chains}
    for region, tier, bucket in sorted(scan_ids):
        for division in DIVISIONS:
            rows = chains.get((region, tier, bucket, division), [])
            pages = [r["page"] for r in rows if type(r["page"]) is int and r["page"] > 0]
            valid = len(pages) == len(rows) and len(set(pages)) == len(rows)
            contiguous = bool(pages) and valid and min(pages) == 1 and max(pages) == len(pages)
            counts = Counter(r["status"] for r in rows)
            complete = contiguous and counts["done"] == len(rows)
            ended = max(r["updated_at"] for r in rows) if complete and all(_number(r["updated_at"]) for r in rows) else None
            created = [r["created_at"] for r in rows if _number(r["created_at"])]
            waiting = [r["page"] for r in rows if r["status"] != "done" and type(r["page"]) is int]
            done = [r["page"] for r in rows if r["status"] == "done" and type(r["page"]) is int]
            status = ("No recorded pages" if not rows else
                      "Page sequence incomplete" if not contiguous else
                      "Needs attention" if counts["failed"] or counts["skipped"] else
                      "End reached (inferred)" if complete else "In progress")
            result = {"region": region, "seed_tier": tier, "division": division,
                      "scan_id": bucket, "scan_started_utc": _utc(min(created)) if created else None,
                      "completed_pages": counts["done"], "highest_done_page": max(done) if done else None,
                      "next_page": min(waiting) if waiting else None, "pending_pages": counts["pending"],
                      "active_pages": counts["in_progress"], "failed_pages": counts["failed"],
                      "skipped_pages": counts["skipped"], "status": status,
                      "completed_at_utc": _utc(ended), "is_latest_scan": False}
            progress.append(result)
            scans[(region, tier, bucket)][division] = (complete, ended, result)

    latest, full_times, readiness = {}, {}, []
    for region in sorted(regions):
        for tier in SEED_TIERS:
            buckets = sorted(b for r, t, b in scans if (r, t) == (region, tier))
            newest = buckets[-1] if buckets else None
            latest[(region, tier)] = newest
            completed = []
            unfinished = 0
            for bucket in buckets:
                parts = scans[(region, tier, bucket)]
                if all(parts[d][0] and parts[d][1] is not None for d in DIVISIONS):
                    completed.append((max(parts[d][1] for d in DIVISIONS), bucket))
                else:
                    unfinished += 1
                for _, _, row in parts.values():
                    row["is_latest_scan"] = bucket == newest
            finished, finished_bucket = max(completed) if completed else (None, None)
            full_times[(region, tier)] = finished
            current = scans.get((region, tier, newest), {})
            readiness.append({"region": region, "seed_tier": tier,
                              "latest_scan_id": newest,
                              "latest_scan_divisions_finished": sum(v[0] for v in current.values()),
                              "unfinished_scans": unfinished,
                              "last_full_scan_id": finished_bucket,
                              "last_full_scan_utc": _utc(finished)})
            if newest is None:
                for division in DIVISIONS:
                    progress.append({"region": region, "seed_tier": tier, "division": division,
                                     "scan_id": None, "is_latest_scan": True,
                                     "status": "No recorded pages", "completed_pages": 0})

    counters = defaultdict(Counter)
    post_states = defaultdict(Counter)
    latest_candidates = {}
    for row in evidence["histories"]:
        region, tier, payload = row["platform"], row["sampling_tier"], row["payload"]
        group = (region, tier)
        observation_id = payload.get("seed_rank_observation_id")
        observation = evidence["observations"].get(observation_id, {}) if type(observation_id) is int else {}
        # Only the observation frozen into the job may assign a division. Never
        # relabel a past check using a player's current rank.
        division = observation.get("division") if (
            observation.get("puuid") == payload.get("puuid") and observation.get("puuid") is not None
            and observation.get("platform") == region and observation.get("tier") == tier) else None
        division = division if division in DIVISIONS else "UNKNOWN"
        created, finished = row["created_at"], full_times.get(group)
        post = _number(created) and finished is not None and created > finished
        if post:
            post_states[group][row["status"]] += 1
        candidate = payload.get("candidate_players")
        if _number(created) and type(candidate) is int and candidate >= 0:
            if group not in latest_candidates or created > latest_candidates[group][0]:
                latest_candidates[group] = (created, candidate)
        if row["status"] != "done" or not _number(row["updated_at"]) or not since <= row["updated_at"] < until:
            continue
        count = payload.get("returned_matches")
        known = type(count) is int and count >= 0
        acc = counters[(region, tier, division)]
        for prefix in ("", "post_") if post else ("",):
            acc[prefix + "completed_checks"] += 1
            acc[prefix + "known_checks"] += known
            acc[prefix + "empty_checks"] += known and count == 0
            acc[prefix + "unknown_outcomes"] += not known

    divisions = []
    available = evidence["histories_available"]
    for region in sorted(regions):
        for tier in SEED_TIERS:
            names = (*DIVISIONS, "UNKNOWN") if (region, tier, "UNKNOWN") in counters else DIVISIONS
            for division in names:
                row = {"region": region, "seed_tier": tier, "division": division}
                acc = counters[(region, tier, division)]
                for prefix in ("", "post_"):
                    usable = available and (not prefix or (full_times[(region, tier)] is not None and evidence["selection_times_available"]))
                    for field in ("completed_checks", "known_checks", "empty_checks", "unknown_outcomes"):
                        row[prefix + field] = acc[prefix + field] if usable else None
                    row[prefix + "empty_rate"] = acc[prefix + "empty_checks"] / acc[prefix + "known_checks"] if usable and acc[prefix + "known_checks"] else None
                divisions.append(row)
    for row in readiness:
        group = (row["region"], row["seed_tier"])
        post = post_states[group]
        row["latest_cohort_candidates"] = latest_candidates.get(group, (None, None))[1]
        row["post_scan_selected_checks"] = sum(post.values()) if available else None
        row["post_scan_unfinished_checks"] = post["pending"] + post["in_progress"] if available else None
        row["post_scan_failed_or_skipped"] = post["failed"] + post["skipped"] if available else None
        row["divisions_with_enough_checks"] = sum(counters[(*group, d)]["post_known_checks"] >= MIN_DIVISION_CHECKS for d in DIVISIONS)
        missing_outcomes = sum(counters[(*group, d)]["post_unknown_outcomes"] for d in (*DIVISIONS, "UNKNOWN"))
        if not available or not evidence["selection_times_available"] or not evidence["division_observations_available"]:
            status = "Evidence unavailable"
        elif full_times[group] is None:
            status = "Waiting for a full ladder scan"
        elif not post:
            status = "Waiting for a post-scan cohort"
        elif post["failed"] or post["skipped"]:
            status = "Review failed or skipped checks"
        elif post["pending"] or post["in_progress"]:
            status = "Post-scan checks still running"
        elif counters[(*group, "UNKNOWN")]["post_completed_checks"] or missing_outcomes:
            status = "Review missing division or outcome evidence"
        elif row["divisions_with_enough_checks"] < 4:
            status = "More post-scan checks needed in this window"
        else:
            status = "Ready to reassess"
        row["status"] = status
    return {"readiness": readiness, "ladder_progress": progress, "division_history": divisions,
            "minimum_checks_per_division": MIN_DIVISION_CHECKS,
            "scope": "Ladder and unfinished-check state: current snapshot. Completed history rates: report window. Post-scan means selected after the latest fully completed four-division scan for that region/tier.",
            "availability": {k: evidence[k] for k in ("histories_available", "selection_times_available", "division_observations_available")},
            "notes": [
                "End reached is inferred from contiguous completed pages starting at page 1 with no successor. Nonempty pages enqueue successors atomically. It is not a measured percentage of all players.",
                "A full scan requires all four divisions in the same scan ID. Newer incomplete refresh scans do not erase an older completed traversal; its completion time remains visible.",
                "Empty rate is empty successful checks / successful checks with a known returned-match count. Unknown outcomes, failures and skipped jobs are not empty results. Checks can repeat players across cohorts.",
                "Division comes only from the exact saved rank observation referenced by the history job. Missing or mismatched observations are UNKNOWN, never inferred from current rank.",
                "Ready to reassess requires a completed full scan, cohorts selected strictly afterwards, no outstanding failed/skipped/unfinished post-scan jobs, and at least 20 known-outcome completed checks per division in the report window. Missing post-scan labels or outcomes block readiness.",
                "The 20-check threshold is a descriptive review checkpoint, not statistical confidence or proof of representative coverage. Scan completion does not establish recent player activity; this report changes no settings.",
            ]}
