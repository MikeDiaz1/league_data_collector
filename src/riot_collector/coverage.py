"""Aggregate coverage and sampling yield from a short, read-only SQLite snapshot.

No collector state object, raw files, API requests, or dataset lock are involved.
Player and match identifiers are used only inside the calculation, never exported.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import statistics
import time

from .dataset import read_database
from .seed_coverage import read_seed_evidence, summarize_seed_coverage
from .diversity import read_diversity_evidence, summarize_diversity


TARGET_TIERS = ("EMERALD", "DIAMOND", "MASTER", "GRANDMASTER", "CHALLENGER")
_TIERS = {*TARGET_TIERS, "IRON", "BRONZE", "SILVER", "GOLD", "PLATINUM"}
_STREAMS = {"recent", "participant", "backfill", "seed"}
_ROUTE_FIELDS = (
    "discovery_routes", "distinct_discovered_matches", "within_slice_repeat_fraction",
    "already_stored_routes", "undownloaded_discovered_matches", "pending_discovered_matches",
    "failed_discovered_matches", "skipped_discovered_matches", "unknown_status_discovered_matches",
)
_INVENTORY_FIELDS = ("selected_recent_history_jobs", "pending_recent_history_jobs",
                     "in_progress_recent_history_jobs", "done_recent_history_jobs",
                     "failed_recent_history_jobs", "skipped_recent_history_jobs")


def _utc(value: float) -> str:
    return datetime.fromtimestamp(value, timezone.utc).isoformat().replace("+00:00", "Z")


def _object(value) -> dict:
    try:
        result = json.loads(value)
    except (TypeError, ValueError):
        return {}
    return result if isinstance(result, dict) else {}


def _tier(value) -> str:
    value = str(value or "UNKNOWN").upper()
    return value if value in _TIERS else "UNKNOWN"


def _stream(value) -> str:
    return value if isinstance(value, str) and value in _STREAMS else "UNKNOWN"


def _patch(value) -> str:
    return str(value) if value else "UNKNOWN"


def _percentile(values: list[float], fraction: float) -> float | None:
    """Linear interpolation between ordered observations, including n=1."""
    if not values:
        return None
    values = sorted(values)
    position = (len(values) - 1) * fraction
    lower = math.floor(position)
    upper = math.ceil(position)
    return values[lower] + (values[upper] - values[lower]) * (position - lower)


def _accumulator() -> dict:
    return {"new_matches": 0, "players": set(), "participant_appearances": 0,
            "lags": [], "invalid_lag_matches": 0, "discovery_routes": 0,
            "discovered": set(), "already_stored_routes": 0,
            "undownloaded": set(), "pending": set(), "failed": set(), "skipped": set(),
            "unknown_status": set(), "completed_recent_history_jobs": 0,
            "empty_recent_history_jobs": 0, "capped_recent_history_jobs": 0,
            "unknown_recent_history_outcomes": 0, **dict.fromkeys(_INVENTORY_FIELDS, 0)}


def _finish(acc: dict, *, discoveries_available: bool, histories_available: bool,
            include_histories: bool) -> dict:
    appearances = acc["participant_appearances"]
    routes = acc["discovery_routes"]
    result = {
        "new_matches": acc["new_matches"], "unique_players": len(acc["players"]),
        "participant_appearances": appearances,
        "repeat_appearance_fraction": 1 - len(acc["players"]) / appearances if appearances else None,
        "median_lag_hours": statistics.median(acc["lags"]) if acc["lags"] else None,
        "p90_lag_hours": _percentile(acc["lags"], .9),
        "invalid_lag_matches": acc["invalid_lag_matches"],
        "discovery_routes": routes, "distinct_discovered_matches": len(acc["discovered"]),
        "within_slice_repeat_fraction": 1 - len(acc["discovered"]) / routes if routes else None,
        "already_stored_routes": acc["already_stored_routes"],
        "undownloaded_discovered_matches": len(acc["undownloaded"]),
        "pending_discovered_matches": len(acc["pending"]),
        "failed_discovered_matches": len(acc["failed"]),
        "skipped_discovered_matches": len(acc["skipped"]),
        "unknown_status_discovered_matches": len(acc["unknown_status"]),
    }
    if not discoveries_available:
        result.update({field: None for field in _ROUTE_FIELDS})
    if include_histories:
        for field in ("completed_recent_history_jobs", "empty_recent_history_jobs",
                      "capped_recent_history_jobs", "unknown_recent_history_outcomes", *_INVENTORY_FIELDS):
            result[field] = acc[field] if histories_available else None
    return result


def coverage_report(data_dir: Path, *, since: float, until: float | None = None,
                    platforms: tuple[str, ...] = ()) -> dict:
    """Return aggregate-only metrics for a half-open UTC completion/discovery window.

    All-time coverage uses original immutable first-discovery metadata. Window
    download yield uses the match job's scheduling attribution. Discovery evidence
    and job completions each have their own event-time selection, so discovery
    counts are not a denominator for same-window downloads.
    """
    generated = time.time()
    since, until = float(since), generated if until is None else float(until)
    if not math.isfinite(since) or not math.isfinite(until) or since >= until:
        raise ValueError("Coverage window requires finite timestamps with since < until")
    # Validate representability before opening the database.
    since_utc, until_utc = _utc(since), _utc(until)
    with read_database(Path(data_dir)) as conn:
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        columns = {row["name"] for row in conn.execute("PRAGMA table_info(jobs)")}
        has_sampling = {"sampling_stream", "sampling_tier"}.issubset(columns)
        has_discoveries = "match_discoveries" in tables
        seed_evidence = read_seed_evidence(conn, tables, columns)
        diversity_evidence = read_diversity_evidence(conn, tables, since, until)
        attribution = "j.sampling_stream,j.sampling_tier" if has_sampling else "NULL,NULL"
        coverage_raw = conn.execute(
            "SELECT platform,seed_tier,patch,COUNT(*) FROM matches GROUP BY platform,seed_tier,patch"
        ).fetchall()
        downloads = conn.execute(
            "SELECT m.match_id,m.platform,m.patch,m.game_creation,m.completed_at," + attribution +
            " FROM matches m LEFT JOIN jobs j ON j.kind='match' AND j.key=m.match_id "
            "WHERE m.completed_at>=? AND m.completed_at<?", (since, until)
        ).fetchall()
        # Keep matches as the outer loop; do not scan all participant appearances
        # to report a small recent window. No temporary tables or indexes needed.
        participants = conn.execute(
            "SELECT p.match_id,p.puuid FROM matches m CROSS JOIN match_participants p "
            "ON p.match_id=m.match_id WHERE m.completed_at>=? AND m.completed_at<?", (since, until)
        ).fetchall()
        discoveries = []
        if has_discoveries:
            discoveries = conn.execute(
                "SELECT d.match_id,d.platform,d.history_payload,d.discovered_at,m.patch,m.completed_at,"
                + attribution + ",mj.status FROM match_discoveries d "
                "LEFT JOIN matches m ON m.match_id=d.match_id "
                "LEFT JOIN jobs j ON j.id=d.history_job_id "
                "LEFT JOIN jobs mj ON mj.kind='match' AND mj.key=d.match_id "
                "WHERE d.discovered_at>=? AND d.discovered_at<?", (since, until)
            ).fetchall()
        histories, history_inventory = [], []
        if has_sampling:
            histories = conn.execute(
                "SELECT platform,sampling_stream,sampling_tier,payload FROM jobs "
                "WHERE status='done' AND kind='recent_history' AND updated_at>=? AND updated_at<?",
                (since, until),
            ).fetchall()
            history_inventory = conn.execute(
                "SELECT platform,sampling_stream,sampling_tier,status,COUNT(*) FROM jobs "
                "WHERE kind='recent_history' GROUP BY platform,sampling_stream,sampling_tier,status"
            ).fetchall()
        metadata = conn.execute(
            "SELECT key,value FROM key_value WHERE key='sampling_schema_v1' "
            "OR key LIKE 'sampling_scheduler:v1:%'"
        ).fetchall() if "key_value" in tables else []

    # The read transaction is closed before set aggregation, quantiles, or output.
    by_match = defaultdict(list)
    player_counts = Counter()
    for match_id, player in participants:
        by_match[match_id].append(player)
        player_counts[player] += 1
    del participants
    groups, slices = defaultdict(_accumulator), defaultdict(_accumulator)
    overall = _accumulator()
    coverage_counts = defaultdict(int)
    regions = set()
    for region, tier, patch, count in coverage_raw:
        regions.add(region)
        coverage_counts[(region, _tier(tier), _patch(patch))] += count
    for match_id, region, patch, creation, completion, stream, tier in downloads:
        key = (region, _stream(stream), _tier(tier))
        regions.add(region)
        lag = None
        if isinstance(creation, (int, float)) and math.isfinite(creation) and creation > 0:
            candidate = (completion - creation / 1000) / 3600
            if math.isfinite(candidate) and candidate >= 0:
                lag = candidate
        for acc in (overall, groups[key], slices[(*key, _patch(patch))]):
            acc["new_matches"] += 1
            acc["players"].update(by_match[match_id])
            acc["participant_appearances"] += len(by_match[match_id])
            if lag is None:
                acc["invalid_lag_matches"] += 1
            else:
                acc["lags"].append(lag)
    del downloads, by_match
    for match_id, region, raw, discovered, patch, completed, stream, tier, status in discoveries:
        payload = _object(raw)
        key = (region, _stream(payload.get("sampling_stream", stream)),
               _tier(payload.get("sampling_tier", tier)))
        regions.add(region)
        stored_by_end = completed is not None and completed < until
        patch = _patch(patch) if stored_by_end else "PENDING"
        for acc in (overall, groups[key], slices[(*key, patch)]):
            acc["discovery_routes"] += 1
            acc["discovered"].add(match_id)
            if completed is not None and completed <= discovered:
                acc["already_stored_routes"] += 1
            if not stored_by_end:
                acc["undownloaded"].add(match_id)
                category = {"pending": "pending", "in_progress": "pending", "failed": "failed",
                            "skipped": "skipped"}.get(status, "unknown_status")
                acc[category].add(match_id)
    del discoveries
    selection_rules = Counter()
    for region, stream, tier, raw in histories:
        key = (region, _stream(stream), _tier(tier))
        regions.add(region)
        payload = _object(raw)
        selection_rules[(*key, payload.get("selection_rule", "legacy_unrecorded"))] += 1
        returned, capped = payload.get("returned_matches"), payload.get("at_cap")
        valid_returned = type(returned) is int and returned >= 0
        for acc in (overall, groups[key]):
            acc["completed_recent_history_jobs"] += 1
            acc["empty_recent_history_jobs"] += valid_returned and returned == 0
            acc["capped_recent_history_jobs"] += capped is True
            acc["unknown_recent_history_outcomes"] += not (valid_returned and type(capped) is bool)
    for region, stream, tier, status, count in history_inventory:
        key = (region, _stream(stream), _tier(tier))
        regions.add(region)
        for acc in (overall, groups[key]):
            acc["selected_recent_history_jobs"] += count
            field = f"{status}_recent_history_jobs"
            if field in _INVENTORY_FIELDS:
                acc[field] += count

    service, migration = [], {}
    scheduler_metadata_present = False
    for key, raw in metadata:
        value = _object(raw)
        if key == "sampling_schema_v1":
            migration = value
            continue
        scheduler_metadata_present = True
        region = key.rsplit(":", 1)[-1]
        regions.add(region)
        served = value.get("served", {})
        if not isinstance(served, dict):
            continue
        for name, count in served.items():
            parts = name.split("|")
            if len(parts) == 3 and type(count) is int and count >= 0:
                stream, kind, tier = parts
                service.append({"region": region, "stream": _stream(stream), "seed_tier": _tier(tier),
                                "kind": kind, "claims": count})
    # Show zero-yield target strata instead of silently dropping unserved tiers.
    for region in regions:
        for tier in TARGET_TIERS:
            groups[(region, "recent", tier)]
    finish_options = {"discoveries_available": has_discoveries, "histories_available": has_sampling}
    summary = _finish(overall, include_histories=True, **finish_options)
    summary["total_matches"] = sum(coverage_counts.values())
    summary["window_matches"] = summary.pop("new_matches")
    summary["window_unique_players"] = summary["unique_players"]
    migration_at = migration.get("migrated_at")
    if not isinstance(migration_at, (float, int)) or not math.isfinite(migration_at):
        migration_at = None
    notes = [
        "Window events use since <= timestamp < until. Coverage is all stored matches at the current read snapshot.",
        "Seed tier is a discovery source's observed rank, never historical match rank or MMR.",
        "Coverage uses original matches.seed_tier. Download yield uses the match job's current scheduling tier/stream; pending jobs can be promoted without changing the original match provenance.",
        "Discovery grouping uses saved history-payload sampling labels, falling back to its source job labels when absent. A match may be discovered by multiple tiers/streams; distinct discovery counts are not additive across groups.",
        "Download regions and discovery regions identify the match platform. Recent-history and scheduler-claim regions identify the worker/seed platform; a cross-platform discovery can differ.",
        "New downloads and discovery routes are separate event-time cohorts. Dividing same-window downloads by discoveries is not a conversion rate; pending downloads right-censor discovery yield.",
        "A discovery route is one persisted history-job/match pair. Retry and repeated-page responses are deduplicated at recording, so within_slice_repeat_fraction is not an API duplicate-response rate.",
        "within_slice_repeat_fraction = 1 - distinct discovered matches / recorded routes in that slice. already_stored_routes counts routes whose match was completed at or before discovery, a different measure of overlap.",
        "PENDING means patch was unavailable by window end, including failed/skipped/unresolved downloads. Undownloaded status breakdown uses current job status; historical job statuses cannot be reconstructed.",
        "Unique players and lag quantiles are recomputed for each whole group, never summed or averaged from patch rows. repeat_appearance_fraction = 1 - unique players / recorded participant appearances.",
        "Collection lag is (completed_at - game_creation/1000) in hours. Missing/nonpositive game_creation and negative/nonfinite lags are excluded and counted as invalid. P90 uses linear interpolation.",
        "Completed recent-history jobs include empty successes. Capped histories returned the requested limit, which does not prove more games existed. Missing outcome evidence is counted separately. Histories have no patch until their matches are downloaded; no per-patch job denominator is invented.",
        "selected_recent_history_jobs and pending/in_progress/done/failed/skipped_recent_history_jobs describe the current inventory across all retained cohorts, independent of the selected event window. completed_recent_history_jobs alone counts successes completed within the window.",
        "Scheduler claims are persisted lifetime service counters since sampling telemetry began, not window counters, HTTP calls, successful completions, or unique matches. Retries also consume claims. Do not divide window yield by lifetime claims.",
        "Legacy databases without sampling columns/discovery tables return unavailable metrics as null and unattributed downloads as UNKNOWN. Zero means observed zero only where telemetry exists.",
    ]
    if migration_at is not None and since < migration_at:
        notes.append("The requested window begins before sampling telemetry migration; discovery and scheduler coverage are incomplete for that earlier period.")
    return {
        "report_version": 1, "generated_at_utc": _utc(generated),
        "window": {"since_utc": since_utc, "until_utc": until_utc,
                   "since_timestamp": since, "until_timestamp": until},
        "metric_availability": {"sampling_attribution": has_sampling, "discovery_routes": has_discoveries,
                                "recent_history_outcomes": has_sampling, "scheduler_claims": scheduler_metadata_present},
        "summary": summary,
        "coverage": [dict(region=key[0], seed_tier=key[1], patch=key[2], matches=count)
                     for key, count in sorted(coverage_counts.items())],
        "yield_rows": [dict(region=key[0], stream=key[1], seed_tier=key[2], patch=key[3],
                            **_finish(acc, include_histories=False, **finish_options))
                       for key, acc in sorted(slices.items())],
        "group_yield": [dict(region=key[0], stream=key[1], seed_tier=key[2],
                             **_finish(acc, include_histories=True, **finish_options))
                        for key, acc in sorted(groups.items())],
        "budget_service": sorted(service, key=lambda row: (row["region"], row["stream"], row["seed_tier"], row["kind"])),
        "budget_service_scope": {"time_basis": "lifetime", "window_filtered": False,
                                 "since_timestamp": migration_at,
                                 "since_utc": _utc(migration_at) if migration_at is not None else None},
        "seed_coverage": summarize_seed_coverage(seed_evidence, set(platforms) | regions, since, until),
        "diversity": summarize_diversity(diversity_evidence, player_counts),
        "selection_rules": [dict(region=k[0], stream=k[1], seed_tier=k[2], selection_rule=k[3], checks=v)
                            for k, v in sorted(selection_rules.items())],
        "notes": notes,
    }
