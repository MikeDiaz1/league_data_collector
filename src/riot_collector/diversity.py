"""Additive collection telemetry; never rewrite raw matches or infer events."""
from collections import Counter, defaultdict
import json
import math


ROLES = {"TOP", "JUNGLE", "MIDDLE", "BOTTOM", "UTILITY"}


def gameplay_signals(info):
    signals, lanes = set(), defaultdict(list)
    known = 0
    for player in info.get("participants", []):
        champion, role = player.get("championId"), player.get("teamPosition")
        if type(champion) is not int or champion <= 0 or not isinstance(role, str) or role not in ROLES:
            continue
        known += 1
        signals.add(("champion_role", json.dumps([role, champion], separators=(",", ":"))))
        if type(player.get("teamId")) is int:
            lanes[role].append((player["teamId"], champion))
    for role, players in lanes.items():
        if len(players) == 2 and players[0][0] != players[1][0]:
            champions = sorted(p[1] for p in players)
            signals.add(("matchup", json.dumps([role, *champions], separators=(",", ":"))))
    return signals, known


def record_match_diversity(conn, job, payload, source_sha256, recorded_at):
    """Called within the match commit, before its participants are inserted."""
    match_id, info = payload["metadata"]["matchId"], payload["info"]
    accounts = {p["puuid"] for p in info["participants"]}
    new_accounts = sum(conn.execute("SELECT 1 FROM match_participants WHERE puuid=? LIMIT 1", (p,)).fetchone() is None
                       for p in accounts)
    signals, known = gameplay_signals(info)
    new = Counter()
    for kind, signal in sorted(signals):
        inserted = conn.execute("INSERT OR IGNORE INTO gameplay_signals VALUES (?,?,?,?)",
                                (kind, signal, recorded_at, match_id)).rowcount
        new[kind] += inserted
        conn.execute("INSERT INTO match_gameplay_signals VALUES (?,?,?)", (match_id, kind, signal))
    conn.execute("INSERT INTO match_diversity VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                 (match_id, source_sha256, match_id.split("_", 1)[0], job.get("sampling_stream", "UNKNOWN"),
                  job.get("sampling_tier", "UNKNOWN"), recorded_at, new_accounts,
                  len(info["participants"]), known, new["champion_role"], new["matchup"]))


def read_diversity_evidence(conn, tables, since, until):
    required = {"match_diversity", "match_gameplay_signals", "api_request_events", "gameplay_signals"}
    if not required.issubset(tables):
        return {"available": False, "matches": [], "signals": [], "requests": [], "started_at": None}
    marker = conn.execute("SELECT value FROM key_value WHERE key='diversity_telemetry_v1'").fetchone()
    started = json.loads(marker[0]).get("started_at") if marker else None
    effective_since = max(since, started) if isinstance(started, (float, int)) else since
    matches = [dict(r) for r in conn.execute(
        "SELECT platform,stream,seed_tier,COUNT(*) AS measured_games,SUM(new_accounts) AS new_accounts,"
        "SUM(participant_slots) AS participant_slots,SUM(known_role_slots) AS known_role_slots,"
        "SUM(new_champion_roles) AS new_champion_roles,SUM(new_matchups) AS new_matchups "
        "FROM match_diversity WHERE recorded_at>=? AND recorded_at<? GROUP BY platform,stream,seed_tier",
        (effective_since, until))]
    signals = [tuple(r) for r in conn.execute(
        "SELECT d.platform,d.stream,d.seed_tier,s.kind,s.signal_key,COUNT(*) FROM match_diversity d "
        "JOIN match_gameplay_signals s ON s.match_id=d.match_id WHERE d.recorded_at>=? AND d.recorded_at<? "
        "GROUP BY d.platform,d.stream,d.seed_tier,s.kind,s.signal_key", (effective_since, until))]
    requests = [dict(r) for r in conn.execute(
        "SELECT platform,stream,seed_tier,COUNT(*) AS request_attempts FROM api_request_events "
        "WHERE dispatched_at>=? AND dispatched_at<? GROUP BY platform,stream,seed_tier", (effective_since, until))]
    return {"available": True, "started_at": started, "effective_since": effective_since,
            "matches": matches, "signals": signals, "requests": requests}


def summarize_diversity(evidence, player_counts):
    """Expose aggregates only. Window throughput is not a cohort conversion rate."""
    rows = {}
    for source in (*evidence["matches"], *evidence["requests"]):
        key = source["platform"], source["stream"], source["seed_tier"]
        row = rows.setdefault(key, {"region": key[0], "stream": key[1], "seed_tier": key[2],
            "measured_games": 0, "new_accounts": 0, "participant_slots": 0, "known_role_slots": 0,
            "new_champion_roles": 0, "new_matchups": 0, "request_attempts": 0})
        row.update({k: v for k, v in source.items() if k not in ("platform", "stream", "seed_tier")})
    sets = defaultdict(lambda: defaultdict(set))
    signal_counts = {"champion_role": Counter(), "matchup": Counter()}
    for region, stream, tier, kind, signal, count in evidence["signals"]:
        sets[(region, stream, tier)][kind].add(signal)
        signal_counts[kind][signal] += count
    overall = {field: sum(row[field] for row in rows.values()) for field in (
        "measured_games", "new_accounts", "participant_slots", "known_role_slots", "new_champion_roles", "new_matchups", "request_attempts")}
    for key, row in rows.items():
        row["champion_role_combinations"] = len(sets[key]["champion_role"])
        row["matchup_combinations"] = len(sets[key]["matchup"])
    overall["champion_role_combinations"] = len(signal_counts["champion_role"])
    overall["matchup_combinations"] = len(signal_counts["matchup"])
    for row in [overall, *rows.values()]:
        for numerator, denominator, output, scale in (
            ("new_accounts", "measured_games", "new_accounts_per_100_games", 100),
            ("new_accounts", "request_attempts", "new_accounts_per_100_requests", 100),
            ("new_champion_roles", "request_attempts", "new_roles_per_100_requests", 100),
            ("new_matchups", "request_attempts", "new_matchups_per_100_requests", 100),
            ("known_role_slots", "participant_slots", "known_role_fraction", 1),
        ):
            row[output] = row[numerator] / row[denominator] * scale if row[denominator] else None
    n = len(player_counts)
    top_n = max(1, math.ceil(n * .01)) if n else 0
    appearances = sum(player_counts.values())
    concentration = {"unique_accounts": n, "top_accounts_count": top_n,
                     "top_one_percent_appearance_share": sum(sorted(player_counts.values(), reverse=True)[:top_n]) / appearances if appearances else None}
    combinations = [{"kind": kind, "role": json.loads(signal)[0],
                     "champion_a": json.loads(signal)[1],
                     "champion_b": json.loads(signal)[2] if kind == "matchup" else None,
                     "games": count} for kind, values in signal_counts.items() for signal, count in sorted(values.items())]
    return {"available": evidence["available"], "started_at": evidence["started_at"],
            "effective_since": evidence.get("effective_since"), "summary": overall,
            "groups": [rows[key] for key in sorted(rows)], "concentration": concentration,
            "combinations": combinations,
            "notes": [
                "New accounts means never previously seen as participants in any stored match, across the entire existing archive. An account is not necessarily a distinct human.",
                "Gameplay combination tracking starts at telemetry activation; new combinations mean first observed since then, not absent from the older archive. Older raw files are not rescanned.",
                "Champion-role pairs use explicit teamPosition; matchups pair exactly two opposing-team players in the same known role. These are role-aligned opponents, not observed lane encounters. Missing or ambiguous roles do not produce matchups.",
                "Top 1% concentration uses all saved games completed inside the report window, including games before telemetry activation. The number of accounts is rounded up.",
                "Requests are recorded HTTP dispatch attempts including retries, history queries, ladder work and downloads. A crash between recording and sending can overcount an in-flight attempt. Historical requests cannot be reconstructed.",
                "Per-100-request metrics are throughput over the telemetry-covered part of the window, not matched-cohort efficiency or causal comparisons: discoveries, downloads and their costs can fall in different windows or groups. Zero denominator means unavailable.",
            ]}
