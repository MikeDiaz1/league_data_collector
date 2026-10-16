"""Synthetic-only coverage reporting fixtures; never open a research dataset."""
from contextlib import contextmanager
import json
import sqlite3

import pytest

from riot_collector import coverage


def database(tmp_path, *, legacy=False):
    conn = sqlite3.connect(tmp_path / "state.sqlite3")
    conn.executescript("""
        CREATE TABLE matches(match_id TEXT PRIMARY KEY,platform TEXT,seed_tier TEXT,
                             patch TEXT,game_creation REAL,completed_at REAL);
        CREATE TABLE match_participants(match_id TEXT,puuid TEXT,PRIMARY KEY(match_id,puuid));
        CREATE TABLE jobs(id INTEGER PRIMARY KEY,kind TEXT,key TEXT,platform TEXT,
                          payload TEXT,status TEXT,updated_at REAL,UNIQUE(kind,key));
        CREATE TABLE key_value(key TEXT PRIMARY KEY,value TEXT);
    """)
    if not legacy:
        conn.executescript("""
            ALTER TABLE jobs ADD COLUMN sampling_stream TEXT;
            ALTER TABLE jobs ADD COLUMN sampling_tier TEXT;
            CREATE TABLE match_discoveries(match_id TEXT,history_job_id INTEGER,platform TEXT,
                                            history_payload TEXT,discovered_at REAL,
                                            PRIMARY KEY(history_job_id,match_id));
        """)
    return conn


def job(conn, job_id, *, key=None, kind="recent_history", stream="recent", tier="EMERALD",
        status="done", updated=11000, payload=None, region="NA1"):
    conn.execute(
        "INSERT INTO jobs VALUES (?,?,?,?,?,?,?,?,?)",
        (job_id, kind, key or f"synthetic-job-{job_id}", region,
         json.dumps(payload if payload is not None else {"returned_matches": 1, "at_cap": False}),
         status, updated, stream, tier),
    )


def match(conn, name, *, completed=11000, patch="15.1", original_tier="CHALLENGER",
          stream="recent", tier="EMERALD", lag=1, players=(), region="NA1", job_id=100):
    creation = (completed - lag * 3600) * 1000 if lag is not None else None
    conn.execute("INSERT INTO matches VALUES (?,?,?,?,?,?)",
                 (name, region, original_tier, patch, creation, completed))
    for player in players:
        conn.execute("INSERT INTO match_participants VALUES (?,?)", (name, player))
    job(conn, job_id, key=name, kind="match", stream=stream, tier=tier, region=region)


def route(conn, name, history_id, discovered, *, tier="EMERALD", stream="recent", payload=None):
    conn.execute("INSERT INTO match_discoveries VALUES (?,?,?,?,?)",
                 (name, history_id, "NA1", json.dumps(payload if payload is not None else
                  {"sampling_stream": stream, "sampling_tier": tier}), discovered))


def group(report, tier="EMERALD", stream="recent", region="NA1"):
    return next(row for row in report["group_yield"]
                if (row["region"], row["stream"], row["seed_tier"]) == (region, stream, tier))


def test_distinct_players_and_lags_are_recomputed_across_patches(tmp_path):
    conn = database(tmp_path)
    match(conn, "synthetic-match-a", players=("private-p1", "private-p2"), job_id=100)
    match(conn, "synthetic-match-b", completed=15000, patch="15.2", lag=3,
          players=("private-p2", "private-p3"), job_id=101)
    match(conn, "synthetic-match-c", completed=16000, patch=None, lag=None,
          players=("private-p3", "private-p4"), job_id=102)
    conn.commit()
    conn.close()

    report = coverage.coverage_report(tmp_path, since=10000, until=20000)
    row = group(report)
    assert row["new_matches"] == 3
    assert row["unique_players"] == 4
    assert row["participant_appearances"] == 6
    assert row["repeat_appearance_fraction"] == pytest.approx(1 / 3)
    assert row["median_lag_hours"] == 2
    assert row["p90_lag_hours"] == pytest.approx(2.8)
    assert row["invalid_lag_matches"] == 1
    assert sum(r["unique_players"] for r in report["yield_rows"]) == 6
    assert {r["seed_tier"] for r in report["coverage"]} == {"CHALLENGER"}
    assert report["summary"]["window_unique_players"] == 4
    assert len(report["group_yield"]) == 5
    assert group(report, "DIAMOND")["new_matches"] == 0
    serialized = json.dumps(report, allow_nan=False)
    assert "private-p" not in serialized
    assert "synthetic-match" not in serialized


def test_discovery_repetition_cross_tier_overlap_and_unresolved_statuses(tmp_path):
    conn = database(tmp_path)
    for history_id in range(1, 10):
        job(conn, history_id)
    match(conn, "stored-before-window", completed=9000, job_id=100)
    match(conn, "new-match", completed=11000, job_id=101)
    match(conn, "unknown-patch", completed=12000, patch=None, job_id=102)
    match(conn, "at-end", completed=20000, job_id=103)
    job(conn, 104, key="waiting", kind="match", status="pending")
    job(conn, 105, key="failed", kind="match", status="failed")
    job(conn, 106, key="skipped", kind="match", status="skipped")
    route(conn, "stored-before-window", 1, 10500)
    route(conn, "new-match", 1, 10500)
    route(conn, "new-match", 2, 11500)
    route(conn, "new-match", 3, 11600, tier="DIAMOND")
    route(conn, "unknown-patch", 1, 12500)
    route(conn, "waiting", 1, 13000)
    route(conn, "failed", 1, 14000)
    route(conn, "skipped", 1, 14500)
    route(conn, "at-end", 1, 15000)
    route(conn, "no-job", 1, 16000)
    conn.commit()
    conn.close()

    report = coverage.coverage_report(tmp_path, since=10000, until=20000)
    emerald, diamond = group(report), group(report, "DIAMOND")
    assert emerald["discovery_routes"] == 9
    assert emerald["distinct_discovered_matches"] == 8
    assert emerald["within_slice_repeat_fraction"] == pytest.approx(1 / 9)
    assert emerald["already_stored_routes"] == 3
    assert emerald["undownloaded_discovered_matches"] == 5
    assert emerald["pending_discovered_matches"] == 1
    assert emerald["failed_discovered_matches"] == 1
    assert emerald["skipped_discovered_matches"] == 1
    assert emerald["unknown_status_discovered_matches"] == 2
    assert diamond["distinct_discovered_matches"] == 1
    assert diamond["already_stored_routes"] == 1
    assert report["summary"]["distinct_discovered_matches"] == 8
    assert report["summary"]["discovery_routes"] == 10
    pending = next(row for row in report["yield_rows"] if row["patch"] == "PENDING")
    assert pending["new_matches"] == 0
    assert pending["discovery_routes"] == 5
    unknown = next(row for row in report["yield_rows"] if row["patch"] == "UNKNOWN")
    assert unknown["new_matches"] == unknown["discovery_routes"] == 1


def test_half_open_windows_and_empty_or_unknown_history_outcomes(tmp_path):
    conn = database(tmp_path)
    for index, completed in enumerate((9999, 10000, 19999, 20000)):
        match(conn, f"match-{index}", completed=completed, job_id=100 + index)
        job(conn, index + 1, updated=completed, payload={"returned_matches": 0, "at_cap": False})
        route(conn, f"match-{index}", index + 1, completed)
    job(conn, 10, updated=11000, payload={"returned_matches": 20, "at_cap": True})
    job(conn, 11, updated=12000, payload={})
    job(conn, 12, updated=12000, status="pending", tier="DIAMOND")
    conn.commit()
    conn.close()

    report = coverage.coverage_report(tmp_path, since=10000, until=20000)
    row = group(report)
    assert report["summary"]["total_matches"] == 4
    assert row["new_matches"] == row["discovery_routes"] == 2
    assert row["completed_recent_history_jobs"] == 4
    assert row["empty_recent_history_jobs"] == 2
    assert row["capped_recent_history_jobs"] == 1
    assert row["unknown_recent_history_outcomes"] == 1
    assert row["selected_recent_history_jobs"] == row["done_recent_history_jobs"] == 6
    assert group(report, "DIAMOND")["pending_recent_history_jobs"] == 1
    assert group(report, "DIAMOND")["selected_recent_history_jobs"] == 1
    assert all("completed_recent_history_jobs" not in row for row in report["yield_rows"])


def test_route_payload_attribution_is_immutable_and_missing_payload_falls_back(tmp_path):
    conn = database(tmp_path)
    job(conn, 1, tier="CHALLENGER", stream="backfill")
    route(conn, "one", 1, 11000, tier="DIAMOND", stream="recent")
    route(conn, "two", 1, 11000, payload={})
    conn.commit()
    conn.close()
    report = coverage.coverage_report(tmp_path, since=10000, until=20000)
    assert group(report, "DIAMOND")["discovery_routes"] == 1
    assert group(report, "CHALLENGER", "backfill")["discovery_routes"] == 1


def test_budget_claims_are_lifetime_not_window_counters(tmp_path):
    conn = database(tmp_path)
    conn.executemany("INSERT INTO key_value VALUES (?,?)", [
        ("sampling_schema_v1", json.dumps({"migrated_at": 5000})),
        ("sampling_scheduler:v1:KR", json.dumps({"served": {
            "recent|match|EMERALD": 80, "participant|recent_history|UNKNOWN": 10,
            "backfill|match|CHALLENGER": 10, "seed|seed|MASTER": 5}})),
    ])
    conn.commit()
    conn.close()
    first = coverage.coverage_report(tmp_path, since=10000, until=20000)
    second = coverage.coverage_report(tmp_path, since=1, until=2)
    assert first["budget_service"] == second["budget_service"]
    assert sum(row["claims"] for row in first["budget_service"]) == 105
    assert first["budget_service_scope"]["window_filtered"] is False
    assert first["budget_service_scope"]["since_timestamp"] == 5000
    assert len(first["group_yield"]) == 5
    assert any("before sampling telemetry" in note for note in second["notes"])


def test_legacy_is_read_only_and_unavailable_is_not_zero(tmp_path, monkeypatch):
    conn = database(tmp_path, legacy=True)
    conn.execute("INSERT INTO matches VALUES ('synthetic-legacy','NA1','EMERALD',NULL,NULL,11000)")
    conn.commit()
    conn.close()
    before = (tmp_path / "state.sqlite3").read_bytes()
    from riot_collector import state
    monkeypatch.setattr(state, "State", lambda *args, **kwargs: pytest.fail("Opened writable State"))

    report = coverage.coverage_report(tmp_path, since=10000, until=20000)
    row = group(report, "UNKNOWN", "UNKNOWN")
    assert row["new_matches"] == 1
    assert row["discovery_routes"] is None
    assert row["completed_recent_history_jobs"] is None
    assert row["selected_recent_history_jobs"] is None
    assert row["unique_players"] == 0
    assert row["repeat_appearance_fraction"] is None
    assert report["budget_service"] == []
    assert not any(report["metric_availability"].values())
    assert (tmp_path / "state.sqlite3").read_bytes() == before
    assert sorted(path.name for path in tmp_path.iterdir()) == ["state.sqlite3"]


def test_empty_database_and_invalid_timestamps(tmp_path):
    conn = database(tmp_path)
    conn.close()
    report = coverage.coverage_report(tmp_path, since=10000, until=20000)
    assert report["summary"]["window_matches"] == 0
    assert report["summary"]["median_lag_hours"] is None
    assert report["summary"]["within_slice_repeat_fraction"] is None
    assert report["coverage"] == report["yield_rows"] == report["group_yield"] == []
    for since, until in ((1, 1), (2, 1), (float("nan"), 1), (1, float("inf"))):
        with pytest.raises(ValueError, match="finite timestamps"):
            coverage.coverage_report(tmp_path, since=since, until=until)


def test_snapshot_allows_a_concurrent_wal_writer_and_remains_consistent(tmp_path, monkeypatch):
    writer = database(tmp_path)
    writer.execute("PRAGMA journal_mode=WAL")
    match(writer, "before-snapshot", job_id=100)
    writer.commit()
    original_read = coverage.read_database
    statements = []

    @contextmanager
    def concurrent_read(data_dir):
        with original_read(data_dir) as reader:
            reader.execute("SELECT count(*) FROM matches").fetchone()  # establish snapshot
            reader.set_trace_callback(statements.append)
            match(writer, "after-snapshot", job_id=101)
            writer.commit()  # would fail if the report acquired an exclusive lock
            yield reader

    monkeypatch.setattr(coverage, "read_database", concurrent_read)
    report = coverage.coverage_report(tmp_path, since=10000, until=20000)
    assert report["summary"]["total_matches"] == report["summary"]["window_matches"] == 1
    assert writer.execute("SELECT count(*) FROM matches").fetchone()[0] == 2
    assert all(sql.lstrip().upper().startswith(("SELECT", "PRAGMA TABLE_INFO")) for sql in statements)
    writer.close()
