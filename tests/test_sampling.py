from collections import Counter
import json
import sqlite3
from types import SimpleNamespace

import pytest

from riot_collector.sampling import BalancedScheduler
from riot_collector.state import SCHEMA, State


def config(**overrides):
    values = dict(recent_sampling_weight=8, participant_sampling_weight=1,
                  backfill_sampling_weight=1, discovery_backlog=2000)
    values.update(overrides)
    return SimpleNamespace(**values)


def queue(state, stream, tier, count=1, kind="match", platform="NA1"):
    for number in range(count):
        key = f"{platform}:{stream}:{tier}:{kind}:{number}"
        state.enqueue(kind, key, platform, {"seed_rank": tier, "synthetic": key},
                      100 if tier == "CHALLENGER" else 0,
                      sampling_stream=stream, sampling_tier=tier)


def take(scheduler, state, count, platform="NA1"):
    jobs = []
    for _ in range(count):
        job = scheduler.claim(platform)
        assert job is not None
        jobs.append(job)
        state.complete(job["id"])
    return jobs


def legacy_database(path):
    conn = sqlite3.connect(path / "state.sqlite3")
    conn.executescript(SCHEMA.replace("    sampling_stream TEXT NOT NULL DEFAULT 'backfill',\n", "")
                      .replace("    sampling_tier TEXT NOT NULL DEFAULT 'UNKNOWN',\n", ""))
    original = []
    examples = [
        ("history", "pending", 100, {"puuid": "synthetic-a", "start": 700, "end_time": 123}),
        ("history", "in_progress", 0, {"seed_rank": {"tier": "EMERALD"}, "start": 200}),
        ("match", "done", 0, {"seed_tier": "DIAMOND", "future": {"untouched": [1, 2]}}),
        ("seed", "failed", 0, {"tier": "MASTER", "page": 3}),
        ("history", "skipped", 0, {"seed_rank": None}),
    ]
    for index, (kind, status, priority, payload) in enumerate(examples, start=1):
        row = (index, kind, f"key-{index}", "NA1", json.dumps(payload, indent=2), status,
               priority, 3, 456.0, "retained", 111.0, 222.0)
        conn.execute("INSERT INTO jobs VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", row)
        original.append(row)
    match = ("NA1_123", "NA1", "AMERICAS", 420, "16.20", "16.20.1", 1, 2, 3,
             1800, "raw/synthetic.json.gz", 999, "a" * 64, "league_seed", "MASTER", 111.0, 222.0)
    conn.execute("INSERT INTO matches VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", match)
    conn.execute("INSERT INTO key_value VALUES ('existing', '{\"untouched\": true}')")
    conn.commit()
    conn.close()
    return original, match


def test_additive_migration_preserves_jobs_matches_and_original_payloads(tmp_path):
    original, match = legacy_database(tmp_path)
    state = State(tmp_path)
    columns = "id,kind,key,platform,payload,status,priority,attempts,not_before,error,created_at,updated_at"
    assert [tuple(row) for row in state.conn.execute(f"SELECT {columns} FROM jobs ORDER BY id")] == original
    assert tuple(state.conn.execute("SELECT * FROM matches").fetchone()) == match
    assert [tuple(row) for row in state.conn.execute(
        "SELECT sampling_stream,sampling_tier FROM jobs ORDER BY id")
    ] == [("backfill", "CHALLENGER"), ("backfill", "EMERALD"), ("backfill", "DIAMOND"),
          ("seed", "MASTER"), ("backfill", "UNKNOWN")]
    assert state.get_meta("existing") == {"untouched": True}
    marker = state.get_meta("sampling_schema_v1")
    state.close()
    state = State(tmp_path)
    assert state.get_meta("sampling_schema_v1") == marker
    assert [tuple(row) for row in state.conn.execute(f"SELECT {columns} FROM jobs ORDER BY id")] == original
    assert state.conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    state.close()


def test_interrupted_migration_rolls_back_columns_and_can_retry(tmp_path, monkeypatch):
    original, _ = legacy_database(tmp_path)
    set_meta = State.set_meta

    def fail_marker(self, key, value):
        if key == "sampling_schema_v1":
            raise OSError("synthetic migration interruption")
        return set_meta(self, key, value)

    with monkeypatch.context() as patch:
        patch.setattr(State, "set_meta", fail_marker)
        with pytest.raises(OSError, match="migration interruption"):
            State(tmp_path)
    with sqlite3.connect(tmp_path / "state.sqlite3") as conn:
        assert "sampling_stream" not in {row[1] for row in conn.execute("PRAGMA table_info(jobs)")}
        assert conn.execute("SELECT * FROM jobs ORDER BY id").fetchall() == original
    state = State(tmp_path)
    assert state.get_meta("sampling_schema_v1")["version"] == 1
    state.close()


def test_all_five_tiers_have_equal_turns_under_large_challenger_backlog(tmp_path):
    state = State(tmp_path)
    tiers = ("EMERALD", "DIAMOND", "MASTER", "GRANDMASTER", "CHALLENGER")
    for tier in tiers:
        queue(state, "recent", tier, 500 if tier == "CHALLENGER" else 30)
    jobs = take(BalancedScheduler(config(), state), state, 100)
    assert Counter(job["sampling_tier"] for job in jobs) == {tier: 20 for tier in tiers}
    state.close()


def test_stream_budgets_and_reserved_seed_turn_survive_saturated_backlogs(tmp_path):
    state = State(tmp_path)
    for stream in ("recent", "participant", "backfill"):
        queue(state, stream, "EMERALD", 200)
    queue(state, "seed", "MASTER", 20, kind="seed")
    jobs = take(BalancedScheduler(config(), state), state, 200)
    assert Counter(job["sampling_stream"] for job in jobs) == {
        "recent": 152, "participant": 19, "backfill": 19, "seed": 10}
    assert [i for i, job in enumerate(jobs) if job["kind"] == "seed"] == list(range(0, 200, 20))
    state.close()


def test_cursor_persistence_prevents_restart_from_repeating_favored_stream_or_tier(tmp_path):
    state = State(tmp_path)
    for stream in ("recent", "participant", "backfill"):
        for tier in ("EMERALD", "CHALLENGER"):
            queue(state, stream, tier, 30)
    claimed = []
    for _ in range(40):
        job = take(BalancedScheduler(config(), state), state, 1)[0]
        claimed.append((job["sampling_stream"], job["sampling_tier"]))
        state.close()
        state = State(tmp_path)
    assert Counter(stream for stream, _ in claimed) == {"recent": 32, "participant": 4, "backfill": 4}
    assert Counter(tier for stream, tier in claimed if stream == "recent") == {"EMERALD": 16, "CHALLENGER": 16}
    assert sum(row["count"] for row in state.sampling_summary()["served"]) == 40
    state.close()


def test_legacy_backlog_cannot_block_recent_histories_or_other_platforms(tmp_path):
    state = State(tmp_path)
    queue(state, "backfill", "CHALLENGER", 50)
    queue(state, "recent", "EMERALD", 2, kind="recent_history")
    queue(state, "recent", "DIAMOND", 1, kind="recent_history", platform="KR")
    scheduler = BalancedScheduler(config(discovery_backlog=1), state)
    assert scheduler.claim("NA1")["kind"] == "recent_history"
    assert scheduler.claim("KR")["kind"] == "recent_history"
    state.close()


def test_own_full_stream_backlog_gates_history_without_spending_other_budgets(tmp_path):
    state = State(tmp_path)
    queue(state, "recent", "EMERALD", 4)
    queue(state, "recent", "DIAMOND", 2, kind="recent_history")
    scheduler = BalancedScheduler(config(discovery_backlog=1), state)
    assert all(job["kind"] == "match" for job in take(scheduler, state, 4))
    assert scheduler.claim("NA1")["kind"] == "recent_history"
    state.close()


def test_recent_and_participant_streams_use_two_match_turns_per_history(tmp_path):
    state = State(tmp_path)
    queue(state, "recent", "EMERALD", 30)
    queue(state, "recent", "EMERALD", 15, kind="recent_history")
    jobs = take(BalancedScheduler(config(), state), state, 30)
    assert Counter(job["kind"] for job in jobs) == {"match": 20, "recent_history": 10}
    state.close()


def test_unknown_tier_fallback_and_delayed_jobs_do_not_starve_ready_work(tmp_path):
    state = State(tmp_path)
    queue(state, "recent", "UNKNOWN", 5, kind="recent_history")
    queue(state, "recent", "CHALLENGER", 5, kind="recent_history")
    queue(state, "recent", "EMERALD", 5, kind="recent_history")
    state.conn.execute("UPDATE jobs SET not_before=99999999999 WHERE sampling_tier='EMERALD'")
    jobs = take(BalancedScheduler(config(), state), state, 6)
    assert Counter(job["sampling_tier"] for job in jobs) == {"CHALLENGER": 3, "UNKNOWN": 3}
    state.close()


def test_promotion_retains_first_payload_and_completed_data(tmp_path):
    state = State(tmp_path)
    first = {"match_id": "NA1_1", "seed_rank": "CHALLENGER", "original": True}
    state.enqueue("match", "NA1_1", "NA1", first)
    state.enqueue("match", "NA1_1", "NA1", {"seed_rank": "EMERALD"},
                  sampling_stream="recent", sampling_tier="EMERALD")
    state.enqueue("match", "NA1_1", "NA1", {"seed_rank": "MASTER"})
    job = BalancedScheduler(config(), state).claim("NA1")
    assert job["payload"] == first
    assert (job["sampling_stream"], job["sampling_tier"]) == ("recent", "EMERALD")
    state.complete(job["id"])
    state.enqueue("match", "NA1_1", "NA1", {}, sampling_stream="recent", sampling_tier="DIAMOND")
    stored = state.conn.execute("SELECT * FROM jobs").fetchone()
    assert stored["status"] == "done" and stored["sampling_tier"] == "EMERALD"
    state.close()


def test_discovery_routes_append_idempotently_and_share_history_transaction(tmp_path):
    state = State(tmp_path)
    queue(state, "recent", "EMERALD", 2, kind="recent_history")
    histories = state.conn.execute("SELECT id FROM jobs ORDER BY id").fetchall()
    first = {"seed_rank": "EMERALD", "sampling_version": "v1", "start_time": 1, "end_time": 2}
    assert state.record_discovery("NA1_1", histories[0][0], "NA1", first)
    assert not state.record_discovery("NA1_1", histories[0][0], "NA1", {"changed": True})
    assert state.record_discovery("NA1_1", histories[1][0], "NA1", {"another": True})
    assert json.loads(state.conn.execute("SELECT history_payload FROM match_discoveries ORDER BY history_job_id").fetchone()[0]) == first
    with pytest.raises(OSError):
        with state.transaction():
            state.record_discovery("NA1_2", histories[0][0], "NA1", first)
            raise OSError("synthetic interruption")
    assert state.conn.execute("SELECT COUNT(*) FROM match_discoveries").fetchone()[0] == 2
    state.close()


def test_claim_and_durable_cursors_roll_back_together(tmp_path, monkeypatch):
    state = State(tmp_path)
    queue(state, "recent", "EMERALD")

    def fail(*_):
        raise OSError("synthetic cursor persistence failure")

    monkeypatch.setattr(state, "set_meta", fail)
    with pytest.raises(OSError):
        BalancedScheduler(config(), state).claim("NA1")
    assert state.conn.execute("SELECT status,attempts FROM jobs").fetchone()[:] == ("pending", 0)
    assert state.get_meta("sampling_scheduler:v1:NA1") is None
    state.close()


def test_scheduler_claim_query_uses_index_without_temporary_sort(tmp_path):
    state = State(tmp_path)
    plan = " ".join(row[3] for row in state.conn.execute(
        "EXPLAIN QUERY PLAN SELECT * FROM jobs INDEXED BY jobs_sampling_claim WHERE status='pending' AND platform=? "
        "AND sampling_stream=? AND kind=? AND sampling_tier=? AND not_before<=? "
        "ORDER BY updated_at,id LIMIT 1", ("NA1", "recent", "recent_history", "EMERALD", 123)))
    assert "jobs_sampling_claim" in plan
    assert "TEMP B-TREE" not in plan
    state.close()


@pytest.mark.parametrize("queue_history", [True, False])
def test_match_commit_can_preserve_players_without_unbounded_history_expansion(tmp_path, queue_history):
    state = State(tmp_path)
    state.enqueue("match", "NA1_1", "NA1", {"match_id": "NA1_1"})
    job = state.claim("match")
    payload = {"metadata": {"matchId": "NA1_1"}, "info": {
        "queueId": 420, "gameVersion": "16.20.1",
        "participants": [{"puuid": f"synthetic-{i}"} for i in range(10)]}}
    stored = {"raw_path": "raw/synthetic.json.gz", "compressed_bytes": 99, "sha256": "a" * 64}
    assert state.commit_match(job, payload, stored, queue_history=queue_history)
    assert state.conn.execute("SELECT COUNT(*) FROM players").fetchone()[0] == 10
    assert state.conn.execute("SELECT COUNT(*) FROM match_participants").fetchone()[0] == 10
    assert state.conn.execute("SELECT COUNT(*) FROM jobs WHERE kind='history'").fetchone()[0] == (10 if queue_history else 0)
    assert state.conn.execute("SELECT status FROM jobs WHERE id=?", (job["id"],)).fetchone()[0] == "done"
    state.close()
