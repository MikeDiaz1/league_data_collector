"""Sampling contracts use only synthetic players, matches and mocked HTTP."""
import asyncio
from dataclasses import replace
import json

from riot_collector.config import Config
from riot_collector.engine import Collector, SAMPLING_TIERS
from riot_collector.state import State
from riot_collector.storage import RawStorage
from test_engine import FakeClient, match_bytes


def add_players(state, platform="NA1", per_tier=5):
    with state.transaction():
        for tier in (*SAMPLING_TIERS, "UNKNOWN"):
            for i in range(per_tier):
                state.conn.execute(
                    "INSERT INTO players(puuid,platform,source,seed_tier,discovered_at) VALUES (?,?,?,?,?)",
                    (f"{tier}-{i}", platform, "participant" if tier == "UNKNOWN" else "league_seed",
                     None if tier == "UNKNOWN" else tier, 100))


def collector(state, **kwargs):
    cfg = replace(Config(), data_dir=state.data_dir, platforms=("NA1",), min_free_gb=0,
                  recent_seed_players_per_tier=2, recent_participant_players_per_region=2,
                  recent_matches_per_player=2, **kwargs)
    return Collector(cfg, state, RawStorage(state.data_dir, 0), FakeClient(lambda *_: []))


def cohorts(state):
    return [json.loads(r[0]) for r in state.conn.execute(
        "SELECT payload FROM jobs WHERE kind='recent_history' ORDER BY key")]


def test_balanced_cohorts_are_bounded_deterministic_and_do_not_rewrite_legacy(tmp_path):
    with_states = [State(tmp_path / "a"), State(tmp_path / "b")]
    try:
        for state in with_states:
            add_players(state)
            state.enqueue("history", "legacy", "NA1", {"puuid": "EMERALD-0", "start": 700, "end_time": 123})
            before = dict(state.conn.execute("SELECT * FROM jobs WHERE key='legacy'").fetchone())
            collector(state)._recent_jobs(1_800_000_000)
            assert dict(state.conn.execute("SELECT * FROM jobs WHERE key='legacy'").fetchone()) == before
            rows = cohorts(state)
            assert len(rows) == 12
            assert {tier: sum(r["sampling_tier"] == tier for r in rows)
                    for tier in (*SAMPLING_TIERS, "UNKNOWN")} == {
                        tier: 2 for tier in (*SAMPLING_TIERS, "UNKNOWN")}
            assert all(r["end_time"] - r["start_time"] == 14 * 86400 for r in rows)
            assert all(r["candidate_players"] == 5 for r in rows)
        assert cohorts(with_states[0]) == cohorts(with_states[1])
    finally:
        for state in with_states:
            state.close()


def test_restart_keeps_frozen_cohort_but_new_epoch_refreshes_participants(tmp_path):
    state = State(tmp_path)
    try:
        add_players(state)
        c = collector(state)
        c._recent_jobs(1_800_000_000)
        original = cohorts(state)
        state.conn.execute("UPDATE jobs SET status='done' WHERE kind='recent_history'")
        add_players(state, "KR")
        collector(state)._recent_jobs(1_800_000_000)
        assert cohorts(state) == original
        collector(state)._recent_jobs(1_800_000_000 + 6 * 3600)
        assert len(cohorts(state)) == 24
        assert state.conn.execute(
            "SELECT count(*) FROM jobs WHERE kind='recent_history' AND status='pending' AND sampling_stream='participant'"
        ).fetchone()[0] == 2
    finally:
        state.close()


def test_empty_groups_can_be_admitted_after_ladder_discovery(tmp_path):
    state = State(tmp_path)
    try:
        c = collector(state)
        c._recent_jobs(1_800_000_000)
        assert not cohorts(state)
        add_players(state)
        c._recent_jobs(1_800_000_000)
        assert len(cohorts(state)) == 12
    finally:
        state.close()


def test_recent_request_has_fixed_bounds_cap_and_duplicate_discovery_provenance(tmp_path):
    state = State(tmp_path)
    try:
        add_players(state)
        c = collector(state)
        c._recent_jobs(1_800_000_000)
        state.enqueue("match", "NA1_1", "NA1", {"match_id": "NA1_1"})
        old_job = state.claim("match", "NA1")
        raw = match_bytes()
        state.commit_match(old_job, json.loads(raw), c.storage.write("NA1_1", raw), discover=False)
        c.client = FakeClient(lambda *_: ["NA1_1", "NA1_2"])
        job = state.claim("recent_history", "NA1")
        asyncio.run(c._history(job, asyncio.Event()))
        params = c.client.calls[0][-1]
        assert params == {"queue": 420, "start": 0, "count": 2,
                          "startTime": job["payload"]["start_time"], "endTime": job["payload"]["end_time"]}
        result = state.conn.execute("SELECT status,payload FROM jobs WHERE id=?", (job["id"],)).fetchone()
        assert result["status"] == "done"
        assert json.loads(result["payload"])["at_cap"] is True
        assert state.conn.execute("SELECT count(*) FROM match_discoveries").fetchone()[0] == 2
        assert c.storage.read(state.conn.execute("SELECT raw_path FROM matches").fetchone()[0]) == raw
        assert state.conn.execute("SELECT sampling_stream FROM jobs WHERE key='NA1_2'").fetchone()[0] == job["sampling_stream"]
        # Replayed responses cannot duplicate provenance or queued raw downloads.
        asyncio.run(c._history(job, asyncio.Event()))
        assert state.conn.execute("SELECT count(*) FROM match_discoveries").fetchone()[0] == 2
        assert state.conn.execute("SELECT count(*) FROM jobs WHERE kind='match'").fetchone()[0] == 2
    finally:
        state.close()


def test_completed_legacy_history_is_not_reopened_by_ladder_refresh(tmp_path):
    state = State(tmp_path)
    try:
        c = collector(state)
        state.enqueue("history", "history:NA1:seed:0", "NA1", {"puuid": "seed", "start": 900, "end_time": 123})
        old = state.claim("history", "NA1")
        state.complete(old["id"])
        state.conn.execute("UPDATE jobs SET updated_at=1 WHERE id=?", (old["id"],))
        c._observe_player("NA1", "seed", {"tier": "EMERALD", "rank": "I"})
        row = state.conn.execute("SELECT status,payload FROM jobs WHERE id=?", (old["id"],)).fetchone()
        assert row["status"] == "done"
        assert json.loads(row["payload"])["start"] == 900
    finally:
        state.close()


def test_zero_participant_weight_disables_new_participant_cohorts(tmp_path):
    state = State(tmp_path)
    try:
        add_players(state)
        collector(state, participant_sampling_weight=0)._recent_jobs(1_800_000_000)
        assert len(cohorts(state)) == 10
        assert all(row["sampling_stream"] == "recent" for row in cohorts(state))
    finally:
        state.close()


def test_seed_rank_timestamp_is_not_replaced_by_later_independent_rank_audit(tmp_path):
    state = State(tmp_path)
    try:
        state.conn.execute(
            "INSERT INTO players(puuid,platform,source,seed_tier,discovered_at,rank_checked_at) "
            "VALUES ('seed','NA1','league_seed','EMERALD',1,200)")
        for tier, observed_at, source in (("EMERALD", 100, "league_seed"), ("GOLD", 200, "rank_refresh")):
            state.conn.execute(
                "INSERT INTO rank_observations(puuid,platform,tier,queue,observed_at,source,raw_json) "
                "VALUES ('seed','NA1',?,'RANKED_SOLO_5x5',?,?, '{}')", (tier, observed_at, source))
        collector(state)._recent_jobs(1_800_000_000)
        row = cohorts(state)[0]
        assert row["seed_rank"] == "EMERALD"
        assert row["rank_observed_at"] == 100
        assert row["seed_rank_observation_id"] == 1
    finally:
        state.close()
