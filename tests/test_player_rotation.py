from dataclasses import replace
import json

from riot_collector.player_sampling import PlayerPool
from riot_collector.engine import Collector
from riot_collector.config import Config
from riot_collector.state import State
from riot_collector.storage import RawStorage
from test_engine import FakeClient
from test_recent_sampling import add_players


def collector(state, **changes):
    cfg = replace(Config(), data_dir=state.data_dir, platforms=("NA1",), min_free_gb=0,
                  **changes)
    return Collector(cfg, state, RawStorage(state.data_dir, 0), FakeClient(lambda *_: []))


def test_pool_is_bounded_deterministic_and_rotates_without_activity_filter():
    def pool(order):
        p = PlayerPool(10, .5, "synthetic")
        for i in order:
            p.add(str(i), 1000 if i < 50 else 0, 4 if i < 50 else 0)
        assert len(p.uniform) == 5 and len(p.rotation) == 10
        return p.selected()
    rows = pool(range(100))
    assert rows == pool(reversed(range(100)))
    assert len({r[0] for r in rows}) == 10
    assert sum(r[1] == "uniform_random" for r in rows) == 5
    assert all(r[2] == 0 for r in rows if r[1] == "least_recently_selected")


def test_small_pools_revisit_without_duplicates_or_exceeding_population():
    pool = PlayerPool(200, .5, "small")
    for i in range(3):
        pool.add(str(i), 100, 10)
    assert len(pool.selected()) == 3
    assert len({r[0] for r in pool.selected()}) == 3


def test_tier_limits_provenance_and_ledger_survive_restart(tmp_path):
    overrides = {"EMERALD": {"players": 200, "matches": 10}, "DIAMOND": {"players": 200, "matches": 10}}
    state = State(tmp_path)
    add_players(state, per_tier=250)
    c = collector(state, recent_tier_limits=overrides)
    now = 1_800_000_000
    c._recent_jobs(now)
    jobs = [dict(r) for r in state.conn.execute("SELECT * FROM jobs WHERE kind='recent_history'")]
    payloads = [json.loads(r["payload"]) for r in jobs]
    for tier in ("EMERALD", "DIAMOND", "MASTER", "GRANDMASTER", "CHALLENGER", "UNKNOWN"):
        subset = [p for p in payloads if p["sampling_tier"] == tier]
        assert len(subset) == (200 if tier in overrides else 100)
        assert {p["match_limit"] for p in subset} == ({10} if tier in overrides else {20})
        assert sum(p["selection_rule"] == "uniform_random" for p in subset) == len(subset) // 2
        assert all(p["previous_selection_count"] == 0 for p in subset)
    ledger = [tuple(r) for r in state.conn.execute("SELECT * FROM player_sampling_history ORDER BY puuid,platform")]
    state.close()
    state = State(tmp_path)
    collector(state, recent_tier_limits=overrides)._recent_jobs(now)
    assert [dict(r) for r in state.conn.execute("SELECT * FROM jobs WHERE kind='recent_history'")] == jobs
    assert [tuple(r) for r in state.conn.execute("SELECT * FROM player_sampling_history ORDER BY puuid,platform")] == ledger
    collector(state, recent_tier_limits=overrides)._recent_jobs(now + 21600)
    assert state.conn.execute("SELECT MAX(selection_count) FROM player_sampling_history").fetchone()[0] == 2
    state.close()


def test_bootstrap_rotates_legacy_selections_without_rewriting_old_jobs(tmp_path):
    state = State(tmp_path)
    state.enqueue("recent_history", "old", "NA1", {"puuid": "old-player", "start": 20, "match_limit": 20})
    row = dict(state.conn.execute("SELECT * FROM jobs WHERE key='old'").fetchone())
    state.conn.execute("DELETE FROM key_value WHERE key='player_rotation_v1'")
    state.close()
    for _ in range(2):
        state = State(tmp_path)
        assert dict(state.conn.execute("SELECT * FROM jobs WHERE key='old'").fetchone()) == row
        saved = state.conn.execute("SELECT * FROM player_sampling_history WHERE puuid='old-player'").fetchone()
        assert saved["selection_count"] == 1 and saved["last_selected_at"] == row["created_at"]
        state.close()


def test_unfinished_ladder_scans_block_new_scan_but_keep_existing_cursors(tmp_path, monkeypatch):
    state = State(tmp_path)
    state.enqueue("seed", "old-page", "NA1", {"tier": "EMERALD", "division": "IV", "bucket": 1, "page": 300})
    state.enqueue("seed", "already-overlapping", "NA1", {"tier": "EMERALD", "division": "IV", "bucket": 2, "page": 250})
    originals = [dict(r) for r in state.conn.execute("SELECT * FROM jobs")]
    monkeypatch.setattr("riot_collector.engine.time.time", lambda: 3 * 86400)
    collector(state)._seed_jobs()
    emerald = [dict(r) for r in state.conn.execute("SELECT * FROM jobs WHERE sampling_tier='EMERALD'")]
    assert emerald == originals
    assert state.conn.execute("SELECT COUNT(*) FROM jobs WHERE kind='seed' AND sampling_tier='DIAMOND'").fetchone()[0] == 4
    collector(state)._seed_jobs()  # Restart in same daily bucket does not duplicate.
    assert [dict(r) for r in state.conn.execute("SELECT * FROM jobs WHERE sampling_tier='EMERALD'")] == originals
    for r in originals:
        state.complete(r["id"])
    monkeypatch.setattr("riot_collector.engine.time.time", lambda: 4 * 86400)
    collector(state)._seed_jobs()
    assert state.conn.execute("SELECT COUNT(*) FROM jobs WHERE kind='seed' AND sampling_tier='EMERALD'").fetchone()[0] == 6
    assert [json.loads(r[0])["page"] for r in state.conn.execute("SELECT payload FROM jobs WHERE key IN ('old-page','already-overlapping') ORDER BY id")] == [300, 250]
    state.close()
