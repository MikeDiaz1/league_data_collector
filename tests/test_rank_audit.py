import asyncio
from dataclasses import replace
import json

from riot_collector.cli import refresh_ranks
from riot_collector.config import Config
from riot_collector.state import State


NOW = 1_800_000_000


def rank_audit_fixture(root, *, reverse_priority=False):
    state = State(root)
    for platform in ("NA1", "KR"):
        match_id = f"{platform}_1"
        state.conn.execute("""
            INSERT INTO matches(match_id,platform,routing,queue_id,raw_path,compressed_bytes,sha256,discovered_at,completed_at)
            VALUES (?,?,?,420,?,1,?,1,1)
        """, (match_id, platform, "americas" if platform == "NA1" else "asia", f"{match_id}.json.gz", "0" * 64))
        for i in range(40):
            puuid = f"synthetic-{i}"
            state.conn.execute("""
                INSERT INTO players(puuid,platform,priority,source,discovered_at,rank_checked_at)
                VALUES (?,?,?,'participant',?,?)
            """, (puuid, platform, 40 - i if reverse_priority else i,
                  40 - i if reverse_priority else i, NOW if i == 0 else None))
            state.conn.execute("INSERT INTO match_participants(match_id,puuid) VALUES (?,?)", (match_id, puuid))
    state.conn.execute("""
        INSERT INTO players(puuid,platform,priority,source,discovered_at)
        VALUES ('seed-with-no-observed-game','NA1',1000,'league_seed',0)
    """)
    state.close()


def fake_rank_client(monkeypatch, *, alternate_unavailable=False):
    from riot_collector import api

    visits = []

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def get(self, route, path, method, service):
            visits.append((route, path.rsplit("/", 1)[1]))
            if alternate_unavailable and len(visits) % 2 == 0:
                raise api.NotFound("Synthetic HTTP 404")
            return json.dumps([]).encode()

    monkeypatch.setattr(api, "RiotClient", FakeClient)
    monkeypatch.setattr("time.time", lambda: NOW)
    return visits


def test_rank_audit_is_budgeted_and_independent_of_priority_and_arrival_order(tmp_path, monkeypatch):
    visits = fake_rank_client(monkeypatch)
    first, second = tmp_path / "first", tmp_path / "second"
    rank_audit_fixture(first)
    rank_audit_fixture(second, reverse_priority=True)
    cfg = replace(Config(), data_dir=first, platforms=("NA1",), min_free_gb=0)
    asyncio.run(refresh_ranks(cfg, "synthetic-key", 12))
    first_sample = list(visits)
    visits.clear()
    asyncio.run(refresh_ranks(replace(cfg, data_dir=second), "synthetic-key", 12))
    assert visits == first_sample
    assert len(first_sample) == 12
    assert len(set(first_sample)) == 12
    assert all(route == "na1" for route, _ in first_sample)
    assert all(puuid not in {"synthetic-0", "seed-with-no-observed-game"} for _, puuid in first_sample)


def test_negative_rank_audits_are_cached_and_next_budget_advances_to_other_players(tmp_path, monkeypatch):
    visits = fake_rank_client(monkeypatch)
    rank_audit_fixture(tmp_path)
    cfg = replace(Config(), data_dir=tmp_path, platforms=("NA1",), min_free_gb=0)
    asyncio.run(refresh_ranks(cfg, "synthetic-key", 12))
    first_sample = set(visits)
    visits.clear()
    asyncio.run(refresh_ranks(cfg, "synthetic-key", 12))
    assert len(visits) == 12
    assert not first_sample.intersection(visits)
    state = State(tmp_path)
    try:
        ranks = list(state.conn.execute("SELECT tier,observed_at,source,raw_json FROM rank_observations"))
        assert len(ranks) == 24
        assert all(row["tier"] is None and row["observed_at"] == NOW and row["source"] == "rank_refresh" for row in ranks)
        assert all(json.loads(row["raw_json"])["lookup_status"] == "unranked" for row in ranks)
        assert state.conn.execute("SELECT COUNT(*) FROM players WHERE rank_checked_at=?", (NOW,)).fetchone()[0] == 26
    finally:
        state.close()


def test_negative_rank_audit_distinguishes_unranked_from_unavailable(tmp_path, monkeypatch):
    visits = fake_rank_client(monkeypatch, alternate_unavailable=True)
    rank_audit_fixture(tmp_path)
    cfg = replace(Config(), data_dir=tmp_path, platforms=("NA1",), min_free_gb=0)
    asyncio.run(refresh_ranks(cfg, "synthetic-key", 2))
    assert len(visits) == 2
    state = State(tmp_path)
    try:
        ranks = list(state.conn.execute("SELECT tier,raw_json FROM rank_observations ORDER BY id"))
        assert [json.loads(row["raw_json"])["lookup_status"] for row in ranks] == ["unranked", "unavailable"]
        assert all(row["tier"] is None for row in ranks)
    finally:
        state.close()
