import asyncio
from dataclasses import replace
import hashlib
import json
import time

import pytest

from riot_collector.api import AuthError, RetryableAPIError
from riot_collector.config import Config, ROUTING
from riot_collector.engine import Collector, CollectorSafetyError
from riot_collector.state import State
from riot_collector.storage import LowDiskError, RawStorage


def match_bytes(match_id="NA1_1", queue=420):
    players = [f"player-{i}" for i in range(10)]
    return json.dumps({"metadata": {"matchId": match_id, "participants": players,
                                    "futureMetadata": {"opaque": True}},
                       "info": {"queueId": queue, "platformId": match_id.split("_")[0],
                                "gameVersion": "16.20.1", "gameDuration": 1800,
                                "gameCreation": 1700000000123,
                                "participants": [{"puuid": p, "items": [1, 2, 3],
                                                  "challenges": {"future": 123},
                                                  "perks": {"styles": []}} for p in players],
                                "teams": [{"teamId": 100, "bans": [], "objectives": {}},
                                          {"teamId": 200, "bans": [], "objectives": {}}],
                                "unknownFutureField": {"keep": [1, 2, 3]}}}, indent=1).encode()


class FakeClient:
    def __init__(self, handler):
        self.handler = handler
        self.calls = []
        self.metrics = {"requests": 0, "retries": 0}
        self.requests_per_minute = 0

    async def get(self, route, path, method, service, params=None):
        await asyncio.sleep(0)
        self.calls.append((route, path, method, service, params))
        self.metrics["requests"] += 1
        value = self.handler(route, path, params)
        if isinstance(value, BaseException):
            raise value
        return value if isinstance(value, bytes) else json.dumps(value).encode()


@pytest.fixture
def components(tmp_path):
    cfg = replace(Config(), data_dir=tmp_path, platforms=("NA1",),
                  min_free_gb=0, history_page_size=2, job_retry_seconds=0.01,
                  progress_seconds=0.02)
    state = State(tmp_path)
    storage = RawStorage(tmp_path, min_free_gb=0)
    yield cfg, state, storage
    state.close()


def engine(components, handler=lambda *_: []):
    cfg, state, storage = components
    return Collector(cfg, state, storage, FakeClient(handler))


def history_job(state, puuid="seed", priority=100, start=0):
    state.enqueue("history", f"history:NA1:{puuid}:0", "NA1",
                  {"puuid": puuid, "start": start, "end_time": 1700000000,
                   "discovery_source": "league_seed", "seed_rank": "MASTER"}, priority)
    return state.claim("history", "NA1")


def enqueue_match(state, match_id="NA1_1"):
    state.enqueue("match", match_id, match_id.split("_")[0],
                  {"match_id": match_id, "discovery_source": "league_seed", "seed_rank": "MASTER"})
    return state.claim("match", match_id.split("_")[0])


def test_pagination_is_anchored_and_deduplicated_without_total_cap(components):
    _, state, _ = components
    pages = {0: ["NA1_1", "NA1_2"], 2: ["NA1_2", "NA1_3"], 4: []}
    collector = engine(components, lambda r, p, params: pages[params["start"]])
    job = history_job(state)
    for _ in range(3):
        asyncio.run(collector._history(job, asyncio.Event()))
        job = state.claim("history", "NA1")
    assert job is None
    assert state.conn.execute("SELECT COUNT(*) FROM jobs WHERE kind='match'").fetchone()[0] == 3
    calls = collector.client.calls
    assert [call[-1]["start"] for call in calls] == [0, 2, 4]
    assert all(call[-1]["endTime"] == 1700000000 and call[-1]["queue"] == 420 for call in calls)


def test_cursor_and_match_ids_roll_back_together_after_crash(components, monkeypatch):
    _, state, _ = components
    collector = engine(components, lambda *_: ["NA1_1", "NA1_2"])
    job = history_job(state)
    enqueue = state.enqueue

    def interrupted(kind, key, *args, **kwargs):
        if key == "NA1_2":
            raise OSError("simulated power failure")
        return enqueue(kind, key, *args, **kwargs)

    monkeypatch.setattr(state, "enqueue", interrupted)
    with pytest.raises(OSError):
        asyncio.run(collector._history(job, asyncio.Event()))
    assert state.conn.execute("SELECT COUNT(*) FROM jobs WHERE kind='match'").fetchone()[0] == 0
    state.recover()
    assert state.claim("history", "NA1")["payload"]["start"] == 0


def test_raw_match_is_complete_and_bounded_participant_discovery_transactional(components):
    _, state, storage = components
    raw = match_bytes()
    collector = engine(components, lambda *_: raw)
    asyncio.run(collector._match(enqueue_match(state)))
    row = state.conn.execute("SELECT * FROM matches").fetchone()
    assert storage.read(row["raw_path"]) == raw
    assert row["sha256"] == hashlib.sha256(raw).hexdigest()
    assert state.conn.execute("SELECT COUNT(*) FROM match_participants").fetchone()[0] == 10
    assert state.conn.execute("SELECT COUNT(*) FROM players").fetchone()[0] == 10
    assert state.conn.execute("SELECT COUNT(*) FROM jobs WHERE kind='history'").fetchone()[0] == 0
    assert state.conn.execute("SELECT COUNT(*) FROM rank_observations").fetchone()[0] == 0
    # Replaying already-completed work must not issue a second request.
    asyncio.run(collector._match({"id": 1, "platform": "NA1", "payload": {"match_id": "NA1_1"}}))
    assert len(collector.client.calls) == 1


def test_file_published_before_database_commit_is_adopted_without_http(components):
    _, state, storage = components
    raw = match_bytes()
    storage.write("NA1_1", raw)
    collector = engine(components, lambda *_: AssertionError("unexpected HTTP"))
    asyncio.run(collector._match(enqueue_match(state)))
    assert not collector.client.calls
    assert state.conn.execute("SELECT COUNT(*) FROM matches").fetchone()[0] == 1


def test_wrong_queue_is_quarantined_and_never_counted(components):
    _, state, storage = components
    raw = match_bytes(queue=440)
    collector = engine(components, lambda *_: raw)
    asyncio.run(collector._match(enqueue_match(state)))
    assert state.conn.execute("SELECT COUNT(*) FROM matches").fetchone()[0] == 0
    assert state.conn.execute("SELECT status FROM jobs").fetchone()[0] == "skipped"
    assert storage.existing("NA1_1", "quarantine")[0] == raw


def test_routing_uses_matching_match_v5_region(components):
    cfg, state, storage = components
    collector = Collector(replace(cfg, platforms=tuple(ROUTING)), state, storage,
                          FakeClient(lambda r, p, params: [] if params else match_bytes(p.rsplit("/", 1)[-1])))
    for platform in ROUTING:
        state.enqueue("history", f"history:{platform}:seed:0", platform,
                      {"puuid": "seed", "start": 0, "end_time": 1700000000})
        asyncio.run(collector._history(state.claim("history", platform), asyncio.Event()))
        asyncio.run(collector._match(enqueue_match(state, f"{platform}_1")))
    assert [call[0] for call in collector.client.calls] == [
        "americas", "americas", "europe", "europe", "asia", "asia"]


def test_transient_request_failure_is_durably_retried(components):
    _, state, _ = components
    calls = 0

    def response(*_):
        nonlocal calls
        calls += 1
        return RetryableAPIError(503) if calls == 1 else match_bytes()

    collector = engine(components, response)
    collector._seed_jobs = lambda: None
    state.enqueue("match", "NA1_1", "NA1", {"match_id": "NA1_1"})
    result = asyncio.run(collector.run(asyncio.Event(), smoke_per_region=1, max_seconds=1))
    assert result["downloaded"] == {"NA1": 1}
    assert state.conn.execute("SELECT attempts FROM jobs WHERE kind='match'").fetchone()[0] == 2


def test_authentication_failure_preserves_claimed_job_and_stops(components):
    _, state, _ = components
    collector = engine(components, lambda *_: AuthError("rejected"))
    collector._seed_jobs = lambda: None
    state.enqueue("match", "NA1_1", "NA1", {"match_id": "NA1_1"})
    with pytest.raises(AuthError):
        asyncio.run(collector.run(asyncio.Event(), max_seconds=1))
    assert state.conn.execute("SELECT status FROM jobs").fetchone()[0] == "pending"
    assert state.get_meta("progress")["updated_at"] > 0


def test_explicit_time_limit_cancels_waiting_request_and_recovers_claim(components):
    _, state, _ = components
    collector = engine(components)
    collector._seed_jobs = lambda: None

    async def sleeping(*_):
        await asyncio.sleep(100)

    collector.client.get = sleeping
    state.enqueue("match", "NA1_1", "NA1", {"match_id": "NA1_1"})
    start = time.monotonic()
    result = asyncio.run(collector.run(asyncio.Event(), max_seconds=0.04))
    assert result["reason"] == "time-limit"
    assert time.monotonic() - start < 1
    assert state.conn.execute("SELECT status FROM jobs").fetchone()[0] == "pending"


def test_milestone_is_logged_but_never_stops_collection(components):
    cfg, state, storage = components
    collector = Collector(replace(cfg, milestone=1), state, storage,
                          FakeClient(lambda r, p, params: [] if params else match_bytes(p.rsplit("/", 1)[-1])))
    collector._seed_jobs = lambda: None
    for match_id in ("NA1_1", "NA1_2"):
        state.enqueue("match", match_id, "NA1", {"match_id": match_id})
    result = asyncio.run(collector.run(asyncio.Event(), max_seconds=2))
    assert sum(result["downloaded"].values()) == 2
    assert state.get_meta("milestone_reached")["matches"] == 1


def test_timeline_backfill_exhausts_independently_of_pending_collect_jobs(components):
    _, state, storage = components
    job = enqueue_match(state)
    body = json.loads(match_bytes())
    state.commit_match(job, body, storage.write("NA1_1", match_bytes()))
    timeline = {"metadata": body["metadata"], "info": {"frames": [{"events": [{"keep": "all"}]}]}}
    collector = engine(components, lambda *_: timeline)
    result = asyncio.run(collector.run(asyncio.Event(), mode="timelines", max_seconds=1))
    assert result["reason"] == "exhausted"
    assert state.conn.execute("SELECT COUNT(*) FROM timelines").fetchone()[0] == 1
    assert len(collector.client.calls) == 1 and collector.client.calls[0][1].endswith("/timeline")
    collector = engine(components, lambda *_: AssertionError("unexpected request"))
    asyncio.run(collector.run(asyncio.Event(), mode="timelines", max_seconds=1))
    assert not collector.client.calls


def test_promoting_participant_preserves_cursor_and_seed_provenance(components):
    _, state, _ = components
    collector = engine(components)
    state.enqueue("history", "history:NA1:seed:0", "NA1",
                  {"puuid": "seed", "start": 200, "end_time": 123,
                   "discovery_source": "participant", "seed_rank": None})
    with state.transaction():
        collector._observe_player("NA1", "seed", {"tier": "MASTER", "rank": "I", "leaguePoints": 123}, 1700000000)
    job = state.claim("history", "NA1")
    assert job["payload"] == {"puuid": "seed", "start": 200, "end_time": 123,
                              "discovery_source": "league_seed", "seed_rank": "MASTER"}
    assert job["priority"] == 80
    assert state.conn.execute("SELECT observed_at FROM rank_observations").fetchone()[0] == 1700000000


def test_seed_pagination_interleaves_tiers_and_rejects_repeated_pages(components):
    _, state, _ = components
    collector = engine(components, lambda *_: [{"puuid": "seed", "rank": "I", "leaguePoints": 20}])
    collector._enqueue_seed_page("NA1", "EMERALD", "I", 1, 1)
    asyncio.run(collector._seed(state.claim("seed", "NA1")))
    second = state.claim("seed", "NA1")
    assert second["payload"]["page"] == 2
    asyncio.run(collector._seed(second))
    assert state.conn.execute("SELECT COUNT(*) FROM jobs WHERE kind='seed'").fetchone()[0] == 2
    assert state.conn.execute("SELECT status FROM jobs WHERE id=?", (second["id"],)).fetchone()[0] == "failed"


def test_backlog_in_one_region_does_not_starve_other_region_seeds(components):
    cfg, state, storage = components
    collector = Collector(replace(cfg, discovery_backlog=1, platforms=("NA1", "KR")), state, storage, FakeClient(lambda *_: []))
    state.enqueue("match", "NA1_1", "NA1", {"match_id": "NA1_1"})
    state.enqueue("seed", "kr-seed", "KR", {"tier": "MASTER"})
    assert collector._next_job("KR")["kind"] == "seed"


def test_unknown_history_has_a_fair_turn_even_with_high_priority_backlog(components):
    _, state, _ = components
    collector = engine(components)
    state.enqueue("history", "high", "NA1", {"puuid": "high"}, 100)
    state.enqueue("history", "unknown", "NA1", {"puuid": "unknown"}, 0)
    claims = [collector._next_job("NA1"), collector._next_job("NA1")]
    assert {job["key"] for job in claims} == {"high", "unknown"}


def test_missing_documented_puuid_safely_retains_seed_instead_of_using_removed_api(components):
    _, state, _ = components
    collector = engine(components, lambda *_: {"entries": [{"summonerId": "old-format"}]})
    collector._seed_jobs = lambda: None
    state.enqueue("seed", "old-id", "NA1", {"tier": "MASTER", "bucket": 1})
    with pytest.raises(CollectorSafetyError, match="missing its documented PUUID"):
        asyncio.run(collector.run(asyncio.Event(), max_seconds=1))
    assert state.conn.execute("SELECT status FROM jobs").fetchone()[0] == "pending"
    assert len(collector.client.calls) == 1


def test_monitor_failure_stops_workers_and_preserves_original_error(components, monkeypatch):
    _, state, _ = components
    collector = engine(components)
    collector._seed_jobs = lambda: None
    state.enqueue("match", "NA1_1", "NA1", {"match_id": "NA1_1"})

    async def sleeping(*_):
        await asyncio.sleep(100)

    collector.client.get = sleeping
    progress = collector._progress
    calls = 0

    def broken_progress():
        nonlocal calls
        calls += 1
        if calls > 1:
            raise RuntimeError("progress database failed")
        return progress()

    monkeypatch.setattr(collector, "_progress", broken_progress)
    with pytest.raises(RuntimeError, match="progress database failed"):
        asyncio.run(collector.run(asyncio.Event(), max_seconds=1))
    assert state.conn.execute("SELECT status FROM jobs").fetchone()[0] == "pending"


def test_unknown_match_has_a_fair_turn_amid_high_rank_matches(components):
    _, state, _ = components
    collector = engine(components)
    state.enqueue("match", "NA1_1", "NA1", {"match_id": "NA1_1"}, 0)
    state.enqueue("match", "NA1_2", "NA1", {"match_id": "NA1_2"}, 100)
    claims = [collector._next_job("NA1"), collector._next_job("NA1")]
    assert {job["key"] for job in claims} == {"NA1_1", "NA1_2"}


def test_disk_safety_monitor_stops_during_long_api_wait(components, monkeypatch):
    _, state, storage = components
    collector = engine(components)
    collector._seed_jobs = lambda: None
    state.enqueue("match", "NA1_1", "NA1", {"match_id": "NA1_1"})

    async def sleeping(*_):
        await asyncio.sleep(100)

    collector.client.get = sleeping
    checks = 0

    def disk():
        nonlocal checks
        checks += 1
        if checks >= 3:
            raise LowDiskError("critical disk reserve")
        return 100 * 1024**3

    monkeypatch.setattr(storage, "check_disk", disk)
    start = time.monotonic()
    with pytest.raises(LowDiskError, match="critical disk reserve"):
        asyncio.run(collector.run(asyncio.Event(), max_seconds=1))
    assert time.monotonic() - start < 1
    assert state.conn.execute("SELECT status FROM jobs").fetchone()[0] == "pending"
