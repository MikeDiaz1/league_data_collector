"""Real HTTP limiter + scheduler + database tests for planned and forced stops."""
import asyncio
from dataclasses import replace
import time

import httpx
import pytest

from riot_collector.api import RateLimiter, RiotClient
from riot_collector.config import Config, ROUTING
from riot_collector.engine import Collector
from riot_collector.state import State
from riot_collector.storage import RawStorage
from test_engine import match_bytes


def learn_long_method_window(api, region):
    api.limiter.observe(region, "league_master", "league-v4", {
        "X-Method-Rate-Limit": "500:600", "X-Method-Rate-Limit-Count": "1:600",
    }, sent_at=time.time())


def collector_for(root, state, api, *, platforms=("NA1",), grace=1):
    config = replace(Config(), data_dir=root, platforms=platforms,
                     min_free_gb=0, progress_seconds=0.01, shutdown_grace_seconds=grace)
    collector = Collector(config, state, RawStorage(root, min_free_gb=0), api)
    collector._seed_jobs = lambda: None
    return collector


def test_three_active_regions_finish_and_commit_without_new_restart_cooldown(tmp_path):
    async def scenario():
        stop, started, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
        state = State(tmp_path)
        seen = []

        async def respond(request):
            seen.append(request.url.path)
            if len(seen) == 3:
                started.set()
            await release.wait()
            return httpx.Response(200, content=match_bytes(request.url.path.rsplit("/", 1)[-1]))

        try:
            async with RiotClient("test-key", tmp_path, transport=httpx.MockTransport(respond),
                                  app_limits=((1, 10),), safety_margin=0) as api:
                for platform, region in ROUTING.items():
                    learn_long_method_window(api, region)
                    for number in (1, 2):
                        match_id = f"{platform}_{number}"
                        state.enqueue("match", match_id, platform, {"match_id": match_id})
                collector = collector_for(tmp_path, state, api, platforms=tuple(ROUTING))
                running = asyncio.create_task(collector.run(stop))
                await asyncio.wait_for(started.wait(), 2)
                stop.set()
                await asyncio.sleep(0)
                assert not running.done()
                assert len(api.limiter.pending) == 3
                release.set()
                result = await asyncio.wait_for(running, 2)
                assert result["reason"] == "interrupted"
                assert result["downloaded"] == {"NA1": 1, "EUW1": 1, "KR": 1}
                assert len(seen) == 3  # No second job or retry dispatched after stop.
                assert api.limiter.pending == {}
            recovered = RateLimiter(tmp_path, app_limits=((1, 10),), safety_margin=0)
            assert not recovered.cooldowns  # In particular, no fabricated 600-second wait.
            for region in ROUTING.values():
                assert recovered.delay(region, "match", "match-v5") > 5  # Existing app quota retained.
                assert recovered.buckets[f"method:{region}:league_master"][600].limit == 500
            assert state.stats()["total_matches"] == 3
            assert state.conn.execute("SELECT count(*) FROM jobs WHERE kind='match' AND status='pending'").fetchone()[0] == 3
            for row in state.conn.execute("SELECT * FROM matches"):
                raw = collector.storage.read(row["raw_path"])
                assert raw == match_bytes(row["match_id"])
        finally:
            state.close()

    asyncio.run(scenario())


def test_stop_cancels_quota_wait_promptly_but_preserves_existing_429_cooldown(tmp_path):
    async def scenario():
        stop, sleeping = asyncio.Event(), asyncio.Event()
        state = State(tmp_path)
        state.enqueue("match", "NA1_1", "NA1", {"match_id": "NA1_1"})

        async def sleep(seconds):
            sleeping.set()
            await asyncio.Event().wait()

        def unexpected(request):
            pytest.fail("A quota-waiting request must not dispatch during shutdown")

        try:
            async with RiotClient("test-key", tmp_path, transport=httpx.MockTransport(unexpected), sleep=sleep) as api:
                learn_long_method_window(api, "americas")
                api.limiter.cool_down("americas", "match", "match-v5",
                                     {"Retry-After": "300.125", "X-Rate-Limit-Type": "service"})
                deadline = api.limiter.cooldowns["service:americas:match-v5"]
                running = asyncio.create_task(collector_for(tmp_path, state, api).run(stop))
                await asyncio.wait_for(sleeping.wait(), 2)
                stop.set()
                result = await asyncio.wait_for(running, 1)
                assert result["reason"] == "interrupted"
                assert api.metrics["requests"] == 0
            recovered = RateLimiter(tmp_path)
            assert recovered.cooldowns == {"service:americas:match-v5": deadline}
            assert state.conn.execute("SELECT status FROM jobs").fetchone()[0] == "pending"
        finally:
            state.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("response_kind", ["429", "503", "network-error"])
def test_stop_suppresses_retries_without_discarding_rate_or_uncertainty_state(tmp_path, response_kind):
    async def scenario():
        stop, started, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
        state = State(tmp_path)
        state.enqueue("match", "NA1_1", "NA1", {"match_id": "NA1_1"})
        calls = []

        async def respond(request):
            calls.append(request)
            started.set()
            await release.wait()
            if response_kind == "network-error":
                raise httpx.ReadError("connection ended without a response", request=request)
            return httpx.Response(int(response_kind), headers={
                "Retry-After": "17.125", "X-Rate-Limit-Type": "service",
            })

        try:
            async with RiotClient("test-key", tmp_path, transport=httpx.MockTransport(respond)) as api:
                learn_long_method_window(api, "americas")
                running = asyncio.create_task(collector_for(tmp_path, state, api).run(stop))
                await asyncio.wait_for(started.wait(), 2)
                stop.set()
                release.set()
                await asyncio.wait_for(running, 2)
                assert len(calls) == 1
                assert api.metrics["retries"] == 0
                pending = dict(api.limiter.pending)
                prior_cooldown = dict(api.limiter.cooldowns)
            recovered = RateLimiter(tmp_path)
            if response_kind == "network-error":
                assert "americas" in pending
                assert recovered.cooldowns["region:americas"] > time.time() + 590
            else:
                assert pending == {}
                assert "region:americas" not in recovered.cooldowns
            if response_kind == "429":
                assert recovered.cooldowns["service:americas:match-v5"] == prior_cooldown["service:americas:match-v5"]
            assert state.conn.execute("SELECT status FROM jobs").fetchone()[0] == "pending"
            assert state.stats()["total_matches"] == 0
        finally:
            state.close()

    asyncio.run(scenario())


def test_unresponsive_request_exceeding_grace_retains_crash_protection(tmp_path):
    async def scenario():
        stop, started = asyncio.Event(), asyncio.Event()
        state = State(tmp_path)
        state.enqueue("match", "NA1_1", "NA1", {"match_id": "NA1_1"})

        async def respond(request):
            started.set()
            await asyncio.Event().wait()

        try:
            async with RiotClient("test-key", tmp_path, transport=httpx.MockTransport(respond)) as api:
                learn_long_method_window(api, "americas")
                collector = collector_for(tmp_path, state, api, grace=0.04)
                running = asyncio.create_task(collector.run(stop))
                await asyncio.wait_for(started.wait(), 2)
                stop.set()
                result = await asyncio.wait_for(running, 1)
                assert result["reason"] == "shutdown-timeout"
                assert "americas" in api.limiter.pending
            assert state.conn.execute("SELECT status FROM jobs").fetchone()[0] == "pending"
            assert RateLimiter(tmp_path).cooldowns["region:americas"] > time.time() + 590
        finally:
            state.close()

    asyncio.run(scenario())


def test_manual_drain_does_not_extend_explicit_wall_clock_limit(tmp_path):
    async def scenario():
        stop, started = asyncio.Event(), asyncio.Event()
        state = State(tmp_path)
        state.enqueue("match", "NA1_1", "NA1", {"match_id": "NA1_1"})

        async def respond(request):
            started.set()
            await asyncio.Event().wait()

        try:
            async with RiotClient("test-key", tmp_path, transport=httpx.MockTransport(respond)) as api:
                running = asyncio.create_task(collector_for(tmp_path, state, api, grace=10).run(stop, max_seconds=0.1))
                await asyncio.wait_for(started.wait(), 2)
                stop.set()
                result = await asyncio.wait_for(running, 1)
                assert result["elapsed_seconds"] < 1
                assert result["reason"] in {"time-limit", "shutdown-timeout"}
        finally:
            state.close()

    asyncio.run(scenario())
