import asyncio
from email.utils import formatdate
import json

import httpx
import pytest

from riot_collector.api import (
    AuthError, NotFound, PermanentAPIError, RateLimiter, RateStateError,
    RetryableAPIError, RiotClient, canonical_region, parse_retry_after,
)


class Clock:
    def __init__(self):
        self.now = 1_700_000_000.0
        self.sleeps = []

    def __call__(self):
        return self.now

    async def sleep(self, seconds):
        assert seconds >= 0
        self.sleeps.append(seconds)
        self.now += seconds
        await asyncio.sleep(0)


def client(tmp_path, clock, handler, **kwargs):
    return RiotClient(
        "test-secret-never-persist", tmp_path, clock=clock, sleep=clock.sleep,
        transport=httpx.MockTransport(handler), app_limits=((1000, 1),), safety_margin=0,
        jitter=lambda: 0, **kwargs,
    )


def run(coroutine):
    return asyncio.run(coroutine)


@pytest.mark.parametrize("route,expected", [
    ("NA1", "americas"), ("americas", "americas"), ("EUW1", "europe"),
    ("europe", "europe"), ("KR", "asia"), ("asia", "asia"),
])
def test_regional_routing(route, expected):
    assert canonical_region(route) == expected


def test_only_official_routes_are_accepted():
    with pytest.raises(ValueError):
        canonical_region("attacker.invalid")


def test_token_is_header_and_complete_raw_body_is_preserved(tmp_path):
    clock = Clock()
    body = b'{"metadata":{"arbitrary":[1,2]}, "info":{"futureField":{"x":true}}}'

    def handler(request):
        assert request.url.host == "americas.api.riotgames.com"
        assert request.headers["X-Riot-Token"] == "test-secret-never-persist"
        assert "api_key" not in request.url.params
        assert request.url.params["queue"] == "420"
        return httpx.Response(200, content=body)

    async def scenario():
        async with client(tmp_path, clock, handler) as api:
            result = await api.get("americas", "/lol/match/v5/matches/NA1_1", "match", "match-v5", {"queue": 420})
            assert result == body
            assert api.metrics["requests"] == 1
            assert api.requests_per_minute == 1

    run(scenario())
    assert "test-secret-never-persist" not in (tmp_path / "rate_state.json").read_text()


def test_platform_and_regional_hostname_share_app_quota(tmp_path):
    clock = Clock()
    limiter = RateLimiter(tmp_path, app_limits=((1, 10),), safety_margin=0, clock=clock)
    limiter.reserve(canonical_region("na1"), "league_entries", "league-v4")
    assert limiter.delay(canonical_region("americas"), "match", "match-v5") == 10
    assert limiter.delay(canonical_region("euw1"), "match", "match-v5") == 0


def test_observed_method_and_service_counts_are_respected(tmp_path):
    clock = Clock()
    limiter = RateLimiter(tmp_path, app_limits=((1000, 1),), safety_margin=0, clock=clock)
    limiter.reserve("americas", "match", "match-v5")
    limiter.observe("americas", "match", "match-v5", {
        "X-Method-Rate-Limit": "2:10,10:60", "X-Method-Rate-Limit-Count": "2:10,2:60",
        "X-Service-Rate-Limit": "3:20", "X-Service-Rate-Limit-Count": "3:20",
    }, sent_at=clock())
    assert limiter.delay("americas", "match", "match-v5") == 20
    assert limiter.delay("americas", "timeline", "match-v5") == 20
    assert limiter.delay("americas", "league", "league-v4") == pytest.approx(0.001, abs=1e-6)
    assert limiter.delay("europe", "match", "match-v5") == 0


def test_headers_can_lower_default_application_allowance(tmp_path):
    clock = Clock()
    limiter = RateLimiter(tmp_path, app_limits=((1000, 1),), safety_margin=0, clock=clock)
    limiter.reserve("americas", "match", "match-v5")
    limiter.observe("americas", "match", "match-v5", {
        "X-App-Rate-Limit": "1:1", "X-App-Rate-Limit-Count": "1:1",
    }, sent_at=clock())
    assert limiter.delay("americas", "different", "league-v4") == 1


def test_headers_cannot_raise_configured_application_cap(tmp_path):
    clock = Clock()
    limiter = RateLimiter(tmp_path, app_limits=((1, 10),), safety_margin=0, clock=clock)
    limiter.reserve("americas", "match", "match-v5")
    limiter.observe("americas", "match", "match-v5", {
        "X-App-Rate-Limit": "100:10", "X-App-Rate-Limit-Count": "1:10",
    }, sent_at=clock())
    assert limiter.delay("americas", "different", "league-v4") == 10


def test_more_restrictive_configuration_applies_on_restart(tmp_path):
    clock = Clock()
    limiter = RateLimiter(tmp_path, app_limits=((100, 10),), safety_margin=0, clock=clock)
    limiter.reserve("americas", "match", "match-v5")
    limiter.observe("americas", "match", "match-v5", {}, sent_at=clock())
    recovered = RateLimiter(tmp_path, app_limits=((1, 10),), safety_margin=0, clock=clock)
    assert recovered.delay("americas", "different", "league-v4") == 10


def test_unclean_startup_waits_full_largest_known_window(tmp_path):
    clock = Clock()
    limiter = RateLimiter(tmp_path, app_limits=((100, 10),), safety_margin=0, clock=clock)
    limiter.reserve("americas", "match", "match-v5")
    limiter.observe("americas", "match", "match-v5", {
        "X-Method-Rate-Limit": "100:60", "X-Method-Rate-Limit-Count": "1:60",
    }, sent_at=clock())
    clock.now += 5
    limiter.reserve("americas", "match", "match-v5")
    clock.now += 5
    recovered = RateLimiter(tmp_path, app_limits=((100, 10),), safety_margin=0, clock=clock)
    assert recovered.delay("americas", "other", "other-v1") == 60
    assert recovered.delay("europe", "other", "other-v1") == 0
    assert json.loads((tmp_path / "rate_state.json").read_text())["pending"] == {}


@pytest.mark.parametrize("limit_type,blocked_method,blocked_service,other_blocked", [
    ("application", "other", "other-v1", True),
    ("method", "match", "match-v5", False),
    ("service", "timeline", "match-v5", False),
    ("", "other", "other-v1", True),
])
def test_429_scope_and_cooldown_survive_restart(tmp_path, limit_type, blocked_method, blocked_service, other_blocked):
    clock = Clock()
    limiter = RateLimiter(tmp_path, app_limits=((1000, 1),), safety_margin=0, clock=clock)
    limiter.cool_down("americas", "match", "match-v5", {
        "Retry-After": "7.125", "X-Rate-Limit-Type": limit_type,
    })
    recovered = RateLimiter(tmp_path, app_limits=((1000, 1),), safety_margin=0, clock=clock)
    assert recovered.delay("americas", blocked_method, blocked_service) == 7.125
    assert bool(recovered.delay("americas", "league", "league-v4")) == other_blocked
    assert recovered.delay("europe", blocked_method, blocked_service) == 0


def test_retry_after_supports_fractional_and_http_date_without_rounding_down():
    assert parse_retry_after("2.125", 100) == 2.125
    assert parse_retry_after(formatdate(110, usegmt=True), 100.25) == 9.75
    assert parse_retry_after("invalid", 100) == 120
    assert parse_retry_after(None, 100) == 120
    assert parse_retry_after("-1", 100) == 120
    assert parse_retry_after("nan", 100) == 120


def test_429_retries_only_after_fractional_retry_after(tmp_path):
    clock = Clock()
    times = []

    def handler(request):
        times.append(clock())
        if len(times) == 1:
            return httpx.Response(429, headers={"Retry-After": "3.125", "X-Rate-Limit-Type": "application"})
        return httpx.Response(200, content=b"{}")

    async def scenario():
        async with client(tmp_path, clock, handler) as api:
            assert await api.get("na1", "/lol/league/v4/test", "test", "league-v4") == b"{}"
            assert api.metrics["rate_limited"] == 1
            assert api.metrics["retries"] == 1

    run(scenario())
    assert times[1] - times[0] >= 3.125
    assert clock.sleeps == [3.125]


def test_429_exhaustion_still_blocks_next_caller(tmp_path):
    clock = Clock()
    times = []

    def handler(request):
        times.append(clock())
        if len(times) == 1:
            return httpx.Response(429, headers={"Retry-After": "4.5"})
        return httpx.Response(200, content=b"{}")

    async def scenario():
        async with client(tmp_path, clock, handler, max_retries=0) as api:
            with pytest.raises(RetryableAPIError) as exc:
                await api.get("na1", "/lol/league/v4/test", "league", "league-v4")
            assert exc.value.retry_after == 4.5
            await api.get("americas", "/lol/match/v5/test", "match", "match-v5")

    run(scenario())
    assert times[1] - times[0] >= 4.5


def test_crash_between_reservation_and_response_does_not_reset_quota(tmp_path):
    clock = Clock()

    def handler(request):
        raise asyncio.CancelledError

    async def scenario():
        async with RiotClient("test-key", tmp_path, transport=httpx.MockTransport(handler),
                              app_limits=((1, 20),), safety_margin=0, clock=clock, sleep=clock.sleep) as api:
            with pytest.raises(asyncio.CancelledError):
                await api.get("na1", "/lol/league/v4/test", "league", "league-v4")

    run(scenario())
    limiter = RateLimiter(tmp_path, app_limits=((1, 20),), safety_margin=0, clock=clock)
    assert limiter.delay("americas", "match", "match-v5") == 20
    assert json.loads((tmp_path / "rate_state.json").read_text())["version"] == 1


def test_corrupt_state_stops_without_overwriting_history(tmp_path):
    path = tmp_path / "rate_state.json"
    path.write_text("corrupt", encoding="utf-8")
    with pytest.raises(RateStateError):
        RateLimiter(tmp_path)
    assert path.read_text() == "corrupt"


def test_network_and_server_errors_retry_with_bounded_backoff(tmp_path):
    clock = Clock()
    attempts = []

    def handler(request):
        attempts.append(clock())
        if len(attempts) == 1:
            raise httpx.ConnectError("sensitive request must not escape", request=request)
        return httpx.Response(503 if len(attempts) == 2 else 200, content=b"{}")

    async def scenario():
        async with client(tmp_path, clock, handler, backoff_base=2, backoff_max=2) as api:
            assert await api.get("americas", "/lol/match/v5/test", "match", "match-v5") == b"{}"
            assert api.metrics["retries"] == 2
            assert api.metrics["network_errors"] == 1

    run(scenario())
    assert clock.sleeps == [1, 1]
    assert len(attempts) == 3


@pytest.mark.parametrize("status,exception", [(401, AuthError), (403, AuthError), (404, NotFound), (400, PermanentAPIError)])
def test_permanent_errors_are_sanitized_and_not_retried(tmp_path, status, exception):
    clock = Clock()
    calls = []

    def handler(request):
        calls.append(request.url.host)
        return httpx.Response(status, text="secret response must not escape")

    async def scenario():
        async with client(tmp_path, clock, handler) as api:
            with pytest.raises(exception) as exc:
                await api.get("americas", "/lol/match/v5/test", "match", "match-v5")
            assert "secret" not in str(exc.value)
            if status in (401, 403):
                with pytest.raises(AuthError):
                    await api.get("europe", "/lol/match/v5/test", "match", "match-v5")
            assert api.metrics["retries"] == 0

    run(scenario())
    assert len(calls) == 1


def test_network_exhaustion_is_sanitized(tmp_path):
    clock = Clock()

    def handler(request):
        raise httpx.ConnectError("API key secret", request=request)

    async def scenario():
        async with client(tmp_path, clock, handler, max_retries=0) as api:
            with pytest.raises(RetryableAPIError) as exc:
                await api.get("americas", "/lol/match/v5/test", "match", "match-v5")
            assert "secret" not in str(exc.value)
            assert exc.value.status is None

    run(scenario())


def test_three_routing_regions_can_request_concurrently(tmp_path):
    clock = Clock()

    async def scenario():
        all_started = asyncio.Event()
        hosts = set()

        async def handler(request):
            hosts.add(request.url.host)
            if len(hosts) == 3:
                all_started.set()
            await all_started.wait()
            return httpx.Response(200, content=b"{}")

        async with client(tmp_path, clock, handler) as api:
            results = await asyncio.wait_for(asyncio.gather(*(
                api.get(route, "/lol/match/v5/test", "match", "match-v5")
                for route in ("americas", "europe", "asia")
            )), timeout=2)
            assert results == [b"{}"] * 3
            assert len(hosts) == 3

    run(scenario())


def test_failed_quota_persistence_prevents_outbound_request(tmp_path, monkeypatch):
    clock = Clock()
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(200, content=b"{}")

    def failed_replace(*args):
        raise OSError("simulated power-loss boundary")

    async def scenario():
        async with client(tmp_path, clock, handler) as api:
            api.limiter.save()
            original = (tmp_path / "rate_state.json").read_bytes()
            monkeypatch.setattr("riot_collector.api._replace_state", failed_replace)
            with pytest.raises(RateStateError):
                await api.get("americas", "/lol/match/v5/test", "match", "match-v5")
            assert (tmp_path / "rate_state.json").read_bytes() == original

    run(scenario())
    assert calls == []


def test_transient_windows_publication_lock_is_retried_before_dispatch(tmp_path, monkeypatch):
    from riot_collector.api import _replace_state

    clock = Clock()
    replacements = []
    waits = []
    actual_replace = _replace_state

    def briefly_locked(source, target):
        replacements.append((source, target))
        if len(replacements) <= 2:
            error = PermissionError(13, "sensitive filesystem details must not escape")
            error.winerror = 32
            raise error
        return actual_replace(source, target)

    def handler(request):
        assert len(replacements) >= 3
        assert (tmp_path / "rate_state.json").exists()
        return httpx.Response(200, content=b"{}")

    async def scenario():
        async with client(tmp_path, clock, handler) as api:
            monkeypatch.setattr("riot_collector.api._replace_state", briefly_locked)
            monkeypatch.setattr("riot_collector.api.time.sleep", waits.append)
            assert await api.get("americas", "/lol/match/v5/test", "match", "match-v5") == b"{}"

    run(scenario())
    assert waits == [0.01, 0.02]
    assert not list(tmp_path.glob("rate_state.*.json.tmp"))


def test_persistent_windows_lock_stops_with_sanitized_diagnostic(tmp_path, monkeypatch):
    clock = Clock()
    calls = []
    waits = []
    replacements = []

    def locked(source, target):
        replacements.append((source, target))
        error = PermissionError(13, "sensitive filesystem details must not escape")
        error.winerror = 33
        raise error

    async def scenario():
        async with client(tmp_path, clock, lambda request: calls.append(request)) as api:
            monkeypatch.setattr("riot_collector.api._replace_state", locked)
            monkeypatch.setattr("riot_collector.api.time.sleep", waits.append)
            with pytest.raises(RateStateError) as exc:
                await api.get("americas", "/lol/match/v5/test", "match", "match-v5")
            assert "winerror=33" in str(exc.value)
            assert "errno=13" in str(exc.value)
            assert "sensitive" not in str(exc.value)

    run(scenario())
    assert len(replacements) == 8
    assert len(waits) == 7
    assert calls == []


def test_windows_state_publication_replaces_with_write_through(tmp_path, monkeypatch):
    import ctypes
    from types import SimpleNamespace
    from riot_collector.api import _replace_state

    calls = []

    class Move:
        def __call__(self, source, target, flags):
            calls.append((source, target, flags))
            return 1

    monkeypatch.setattr("riot_collector.api._WINDOWS", True)
    monkeypatch.setattr(ctypes, "WinDLL", lambda *args, **kwargs: SimpleNamespace(MoveFileExW=Move()), raising=False)
    _replace_state(tmp_path / "staged", tmp_path / "rate_state.json")
    assert calls[0][2] == 0x1 | 0x8


def test_posix_state_publication_fsyncs_parent_after_replace(tmp_path, monkeypatch):
    from riot_collector.api import _replace_state

    calls = []
    monkeypatch.setattr("riot_collector.api._WINDOWS", False)
    monkeypatch.setattr("riot_collector.api.os.replace", lambda *args: calls.append("replace"))
    monkeypatch.setattr("riot_collector.api.os.open", lambda *args: calls.append("open_directory") or 123)
    monkeypatch.setattr("riot_collector.api.os.fsync", lambda descriptor: calls.append(("fsync", descriptor)))
    monkeypatch.setattr("riot_collector.api.os.close", lambda descriptor: calls.append(("close", descriptor)))
    _replace_state(tmp_path / "staged", tmp_path / "rate_state.json")
    assert calls == ["replace", "open_directory", ("fsync", 123), ("close", 123)]
