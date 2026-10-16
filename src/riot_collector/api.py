"""Official Riot HTTP client with durable, conservative regional quotas.

Application quotas are deliberately shared between a platform hostname and its
Match-V5 routing region. Riot documents regional quotas, not an independent
allowance for every hostname. Local rolling windows and smooth pacing are more
conservative than the server's buckets. They also survive process restarts.
"""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from email.utils import parsedate_to_datetime
import json
import math
import os
from pathlib import Path
import random
import tempfile
import time
from typing import Any

import httpx


REGIONS = {
    "na1": "americas", "americas": "americas",
    "euw1": "europe", "europe": "europe",
    "kr": "asia", "asia": "asia",
}
_WINDOWS = os.name == "nt"


def _replace_state(temporary: Path, destination: Path) -> None:
    """Publish an already fsynced snapshot and flush the name change as well."""
    if _WINDOWS:
        import ctypes
        from ctypes import wintypes

        move = ctypes.WinDLL("kernel32", use_last_error=True).MoveFileExW
        move.argtypes = (wintypes.LPCWSTR, wintypes.LPCWSTR, wintypes.DWORD)
        move.restype = wintypes.BOOL
        # MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH, same directory
        # and volume. Both the contents and their publication are durable before
        # any request is dispatched using the newly reserved quota slots.
        if not move(str(temporary.resolve()), str(destination.resolve()), 0x1 | 0x8):
            raise ctypes.WinError(ctypes.get_last_error())
    else:
        os.replace(temporary, destination)
        descriptor = os.open(destination.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


class APIError(Exception):
    """An intentionally sanitized API error, safe for persistent logs."""


class AuthError(APIError):
    """The key is rejected; all collection should stop until it is replaced."""


class NotFound(APIError):
    status = 404


class PermanentAPIError(APIError):
    def __init__(self, status: int):
        self.status = status
        super().__init__(f"Riot API returned HTTP {status}; request requires inspection")


class RetryableAPIError(APIError):
    def __init__(self, status: int | None = None, *, retry_after: float = 0.0):
        self.status = status
        self.retry_after = retry_after
        reason = f"HTTP {status}" if status is not None else "network failure"
        super().__init__(f"Riot API temporary {reason}; progress is retained")


class RateStateError(APIError):
    """Unknown quota history must not be silently reset."""


class RequestStopped(APIError):
    """A graceful stop prevented an attempt that had not been dispatched."""


def canonical_region(route: str) -> str:
    try:
        return REGIONS[route.lower()]
    except KeyError:
        raise ValueError("Unsupported Riot route; expected NA1, EUW1, KR, AMERICAS, EUROPE, or ASIA") from None


def parse_retry_after(value: str | None, now: float, *, fallback: float = 120.0) -> float:
    """Return seconds without rounding down fractional or HTTP-date delays."""
    if value:
        try:
            seconds = float(value)
            if math.isfinite(seconds) and seconds >= 0:
                return seconds
        except ValueError:
            pass
        try:
            return max(0.0, parsedate_to_datetime(value).timestamp() - now)
        except (ValueError, TypeError, OverflowError):
            pass
    return fallback


def _pairs(value: str | None) -> dict[float, int]:
    result = {}
    for pair in (value or "").split(","):
        try:
            count, window = pair.strip().split(":")
            count, window = int(count), float(window)
            if count >= 0 and math.isfinite(window) and window > 0:
                result[window] = count
        except (ValueError, TypeError):
            continue
    return result


@dataclass
class _Bucket:
    limit: int
    window: float
    events: deque[float] = field(default_factory=deque)

    def prune(self, now: float) -> None:
        while self.events and self.events[0] + self.window <= now:
            self.events.popleft()


class RateLimiter:
    """Durable reservations; caller serializes access through regional locks.

    Every outbound attempt is reserved and fsynced before the network call. A
    crash can waste a slot but cannot erase one. Counts reported by the server
    that exceed our local rolling count become conservative extra reservations.
    No key, request path, or response payload is persisted here.
    """

    def __init__(
        self, state_dir: Path, *, app_limits: tuple[tuple[int, float], ...] = ((20, 1), (100, 120)),
        safety_margin: float = 0.1, clock: Callable[[], float] = time.time,
    ):
        if not 0 <= safety_margin < 1:
            raise ValueError("Rate safety margin must be at least zero and less than one")
        if not app_limits or any(limit < 1 or not math.isfinite(window) or window <= 0 for limit, window in app_limits):
            raise ValueError("Application limits must contain positive counts and windows")
        self.path = Path(state_dir) / "rate_state.json"
        self.clock = clock
        self.safety_margin = safety_margin
        self.app_limits = tuple((int(n), float(w)) for n, w in app_limits)
        self.buckets: dict[str, dict[float, _Bucket]] = {}
        self.next_at: dict[str, float] = {}
        self.cooldowns: dict[str, float] = {}
        self.pending: dict[str, float] = {}
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
            if data["version"] != 1:
                raise ValueError("unsupported version")
            self.next_at = {key: float(value) for key, value in data["next_at"].items()}
            self.cooldowns = {key: float(value) for key, value in data["cooldowns"].items()}
            for scope, records in data["buckets"].items():
                self.buckets[scope] = {}
                for record in records:
                    limit, window = int(record["limit"]), float(record["window"])
                    events = [float(value) for value in record["events"]]
                    if limit < 1 or window <= 0 or not all(math.isfinite(v) for v in [window, *events]):
                        raise ValueError("invalid quota")
                    self.buckets[scope][window] = _Bucket(limit, window, deque(sorted(events)))
            if not all(math.isfinite(v) for v in [*self.next_at.values(), *self.cooldowns.values()]):
                raise ValueError("invalid deadline")
            pending = data.get("pending", {})
            if not isinstance(pending, dict) or any(region not in set(REGIONS.values()) for region in pending):
                raise ValueError("invalid pending reservation")
        except (OSError, ValueError, KeyError, TypeError, OverflowError):
            raise RateStateError("Cannot read durable rate_state.json; collection stopped without resetting quota history") from None
        if pending:
            # A killed request may have consumed an as-yet unknown method slot.
            # Wait the largest known regional window after an unclean restart,
            # in addition to the reservations already recorded before sending.
            now = self.clock()
            for region in pending:
                window = max([w for _, w in self.app_limits] + [
                    b.window for scope, buckets in self.buckets.items()
                    if scope.split(":")[1] == region for b in buckets.values()
                ])
                scope = f"region:{region}"
                self.cooldowns[scope] = max(self.cooldowns.get(scope, 0), now + window)
            self.save()

    def save(self) -> None:
        now = self.clock()
        for buckets in self.buckets.values():
            for bucket in buckets.values():
                bucket.prune(now)
        data = {
            "version": 1,
            "buckets": {scope: [{"limit": b.limit, "window": b.window, "events": list(b.events)}
                                 for b in buckets.values()] for scope, buckets in self.buckets.items()},
            "next_at": {k: v for k, v in self.next_at.items() if v > now},
            "cooldowns": {k: v for k, v in self.cooldowns.items() if v > now},
            "pending": self.pending,
        }
        temporary = None
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            descriptor, temporary_name = tempfile.mkstemp(
                prefix="rate_state.", suffix=".json.tmp", dir=self.path.parent,
            )
            temporary = Path(temporary_name)
            with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
                json.dump(data, handle, separators=(",", ":"), allow_nan=False)
                handle.flush()
                os.fsync(handle.fileno())
            # Windows scanners and synchronization clients sometimes hold the
            # destination briefly without FILE_SHARE_DELETE. Retry only those
            # local sharing/access violations, with a strict bounded delay.
            # No HTTP dispatch can proceed until publication succeeds.
            for attempt in range(8):
                try:
                    _replace_state(temporary, self.path)
                    break
                except OSError as exc:
                    if getattr(exc, "winerror", None) not in (5, 32, 33) or attempt == 7:
                        raise
                    time.sleep(min(0.5, 0.01 * (2 ** attempt)))
        except OSError as exc:
            detail = f"{type(exc).__name__}, winerror={getattr(exc, 'winerror', None)}, errno={exc.errno}"
            raise RateStateError(
                f"Cannot persist quota reservations ({detail}); collection stopped before further requests"
            ) from None
        finally:
            if temporary is not None:
                try:
                    temporary.unlink(missing_ok=True)
                except OSError:
                    # An unpublishable staging file is never treated as state;
                    # leave it for inspection if another process holds it.
                    pass

    @staticmethod
    def scopes(region: str, method: str, service: str) -> tuple[str, str, str, str]:
        return f"region:{region}", f"app:{region}", f"method:{region}:{method}", f"service:{region}:{service}"

    def _ensure_app(self, region: str) -> None:
        buckets = self.buckets.setdefault(f"app:{region}", {})
        for limit, window in self.app_limits:
            if window not in buckets:
                buckets[window] = _Bucket(limit, window)
            else:
                # A more restrictive configuration must apply after a restart,
                # even when the server previously advertised a higher limit.
                buckets[window].limit = min(buckets[window].limit, limit)

    def _capacity(self, bucket: _Bucket) -> int:
        return max(1, math.floor(bucket.limit * (1.0 - self.safety_margin)))

    def delay(self, region: str, method: str, service: str) -> float:
        self._ensure_app(region)
        now = self.clock()
        deadline = now
        for scope in self.scopes(region, method, service):
            deadline = max(deadline, self.cooldowns.get(scope, 0), self.next_at.get(scope, 0))
            for bucket in self.buckets.get(scope, {}).values():
                bucket.prune(now)
                capacity = self._capacity(bucket)
                if len(bucket.events) >= capacity:
                    # Headers can report more requests than this process made.
                    deadline = max(deadline, bucket.events[-capacity] + bucket.window)
        return max(0.0, deadline - now)

    def reserve(self, region: str, method: str, service: str) -> None:
        self._ensure_app(region)
        now = self.clock()
        for scope in self.scopes(region, method, service)[1:]:
            buckets = self.buckets.get(scope, {})
            for bucket in buckets.values():
                bucket.prune(now)
                bucket.events.append(now)
            if buckets:
                spacing = max(b.window / self._capacity(b) for b in buckets.values())
                self.next_at[scope] = max(self.next_at.get(scope, 0), now + spacing)
        self.pending[region] = now
        self.save()

    def observe(self, region: str, method: str, service: str, headers: Mapping[str, str], *, sent_at: float) -> None:
        now = self.clock()
        for kind, scope in zip(("App", "Method", "Service"), self.scopes(region, method, service)[1:], strict=True):
            limits = _pairs(headers.get(f"X-{kind}-Rate-Limit"))
            counts = _pairs(headers.get(f"X-{kind}-Rate-Limit-Count"))
            buckets = self.buckets.setdefault(scope, {})
            for window, limit in limits.items():
                if limit < 1:
                    continue
                if kind == "App":
                    configured = dict((w, n) for n, w in self.app_limits)
                    limit = min(limit, configured.get(window, limit))
                bucket = buckets.get(window)
                if bucket is None:
                    # Current attempt happened before this limit was known.
                    bucket = buckets[window] = _Bucket(limit, window, deque([sent_at]))
                else:
                    bucket.limit = limit
            for window, bucket in buckets.items():
                bucket.prune(now)
                extra = max(0, counts.get(window, 0) - len(bucket.events))
                bucket.events.extend([now] * extra)
            if buckets:
                spacing = max(b.window / self._capacity(b) for b in buckets.values())
                self.next_at[scope] = max(self.next_at.get(scope, 0), sent_at + spacing)
        self.pending.pop(region, None)
        self.save()

    def cool_down(self, region: str, method: str, service: str, headers: Mapping[str, str]) -> float:
        now = self.clock()
        fallback = max(window for _, window in self.app_limits)
        seconds = parse_retry_after(headers.get("Retry-After"), now, fallback=fallback)
        region_scope, app_scope, method_scope, service_scope = self.scopes(region, method, service)
        scope = {"application": app_scope, "app": app_scope, "method": method_scope, "service": service_scope}.get(
            headers.get("X-Rate-Limit-Type", "").strip().lower(), region_scope)
        self.cooldowns[scope] = max(self.cooldowns.get(scope, 0), now + seconds)
        self.save()
        return seconds


class RiotClient:
    def __init__(
        self, api_key: str, state_dir: Path, *, timeout: float = 30.0, max_retries: int = 5,
        app_limits: tuple[tuple[int, float], ...] = ((20, 1), (100, 120)), safety_margin: float = 0.1,
        transport: httpx.AsyncBaseTransport | None = None, clock: Callable[[], float] = time.time,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep, jitter: Callable[[], float] = random.random,
        backoff_base: float = 1.0, backoff_max: float = 60.0,
    ):
        if not api_key or api_key.strip() != api_key or "\n" in api_key or "\r" in api_key:
            raise AuthError("RIOT_API_KEY is missing or malformed")
        if max_retries < 0:
            raise ValueError("max_retries must be nonnegative")
        self._http = httpx.AsyncClient(
            headers={"X-Riot-Token": api_key, "User-Agent": "riot-research-collector/0.1"},
            timeout=timeout, transport=transport, follow_redirects=False, trust_env=False,
        )
        self.limiter = RateLimiter(Path(state_dir), app_limits=app_limits, safety_margin=safety_margin, clock=clock)
        self._clock, self._sleep, self._jitter = clock, sleep, jitter
        self.max_retries, self.backoff_base, self.backoff_max = max_retries, backoff_base, backoff_max
        self._locks = {region: asyncio.Lock() for region in set(REGIONS.values())}
        self._auth_failed = False
        self._stop_event: asyncio.Event | None = None
        self.metrics: dict[str, int] = {"requests": 0, "retries": 0, "rate_limited": 0, "network_errors": 0}
        self._recent: deque[float] = deque()
        self.request_observer = None

    async def __aenter__(self) -> RiotClient:
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._http.aclose()

    def set_stop_event(self, stop: asyncio.Event) -> None:
        self._stop_event = stop

    def _check_stopping(self) -> None:
        if self._stop_event is not None and self._stop_event.is_set():
            raise RequestStopped("Graceful stop: no additional Riot request dispatched")

    async def _wait_before_attempt(self, seconds: float) -> None:
        """Interrupt quota/backoff waits, without cancelling an HTTP request."""
        self._check_stopping()
        if self._stop_event is None:
            await self._sleep(seconds)
            return
        sleeping = asyncio.create_task(self._sleep(seconds))
        stopping = asyncio.create_task(self._stop_event.wait())
        try:
            await asyncio.wait((sleeping, stopping), return_when=asyncio.FIRST_COMPLETED)
            self._check_stopping()
            await sleeping
        finally:
            for task in (sleeping, stopping):
                if not task.done():
                    task.cancel()
            await asyncio.gather(sleeping, stopping, return_exceptions=True)

    @property
    def requests_per_minute(self) -> float:
        cutoff = self._clock() - 60
        while self._recent and self._recent[0] <= cutoff:
            self._recent.popleft()
        return float(len(self._recent))

    async def get(self, route: str, path: str, method: str, service: str, params: dict | None = None) -> bytes:
        region = canonical_region(route)
        if not path.startswith("/") or path.startswith("//") or "?" in path or "#" in path:
            raise ValueError("Riot endpoint must be an absolute API path without a query or fragment")
        if not method or not service:
            raise ValueError("Stable method and service identifiers are required for rate accounting")
        url = f"https://{route.lower()}.api.riotgames.com{path}"
        for attempt in range(self.max_retries + 1):
            response = None
            rate_delay = 0.0
            while True:
                self._check_stopping()
                if self._auth_failed:
                    raise AuthError("Riot API key was rejected; replace RIOT_API_KEY and resume")
                async with self._locks[region]:
                    self._check_stopping()
                    if self._auth_failed:
                        raise AuthError("Riot API key was rejected; replace RIOT_API_KEY and resume")
                    delay = self.limiter.delay(region, method, service)
                    if delay <= 0:
                        sent_at = self._clock()
                        self.limiter.reserve(region, method, service)
                        self.metrics["requests"] += 1
                        self._recent.append(sent_at)
                        if self.request_observer is not None:
                            self.request_observer(route, method, sent_at)
                        try:
                            response = await self._http.get(url, params=params)
                        except httpx.RequestError:
                            self.metrics["network_errors"] += 1
                        if response is not None:
                            if response.status_code == 429:
                                self.metrics["rate_limited"] += 1
                                rate_delay = self.limiter.cool_down(region, method, service, response.headers)
                            # Persist Retry-After before clearing the in-flight
                            # marker, so a crash cannot leave a clean reservation
                            # with its received cooldown discarded.
                            self.limiter.observe(region, method, service, response.headers, sent_at=sent_at)
                            if response.status_code in (401, 403):
                                self._auth_failed = True
                        break
                # Sleep outside the regional lock so a method cooldown does not
                # unnecessarily hold up a different endpoint in that region.
                await self._wait_before_attempt(delay)
            status = response.status_code if response is not None else None
            if status is not None and 200 <= status < 300:
                return response.content
            if status in (401, 403):
                raise AuthError("Riot API key was rejected (HTTP 401/403); replace RIOT_API_KEY and resume")
            if status == 404:
                raise NotFound("Riot API resource was not found")
            transient = status is None or status in (408, 429) or 500 <= status < 600
            if not transient:
                raise PermanentAPIError(status)
            # Received responses (including 429 headers) have been persisted.
            # Stop before any retry; an unanswered network attempt deliberately
            # retains its pending marker and crash-recovery protection.
            self._check_stopping()
            if attempt >= self.max_retries:
                # A 429 cooldown was persisted even if this caller exhausts its
                # retry budget. Every subsequent caller must still honor it.
                raise RetryableAPIError(status, retry_after=rate_delay)
            self.metrics["retries"] += 1
            if status == 429:
                # The limiter applies exactly Retry-After, including fractions;
                # normal quota windows may conservatively require longer.
                continue
            bound = min(self.backoff_max, self.backoff_base * (2 ** attempt))
            await self._wait_before_attempt(bound * (0.5 + 0.5 * self._jitter()))
        raise AssertionError("unreachable")
