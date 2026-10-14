"""Resumable discovery and collection using bounded, region-fair workers.

The database owns all cursors. A response is allowed to advance a history cursor
only in the same transaction that queues its match IDs. Raw data is committed
before any database completion, allowing recovery of a crash between the two.
"""
from __future__ import annotations

import asyncio
from collections import Counter
import hashlib
import json
import logging
import sys
import time
from typing import Any
from urllib.parse import quote

from .api import AuthError, NotFound, PermanentAPIError, RetryableAPIError
from .config import ROUTING
from .storage import LowDiskError, StorageError

log = logging.getLogger(__name__)
QUEUE = "RANKED_SOLO_5x5"
PRIORITIES = {"CHALLENGER": 100, "GRANDMASTER": 90, "MASTER": 80,
              "DIAMOND": 60, "EMERALD": 40}


class CollectorSafetyError(RuntimeError):
    """A changed API contract requires inspection before issuing more requests."""


class Collector:
    def __init__(self, config, state, storage, client):
        self.config = config
        self.state = state
        self.storage = storage
        self.client = client
        self.downloaded: Counter[str] = Counter()
        self._turns: Counter[str] = Counter()
        self._started = 0.0
        self._reason = "exhausted"
        self._fatal: BaseException | None = None
        self._smoke: int | None = None
        self._mode = "collect"
        self._last_seed_bucket = None

    async def run(self, stop: asyncio.Event, *, mode: str = "collect",
                  max_seconds: float | None = None,
                  smoke_per_region: int | None = None) -> dict:
        """Run until interrupted, exhausted, time limited, or a safety stop.

        The milestone is informational. It never controls the stopping test.
        Only one process may own this database; the CLI enforces that lock.
        """
        if mode == "timelines":
            mode = "backfill-timelines"
        if mode not in {"collect", "backfill-timelines"}:
            raise ValueError("mode must be collect or backfill-timelines")
        self._mode, self._smoke = mode, smoke_per_region
        self._started = time.monotonic()
        self._fatal, self._reason = None, "exhausted"
        self.downloaded.clear()
        self.state.recover()
        if mode == "collect":
            self._seed_jobs()
        else:
            self._timeline_jobs()
        self._progress()
        workers = [asyncio.create_task(self._worker(p, stop), name=f"collector-{p}")
                   for p in self.config.platforms]
        monitor = asyncio.create_task(self._monitor(stop, max_seconds))
        waiter = asyncio.create_task(stop.wait())
        group = asyncio.gather(*workers)
        try:
            await asyncio.wait((group, waiter, monitor), return_when=asyncio.FIRST_COMPLETED)
            if monitor.done():
                await monitor
            if group.done():
                await group
                if smoke_per_region is not None and all(
                        self.downloaded[p] >= smoke_per_region for p in self.config.platforms):
                    self._reason = "smoke-complete"
            elif self._reason == "exhausted":
                self._reason = "interrupted"
        except asyncio.CancelledError:
            self._reason = "interrupted"
            stop.set()
            raise
        except Exception:
            self._reason = "safety-stop"
            stop.set()
            raise
        finally:
            primary_error = self._fatal or sys.exc_info()[1]
            stop.set()
            monitor.cancel()
            waiter.cancel()
            for worker in workers:
                if not worker.done():
                    worker.cancel()
            await asyncio.gather(*workers, monitor, waiter, group, return_exceptions=True)
            try:
                self.state.recover()
                self._progress()
            except Exception:
                if primary_error is None:
                    raise
                log.exception("Final recovery/status failed; retaining the original safety-stop error")
        if self._fatal is not None:
            raise self._fatal
        return {"reason": self._reason, "downloaded": dict(self.downloaded),
                "elapsed_seconds": time.monotonic() - self._started, "mode": mode}

    def _seed_jobs(self) -> None:
        bucket = int(time.time() // (self.config.seed_refresh_hours * 3600))
        if bucket == self._last_seed_bucket:
            return
        self._last_seed_bucket = bucket
        with self.state.transaction():
            for platform in self.config.platforms:
                for tier in ("CHALLENGER", "GRANDMASTER", "MASTER"):
                    self.state.enqueue("seed", f"seed:{platform}:{tier}:{bucket}", platform,
                                       {"tier": tier, "bucket": bucket})
                for division in ("I", "II", "III", "IV"):
                    for tier in ("DIAMOND", "EMERALD"):
                        self._enqueue_seed_page(platform, tier, division, bucket, 1)

    def _enqueue_seed_page(self, platform, tier, division, bucket, page, fingerprint=None):
        self.state.enqueue("seed", f"seed:{platform}:{tier}:{division}:{bucket}:{page}",
                           platform, {"tier": tier, "division": division,
                                      "bucket": bucket, "page": page,
                                      "previous_fingerprint": fingerprint})

    def _timeline_jobs(self) -> None:
        marks = ",".join("?" for _ in self.config.platforms)
        rows = self.state.conn.execute(
            f"SELECT match_id,platform FROM matches WHERE platform IN ({marks}) "
            "AND match_id NOT IN (SELECT match_id FROM timelines)",
            self.config.platforms)
        while batch := rows.fetchmany(1000):
            with self.state.transaction():
                for row in batch:
                    self.state.enqueue("timeline", row["match_id"], row["platform"],
                                       {"match_id": row["match_id"]})

    async def _worker(self, platform: str, stop: asyncio.Event) -> None:
        while not stop.is_set():
            if self._smoke is not None and self.downloaded[platform] >= self._smoke:
                return
            job = self._next_job(platform)
            if job is None:
                kinds = (("timeline",) if self._mode == "backfill-timelines" else
                         (("match",) if self._backlog(platform) >= self.config.discovery_backlog else
                          ("match", "history", "seed")))
                marks = ",".join("?" for _ in kinds)
                due = self.state.conn.execute(
                    f"SELECT MIN(not_before) FROM jobs WHERE platform=? AND status='pending' AND kind IN ({marks})",
                    (platform, *kinds)).fetchone()[0]
                if due is None:
                    return
                await self._pause(stop, min(5.0, max(0.05, due - time.time())))
                continue
            try:
                self.storage.check_disk()
                await self._process(job, stop)
            except asyncio.CancelledError:
                self.state.retry(job["id"], "Interrupted; safe to resume", delay=0)
                raise
            except NotFound:
                self.state.finish(job["id"], status="skipped", error="Unavailable: Riot HTTP 404")
            except RetryableAPIError as exc:
                self.state.retry(job["id"], str(exc), delay=max(
                    self.config.job_retry_seconds, getattr(exc, "retry_after", 0)))
                log.warning("Temporary request failure platform=%s kind=%s; queued for retry",
                            platform, job["kind"])
            except (AuthError, PermanentAPIError, LowDiskError, StorageError, CollectorSafetyError) as exc:
                self.state.retry(job["id"], type(exc).__name__, delay=0)
                self._fatal, self._reason = exc, "safety-stop"
                stop.set()
            except (OSError, ValueError, KeyError, TypeError) as exc:
                # Unexpected response/data errors are durable, inspectable and
                # rate bounded. Never silently advance a cursor after failure.
                self.state.retry(job["id"], f"{type(exc).__name__}: invalid response or local I/O",
                                 delay=self.config.job_retry_seconds)
                log.exception("Job retained after response or I/O failure platform=%s kind=%s",
                              platform, job["kind"])
        if self._reason == "exhausted":
            self._reason = "interrupted"

    def _next_job(self, platform: str) -> dict | None:
        if self._mode == "backfill-timelines":
            return self.state.claim("timeline", platform)
        self._turns[platform] += 1
        backlog = self._backlog(platform)
        if backlog >= self.config.discovery_backlog:
            return self._claim_match(platform)
        order = ("match", "history", "match", "seed", "match", "history", "history", "seed")
        first = self._turns[platform] % len(order)
        for kind in dict.fromkeys(order[first:] + order[:first]):
            if kind == "history" and self._turns[platform] % 5 == 0:
                job = self._claim_history(platform, unknown=True)
                if job:
                    return job
            job = (self._claim_history(platform) if kind == "history" else
                   self._claim_match(platform) if kind == "match" else self.state.claim(kind, platform))
            if job:
                return job
        return None

    def _backlog(self, platform: str) -> int:
        return self.state.conn.execute(
            "SELECT COUNT(*) FROM jobs WHERE kind='match' AND status='pending' AND platform=?",
            (platform,)).fetchone()[0]

    def _claim_match(self, platform: str) -> dict | None:
        if self._turns[platform] % 5:
            return self.state.claim("match", platform)
        # Periodically drain the oldest match regardless of discovery rank.
        # A continuing high-priority stream cannot strand unknown-derived IDs.
        with self.state.transaction():
            row = self.state.conn.execute(
                "SELECT * FROM jobs WHERE kind='match' AND platform=? AND status='pending' "
                "AND not_before<=? ORDER BY id LIMIT 1", (platform, time.time())).fetchone()
            if row is None:
                return None
            self.state.conn.execute(
                "UPDATE jobs SET status='in_progress',attempts=attempts+1,updated_at=? WHERE id=?",
                (time.time(), row["id"]))
            job = dict(row)
            job["payload"] = json.loads(job["payload"])
            job["attempts"] += 1
            return job

    def _claim_history(self, platform: str, unknown: bool = False) -> dict | None:
        # Persistent priority alone can starve unknown participants forever.
        with self.state.transaction():
            row = self.state.conn.execute(
                "SELECT * FROM jobs WHERE kind='history' AND platform=? AND status='pending' "
                "AND not_before<=? " + ("AND priority=0 " if unknown else "") +
                "ORDER BY priority DESC,updated_at,id LIMIT 1",
                (platform, time.time())).fetchone()
            if row is None:
                return None
            self.state.conn.execute(
                "UPDATE jobs SET status='in_progress',attempts=attempts+1,updated_at=? WHERE id=?",
                (time.time(), row["id"]))
            job = dict(row)
            job["payload"] = json.loads(job["payload"])
            job["attempts"] += 1
            return job

    async def _process(self, job: dict, stop: asyncio.Event) -> None:
        kind = job["kind"]
        if kind == "seed":
            await self._seed(job)
        elif kind == "history":
            await self._history(job, stop)
        elif kind == "match":
            await self._match(job)
        elif kind == "timeline":
            await self._timeline(job)
        else:
            raise ValueError("Unknown durable job kind")

    async def _seed(self, job: dict) -> None:
        payload, platform = job["payload"], job["platform"]
        tier = payload["tier"]
        if tier in {"CHALLENGER", "GRANDMASTER", "MASTER"}:
            path = f"/lol/league/v4/{tier.lower()}leagues/by-queue/{QUEUE}"
            body = json.loads(await self.client.get(platform, path, f"league_{tier.lower()}", "league-v4"))
            if body.get("queue", QUEUE) != QUEUE:
                raise ValueError("Unexpected league queue")
            entries = body["entries"]
        else:
            path = f"/lol/league/v4/entries/{QUEUE}/{tier}/{payload['division']}"
            entries = json.loads(await self.client.get(platform, path, "league_entries", "league-v4",
                                                       {"page": payload["page"]}))
        if not isinstance(entries, list):
            raise ValueError("League entries must be a list")
        observed_at = time.time()
        fingerprint = hashlib.sha256(json.dumps(entries, sort_keys=True).encode()).hexdigest()
        if entries and fingerprint == payload.get("previous_fingerprint"):
            self.state.finish(job["id"], status="failed", error="League API repeated the previous page")
            log.warning("Repeated ladder page retained as failed platform=%s tier=%s page=%s",
                        platform, tier, payload.get("page"))
            return
        with self.state.transaction():
            for entry in entries:
                if entry.get("queueType", QUEUE) != QUEUE:
                    continue
                rank = dict(entry, tier=tier)
                if payload.get("division"):
                    rank.setdefault("rank", payload["division"])
                puuid = entry.get("puuid")
                if puuid:
                    self._observe_player(platform, puuid, rank, observed_at)
                else:
                    raise CollectorSafetyError("League-V4 entry is missing its documented PUUID; "
                                               "seed work is retained for inspection")
            if payload.get("page") and entries:
                self._enqueue_seed_page(platform, tier, payload["division"], payload["bucket"],
                                        payload["page"] + 1, fingerprint)
            self.state.complete(job["id"])

    def _observe_player(self, platform: str, puuid: str, rank: dict,
                        observed_at: float | None = None) -> None:
        now = time.time()
        observed_at = now if observed_at is None else observed_at
        tier = rank["tier"]
        priority = PRIORITIES.get(tier, 0)
        self.state.conn.execute(
            "INSERT INTO players(puuid,platform,priority,source,seed_tier,discovered_at,rank_checked_at) "
            "VALUES (?,?,?,'league_seed',?,?,?) ON CONFLICT(puuid,platform) DO UPDATE SET "
            "priority=MAX(priority,excluded.priority),source='league_seed',seed_tier=excluded.seed_tier,"
            "rank_checked_at=excluded.rank_checked_at",
            (puuid, platform, priority, tier, now, observed_at))
        previous = self.state.conn.execute(
            "SELECT * FROM rank_observations WHERE puuid=? AND platform=? ORDER BY observed_at DESC LIMIT 1",
            (puuid, platform)).fetchone()
        observed = (tier, rank.get("rank"), rank.get("leaguePoints"), QUEUE)
        if (previous is None or tuple(previous[k] for k in ("tier", "division", "lp", "queue")) != observed
                or previous["observed_at"] < now - self.config.rank_cache_hours * 3600):
            self.state.conn.execute(
                "INSERT INTO rank_observations(puuid,platform,tier,division,lp,queue,observed_at,source,raw_json) "
                "VALUES (?,?,?,?,?,?,?,'league_seed',?)", (puuid, platform, *observed, observed_at, json.dumps(rank)))
        history_key = f"history:{platform}:{puuid}:0"
        history = {"puuid": puuid, "start": 0, "end_time": int(now),
                   "discovery_source": "league_seed", "seed_rank": tier}
        self.state.enqueue("history", history_key, platform, history, priority)
        current = self.state.conn.execute(
            "SELECT id,payload FROM jobs WHERE kind='history' AND key=? AND status='pending'",
            (history_key,)).fetchone()
        if current:
            upgraded = json.loads(current["payload"])
            upgraded.update(discovery_source="league_seed", seed_rank=tier)
            self.state.conn.execute("UPDATE jobs SET payload=? WHERE id=?",
                                    (json.dumps(upgraded), current["id"]))
        # A refreshed ladder revisits exhausted players so ongoing games can be
        # discovered. Never disturb a crawl that still has historical pages.
        self.state.conn.execute(
            "UPDATE jobs SET status='pending',payload=?,not_before=?,updated_at=? "
            "WHERE kind='history' AND key=? AND platform=? AND status='done' AND updated_at<?",
            (json.dumps(history), now, now, history_key, platform,
             now - self.config.seed_refresh_hours * 3600))

    async def _history(self, job: dict, stop: asyncio.Event) -> None:
        payload, platform = dict(job["payload"]), job["platform"]
        payload.setdefault("end_time", int(time.time()))
        puuid = quote(payload["puuid"], safe="")
        count = self.config.history_page_size
        # One page per turn diversifies players; continuation has no total cap.
        ids = json.loads(await self.client.get(
            ROUTING[platform], f"/lol/match/v5/matches/by-puuid/{puuid}/ids",
            "match_ids", "match-v5", {"queue": 420, "start": payload.get("start", 0),
                                       "count": count, "endTime": payload["end_time"]}))
        if not isinstance(ids, list) or any(not isinstance(value, str) for value in ids):
            raise ValueError("History response must contain match IDs")
        with self.state.transaction():
            for match_id in ids:
                actual_platform = match_id.partition("_")[0].upper()
                if actual_platform not in self.config.platforms:
                    continue
                if self.state.conn.execute("SELECT 1 FROM matches WHERE match_id=?", (match_id,)).fetchone():
                    continue
                self.state.enqueue("match", match_id, actual_platform,
                                   {"match_id": match_id, "discovery_puuid": payload["puuid"],
                                    "discovery_source": payload.get("discovery_source", "participant"),
                                    "seed_rank": payload.get("seed_rank")}, job["priority"])
            payload["start"] = payload.get("start", 0) + len(ids)
            if len(ids) < count:
                self.state.complete(job["id"])
            else:
                self._continue(job, payload)

    def _continue(self, job: dict, payload: dict) -> None:
        self.state.conn.execute(
            "UPDATE jobs SET payload=?,status='pending',not_before=?,updated_at=? WHERE id=?",
            (json.dumps(payload), time.time(), time.time(), job["id"]))

    async def _match(self, job: dict) -> None:
        match_id = job["payload"]["match_id"]
        if self.state.conn.execute("SELECT 1 FROM matches WHERE match_id=?", (match_id,)).fetchone():
            self.state.complete(job["id"])
            return
        existing = self.storage.existing(match_id)
        if existing:
            raw, stored = existing
        else:
            raw = await self.client.get(ROUTING[job["platform"]],
                                        f"/lol/match/v5/matches/{quote(match_id, safe='')}",
                                        "match", "match-v5")
            body = json.loads(raw)
            if body.get("info", {}).get("queueId") != 420:
                self.storage.write(match_id, raw, kind="quarantine")
                self.state.finish(job["id"], status="skipped", error="Unexpected non-420 match; raw quarantined")
                return
            self.storage.validate_match(raw, match_id)
            stored = self.storage.write(match_id, raw)
        body = self.storage.validate_match(raw, match_id)
        self.state.commit_match(job, body, stored, discover=True, history_end_time=int(time.time()))
        self.downloaded[job["platform"]] += 1
        total = self.state.conn.execute("SELECT COUNT(*) FROM matches").fetchone()[0]
        if total >= self.config.milestone and not self.state.get_meta("milestone_reached"):
            self.state.set_meta("milestone_reached", {"matches": total, "at": time.time()})
            log.info("Initial milestone reached: %s matches. Collection continues.", total)

    async def _timeline(self, job: dict) -> None:
        match_id = job["payload"]["match_id"]
        if self.state.conn.execute("SELECT 1 FROM timelines WHERE match_id=?", (match_id,)).fetchone():
            self.state.complete(job["id"])
            return
        existing = self.storage.existing(match_id, kind="timelines")
        if existing:
            raw, stored = existing
        else:
            raw = await self.client.get(ROUTING[job["platform"]],
                f"/lol/match/v5/matches/{quote(match_id, safe='')}/timeline", "timeline", "match-v5")
            self.storage.validate_timeline(raw, match_id)
            stored = self.storage.write(match_id, raw, kind="timelines")
        self.storage.validate_timeline(raw, match_id)
        self.state.commit_timeline(job, stored)
        self.downloaded[job["platform"]] += 1

    async def _monitor(self, stop: asyncio.Event, max_seconds: float | None) -> None:
        while not stop.is_set():
            elapsed = time.monotonic() - self._started
            if max_seconds is not None and elapsed >= max_seconds:
                self._reason = "time-limit"
                stop.set()
                return
            if self._smoke is not None and all(self.downloaded[p] >= self._smoke for p in self.config.platforms):
                self._reason = "smoke-complete"
                stop.set()
                return
            if self._mode == "collect":
                self._seed_jobs()
            delay = self.config.progress_seconds
            if max_seconds is not None:
                delay = min(delay, max(0.01, max_seconds - elapsed))
            await self._pause(stop, delay)
            self._progress()

    @staticmethod
    async def _pause(stop: asyncio.Event, seconds: float) -> None:
        try:
            await asyncio.wait_for(stop.wait(), timeout=seconds)
        except TimeoutError:
            pass

    def _progress(self) -> None:
        elapsed = max(0.001, time.monotonic() - self._started)
        counts = dict(self.state.conn.execute(
            "SELECT platform,COUNT(*) FROM matches GROUP BY platform").fetchall())
        row = self.state.conn.execute(
            "SELECT COUNT(*),COALESCE(SUM(compressed_bytes),0),COUNT(DISTINCT patch) FROM matches").fetchone()
        free = self.storage.check_disk()
        avg = row[1] / row[0] if row[0] else None
        pending = dict(self.state.conn.execute(
            "SELECT kind,COUNT(*) FROM jobs WHERE status='pending' GROUP BY kind").fetchall())
        metrics = getattr(self.client, "metrics", {})
        progress = {"updated_at": time.time(), "requests_per_minute": getattr(self.client, "requests_per_minute", 0),
                    "retries": metrics.get("retries", 0), "matches_per_minute": sum(self.downloaded.values()) * 60 / elapsed,
                    "total_matches": row[0], "per_region": counts, "compressed_bytes": row[1],
                    "patches": row[2], "pending": pending, "free_bytes": free,
                    "mean_compressed_bytes": avg,
                    "estimated_remaining_matches": int(max(0, free - self.config.min_free_gb * 1024**3) / avg) if avg else None,
                    "mode": self._mode}
        self.state.set_meta("progress", progress)
        log.info("Progress matches=%s regions=%s requests/min=%.2f retries=%s matches/min=%.2f "
                 "compressed_gib=%.3f free_gib=%.2f patches=%s pending=%s remaining_capacity_matches=%s",
                 row[0], counts, progress["requests_per_minute"], progress["retries"], progress["matches_per_minute"],
                 row[1] / 1024**3, free / 1024**3, row[2], pending, progress["estimated_remaining_matches"])
