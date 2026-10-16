"""Durable collection state. Raw API responses remain the source of truth."""

from __future__ import annotations

from contextlib import contextmanager
import json
from pathlib import Path
import sqlite3
import time
from typing import Any, Iterator
from .diversity import record_match_diversity


SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY,
    kind TEXT NOT NULL,
    key TEXT NOT NULL,
    platform TEXT NOT NULL,
    payload TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending'
      CHECK(status IN ('pending','in_progress','done','failed','skipped')),
    priority INTEGER NOT NULL DEFAULT 0,
    sampling_stream TEXT NOT NULL DEFAULT 'backfill',
    sampling_tier TEXT NOT NULL DEFAULT 'UNKNOWN',
    attempts INTEGER NOT NULL DEFAULT 0,
    not_before REAL NOT NULL DEFAULT 0,
    error TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    UNIQUE(kind, key)
);
CREATE INDEX IF NOT EXISTS jobs_claim ON jobs(status, kind, platform, not_before, priority DESC, id);
CREATE TABLE IF NOT EXISTS matches (
    match_id TEXT PRIMARY KEY,
    platform TEXT NOT NULL,
    routing TEXT NOT NULL,
    queue_id INTEGER NOT NULL,
    patch TEXT,
    game_version TEXT,
    game_creation INTEGER,
    game_start INTEGER,
    game_end INTEGER,
    duration INTEGER,
    raw_path TEXT NOT NULL UNIQUE,
    compressed_bytes INTEGER NOT NULL,
    sha256 TEXT NOT NULL,
    discovery_source TEXT,
    seed_tier TEXT,
    discovered_at REAL NOT NULL,
    completed_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS matches_platform ON matches(platform);
CREATE INDEX IF NOT EXISTS matches_patch ON matches(patch);
CREATE TABLE IF NOT EXISTS players (
    puuid TEXT NOT NULL,
    platform TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 0,
    source TEXT,
    seed_tier TEXT,
    discovered_at REAL NOT NULL,
    rank_checked_at REAL,
    PRIMARY KEY(puuid, platform)
);
CREATE TABLE IF NOT EXISTS rank_observations (
    id INTEGER PRIMARY KEY,
    puuid TEXT NOT NULL,
    platform TEXT NOT NULL,
    tier TEXT,
    division TEXT,
    lp INTEGER,
    queue TEXT NOT NULL,
    observed_at REAL NOT NULL,
    source TEXT,
    raw_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS rank_player_time ON rank_observations(puuid, platform, observed_at DESC);
CREATE TABLE IF NOT EXISTS match_participants (
    match_id TEXT NOT NULL REFERENCES matches(match_id),
    puuid TEXT NOT NULL,
    PRIMARY KEY(match_id, puuid)
);
CREATE INDEX IF NOT EXISTS participant_player ON match_participants(puuid);
CREATE TABLE IF NOT EXISTS timelines (
    match_id TEXT PRIMARY KEY REFERENCES matches(match_id),
    raw_path TEXT NOT NULL UNIQUE,
    compressed_bytes INTEGER NOT NULL,
    sha256 TEXT NOT NULL,
    completed_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS key_value (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS match_discoveries (
    match_id TEXT NOT NULL,
    history_job_id INTEGER NOT NULL REFERENCES jobs(id),
    platform TEXT NOT NULL,
    history_payload TEXT NOT NULL,
    discovered_at REAL NOT NULL,
    PRIMARY KEY(history_job_id, match_id)
);
CREATE INDEX IF NOT EXISTS discovery_match ON match_discoveries(match_id);
CREATE TABLE IF NOT EXISTS player_sampling_history (
    puuid TEXT NOT NULL, platform TEXT NOT NULL, last_selected_at REAL NOT NULL,
    selection_count INTEGER NOT NULL, PRIMARY KEY(puuid,platform)
);
CREATE TABLE IF NOT EXISTS match_diversity (
    match_id TEXT PRIMARY KEY REFERENCES matches(match_id), source_sha256 TEXT NOT NULL,
    platform TEXT NOT NULL, stream TEXT NOT NULL, seed_tier TEXT NOT NULL,
    recorded_at REAL NOT NULL, new_accounts INTEGER NOT NULL, participant_slots INTEGER NOT NULL,
    known_role_slots INTEGER NOT NULL, new_champion_roles INTEGER NOT NULL, new_matchups INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS match_diversity_time ON match_diversity(recorded_at);
CREATE TABLE IF NOT EXISTS gameplay_signals (
    kind TEXT NOT NULL, signal_key TEXT NOT NULL, first_seen_at REAL NOT NULL,
    first_match_id TEXT NOT NULL REFERENCES matches(match_id), PRIMARY KEY(kind,signal_key)
);
CREATE TABLE IF NOT EXISTS match_gameplay_signals (
    match_id TEXT NOT NULL REFERENCES matches(match_id), kind TEXT NOT NULL, signal_key TEXT NOT NULL,
    PRIMARY KEY(match_id,kind,signal_key)
);
CREATE TABLE IF NOT EXISTS api_request_events (
    id INTEGER PRIMARY KEY, dispatched_at REAL NOT NULL, platform TEXT NOT NULL,
    stream TEXT NOT NULL, seed_tier TEXT NOT NULL, kind TEXT NOT NULL, method TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS api_request_time ON api_request_events(dispatched_at);
"""


SAMPLING_TIERS = ("EMERALD", "DIAMOND", "MASTER", "GRANDMASTER", "CHALLENGER", "UNKNOWN")
SAMPLING_STREAMS = ("recent", "participant", "backfill", "seed")


def infer_sampling_tier(payload: dict[str, Any], priority: int = 0) -> str:
    """Return the discovery seed tier, never a claim about historical lobby rank."""
    for name in ("tier", "seed_rank", "seed_tier"):
        value = payload.get(name)
        if isinstance(value, dict):
            value = value.get("tier")
        if isinstance(value, str) and value.upper() in SAMPLING_TIERS:
            return value.upper()
    return {100: "CHALLENGER", 90: "GRANDMASTER", 80: "MASTER",
            60: "DIAMOND", 40: "EMERALD"}.get(priority, "UNKNOWN")


class State:
    def __init__(self, data_dir: Path | str):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.path = self.data_dir / "state.sqlite3"
        self.conn = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA busy_timeout=30000")
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.executescript(SCHEMA)
        self._savepoint = 0
        self._migrate_sampling()
        self._migrate_diversity()

    def _migrate_diversity(self) -> None:
        with self.transaction():
            if not self.get_meta("player_rotation_v1"):
                for row in self.conn.execute("SELECT platform,payload,created_at FROM jobs WHERE kind='recent_history'"):
                    payload = json.loads(row["payload"])
                    if payload.get("puuid"):
                        self.record_player_selection(payload["puuid"], row["platform"], row["created_at"])
                self.set_meta("player_rotation_v1", {"initialized_at": time.time()})
            if not self.get_meta("diversity_telemetry_v1"):
                self.set_meta("diversity_telemetry_v1", {"started_at": time.time()})

    def record_player_selection(self, puuid, platform, selected_at):
        self.conn.execute(
            "INSERT INTO player_sampling_history VALUES (?,?,?,1) ON CONFLICT(puuid,platform) "
            "DO UPDATE SET last_selected_at=MAX(last_selected_at,excluded.last_selected_at), "
            "selection_count=selection_count+1", (puuid, platform, selected_at))

    def _migrate_sampling(self) -> None:
        """Add scheduling metadata without rewriting research inputs or cursors.

        The CLI holds the exclusive dataset lock before constructing State.
        ALTER, attribution, index creation and the version marker commit together.
        A crash rolls this migration back and the next open can retry it safely.
        """
        with self.transaction():
            columns = {row["name"] for row in self.conn.execute("PRAGMA table_info(jobs)")}
            if "sampling_stream" not in columns:
                self.conn.execute("ALTER TABLE jobs ADD COLUMN sampling_stream TEXT NOT NULL DEFAULT 'backfill'")
            if "sampling_tier" not in columns:
                self.conn.execute("ALTER TABLE jobs ADD COLUMN sampling_tier TEXT NOT NULL DEFAULT 'UNKNOWN'")
            if not self.get_meta("sampling_schema_v1"):
                def infer(raw: str, priority: int) -> str:
                    try:
                        payload = json.loads(raw)
                    except (TypeError, ValueError):
                        payload = {}
                    return infer_sampling_tier(payload if isinstance(payload, dict) else {}, priority)

                self.conn.create_function("infer_sampling_tier", 2, infer)
                try:
                    self.conn.execute(
                        "UPDATE jobs SET sampling_tier=infer_sampling_tier(payload,priority), "
                        "sampling_stream=CASE WHEN kind='seed' THEN 'seed' ELSE sampling_stream END"
                    )
                finally:
                    self.conn.create_function("infer_sampling_tier", 2, None)
                self.set_meta("sampling_schema_v1", {"version": 1, "migrated_at": time.time()})
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS jobs_sampling_claim ON jobs "
                "(status,platform,sampling_stream,kind,sampling_tier,updated_at,id)"
            )

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """Begin immediately; nesting uses savepoints and never commits its parent."""
        if self.conn.in_transaction:
            self._savepoint += 1
            name = f"state_{self._savepoint}"
            self.conn.execute(f"SAVEPOINT {name}")
            try:
                yield self.conn
            except BaseException:
                self.conn.execute(f"ROLLBACK TO {name}")
                self.conn.execute(f"RELEASE {name}")
                raise
            else:
                self.conn.execute(f"RELEASE {name}")
        else:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                yield self.conn
            except BaseException:
                self.conn.rollback()
                raise
            else:
                self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    def enqueue(
        self, kind: str, key: str, platform: str, payload: dict[str, Any], priority: int = 0,
        *, sampling_stream: str = "backfill", sampling_tier: str | None = None,
    ) -> bool:
        """Insert once; promote scheduling metadata, retaining the first payload."""
        if kind == "seed":
            sampling_stream = "seed"
        if sampling_stream not in SAMPLING_STREAMS:
            raise ValueError(f"Unknown sampling stream: {sampling_stream}")
        tier = sampling_tier if sampling_tier is not None else infer_sampling_tier(payload, priority)
        tier = str(tier).upper()
        if tier not in SAMPLING_TIERS:
            tier = "UNKNOWN"
        now = time.time()
        with self.transaction():
            cursor = self.conn.execute(
                """INSERT OR IGNORE INTO jobs
                   (kind,key,platform,payload,priority,sampling_stream,sampling_tier,created_at,updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (kind, key, platform.upper(), json.dumps(payload), priority, sampling_stream, tier, now, now),
            )
            inserted = cursor.rowcount == 1
            if not inserted:
                existing = self.conn.execute(
                    "SELECT sampling_stream,sampling_tier FROM jobs WHERE kind=? AND key=? AND status='pending'",
                    (kind, key),
                ).fetchone()
                promote = existing is not None and kind == "match" and (
                    {"backfill": 0, "participant": 1, "recent": 2}.get(sampling_stream, -1)
                    > {"backfill": 0, "participant": 1, "recent": 2}.get(existing["sampling_stream"], -1)
                )
                stream = sampling_stream if promote else (existing["sampling_stream"] if existing else sampling_stream)
                if existing is not None and not promote and existing["sampling_tier"] != "UNKNOWN":
                    tier = existing["sampling_tier"]
                self.conn.execute(
                    """UPDATE jobs SET priority=MAX(priority,?),sampling_stream=?,sampling_tier=?
                       WHERE kind=? AND key=? AND status='pending'""",
                    (priority, stream, tier, kind, key),
                )
        return inserted

    def record_discovery(self, match_id: str, history_job_id: int, platform: str,
                         payload: dict[str, Any]) -> bool:
        """Append a discovery route, including repeats of an already stored match.

        A retry of the same history job cannot overwrite its original evidence.
        No inclusion probability or historical rank is inferred from this record.
        """
        with self.transaction():
            cursor = self.conn.execute(
                "INSERT OR IGNORE INTO match_discoveries "
                "(match_id,history_job_id,platform,history_payload,discovered_at) VALUES (?,?,?,?,?)",
                (match_id, history_job_id, platform.upper(), json.dumps(payload), time.time()),
            )
            return cursor.rowcount == 1

    def sampling_summary(self) -> dict[str, Any]:
        groups = [dict(row) for row in self.conn.execute(
            "SELECT platform,sampling_stream AS stream,sampling_tier AS tier,kind,status,COUNT(*) AS count "
            "FROM jobs INDEXED BY jobs_sampling_claim GROUP BY status,platform,sampling_stream,kind,sampling_tier "
            "ORDER BY platform,sampling_stream,sampling_tier,kind,status"
        )]
        served = []
        for row in self.conn.execute(
            "SELECT key,value FROM key_value WHERE key LIKE 'sampling_scheduler:v1:%' ORDER BY key"
        ):
            platform = row["key"].rsplit(":", 1)[1]
            for key, count in sorted(json.loads(row["value"]).get("served", {}).items()):
                stream, kind, tier = key.split("|")
                served.append({"platform": platform, "stream": stream, "kind": kind, "tier": tier, "count": count})
        return {"version": "balanced-v1", "groups": groups, "served": served}

    def claim(self, kind: str | None = None, platform: str | None = None) -> dict[str, Any] | None:
        predicates = ["status='pending'", "not_before<=?"]
        args: list[Any] = [time.time()]
        if kind is not None:
            predicates.append("kind=?")
            args.append(kind)
        if platform is not None:
            predicates.append("platform=?")
            args.append(platform.upper())
        with self.transaction():
            row = self.conn.execute(
                "SELECT * FROM jobs WHERE " + " AND ".join(predicates)
                + " ORDER BY priority DESC,id LIMIT 1", args,
            ).fetchone()
            if row is None:
                return None
            self.conn.execute(
                "UPDATE jobs SET status='in_progress',attempts=attempts+1,updated_at=? WHERE id=?",
                (time.time(), row["id"]),
            )
            result = dict(row)
            result["status"] = "in_progress"
            result["attempts"] += 1
            result["payload"] = json.loads(result["payload"])
            return result

    def complete(self, job_id: int) -> None:
        self.finish(job_id)

    def finish(self, job_id: int, status: str = "done", error: str | None = None) -> None:
        if status not in {"done", "failed", "skipped"}:
            raise ValueError(f"Invalid terminal job status: {status}")
        with self.transaction():
            self.conn.execute(
                "UPDATE jobs SET status=?,error=?,updated_at=? WHERE id=?",
                (status, error, time.time(), job_id),
            )

    def retry(self, job_id: int, error: str, delay: float = 60) -> None:
        with self.transaction():
            self.conn.execute(
                """UPDATE jobs SET status='pending',error=?,not_before=?,updated_at=?
                   WHERE id=? AND status!='done'""",
                (error, time.time() + max(0, delay), time.time(), job_id),
            )

    def recover(self) -> int:
        """Run only after obtaining the exclusive dataset lock."""
        with self.transaction():
            cursor = self.conn.execute(
                "UPDATE jobs SET status='pending',updated_at=? WHERE status='in_progress'",
                (time.time(),),
            )
            return cursor.rowcount

    def pending_count(self) -> int:
        return self.conn.execute("SELECT COUNT(*) FROM jobs WHERE status='pending'").fetchone()[0]

    def set_meta(self, key: str, value: Any) -> None:
        with self.transaction():
            self.conn.execute(
                "INSERT INTO key_value(key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, json.dumps(value)),
            )

    def get_meta(self, key: str, default: Any = None) -> Any:
        row = self.conn.execute("SELECT value FROM key_value WHERE key=?", (key,)).fetchone()
        return default if row is None else json.loads(row[0])

    def commit_match(
        self,
        job: dict[str, Any],
        payload: dict[str, Any],
        stored: dict[str, Any],
        discover: bool = True,
        history_end_time: int | None = None,
        *, queue_history: bool = True,
    ) -> bool:
        """Commit raw-file metadata, participant discovery and completion together.

        The caller must successfully publish and fsync the raw file first. A crash
        before this transaction leaves an orphan that can be adopted without HTTP.
        queue_history=False keeps participant discovery without creating new
        unbounded archival crawls; balanced cohorts can sample those players later.
        """
        metadata, info = payload["metadata"], payload["info"]
        match_id = metadata["matchId"]
        platform = match_id.split("_", 1)[0]
        routing = {"NA1": "AMERICAS", "EUW1": "EUROPE", "KR": "ASIA"}[platform]
        now = time.time()
        source = job.get("payload") or {}
        if isinstance(source, str):
            source = json.loads(source)
        version = str(info.get("gameVersion", ""))
        patch = ".".join(version.split(".")[:2]) or None
        seed_tier = source.get("seed_tier", source.get("seed_rank"))
        if isinstance(seed_tier, dict):
            seed_tier = seed_tier.get("tier")
        with self.transaction():
            cursor = self.conn.execute(
                """INSERT INTO matches
                   (match_id,platform,routing,queue_id,patch,game_version,game_creation,
                    game_start,game_end,duration,raw_path,compressed_bytes,sha256,
                    discovery_source,seed_tier,discovered_at,completed_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(match_id) DO NOTHING""",
                (match_id, platform, routing, info["queueId"], patch, version or None,
                 info.get("gameCreation"), info.get("gameStartTimestamp"), info.get("gameEndTimestamp"),
                 info.get("gameDuration"), str(stored["raw_path"]), stored["compressed_bytes"],
                 stored["sha256"], source.get("discovery_source", "history"), seed_tier,
                 job.get("created_at", now), now),
            )
            inserted = cursor.rowcount == 1
            if not inserted:
                # Match records and discovery were committed together originally.
                # A duplicate completion must not introduce participant records
                # from a different response into this immutable raw match.
                self.complete(job["id"])
                return False
            record_match_diversity(self.conn, job, payload, stored["sha256"], now)
            for participant in info["participants"]:
                puuid = participant["puuid"]
                self.conn.execute(
                    "INSERT OR IGNORE INTO match_participants(match_id,puuid) VALUES (?,?)",
                    (match_id, puuid),
                )
                if discover:
                    self.conn.execute(
                        """INSERT OR IGNORE INTO players
                           (puuid,platform,priority,source,discovered_at) VALUES (?,?,0,'participant',?)""",
                        (puuid, platform, now),
                    )
                    if queue_history:
                        self.enqueue(
                            "history", f"history:{platform}:{puuid}:0", platform,
                            {"puuid": puuid, "start": 0,
                             "end_time": history_end_time if history_end_time is not None else int(now),
                             "discovery_source": "participant", "seed_rank": None},
                        )
            self.complete(job["id"])
        return inserted

    def commit_timeline(self, job: dict[str, Any], stored: dict[str, Any]) -> bool:
        match_id = (job.get("payload") or {}).get("match_id", job["key"])
        with self.transaction():
            cursor = self.conn.execute(
                """INSERT INTO timelines
                   (match_id,raw_path,compressed_bytes,sha256,completed_at) VALUES (?,?,?,?,?)
                   ON CONFLICT(match_id) DO NOTHING""",
                (match_id, str(stored["raw_path"]), stored["compressed_bytes"], stored["sha256"], time.time()),
            )
            self.complete(job["id"])
            return cursor.rowcount == 1

    def stats(self) -> dict[str, Any]:
        totals = self.conn.execute(
            "SELECT COUNT(*),COALESCE(SUM(compressed_bytes),0),AVG(compressed_bytes) FROM matches"
        ).fetchone()
        jobs = [dict(row) for row in self.conn.execute(
            "SELECT kind,status,COUNT(*) AS count FROM jobs GROUP BY kind,status ORDER BY kind,status"
        )]
        return {
            "total_matches": totals[0],
            "compressed_bytes": totals[1],
            "average_compressed_bytes": totals[2] or 0,
            "per_region": dict(self.conn.execute("SELECT platform,COUNT(*) FROM matches GROUP BY platform")),
            "patches": dict(self.conn.execute("SELECT COALESCE(patch,'unknown'),COUNT(*) FROM matches GROUP BY patch")),
            "players": self.conn.execute("SELECT COUNT(*) FROM players").fetchone()[0],
            "rank_observations": self.conn.execute("SELECT COUNT(*) FROM rank_observations").fetchone()[0],
            "timelines": self.conn.execute("SELECT COUNT(*) FROM timelines").fetchone()[0],
            "pending_jobs": sum(row["count"] for row in jobs if row["status"] == "pending"),
            "jobs": jobs,
        }
