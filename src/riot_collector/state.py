"""Durable collection state. Raw API responses remain the source of truth."""

from __future__ import annotations

from contextlib import contextmanager
import json
from pathlib import Path
import sqlite3
import time
from typing import Any, Iterator


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
"""


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
        self, kind: str, key: str, platform: str, payload: dict[str, Any], priority: int = 0
    ) -> bool:
        """Insert once; promote pending work without reopening completed jobs."""
        now = time.time()
        with self.transaction():
            cursor = self.conn.execute(
                """INSERT OR IGNORE INTO jobs
                   (kind,key,platform,payload,priority,created_at,updated_at)
                   VALUES (?,?,?,?,?,?,?)""",
                (kind, key, platform.upper(), json.dumps(payload), priority, now, now),
            )
            inserted = cursor.rowcount == 1
            if not inserted:
                self.conn.execute(
                    """UPDATE jobs SET priority=MAX(priority,?), updated_at=?
                       WHERE kind=? AND key=? AND status='pending'""",
                    (priority, now, kind, key),
                )
        return inserted

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
    ) -> bool:
        """Commit raw-file metadata, participant discovery and completion together.

        The caller must successfully publish and fsync the raw file first. A crash
        before this transaction leaves an orphan that can be adopted without HTTP.
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
