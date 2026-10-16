"""Read-only inspection and replaceable offline metadata; raw files are never edited."""
from __future__ import annotations

from collections import Counter
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import tempfile
import time


TIER_ORDER = ("IRON", "BRONZE", "SILVER", "GOLD", "PLATINUM", "EMERALD", "DIAMOND", "MASTER", "GRANDMASTER", "CHALLENGER")
RANK_NOTE = (
    "Latest observed current solo rank on the match platform, not historical match rank. "
    "Skill population is a coarse estimate from known participants only; selection bias and temporal drift apply."
)


@contextmanager
def read_database(data_dir: Path):
    path = Path(data_dir).resolve() / "state.sqlite3"
    if not path.exists():
        raise FileNotFoundError("No dataset yet; run collect after setting RIOT_API_KEY")
    conn = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        # A consistent snapshot while an unattended writer continues collecting.
        conn.execute("BEGIN")
        yield conn
    finally:
        conn.close()


def _has_sampling_discoveries(conn) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='match_discoveries'"
    ).fetchone() is not None


def status(data_dir: Path, min_free_gb: float = 10.0) -> dict:
    data_dir = Path(data_dir)
    with read_database(data_dir) as conn:
        matches, compressed = conn.execute(
            "SELECT count(*),coalesce(sum(compressed_bytes),0) FROM matches"
        ).fetchone()
        regions = dict(conn.execute("SELECT platform,count(*) FROM matches GROUP BY platform"))
        patches = dict(conn.execute("SELECT patch,count(*) FROM matches GROUP BY patch ORDER BY patch"))
        jobs = [dict(row) for row in conn.execute("SELECT kind,status,count(*) AS count FROM jobs GROUP BY kind,status")]
        players = conn.execute("SELECT count(*) FROM players").fetchone()[0]
        ranks = conn.execute("SELECT count(*) FROM rank_observations").fetchone()[0]
        progress_row = conn.execute("SELECT value FROM key_value WHERE key='progress'").fetchone()
        progress = json.loads(progress_row[0]) if progress_row else None
        sampling = dict(progress["sampling"]) if progress and progress.get("sampling") else None
        if _has_sampling_discoveries(conn):
            sampling = sampling or {}
            routes = conn.execute("SELECT COUNT(*) FROM match_discoveries").fetchone()[0]
            distinct_matches = conn.execute(
                "SELECT COUNT(DISTINCT match_id) FROM match_discoveries"
            ).fetchone()[0]
            sampling.update(discovery_routes=routes, distinct_discovered_matches=distinct_matches,
                            repeated_discovery_routes=routes - distinct_matches)
        timeline_count, timeline_bytes = conn.execute(
            "SELECT count(*),coalesce(sum(compressed_bytes),0) FROM timelines"
        ).fetchone()
    free = shutil.disk_usage(data_dir).free
    mean = compressed / matches if matches else None
    overhead = sum(p.stat().st_size for p in data_dir.glob("state.sqlite3*"))
    logs = sum(p.stat().st_size for p in (data_dir / "logs").glob("*") if p.is_file())
    return {
        "matches": matches, "per_platform": regions, "patches": patches,
        "timelines": timeline_count, "players_discovered": players, "rank_observations": ranks,
        "jobs": jobs, "compressed_match_bytes": compressed,
        "tracked_dataset_bytes": compressed + timeline_bytes + overhead + logs,
        "free_disk_bytes": free, "min_free_gb": min_free_gb,
        "mean_compressed_bytes_per_match": mean,
        "estimated_additional_matches_until_disk_floor":
            max(0, int((free - min_free_gb * 1024**3) / mean)) if mean else None,
        "capacity_estimate_note": "Based on compressed match mean; additional indexes, logs and timelines reduce capacity.",
        "last_progress": progress,
        "sampling": sampling,
        "heartbeat_age_seconds": max(0, time.time() - progress["updated_at"])
            if progress and "updated_at" in progress else None,
    }


def _ranks(conn, match_id: str, platform: str) -> list[dict]:
    rows = conn.execute("""
        SELECT p.puuid, r.tier, r.division, r.lp, r.observed_at, r.source
        FROM match_participants p LEFT JOIN rank_observations r ON r.id=(
          SELECT r2.id FROM rank_observations r2
          WHERE r2.puuid=p.puuid AND r2.platform=? AND r2.queue='RANKED_SOLO_5x5'
          ORDER BY r2.observed_at DESC,r2.id DESC LIMIT 1
        ) WHERE p.match_id=? ORDER BY p.puuid
    """, (platform, match_id))
    return [dict(row) for row in rows]


def _sampling_discoveries(conn, match_id: str) -> list[dict]:
    rows = conn.execute(
        "SELECT history_job_id,platform,history_payload,discovered_at FROM match_discoveries "
        "WHERE match_id=? ORDER BY discovered_at,history_job_id", (match_id,)
    )
    return [dict(row, history_payload=json.loads(row["history_payload"])) for row in rows]


def export_manifest(data_dir: Path, output: Path) -> int:
    output = Path(output)
    resolved = output.resolve()
    root = Path(data_dir).resolve()
    if resolved.is_relative_to(root) and not (
        resolved.is_relative_to(root / "manifests")
        or (resolved.parent == root and resolved.name in {"matches.jsonl", "manifest.jsonl"})
    ):
        raise ValueError("Export into data/manifests or outside the dataset; dataset state and raw files are protected")
    output.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    handle, temp = tempfile.mkstemp(prefix=output.name + ".", suffix=".tmp", dir=output.parent)
    try:
        with os.fdopen(handle, "w", encoding="utf-8", newline="\n") as stream, read_database(data_dir) as conn:
            has_sampling = _has_sampling_discoveries(conn)
            for row in conn.execute("SELECT * FROM matches ORDER BY match_id"):
                item = dict(row)
                ranks = _ranks(conn, row["match_id"], row["platform"])
                known = [r for r in ranks if r["tier"] in TIER_ORDER]
                distribution = Counter(r["tier"] for r in known)
                mean = sum(TIER_ORDER.index(r["tier"]) for r in known) / len(known) if known else None
                item.update({
                    "manifest_version": 2 if has_sampling else 1,
                    "date_utc": datetime.fromtimestamp(row["game_creation"] / 1000, timezone.utc).isoformat()
                        if row["game_creation"] else None,
                    "participant_rank_observations": ranks,
                    "known_participant_rank_distribution": dict(distribution),
                    "fraction_participants_with_known_rank": len(known) / len(ranks) if ranks else 0.0,
                    "estimated_skill_population": TIER_ORDER[int(mean + 0.5)] if mean is not None else "UNKNOWN",
                    "estimated_skill_method": "rounded mean of ordinal tier index among known current-rank observations",
                    "rank_interpretation": RANK_NOTE,
                })
                if has_sampling:
                    item["sampling_discoveries"] = _sampling_discoveries(conn, row["match_id"])
                stream.write(json.dumps(item, ensure_ascii=False, separators=(",", ":")) + "\n")
                count += 1
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp, output)
    finally:
        Path(temp).unlink(missing_ok=True)
    return count


def validate(data_dir: Path) -> dict:
    from .storage import RawStorage, StorageError

    storage = RawStorage(data_dir)
    errors = []
    checked = {"matches": 0, "timelines": 0}
    with read_database(data_dir) as conn:
        integrity = [r[0] for r in conn.execute("PRAGMA integrity_check")]
        if integrity != ["ok"]:
            errors.extend(integrity)
        for kind in ("matches", "timelines"):
            for row in conn.execute(f"SELECT match_id,raw_path,compressed_bytes,sha256 FROM {kind}"):
                try:
                    raw = storage.read(row["raw_path"])
                    payload = json.loads(raw)
                    if payload.get("metadata", {}).get("matchId") != row["match_id"]:
                        raise ValueError("metadata.matchId mismatch")
                    if hashlib.sha256(raw).hexdigest() != row["sha256"]:
                        raise ValueError("raw response SHA-256 mismatch")
                    if (Path(data_dir) / row["raw_path"]).stat().st_size != row["compressed_bytes"]:
                        raise ValueError("compressed file size mismatch")
                    if kind == "matches":
                        storage.validate_match(raw, row["match_id"])
                    elif not isinstance(payload.get("info", {}).get("frames"), list):
                        raise ValueError("missing timeline frames")
                except (OSError, ValueError, KeyError, TypeError, EOFError, StorageError) as exc:
                    errors.append(f"{kind}/{row['match_id']}: {exc}")
                checked[kind] += 1
    return {"ok": not errors, "checked": checked, "errors": errors}
