"""Durable, work-conserving collection budgets with equal seed-tier turns.

These are job-claim budgets, not guarantees of equal unique matches or historical
lobby rank. Each platform has its own worker and persistent scheduling cursors.
"""
from __future__ import annotations

import json
import time
from typing import Any

from .state import SAMPLING_TIERS


class BalancedScheduler:
    SEED_INTERVAL = 20

    def __init__(self, config, state):
        self.config = config
        self.state = state
        self.slots = tuple(
            stream
            for stream, weight in (
                ("recent", config.recent_sampling_weight),
                ("participant", config.participant_sampling_weight),
                ("backfill", config.backfill_sampling_weight),
            )
            for _ in range(weight)
        )
        if not self.slots:
            raise ValueError("At least one sampling stream must have a positive weight")

    def claim(self, platform: str) -> dict[str, Any] | None:
        """Claim work and advance every fairness cursor in one transaction.

        Unavailable or delayed streams donate their slots to other enabled
        streams. Within each stream, two match turns alternate with one history
        turn. A full match backlog gates only that stream's additional histories.
        Ladder refresh receives a reserved turn even when any backlog is full.
        """
        platform = platform.upper()
        key = f"sampling_scheduler:v1:{platform}"
        with self.state.transaction():
            cursors = self.state.get_meta(key, {
                "stream": 0, "kinds": {}, "tiers": {}, "served": {},
                "since_seed": self.SEED_INTERVAL - 1,
            })
            now = time.time()
            job = None
            if cursors["since_seed"] >= self.SEED_INTERVAL - 1:
                job = self._tier_claim(platform, "seed", "seed", cursors, now)
            if job is None:
                start = cursors["stream"] % len(self.slots)
                tried = set()
                for offset in range(len(self.slots)):
                    slot = (start + offset) % len(self.slots)
                    stream = self.slots[slot]
                    if stream in tried:
                        continue
                    tried.add(stream)
                    job = self._stream_claim(platform, stream, cursors, now)
                    if job is not None:
                        cursors["stream"] = (slot + 1) % len(self.slots)
                        break
            if job is None:
                # A new dataset may have only ladder work, with no discovered
                # matches yet. Do useful seed work instead of idling or exiting.
                job = self._tier_claim(platform, "seed", "seed", cursors, now)
            if job is None:
                return None
            cursors["since_seed"] = 0 if job["kind"] == "seed" else cursors["since_seed"] + 1
            counter = f"{job['sampling_stream']}|{job['kind']}|{job['sampling_tier']}"
            cursors["served"][counter] = cursors["served"].get(counter, 0) + 1
            self.state.set_meta(key, cursors)
            return job

    def _stream_claim(self, platform: str, stream: str, cursors: dict, now: float) -> dict | None:
        history_kind = "history" if stream == "backfill" else "recent_history"
        kinds = ("match", "match", history_kind)
        start = cursors["kinds"].get(stream, 0) % len(kinds)
        tried = set()
        for offset in range(len(kinds)):
            slot = (start + offset) % len(kinds)
            kind = kinds[slot]
            if kind in tried:
                continue
            tried.add(kind)
            if kind == history_kind and self._backlog_full(platform, stream):
                continue
            job = self._tier_claim(platform, stream, kind, cursors, now)
            if job is not None:
                cursors["kinds"][stream] = (slot + 1) % len(kinds)
                return job
        return None

    def _backlog_full(self, platform: str, stream: str) -> bool:
        # Only count up to the cap. Large legacy backlogs must neither trigger
        # recent-history backpressure nor require a full-table count each turn.
        cap = self.config.discovery_backlog
        count = self.state.conn.execute(
            "SELECT COUNT(*) FROM (SELECT 1 FROM jobs INDEXED BY jobs_sampling_claim WHERE status IN ('pending','in_progress') "
            "AND platform=? AND sampling_stream=? AND kind='match' LIMIT ?)",
            (platform, stream, cap),
        ).fetchone()[0]
        return count >= cap

    def _tier_claim(self, platform: str, stream: str, kind: str,
                    cursors: dict, now: float) -> dict | None:
        cursor_key = f"{stream}|{kind}"
        start = cursors["tiers"].get(cursor_key, 0) % len(SAMPLING_TIERS)
        for offset in range(len(SAMPLING_TIERS)):
            index = (start + offset) % len(SAMPLING_TIERS)
            tier = SAMPLING_TIERS[index]
            row = self.state.conn.execute(
                "SELECT * FROM jobs INDEXED BY jobs_sampling_claim WHERE status='pending' AND platform=? AND sampling_stream=? "
                "AND kind=? AND sampling_tier=? AND not_before<=? ORDER BY updated_at,id LIMIT 1",
                (platform, stream, kind, tier, now),
            ).fetchone()
            if row is None:
                continue
            self.state.conn.execute(
                "UPDATE jobs SET status='in_progress',attempts=attempts+1,updated_at=? WHERE id=?",
                (now, row["id"]),
            )
            cursors["tiers"][cursor_key] = (index + 1) % len(SAMPLING_TIERS)
            job = dict(row)
            job.update(status="in_progress", attempts=job["attempts"] + 1, updated_at=now)
            job["payload"] = json.loads(job["payload"])
            return job
        return None
