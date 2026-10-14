"""Immutable, atomic gzip storage of complete original Riot response bodies."""

from __future__ import annotations

import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import tempfile
from typing import Any
import zlib


class StorageError(RuntimeError):
    pass


class LowDiskError(StorageError):
    pass


MATCH_ID = re.compile(r"^[A-Z][A-Z0-9]*_[0-9]+$")


class RawStorage:
    def __init__(self, data_dir: Path | str, min_free_gb: float = 10):
        if min_free_gb < 0:
            raise ValueError("min_free_gb must be nonnegative")
        self.root = self.data_dir = Path(data_dir)
        self.root.mkdir(parents=True, exist_ok=True)
        self.min_free_bytes = int(min_free_gb * 1024**3)

    def path(self, match_id: str, kind: str = "matches") -> Path:
        if not MATCH_ID.fullmatch(match_id):
            raise StorageError("Invalid Riot match ID")
        if kind not in {"matches", "timelines", "quarantine"}:
            raise StorageError("Raw storage kind must be matches, timelines or quarantine")
        platform, number = match_id.split("_", 1)
        return self.root / "raw" / kind / platform / number[-3:].zfill(3) / f"{match_id}.json.gz"

    def check_disk(self, required_bytes: int = 0) -> int:
        free = shutil.disk_usage(self.root).free
        if free - required_bytes < self.min_free_bytes:
            raise LowDiskError(
                f"Disk safety threshold reached: {free / 1024**3:.2f} GiB free, "
                f"reserve {self.min_free_bytes / 1024**3:.2f} GiB"
            )
        return free

    def read(self, relative_path: str | Path) -> bytes:
        candidate = (self.root / relative_path).resolve()
        if not candidate.is_relative_to(self.root.resolve()):
            raise StorageError("Raw path escapes dataset directory")
        try:
            with gzip.open(candidate, "rb") as stream:
                return stream.read()
        except (OSError, EOFError, zlib.error) as exc:
            raise StorageError(f"Cannot read compressed raw file: {candidate.name}") from exc

    @staticmethod
    def _parse(raw: bytes, match_id: str) -> dict[str, Any]:
        try:
            document = json.loads(raw)
        except (ValueError, UnicodeError) as exc:
            raise StorageError("Response is not valid complete JSON") from exc
        if not isinstance(document, dict):
            raise StorageError("Response must be a JSON object")
        metadata, info = document.get("metadata"), document.get("info")
        if not isinstance(metadata, dict) or not isinstance(info, dict):
            raise StorageError("Response is missing metadata or info")
        if metadata.get("matchId") != match_id:
            raise StorageError("Response match ID does not match requested match")
        puuids = metadata.get("participants")
        if (not isinstance(puuids, list) or len(puuids) != 10
                or any(not isinstance(p, str) or not p for p in puuids)
                or len(set(puuids)) != 10):
            raise StorageError("Response must contain ten unique participant PUUIDs")
        return document

    @classmethod
    def validate_match(cls, raw: bytes, match_id: str) -> dict[str, Any]:
        document = cls._parse(raw, match_id)
        info = document["info"]
        if info.get("queueId") != 420:
            raise StorageError("Only Ranked Solo/Duo queue 420 matches may be collected")
        participants = info.get("participants")
        if not isinstance(participants, list) or len(participants) != 10:
            raise StorageError("Match must contain all ten participant records")
        puuids = [p.get("puuid") if isinstance(p, dict) else None for p in participants]
        if (any(not isinstance(p, str) or not p for p in puuids)
                or set(puuids) != set(document["metadata"]["participants"])):
            raise StorageError("Participant records disagree with metadata")
        teams = info.get("teams")
        if not isinstance(teams, list) or len(teams) != 2 or any(not isinstance(t, dict) for t in teams):
            raise StorageError("Match must contain both complete team records")
        platform = info.get("platformId")
        if platform is not None and platform != match_id.split("_", 1)[0]:
            raise StorageError("Match platform disagrees with its ID")
        return document

    @classmethod
    def validate_timeline(cls, raw: bytes, match_id: str) -> dict[str, Any]:
        document = cls._parse(raw, match_id)
        frames = document["info"].get("frames")
        if not isinstance(frames, list) or not frames or any(not isinstance(frame, dict) for frame in frames):
            raise StorageError("Timeline must contain complete frame records")
        return document

    def _stored(self, path: Path, raw: bytes, reused: bool) -> dict[str, Any]:
        return {
            "raw_path": path.relative_to(self.root).as_posix(),
            "compressed_bytes": path.stat().st_size,
            "sha256": hashlib.sha256(raw).hexdigest(),
            "reused": reused,
        }

    def existing(self, match_id: str, kind: str = "matches") -> tuple[bytes, dict[str, Any]] | None:
        destination = self.path(match_id, kind)
        if not destination.exists():
            return None
        raw = self.read(destination.relative_to(self.root))
        if kind != "quarantine":
            validator = self.validate_match if kind == "matches" else self.validate_timeline
            validator(raw, match_id)
        return raw, self._stored(destination, raw, reused=True)

    @staticmethod
    def _sync_directory(directory: Path) -> None:
        # Windows does not expose a portable directory fsync. The file is flushed
        # before publication; NTFS rename/link journal recovery preserves atomicity.
        if os.name != "nt":
            descriptor = os.open(directory, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)

    @staticmethod
    def _publish(temporary: Path, destination: Path) -> None:
        if os.name == "nt":
            # Same-volume atomic move, no replacement, with write-through. This
            # also supports Windows volumes that cannot create hard links.
            import ctypes
            from ctypes import wintypes

            move = ctypes.WinDLL("kernel32", use_last_error=True).MoveFileExW
            move.argtypes = (wintypes.LPCWSTR, wintypes.LPCWSTR, wintypes.DWORD)
            move.restype = wintypes.BOOL
            if not move(str(temporary.resolve()), str(destination.resolve()), 0x8):
                error = ctypes.get_last_error()
                if error in {80, 183}:
                    raise FileExistsError(error, "Raw destination already exists", str(destination))
                raise ctypes.WinError(error)
        else:
            # Atomic no-clobber publication; directory fsync follows below.
            os.link(temporary, destination)

    def write(self, match_id: str, raw: bytes, kind: str = "matches") -> dict[str, Any]:
        destination = self.path(match_id, kind)
        if kind != "quarantine":
            validator = self.validate_match if kind == "matches" else self.validate_timeline
            validator(raw, match_id)
        previous = self.existing(match_id, kind)
        if previous is not None:
            return previous[1]
        compressed = gzip.compress(raw, compresslevel=6, mtime=0)
        self.check_disk(len(compressed))
        destination.parent.mkdir(parents=True, exist_ok=True)
        descriptor, name = tempfile.mkstemp(prefix=f".{match_id}.", suffix=".tmp", dir=destination.parent)
        temporary = Path(name)
        try:
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(compressed)
                stream.flush()
                os.fsync(stream.fileno())
            try:
                self._publish(temporary, destination)
            except FileExistsError:
                previous = self.existing(match_id, kind)
                if previous is None:
                    raise StorageError("Concurrent raw publication failed")
                return previous[1]
            self._sync_directory(destination.parent)
            return self._stored(destination, raw, reused=False)
        finally:
            temporary.unlink(missing_ok=True)
