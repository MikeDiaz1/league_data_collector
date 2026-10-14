"""OS-owned lock: a crash releases it; a stale lock file does not block resumption."""
from __future__ import annotations

import json
import os
from pathlib import Path
import time


class DatasetBusyError(RuntimeError):
    pass


class DatasetLock:
    def __init__(self, data_dir: Path):
        self.data_dir = Path(data_dir)
        self.stream = None

    def __enter__(self):
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.stream = (self.data_dir / "collector.lock").open("a+b")
        self.stream.seek(0, os.SEEK_END)
        if self.stream.tell() == 0:
            self.stream.write(b"\0")
            self.stream.flush()
        self.stream.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(self.stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self.stream.close()
            self.stream = None
            raise DatasetBusyError("Another collector, timeline backfill, or rank refresh owns this dataset") from None
        (self.data_dir / "collector.pid").write_text(
            json.dumps({"pid": os.getpid(), "parent_pid": os.getppid(), "started_at": time.time()}), encoding="utf-8"
        )
        return self

    def __exit__(self, *args):
        if self.stream is not None:
            # Remove our PID while we still own the lock. Otherwise a newly
            # started collector could publish its PID between unlock and unlink.
            try:
                (self.data_dir / "collector.pid").unlink(missing_ok=True)
            finally:
                if os.name == "nt":
                    import msvcrt
                    self.stream.seek(0)
                    msvcrt.locking(self.stream.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(self.stream.fileno(), fcntl.LOCK_UN)
                self.stream.close()
                self.stream = None

