import gzip
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
from types import SimpleNamespace

import pytest

from riot_collector.state import State
from riot_collector.storage import LowDiskError, RawStorage, StorageError


def match_bytes(match_id="NA1_123456", queue=420):
    puuids = [f"player-{number}" for number in range(10)]
    return json.dumps({
        "metadata": {"dataVersion": "2", "matchId": match_id, "participants": puuids,
                     "futureMetadata": {"preserved": True}},
        "info": {
            "queueId": queue, "platformId": match_id.split("_")[0],
            "gameVersion": "16.20.1.1234", "gameCreation": 1791900000000,
            "gameStartTimestamp": 1791900001000, "gameEndTimestamp": 1791901801000,
            "gameDuration": 1800,
            "participants": [{"puuid": puuid, "participantId": index + 1,
                              "perks": {"styles": [{"selections": [{"perk": 8005}]}]},
                              "challenges": {"arbitraryFutureField": index},
                              "item0": 1001, "unrecognized": [1, {"a": "b"}]}
                             for index, puuid in enumerate(puuids)],
            "teams": [{"teamId": team, "bans": [{"championId": -1}],
                       "objectives": {"baron": {"kills": 1}}} for team in (100, 200)],
        },
        "new_top_level": ["preserve", "everything"],
    }, indent=2).encode()


def timeline_bytes(match_id="NA1_123456"):
    return json.dumps({
        "metadata": {"matchId": match_id, "participants": [f"player-{i}" for i in range(10)]},
        "info": {"frameInterval": 60000, "frames": [{"timestamp": 0, "events": []}]},
    }).encode()


def match_job(state, match_id="NA1_123456"):
    state.enqueue("match", match_id, match_id.split("_")[0],
                  {"match_id": match_id, "discovery_source": "league", "seed_rank": "MASTER"})
    return state.claim("match")


def test_queue_deduplicates_and_does_not_reopen_completed(tmp_path):
    state = State(tmp_path)
    assert state.enqueue("match", "NA1_123456", "NA1", {"first": True}, priority=1)
    assert not state.enqueue("match", "NA1_123456", "NA1", {"first": False}, priority=100)
    job = state.claim()
    assert job["priority"] == 100
    assert job["payload"] == {"first": True}
    assert job["attempts"] == 1
    assert state.claim() is None
    state.complete(job["id"])
    assert not state.enqueue("match", "NA1_123456", "NA1", {}, priority=200)
    assert state.claim() is None
    state.close()


def test_retry_and_recover_preserve_every_queue_item(tmp_path):
    state = State(tmp_path)
    state.enqueue("history", "a", "NA1", {}, priority=1)
    state.enqueue("history", "b", "KR", {}, priority=2)
    first = state.claim()
    assert first["key"] == "b"
    state.retry(first["id"], "temporary network error", delay=3600)
    second = state.claim(platform="na1")
    assert second["key"] == "a"
    state.close()
    resumed = State(tmp_path)
    assert resumed.recover() == 1
    assert resumed.recover() == 0
    recovered = resumed.claim()
    assert recovered["id"] == second["id"]
    assert recovered["attempts"] == 2
    assert resumed.claim() is None  # Retry-After remains in force for b.
    resumed.close()


def test_process_crash_after_claim_resumes_without_loss(tmp_path):
    code = """import os,sys
from riot_collector.state import State
s=State(sys.argv[1])
s.enqueue('match','NA1_999','NA1',{'match_id':'NA1_999'})
assert s.claim()['key']=='NA1_999'
os._exit(23)
"""
    env = dict(os.environ)
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1] / "src")
    child = subprocess.run([sys.executable, "-c", code, str(tmp_path)], env=env, check=False)
    assert child.returncode == 23
    state = State(tmp_path)
    assert state.recover() == 1
    assert state.claim()["key"] == "NA1_999"
    state.close()


def test_process_crash_during_transaction_rolls_back_every_write(tmp_path):
    code = """import os,sys
from riot_collector.state import State
s=State(sys.argv[1])
with s.transaction():
    s.enqueue('history','unfinished','NA1',{'puuid':'new-player'})
    s.set_meta('unfinished-cursor',100)
    os._exit(24)
"""
    env = dict(os.environ)
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1] / "src")
    child = subprocess.run([sys.executable, "-c", code, str(tmp_path)], env=env, check=False)
    assert child.returncode == 24
    state = State(tmp_path)
    assert state.pending_count() == 0
    assert state.get_meta("unfinished-cursor") is None
    assert state.conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    state.close()


def test_nested_transaction_rolls_back_on_interruption(tmp_path):
    state = State(tmp_path)
    with pytest.raises(KeyboardInterrupt):
        with state.transaction():
            state.enqueue("history", "a", "NA1", {"puuid": "a"})
            state.set_meta("cursor", 100)
            raise KeyboardInterrupt
    assert state.conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == 0
    assert state.get_meta("cursor") is None
    state.close()


def test_match_file_and_all_unknown_fields_round_trip_exactly(tmp_path):
    storage = RawStorage(tmp_path, min_free_gb=0)
    raw = match_bytes()
    stored = storage.write("NA1_123456", raw)
    assert storage.read(stored["raw_path"]) == raw
    assert stored["sha256"] == hashlib.sha256(raw).hexdigest()
    assert stored["compressed_bytes"] == (tmp_path / stored["raw_path"]).stat().st_size
    assert gzip.decompress((tmp_path / stored["raw_path"]).read_bytes()) == raw
    assert json.loads(raw)["info"]["participants"][0]["unrecognized"] == [1, {"a": "b"}]
    assert storage.existing("NA1_123456")[1]["reused"] is True


def test_existing_raw_file_is_never_overwritten(tmp_path):
    storage = RawStorage(tmp_path, min_free_gb=0)
    raw = match_bytes()
    first = storage.write("NA1_123456", raw)
    altered = json.loads(raw)
    altered["new_top_level"] = "changed upstream"
    second = storage.write("NA1_123456", json.dumps(altered).encode())
    assert second["reused"] is True
    assert second["sha256"] == first["sha256"]
    assert storage.read(second["raw_path"]) == raw


def test_atomic_publication_failure_leaves_no_final_or_temporary(tmp_path, monkeypatch):
    storage = RawStorage(tmp_path, min_free_gb=0)

    def fail(*args):
        raise OSError("simulated interruption before publication")

    monkeypatch.setattr(storage, "_publish", fail)
    with pytest.raises(OSError):
        storage.write("NA1_123456", match_bytes())
    assert not storage.path("NA1_123456").exists()
    assert list(tmp_path.rglob("*.tmp")) == []


def test_atomic_publication_cannot_clobber_existing_destination(tmp_path):
    temporary = tmp_path / "new.tmp"
    destination = tmp_path / "existing.json.gz"
    temporary.write_bytes(b"new content")
    destination.write_bytes(b"permanent original")
    with pytest.raises(FileExistsError):
        RawStorage._publish(temporary, destination)
    assert destination.read_bytes() == b"permanent original"
    assert temporary.read_bytes() == b"new content"


def test_crash_after_publication_recovers_orphan_without_redownload(tmp_path, monkeypatch):
    state = State(tmp_path)
    storage = RawStorage(tmp_path, min_free_gb=0)
    job = match_job(state)
    publish = storage._publish

    def crash_after_publish(temporary, destination):
        publish(temporary, destination)
        raise KeyboardInterrupt

    monkeypatch.setattr(storage, "_publish", crash_after_publish)
    with pytest.raises(KeyboardInterrupt):
        storage.write("NA1_123456", match_bytes())
    assert state.stats()["total_matches"] == 0
    state.close()
    resumed = State(tmp_path)
    resumed.recover()
    reclaimed = resumed.claim("match")
    assert reclaimed["id"] == job["id"]
    raw, persisted = storage.existing("NA1_123456")
    assert resumed.commit_match(reclaimed, storage.validate_match(raw, "NA1_123456"), persisted)
    assert resumed.stats()["total_matches"] == 1
    assert resumed.stats()["players"] == 10
    assert resumed.pending_count() == 10
    assert resumed.conn.execute("SELECT status FROM jobs WHERE id=?", (job["id"],)).fetchone()[0] == "done"
    resumed.close()


def test_match_commit_and_recursive_discovery_roll_back_together(tmp_path):
    state = State(tmp_path)
    storage = RawStorage(tmp_path, min_free_gb=0)
    job = match_job(state)
    stored = storage.write("NA1_123456", match_bytes())
    payload = json.loads(match_bytes())
    del payload["info"]["participants"][5]["puuid"]
    with pytest.raises(KeyError):
        state.commit_match(job, payload, stored)
    assert state.stats()["total_matches"] == 0
    assert state.stats()["players"] == 0
    assert state.conn.execute("SELECT COUNT(*) FROM match_participants").fetchone()[0] == 0
    assert state.conn.execute("SELECT status FROM jobs WHERE id=?", (job["id"],)).fetchone()[0] == "in_progress"
    state.close()


def test_global_match_dedup_and_separate_timeline_resume(tmp_path):
    state = State(tmp_path)
    storage = RawStorage(tmp_path, min_free_gb=0)
    job = match_job(state)
    raw = match_bytes()
    stored = storage.write("NA1_123456", raw)
    assert state.commit_match(job, json.loads(raw), stored)
    assert not state.commit_match(job, json.loads(raw), stored)
    assert state.stats()["total_matches"] == 1
    assert state.conn.execute("SELECT COUNT(*) FROM match_participants").fetchone()[0] == 10
    assert state.enqueue("timeline", "NA1_123456", "NA1", {"match_id": "NA1_123456"})
    timeline_job = state.claim("timeline")
    state.close()
    resumed = State(tmp_path)
    assert resumed.recover() == 1
    assert resumed.stats()["total_matches"] == 1
    timeline_job = resumed.claim("timeline")
    timeline_stored = storage.write("NA1_123456", timeline_bytes(), kind="timelines")
    assert resumed.commit_timeline(timeline_job, timeline_stored)
    assert resumed.stats()["timelines"] == 1
    assert resumed.claim("timeline") is None
    resumed.close()


def test_duplicate_completion_cannot_mutate_immutable_participant_set(tmp_path):
    state = State(tmp_path)
    storage = RawStorage(tmp_path, min_free_gb=0)
    job = match_job(state)
    raw = match_bytes()
    stored = storage.write("NA1_123456", raw)
    assert state.commit_match(job, json.loads(raw), stored)
    changed = json.loads(raw)
    changed["info"]["participants"][0]["puuid"] = "different-upstream-puuid"
    assert not state.commit_match(job, changed, stored)
    assert state.conn.execute("SELECT COUNT(*) FROM match_participants").fetchone()[0] == 10
    assert state.conn.execute("SELECT 1 FROM players WHERE puuid='different-upstream-puuid'").fetchone() is None
    state.close()


def test_inconsistent_raw_path_cannot_silently_complete_a_different_match(tmp_path):
    state = State(tmp_path)
    storage = RawStorage(tmp_path, min_free_gb=0)
    first = match_job(state)
    original = match_bytes()
    stored = storage.write("NA1_123456", original)
    state.commit_match(first, json.loads(original), stored)
    second = match_job(state, "NA1_987654")
    with pytest.raises(sqlite3.IntegrityError):
        state.commit_match(second, json.loads(match_bytes("NA1_987654")), stored)
    assert state.stats()["total_matches"] == 1
    assert state.conn.execute("SELECT status FROM jobs WHERE id=?", (second["id"],)).fetchone()[0] == "in_progress"
    state.close()


@pytest.mark.parametrize("match_id", ["../escape", "NA1_1/../x", "NA1_x", "NA1_1\\x", "NA1_1:bad"])
def test_unsafe_match_paths_rejected(tmp_path, match_id):
    with pytest.raises(StorageError):
        RawStorage(tmp_path).path(match_id)


def test_bad_queue_and_incomplete_match_cannot_be_completed(tmp_path):
    storage = RawStorage(tmp_path, min_free_gb=0)
    wrong_queue = match_bytes(queue=440)
    with pytest.raises(StorageError, match="queue 420"):
        storage.write("NA1_123456", wrong_queue)
    incomplete = json.loads(match_bytes())
    incomplete["info"]["participants"].pop()
    with pytest.raises(StorageError, match="ten participant"):
        storage.write("NA1_123456", json.dumps(incomplete).encode())
    quarantined = storage.write("NA1_123456", wrong_queue, kind="quarantine")
    assert storage.read(quarantined["raw_path"]) == wrong_queue
    assert not storage.path("NA1_123456").exists()


@pytest.mark.parametrize("damage", [b"damaged gzip", b"\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\x03\x07" + b"\x00" * 8])
def test_corrupt_existing_data_is_preserved_and_reported(tmp_path, damage):
    storage = RawStorage(tmp_path, min_free_gb=0)
    path = storage.path("NA1_123456")
    path.parent.mkdir(parents=True)
    path.write_bytes(damage)
    with pytest.raises(StorageError):
        storage.write("NA1_123456", match_bytes())
    assert path.read_bytes() == damage


def test_disk_reserve_accounts_for_pending_compressed_write(tmp_path, monkeypatch):
    storage = RawStorage(tmp_path, min_free_gb=1)
    monkeypatch.setattr("riot_collector.storage.shutil.disk_usage",
                        lambda _: SimpleNamespace(free=1024**3 + 10))
    with pytest.raises(LowDiskError):
        storage.write("NA1_123456", match_bytes())
    assert not storage.path("NA1_123456").exists()


def test_raw_reads_cannot_escape_data_root(tmp_path):
    storage = RawStorage(tmp_path)
    with pytest.raises(StorageError, match="escapes"):
        storage.read("../outside.json.gz")
