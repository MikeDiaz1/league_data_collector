import asyncio
import json
import os
from pathlib import Path
import subprocess
import sys
import time

import pytest

from riot_collector.cli import main
from riot_collector.config import load_config
from riot_collector.dataset import export_manifest, status, validate
from riot_collector.locking import DatasetBusyError, DatasetLock
from riot_collector.state import State
from riot_collector.storage import RawStorage


def populate(root):
    # Explicit fixture exercises unknown fields, exact timestamps and two teams.
    participants = [{"puuid": f"p{i}", "item0": 1001, "perks": {"styles": []},
                     "challenges": {"future": 1}, "newField": [False, None]} for i in range(10)]
    raw = json.dumps({"metadata": {"matchId": "NA1_100", "dataVersion": "2",
                                    "participants": [p["puuid"] for p in participants]},
                      "info": {"queueId": 420, "platformId": "NA1", "participants": participants,
                               "gameVersion": "16.20.123.456", "gameCreation": 1_700_000_000_123,
                               "gameStartTimestamp": 1_700_000_010_123,
                               "gameEndTimestamp": 1_700_001_500_123, "gameDuration": 1490,
                               "teams": [{"teamId": 100, "bans": [], "objectives": {}},
                                         {"teamId": 200, "bans": [], "objectives": {}}]}}).encode()
    state = State(root)
    state.enqueue("match", "NA1_100", "NA1", {"match_id": "NA1_100", "seed_rank": "MASTER", "discovery_source": "league_seed"})
    job = state.claim("match")
    storage = RawStorage(root, min_free_gb=0)
    stored = storage.write("NA1_100", raw)
    state.commit_match(job, json.loads(raw), stored)
    return state, storage, raw


def test_offline_export_uses_latest_observed_rank_without_historical_claim(tmp_path):
    state, storage, raw = populate(tmp_path)
    for platform, tier, observed in (("NA1", "GOLD", 10), ("NA1", "EMERALD", 20), ("KR", "CHALLENGER", 30)):
        state.conn.execute("""INSERT INTO rank_observations
          (puuid,platform,tier,division,lp,queue,observed_at,source,raw_json)
          VALUES ('p0',?,?,'I',10,'RANKED_SOLO_5x5',?,'test','{}')""", (platform, tier, observed))
    state.close()
    output = tmp_path / "manifests" / "all.jsonl"
    assert export_manifest(tmp_path, output) == 1
    item = json.loads(output.read_text())
    assert item["game_creation"] == 1_700_000_000_123
    assert item["patch"] == "16.20"
    assert item["seed_tier"] == "MASTER"
    assert item["fraction_participants_with_known_rank"] == 0.1
    assert item["known_participant_rank_distribution"] == {"EMERALD": 1}
    assert item["estimated_skill_population"] == "EMERALD"
    assert "not historical match rank" in item["rank_interpretation"]
    assert "current" in item["rank_interpretation"]
    assert storage.read(item["raw_path"]) == raw


def test_status_and_validation_report_corruption_without_rewriting(tmp_path):
    state, storage, raw = populate(tmp_path)
    state.close()
    assert validate(tmp_path)["ok"]
    info = status(tmp_path)
    assert info["matches"] == 1 and info["per_platform"] == {"NA1": 1}
    assert info["mean_compressed_bytes_per_match"] > 0
    assert info["estimated_additional_matches_until_disk_floor"] > 0
    path = storage.path("NA1_100")
    path.write_bytes(b"damaged")
    report = validate(tmp_path)
    assert report["ok"] is False
    assert report["checked"]["matches"] == 1
    assert path.read_bytes() == b"damaged"


def test_manifest_cannot_overwrite_raw_or_state(tmp_path):
    state, storage, raw = populate(tmp_path)
    state.close()
    for target in (storage.path("NA1_100"), tmp_path / "state.sqlite3", tmp_path / "rate_state.json", tmp_path / "logs" / "collector.log"):
        with pytest.raises(ValueError):
            export_manifest(tmp_path, target)
    assert storage.read(storage.path("NA1_100").relative_to(tmp_path)) == raw


def test_latest_negative_rank_observation_does_not_reuse_stale_rank(tmp_path):
    state, _, _ = populate(tmp_path)
    for tier, timestamp in (("MASTER", 1), (None, 2)):
        state.conn.execute("""INSERT INTO rank_observations
          (puuid,platform,tier,queue,observed_at,source,raw_json)
          VALUES ('p0','NA1',?,'RANKED_SOLO_5x5',?,'test','{}')""", (tier, timestamp))
    state.close()
    output = tmp_path / "manifests" / "negative.jsonl"
    export_manifest(tmp_path, output)
    assert json.loads(output.read_text())["fraction_participants_with_known_rank"] == 0


def test_dataset_lock_prevents_two_writers_and_recovers_after_process_crash(tmp_path):
    with DatasetLock(tmp_path):
        with pytest.raises(DatasetBusyError):
            with DatasetLock(tmp_path):
                pass
    child = subprocess.run([sys.executable, "-c",
        "from pathlib import Path; import os,sys; from riot_collector.locking import DatasetLock; "
        "lock=DatasetLock(Path(sys.argv[1])); lock.__enter__(); os._exit(19)", str(tmp_path)], check=False)
    assert child.returncode == 19
    with DatasetLock(tmp_path):
        assert (tmp_path / "collector.pid").exists()
    assert not (tmp_path / "collector.pid").exists()


def test_dataset_pid_is_removed_while_lock_is_still_owned(tmp_path, monkeypatch):
    original_unlink = Path.unlink
    checked = []

    def checked_unlink(path, *args, **kwargs):
        if path.name == "collector.pid":
            with pytest.raises(DatasetBusyError):
                with DatasetLock(tmp_path):
                    pass
            checked.append(True)
        return original_unlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", checked_unlink)
    with DatasetLock(tmp_path):
        pass
    assert checked == [True]


@pytest.mark.parametrize("text", [
    "[collector]\nhistory_page_size=101", "[collector]\nplatforms=['NA1','NA1']",
    "[collector]\nmin_free_gb=-1", "[collector]\nunknown=true",
    "[rate_limits]\napplication=[[0,120]]", "[rate_limits]\nsafety_margin=1.0",
])
def test_invalid_config_is_rejected(tmp_path, text):
    path = tmp_path / "config.toml"
    path.write_text(text)
    with pytest.raises(ValueError):
        load_config(path)


def test_config_resolves_data_relative_to_config_file(tmp_path):
    path = tmp_path / "config.toml"
    path.write_text("[collector]\ndata_dir='dataset'\n")
    assert load_config(path).data_dir == tmp_path / "dataset"


def test_missing_key_stops_before_creating_dataset(tmp_path, monkeypatch, capsys):
    monkeypatch.delenv("RIOT_API_KEY", raising=False)
    path = tmp_path / "config.toml"
    path.write_text("[collector]\n")
    assert main(["--config", str(path), "collect"]) == 2
    assert "RIOT_API_KEY is not set" in capsys.readouterr().err
    assert not (tmp_path / "data").exists()


def test_cli_smoke_reopens_for_second_pass_without_stop_event_left_set(tmp_path, monkeypatch):
    from riot_collector import cli, engine
    from riot_collector.api import RiotClient
    from riot_collector.config import Config
    from types import SimpleNamespace

    visits = []

    class FakeCollector:
        def __init__(self, config, state, storage, client):
            self.state = state

        async def run(self, stop, **kwargs):
            assert not stop.is_set()
            visits.append(id(self.state))
            stop.set()
            return {"downloaded": {"NA1": 1, "EUW1": 1, "KR": 1}}

    monkeypatch.setattr(engine, "Collector", FakeCollector)
    args = SimpleNamespace(command="smoke", max_seconds=1, per_region=1)
    asyncio.run(cli.run_collection(Config(data_dir=tmp_path), args, "test-key"))
    assert len(visits) == 2
