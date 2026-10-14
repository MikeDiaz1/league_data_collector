"""Exercise real CLI, scheduler, HTTP limiter, SQLite and files together offline."""
import asyncio
from collections import Counter
from dataclasses import replace
import json
from types import SimpleNamespace

import httpx

from riot_collector import api, cli
from riot_collector.config import Config
from riot_collector.dataset import validate
from riot_collector.locking import DatasetLock
from riot_collector.state import State
from test_engine import match_bytes


def test_real_smoke_two_resumable_runs_on_three_regions_with_mocked_http(tmp_path, monkeypatch):
    real_client = api.RiotClient
    calls = []
    clock = [1_700_000_000.0]

    async def sleep(seconds):
        clock[0] += seconds
        await asyncio.sleep(0)

    routes = {"na1": "NA1", "americas": "NA1", "euw1": "EUW1", "europe": "EUW1", "kr": "KR", "asia": "KR"}

    def handle(request):
        assert request.headers["X-Riot-Token"] == "offline-fixture-key"
        host = request.url.host
        assert host.endswith(".api.riotgames.com")
        platform = routes[host.split(".")[0]]
        path = request.url.path
        calls.append((host, path))
        if "leagues/by-queue" in path:
            body = {"queue": "RANKED_SOLO_5x5", "entries": [
                {"puuid": f"{platform}-seed", "rank": "I", "leaguePoints": 123}]}
        elif path.startswith("/lol/league/"):
            body = []
        elif path.endswith("/ids"):
            assert request.url.params["queue"] == "420"
            body = [f"{platform}_1", f"{platform}_2"]
        else:
            return httpx.Response(200, content=match_bytes(path.rsplit("/", 1)[-1]))
        return httpx.Response(200, json=body)

    def mocked_client(key, data_dir, **kwargs):
        return real_client(key, data_dir, **kwargs, transport=httpx.MockTransport(handle),
                           clock=lambda: clock[0], sleep=sleep)

    monkeypatch.setattr(api, "RiotClient", mocked_client)
    cfg = replace(Config(), data_dir=tmp_path, min_free_gb=0,
                  progress_seconds=0.1, app_limits=((1000, 1),), safety_margin=0)
    with DatasetLock(tmp_path):
        asyncio.run(cli.run_collection(cfg, SimpleNamespace(command="smoke", max_seconds=5, per_region=1),
                                      "offline-fixture-key"))
    state = State(tmp_path)
    try:
        assert state.stats()["per_region"] == {"NA1": 2, "EUW1": 2, "KR": 2}
        assert state.stats()["timelines"] == 0
        assert state.stats()["rank_observations"] >= 3
        assert state.conn.execute("SELECT count(*) FROM match_participants").fetchone()[0] == 60
        assert state.pending_count() > 0
    finally:
        state.close()
    downloaded = Counter(path for _, path in calls if path.startswith("/lol/match/v5/matches/") and not path.endswith("/ids"))
    assert len(downloaded) == 6
    assert set(downloaded.values()) == {1}
    assert validate(tmp_path)["ok"]
