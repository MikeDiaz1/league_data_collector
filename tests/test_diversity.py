import asyncio
from collections import Counter
import json

import httpx
import pytest

from riot_collector.api import RiotClient
from riot_collector.coverage import coverage_report
from riot_collector.diversity import gameplay_signals
from riot_collector.state import State
from test_engine import match_bytes


def body(match_id="NA1_1", *, new_account=False):
    raw = json.loads(match_bytes(match_id))
    roles = ("TOP", "JUNGLE", "MIDDLE", "BOTTOM", "UTILITY")
    for i, player in enumerate(raw["info"]["participants"]):
        player.update(championId=i + 1, teamId=100 if i < 5 else 200, teamPosition=roles[i % 5])
    if new_account:
        raw["info"]["participants"][0]["puuid"] = "new-private-account"
        raw["metadata"]["participants"][0] = "new-private-account"
    return raw


def save(state, raw):
    match_id = raw["metadata"]["matchId"]
    state.enqueue("match", match_id, "NA1", {"match_id": match_id}, sampling_stream="recent", sampling_tier="EMERALD")
    job = state.claim("match", "NA1")
    stored = {"raw_path": f"synthetic/{match_id}.json.gz", "sha256": "a" * 64, "compressed_bytes": 100}
    assert state.commit_match(job, raw, stored, queue_history=False)
    return job, stored


def test_novel_accounts_use_entire_participant_archive_and_duplicates_add_nothing(tmp_path):
    state = State(tmp_path)
    first = body()
    # Already known as a ladder seed is still new to the participant archive.
    state.conn.execute("INSERT INTO players(puuid,platform,discovered_at) VALUES ('player-0','NA1',1)")
    save(state, first)
    # Model a pre-telemetry match: preserve its participant baseline, remove only synthetic derived rows.
    state.conn.execute("DELETE FROM match_diversity")
    state.conn.execute("DELETE FROM match_gameplay_signals")
    state.conn.execute("DELETE FROM gameplay_signals")
    job, stored = save(state, body("NA1_2", new_account=True))
    record = dict(state.conn.execute("SELECT * FROM match_diversity").fetchone())
    assert record["new_accounts"] == 1
    assert record["source_sha256"] == stored["sha256"]
    assert record["new_champion_roles"] == 10 and record["new_matchups"] == 5
    assert not state.commit_match(job, body("NA1_2"), stored)
    assert dict(state.conn.execute("SELECT * FROM match_diversity").fetchone()) == record
    save(state, body("NA1_3", new_account=True))
    last = dict(state.conn.execute("SELECT * FROM match_diversity WHERE match_id='NA1_3'").fetchone())
    assert last["new_accounts"] == last["new_champion_roles"] == last["new_matchups"] == 0
    state.close()


def test_ambiguous_or_missing_roles_do_not_invent_matchups():
    raw = body()["info"]
    signals, known = gameplay_signals(raw)
    assert known == 10
    assert Counter(k for k, _ in signals) == {"champion_role": 10, "matchup": 5}
    raw["participants"][0]["teamPosition"] = []
    raw["participants"][1]["teamPosition"] = "FUTURE_ROLE"
    raw["participants"][2]["teamId"] = 200  # Both middle players now on same team.
    signals, known = gameplay_signals(raw)
    assert known == 8
    assert sum(k == "matchup" for k, _ in signals) == 2


def test_match_and_telemetry_commit_roll_back_together(tmp_path, monkeypatch):
    state = State(tmp_path)
    from riot_collector import state as module
    original = module.record_match_diversity
    def fail(*args):
        original(*args)
        raise RuntimeError("synthetic interrupted commit")
    monkeypatch.setattr(module, "record_match_diversity", fail)
    with pytest.raises(RuntimeError):
        save(state, body())
    for table in ("matches", "match_participants", "match_diversity", "gameplay_signals", "match_gameplay_signals"):
        assert state.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == 0
    state.close()


def test_report_shows_window_throughput_concentration_and_no_identifiers(tmp_path):
    state = State(tmp_path)
    state.set_meta("diversity_telemetry_v1", {"started_at": 0})
    save(state, body())
    save(state, body("NA1_2", new_account=True))
    state.conn.execute("UPDATE matches SET completed_at=100")
    state.conn.execute("UPDATE match_diversity SET recorded_at=100")
    for timestamp in (99, 100, 199, 200):
        state.conn.execute("INSERT INTO api_request_events(dispatched_at,platform,stream,seed_tier,kind,method) VALUES (?,'NA1','recent','EMERALD','match','match')", (timestamp,))
    state.close()
    r = coverage_report(tmp_path, since=100, until=200)
    d = r["diversity"]
    assert d["summary"]["measured_games"] == 2
    assert d["summary"]["new_accounts"] == 11
    assert d["summary"]["request_attempts"] == 2
    assert d["summary"]["new_accounts_per_100_games"] == 550
    assert d["summary"]["new_accounts_per_100_requests"] == 550
    assert d["summary"]["champion_role_combinations"] == 10
    assert d["summary"]["matchup_combinations"] == 5
    assert d["concentration"]["unique_accounts"] == 11
    assert d["concentration"]["top_accounts_count"] == 1
    assert d["concentration"]["top_one_percent_appearance_share"] == .1
    assert "new-private-account" not in json.dumps(r)
    from riot_collector.coverage_export import write_coverage_report
    bundle = write_coverage_report(r, tmp_path)
    page = (bundle / "report.html").read_text(encoding="utf-8")
    assert "Player and gameplay variety" in page
    assert "entire participant archive" in page
    for name in ("diversity.csv", "gameplay-combinations.csv", "selection-rules.csv"):
        assert (bundle / name).is_file()
        assert "new-private-account" not in (bundle / name).read_text(encoding="utf-8")
    assert "new-private-account" not in page


def test_http_observer_counts_retries_without_recording_url_or_key(tmp_path):
    events = []
    async def run():
        attempts = 0
        def handler(request):
            nonlocal attempts
            attempts += 1
            return httpx.Response(500 if attempts == 1 else 200, content=b"[]")
        async def sleep(_):
            pass
        async with RiotClient("synthetic-test-key", tmp_path, transport=httpx.MockTransport(handler),
                              sleep=sleep, backoff_base=0, app_limits=((1000, 1),)) as client:
            client.request_observer = lambda *event: events.append(event)
            assert await client.get("americas", "/synthetic-private-path", "match_ids", "match-v5") == b"[]"
    asyncio.run(run())
    assert len(events) == 2
    assert all(e[:2] == ("americas", "match_ids") for e in events)
    assert "private-path" not in str(events) and "synthetic-test-key" not in str(events)
