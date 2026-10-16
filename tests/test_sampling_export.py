"""Read-only sampling audits and exports use synthetic datasets only."""
import hashlib
import json
import sqlite3

from riot_collector.dataset import export_manifest, status
from test_cli_dataset import populate


def record_route(state, match_id, key, tier):
    payload = {"puuid": f"synthetic-{key}", "sampling_version": "balanced-recent-v1",
               "cohort": "synthetic-cohort", "sampling_stream": "recent",
               "sampling_tier": tier, "start_time": 100, "end_time": 200,
               "future_field": {"kept": [True, None]}}
    state.enqueue("recent_history", key, "NA1", payload,
                  sampling_stream="recent", sampling_tier=tier)
    history_id = state.conn.execute("SELECT id FROM jobs WHERE key=?", (key,)).fetchone()[0]
    state.record_discovery(match_id, history_id, "NA1", payload)
    return history_id, payload


def test_sampling_export_keeps_routes_and_original_match_metadata(tmp_path):
    state, storage, raw = populate(tmp_path)
    original = dict(state.conn.execute("SELECT * FROM matches").fetchone())
    compressed = storage.path("NA1_100").read_bytes()
    routes = [record_route(state, "NA1_100", "route-a", "EMERALD"),
              record_route(state, "NA1_100", "route-b", "CHALLENGER")]
    state.close()
    output = tmp_path / "manifests" / "sampling.jsonl"
    assert export_manifest(tmp_path, output) == 1
    item = json.loads(output.read_text())
    assert item["manifest_version"] == 2
    assert {key: item[key] for key in original} == original
    assert item["seed_tier"] == "MASTER"
    assert item["sha256"] == hashlib.sha256(raw).hexdigest()
    discoveries = item["sampling_discoveries"]
    assert [entry["history_job_id"] for entry in discoveries] == [route[0] for route in routes]
    assert [entry["history_payload"] for entry in discoveries] == [route[1] for route in routes]
    assert all(entry["platform"] == "NA1" and entry["discovered_at"] > 0 for entry in discoveries)
    assert storage.read(item["raw_path"]) == raw
    assert storage.path("NA1_100").read_bytes() == compressed
    # Version 2 represents matches predating the new discovery tracking explicitly.
    with sqlite3.connect(tmp_path / "state.sqlite3") as conn:
        conn.execute("DELETE FROM match_discoveries")
    export_manifest(tmp_path, output)
    assert json.loads(output.read_text())["sampling_discoveries"] == []


def test_status_reports_persisted_service_and_current_discovery_overlap(tmp_path):
    state, _, _ = populate(tmp_path)
    sample = {"version": "balanced-v1", "groups": [{"tier": "EMERALD", "count": 3}],
              "served": [{"platform": "NA1", "stream": "recent", "count": 2}]}
    state.set_meta("progress", {"updated_at": 123, "sampling": sample})
    record_route(state, "NA1_100", "route-a", "EMERALD")
    record_route(state, "NA1_100", "route-b", "CHALLENGER")
    record_route(state, "NA1_200", "route-c", "DIAMOND")
    state.close()
    report = status(tmp_path)
    assert report["last_progress"]["sampling"] == sample
    assert report["sampling"] == dict(sample, discovery_routes=3,
                                      distinct_discovered_matches=2,
                                      repeated_discovery_routes=1)
    assert report["matches"] == 1  # Discovery counts also cover queued match IDs.


def test_legacy_status_and_manifest_require_no_schema_migration(tmp_path):
    state, storage, raw = populate(tmp_path)
    state.conn.execute("DROP TABLE match_discoveries")
    state.conn.execute("DROP INDEX jobs_sampling_claim")
    state.conn.execute("ALTER TABLE jobs DROP COLUMN sampling_stream")
    state.conn.execute("ALTER TABLE jobs DROP COLUMN sampling_tier")
    state.close()
    with sqlite3.connect(tmp_path / "state.sqlite3") as conn:
        schema = conn.execute("SELECT sql FROM sqlite_master ORDER BY name").fetchall()
    output = tmp_path / "manifests" / "legacy.jsonl"
    assert export_manifest(tmp_path, output) == 1
    item = json.loads(output.read_text())
    assert item["manifest_version"] == 1
    assert "sampling_discoveries" not in item
    assert status(tmp_path)["sampling"] is None
    with sqlite3.connect(tmp_path / "state.sqlite3") as conn:
        assert conn.execute("SELECT sql FROM sqlite_master ORDER BY name").fetchall() == schema
    assert storage.read(item["raw_path"]) == raw
