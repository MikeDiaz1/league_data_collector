"""Synthetic pagination and frozen observation evidence; no live data or HTTP."""
import json

import pytest

from riot_collector.coverage import coverage_report
from riot_collector.coverage_export import render_html, write_coverage_report
from test_coverage import database


@pytest.fixture
def db(tmp_path):
    conn = database(tmp_path)
    conn.executescript("""
        ALTER TABLE jobs ADD COLUMN created_at REAL;
        CREATE TABLE rank_observations(id INTEGER PRIMARY KEY,puuid TEXT,platform TEXT,tier TEXT,division TEXT);
    """)
    yield conn
    conn.close()


def add_job(db, *, kind, payload, status="done", created=200, updated=300, tier="EMERALD", region="NA1"):
    index = db.execute("SELECT COALESCE(MAX(id),0)+1 FROM jobs").fetchone()[0]
    db.execute("INSERT INTO jobs(id,kind,key,platform,payload,status,updated_at,sampling_stream,sampling_tier,created_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
               (index, kind, f"private-job-{index}", region, json.dumps(payload), status, updated,
                "recent" if kind == "recent_history" else "seed", tier, created))
    return index


def page(db, division="I", *, bucket=1, page=1, status="done", updated=100, **kwargs):
    tier = kwargs.get("tier", "EMERALD")
    return add_job(db, kind="seed", payload={"tier": tier, "division": division, "bucket": bucket, "page": page},
                   status=status, created=50, updated=updated, **kwargs)


def full_scan(db, *, bucket=1, updated=100):
    for division in ("I", "II", "III", "IV"):
        page(db, division, bucket=bucket, updated=updated)


def history(db, division="I", *, returned=0, observation=True, **kwargs):
    index = db.execute("SELECT COALESCE(MAX(id),0)+1 FROM jobs").fetchone()[0]
    payload = {"puuid": f"private-player-{index}", "seed_rank_observation_id": index,
               "candidate_players": 1000, "returned_matches": returned}
    if observation:
        db.execute("INSERT INTO rank_observations VALUES (?,?,?,?,?)",
                   (index, payload["puuid"], kwargs.get("region", "NA1"), kwargs.get("tier", "EMERALD"), division))
    return add_job(db, kind="recent_history", payload=payload, **kwargs)


def report(db, tmp_path, *, since=250, until=350, platforms=("NA1",)):
    db.commit()
    return coverage_report(tmp_path, since=since, until=until, platforms=platforms)


def readiness(r, region="NA1", tier="EMERALD"):
    return next(x for x in r["seed_coverage"]["readiness"] if x["region"] == region and x["seed_tier"] == tier)


def division(r, name="I"):
    return next(x for x in r["seed_coverage"]["division_history"]
                if x["region"] == "NA1" and x["seed_tier"] == "EMERALD" and x["division"] == name)


def test_ready_needs_full_scan_and_post_selection_checks_in_every_division(db, tmp_path):
    full_scan(db)
    for d in ("I", "II", "III", "IV"):
        for i in range(20):
            history(db, d, returned=0 if i < 5 else 10)
    r = report(db, tmp_path)
    checkpoint = readiness(r)
    assert checkpoint["status"] == "Ready to reassess"
    assert checkpoint["latest_scan_divisions_finished"] == checkpoint["divisions_with_enough_checks"] == 4
    assert division(r)["empty_rate"] == division(r)["post_empty_rate"] == .25
    assert checkpoint["latest_cohort_candidates"] == 1000
    encoded = json.dumps(r)
    assert "private-player" not in encoded and "private-job" not in encoded


def test_pre_scan_cohorts_do_not_become_post_scan_when_they_finish_later(db, tmp_path):
    full_scan(db, updated=210)
    history(db, created=200, updated=300)
    r = report(db, tmp_path)
    assert division(r)["known_checks"] == 1
    assert division(r)["post_known_checks"] == 0
    assert readiness(r)["status"] == "Waiting for a post-scan cohort"


def test_zero_missing_and_failed_ladder_chains_never_look_complete(db, tmp_path):
    page(db, "I", page=1)
    page(db, "I", page=3)  # Missing page 2.
    page(db, "II", status="failed")
    page(db, "III", status="pending")
    r = report(db, tmp_path, platforms=("NA1", "KR"))
    assert readiness(r)["status"] == "Waiting for a full ladder scan"
    rows = {x["division"]: x for x in r["seed_coverage"]["ladder_progress"] if x["region"] == "NA1" and x["seed_tier"] == "EMERALD"}
    assert rows["I"]["status"] == "Page sequence incomplete"
    assert rows["II"]["status"] == "Needs attention"
    assert rows["III"]["next_page"] == 1
    assert rows["IV"]["status"] == "No recorded pages"
    assert readiness(r, "KR")["latest_scan_divisions_finished"] == 0


def test_four_divisions_from_different_scans_are_not_one_full_scan(db, tmp_path):
    for i, d in enumerate(("I", "II", "III", "IV")):
        page(db, d, bucket=i)
    assert readiness(report(db, tmp_path))["last_full_scan_utc"] is None


def test_new_partial_scan_does_not_erase_previous_full_scan(db, tmp_path):
    full_scan(db)
    page(db, "I", bucket=2, status="pending")
    history(db)
    r = report(db, tmp_path)
    row = readiness(r)
    assert row["last_full_scan_id"] == 1 and row["latest_scan_id"] == 2
    assert row["unfinished_scans"] == 1
    assert row["latest_scan_divisions_finished"] == 0
    assert division(r)["post_known_checks"] == 1
    assert len([x for x in r["seed_coverage"]["ladder_progress"] if x.get("is_latest_scan") and x["seed_tier"] == "EMERALD"]) == 4


def test_latest_full_scan_resets_post_scan_comparison(db, tmp_path):
    full_scan(db)
    history(db, created=200)
    full_scan(db, bucket=2, updated=250)
    r = report(db, tmp_path)
    assert readiness(r)["last_full_scan_id"] == 2
    assert readiness(r)["status"] == "Waiting for a post-scan cohort"
    assert division(r)["known_checks"] == 1 and division(r)["post_known_checks"] == 0


def test_division_uses_frozen_observation_and_unknown_outcomes_are_not_empty(db, tmp_path):
    full_scan(db)
    index = history(db, "III", returned=0)
    db.execute("INSERT INTO rank_observations VALUES (?,?,?,?,?)", (99999, f"private-player-{index}", "NA1", "EMERALD", "I"))
    history(db, "III", returned=None)
    history(db, "III", returned=False)  # Boolean is not a returned-match count.
    history(db, "III", returned=5)
    history(db, "II", observation=False)
    history(db, "II", status="failed")
    r = report(db, tmp_path)
    row = division(r, "III")
    assert row["completed_checks"] == 4 and row["known_checks"] == 2
    assert row["empty_checks"] == 1 and row["empty_rate"] == .5
    assert row["unknown_outcomes"] == 2
    assert division(r, "I")["known_checks"] == 0
    assert division(r, "UNKNOWN")["known_checks"] == 1
    assert readiness(r)["status"] == "Review failed or skipped checks"


@pytest.mark.parametrize("status,expected", [("pending", "Post-scan checks still running"),
    ("skipped", "Review failed or skipped checks")])
def test_unfinished_and_skipped_checks_block_readiness(db, tmp_path, status, expected):
    full_scan(db)
    history(db, status=status)
    assert readiness(report(db, tmp_path))["status"] == expected


def test_rates_use_half_open_completion_window_but_ladder_is_snapshot_state(db, tmp_path):
    full_scan(db)
    for timestamp in (249, 250, 349, 350):
        history(db, updated=timestamp)
    r = report(db, tmp_path)
    assert division(r)["known_checks"] == division(r)["post_known_checks"] == 2
    assert readiness(r)["post_scan_selected_checks"] == 4
    assert readiness(r)["last_full_scan_id"] == 1


def test_html_and_csv_explain_scope_and_preserve_privacy(db, tmp_path):
    full_scan(db)
    history(db)
    r = report(db, tmp_path)
    page_text = render_html(r)
    assert 'data-tab="seeds"' in page_text and 'data-filter="division"' in page_text
    assert "not just after 24 hours" in page_text
    assert "Total pages and total players are unknown" in page_text
    assert 'data-division="I"' in page_text
    assert "private-player" not in page_text
    out = write_coverage_report(r, tmp_path)
    for name in ("seed-readiness.csv", "ladder-progress.csv", "division-history.csv"):
        assert (out / name).exists()
        assert "private-player" not in (out / name).read_text(encoding="utf-8")


def test_legacy_missing_selection_times_and_observations_stays_unavailable(tmp_path):
    conn = database(tmp_path, legacy=True)
    conn.close()
    before = (tmp_path / "state.sqlite3").read_bytes()
    r = coverage_report(tmp_path, since=250, until=350, platforms=("NA1",))
    assert readiness(r)["status"] == "Evidence unavailable"
    assert division(r)["known_checks"] is None
    assert division(r)["empty_rate"] is None
    assert (tmp_path / "state.sqlite3").read_bytes() == before
