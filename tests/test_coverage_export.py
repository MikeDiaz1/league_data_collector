"""Report rendering, output isolation and read-only CLI safety."""
import csv
import json
from pathlib import Path

import pytest

from riot_collector import cli
from riot_collector.coverage_export import write_coverage_report, render_html
from riot_collector.state import State
from test_cli_dataset import populate


def example_report():
    row = {"region": "NA1", "stream": "recent", "seed_tier": "EMERALD", "new_matches": 2,
           "unique_players": 15, "participant_appearances": 20, "repeat_appearance_fraction": .25,
           "median_lag_hours": 12, "p90_lag_hours": 18, "discovery_routes": 3,
           "distinct_discovered_matches": 2, "within_slice_repeat_fraction": 1 / 3,
           "already_stored_routes": 1, "pending_discovered_matches": 0,
           "completed_recent_history_jobs": 1, "empty_recent_history_jobs": 0,
           "capped_recent_history_jobs": 0}
    return {"report_version": 1, "generated_at_utc": "2026-10-15T00:00:00Z",
            "window": {"since_utc": "2026-10-14T00:00:00Z", "until_utc": "2026-10-15T00:00:00Z"},
            "summary": {"total_matches": 22, "window_matches": 2, "window_unique_players": 15},
            "coverage": [{"region": "NA1", "seed_tier": "EMERALD", "patch": "16.20", "matches": 22}],
            "yield_rows": [dict(row, patch="16.20")], "group_yield": [row],
            "budget_service": [{"region": "NA1", "stream": "recent", "seed_tier": "EMERALD", "kind": "match", "claims": 5}],
            "configured_budgets": {"weights": {"recent": 8, "participant": 1, "backfill": 1}},
            "notes": ["Seed rank is not historical lobby rank."]}


def test_bundle_contains_readable_and_machine_readable_aggregate_views(tmp_path):
    report = example_report()
    destination = write_coverage_report(report, tmp_path)
    assert destination.parent == tmp_path / "reports"
    assert {p.name for p in destination.iterdir()} == {
        "report.html", "report.json", "coverage.csv", "yield.csv", "group-yield.csv", "service.csv"}
    assert json.loads((destination / "report.json").read_text()) == report
    page = (destination / "report.html").read_text(encoding="utf-8")
    assert "25.0%" in page and "33.3%" in page
    assert 'href="yield.csv"' in page and 'data-tab="budgets"' in page
    assert "not API calls" in page
    assert "<script src=" not in page
    with (destination / "group-yield.csv").open(encoding="utf-8", newline="") as stream:
        rows = list(csv.DictReader(stream))
    assert rows[0]["new_matches"] == "2"
    assert rows[0]["repeat_appearance_fraction"] == "0.25"


def test_no_overwrite_or_output_into_raw_state_manifests_or_backups(tmp_path):
    report = example_report()
    for name in ("raw/new", "manifests/new", "backups/new", "logs/new", "state.sqlite3", "reports"):
        with pytest.raises(ValueError):
            write_coverage_report(report, tmp_path, tmp_path / name)
    assert list(tmp_path.iterdir()) == []
    destination = write_coverage_report(report, tmp_path, tmp_path / "reports/first")
    saved = (destination / "report.json").read_bytes()
    with pytest.raises(FileExistsError):
        write_coverage_report(report, tmp_path, destination)
    assert (destination / "report.json").read_bytes() == saved


def test_untrusted_labels_are_escaped_in_html_and_spreadsheet_csv(tmp_path):
    report = example_report()
    label = '=1+1<script>alert("unsafe")</script>'
    report["coverage"][0]["patch"] = label
    page = render_html(report)
    assert label not in page
    assert "&lt;script&gt;" in page
    destination = write_coverage_report(report, tmp_path)
    with (destination / "coverage.csv").open(encoding="utf-8", newline="") as stream:
        assert list(csv.DictReader(stream))[0]["patch"] == "'" + label


def test_cli_report_needs_no_key_writer_lock_or_state_initialization(tmp_path, monkeypatch, capsys):
    data = tmp_path / "data"
    state, storage, raw = populate(data)
    state.close()
    config = tmp_path / "collector.toml"
    config.write_text("[collector]\ndata_dir='data'\n")
    (data / "collector.pid").write_text("synthetic running owner")
    (data / "rate_state.json").write_text("synthetic quota protection")
    before = {name: (data / name).read_bytes() for name in ("state.sqlite3", "collector.pid", "rate_state.json")}
    monkeypatch.delenv("RIOT_API_KEY", raising=False)

    def forbidden(*_args, **_kwargs):
        raise AssertionError("Report must not acquire writer lock or initialize State")

    monkeypatch.setattr(cli, "DatasetLock", forbidden)
    monkeypatch.setattr(State, "__init__", forbidden)
    assert cli.main(["--config", str(config), "coverage-report"]) == 0
    output = json.loads(capsys.readouterr().out)
    assert Path(output["report"]).exists()
    assert output["window"]["selection"] == "balanced-sampling migration"
    assert output["new_matches"] == 1
    assert all((data / name).read_bytes() == contents for name, contents in before.items())
    assert storage.read(storage.path("NA1_100").relative_to(data)) == raw
    assert not (data / "stop.request").exists()


@pytest.mark.parametrize("options", [["--hours", "0"], ["--hours", "nan"],
                                    ["--since", "2026-10-14T12:00:00"]])
def test_invalid_window_fails_without_creating_output(tmp_path, capsys, options):
    config = tmp_path / "collector.toml"
    config.write_text("[collector]\ndata_dir='data'\n")
    assert cli.main(["--config", str(config), "coverage-report", *options]) == 2
    assert not (tmp_path / "data").exists()
    assert capsys.readouterr().err
