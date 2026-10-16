"""Balance guidance must not turn sparse or ambiguous evidence into certainty."""
from copy import deepcopy

from riot_collector.coverage import TARGET_TIERS
from riot_collector.coverage_assessment import assess_balance
from riot_collector.coverage_export import render_html
from test_coverage_export import example_report


def balanced_report():
    report = example_report()
    report["configured_budgets"]["platforms"] = ["NA1", "KR"]
    report["group_yield"] = [dict(report["group_yield"][0], region=region, seed_tier=tier,
                                 new_matches=100) for region in ["NA1", "KR"] for tier in TARGET_TIERS]
    return report


def test_equal_tiers_and_regions_pass_without_mutating_input():
    report = balanced_report()
    before = deepcopy(report)
    result = assess_balance(report)
    assert result["verdict"] == "Tier coverage is within the working range"
    assert all(row["share"] == .2 for row in result["rows"])
    assert all(row["status"] == "Within target range" for row in result["region_rows"])
    assert report == before


def test_missing_tier_is_zero_not_silently_removed():
    report = balanced_report()
    report["group_yield"] = [r for r in report["group_yield"] if r["seed_tier"] != "EMERALD"]
    result = assess_balance(report)
    assert result["verdict"] == "Tier coverage needs review"
    emerald = [r for r in result["rows"] if r["seed_tier"] == "EMERALD"]
    assert len(emerald) == 2
    assert all(r["share"] == 0 and r["status"] == "Below target" for r in emerald)


def test_missing_configured_region_is_included_and_flagged_regionally():
    report = balanced_report()
    report["configured_budgets"]["platforms"].append("EUW1")
    result = assess_balance(report)
    assert result["verdict"] == "Too early to judge overall balance"
    assert len(result["rows"]) == 15
    assert next(r for r in result["region_rows"] if r["region"] == "EUW1")["status"] == "Below target"


def test_equal_tiers_do_not_hide_regional_imbalance_or_other_streams():
    report = balanced_report()
    for row in report["group_yield"]:
        if row["region"] == "KR":
            row["new_matches"] = 1000
    report["group_yield"].append(dict(report["group_yield"][0], stream="backfill", new_matches=100000))
    result = assess_balance(report)
    assert result["verdict"] == "Regional coverage needs review"
    assert all(row["share"] == .2 for row in result["rows"])


def test_small_or_short_window_is_not_judged_balanced():
    report = balanced_report()
    report["window"]["since_utc"] = "2026-10-14T23:00:00Z"
    assert assess_balance(report)["verdict"] == "Too early to judge overall balance"
    report = balanced_report()
    for row in report["group_yield"]:
        row["new_matches"] = 1
    assert assess_balance(report)["verdict"] == "Too early to judge overall balance"


def test_working_range_boundaries_are_inclusive():
    report = balanced_report()
    for region in ("NA1", "KR"):
        for tier, count in zip(TARGET_TIERS, [75, 125, 100, 100, 100]):
            next(r for r in report["group_yield"] if r["region"] == region and r["seed_tier"] == tier)["new_matches"] = count
    assert assess_balance(report)["verdict"] == "Tier coverage is within the working range"
    report["group_yield"][0]["new_matches"] = 74
    report["group_yield"][1]["new_matches"] = 126
    assert assess_balance(report)["verdict"] == "Tier coverage needs review"


def test_unknown_attribution_blocks_positive_verdict():
    report = balanced_report()
    report["group_yield"].append(dict(report["group_yield"][0], stream="UNKNOWN", new_matches=1))
    assert assess_balance(report)["verdict"] == "Attribution incomplete"
    report = balanced_report()
    report["metric_availability"] = {"sampling_attribution": False}
    assert assess_balance(report)["verdict"] == "Attribution incomplete"


def test_diagnostics_require_denominators_and_known_outcomes():
    report = balanced_report()
    row = report["group_yield"][0]
    row.update(completed_recent_history_jobs=20, empty_recent_history_jobs=15,
               capped_recent_history_jobs=0, unknown_recent_history_outcomes=0,
               discovery_routes=100, already_stored_routes=80, pending_discovered_matches=200)
    messages = " ".join(assess_balance(report)["diagnostics"])
    assert "15/20" in messages and "80/100" in messages and "200" in messages
    row["unknown_recent_history_outcomes"] = 1
    assert "empty history checks" not in " ".join(assess_balance(report)["diagnostics"])
    row.update(discovery_routes=None, already_stored_routes=None, pending_discovered_matches=None)
    assert not assess_balance(report)["diagnostics"]


def test_balance_is_default_with_glossary_and_optional_detail_columns():
    report = balanced_report()
    report["group_yield"][0]["region"] = '<script>alert("x")</script>'
    report["configured_budgets"]["platforms"].append('<script>alert("x")</script>')
    page = render_html(report)
    assert 'data-tab="balance" aria-pressed="true"' in page
    assert '<section id="groups" class="view" hidden>' in page
    assert 'id="show-diagnostics"' in page
    assert "Plain-language glossary" in page
    assert "20% per tier" in page and "not statistical confidence limits" in page
    assert '<script>alert("x")</script>' not in page
    assert "&lt;script&gt;" in page
    assert "does not change with filters" in page
