"""Standalone, aggregate-only report bundles; no collector state writes."""
from __future__ import annotations

import csv
from datetime import datetime, timezone
import html
import json
from pathlib import Path

from .coverage_assessment import assess_balance


def _display(value, *, fraction=False):
    if value is None:
        return "—"
    if isinstance(value, (float, int)):
        if fraction:
            return f"{value:.1%}"
        if isinstance(value, int):
            return f"{value:,}"
        return f"{value:,.2f}"
    return str(value)


GROUP_COLUMNS = (
    ("region", "Region"), ("seed_tier", "Seed tier"), ("stream", "Stream"),
    ("new_matches", "New matches"), ("unique_players", "Unique players"),
    ("participant_appearances", "Player appearances"),
    ("repeat_appearance_fraction", "Repeat appearances"),
    ("median_lag_hours", "Median age at collection · h"),
    ("p90_lag_hours", "P90 age · h"),
    ("discovery_routes", "Recorded routes"),
    ("distinct_discovered_matches", "Distinct discovered matches"),
    ("within_slice_repeat_fraction", "Repeated routes within group"),
    ("already_stored_routes", "Routes to already stored matches"),
    ("undownloaded_discovered_matches", "Not stored by window end"),
    ("pending_discovered_matches", "Pending discovered matches"),
    ("failed_discovered_matches", "Failed discovered matches"),
    ("skipped_discovered_matches", "Skipped discovered matches"),
    ("unknown_status_discovered_matches", "Other unresolved matches"),
    ("completed_recent_history_jobs", "Completed recent histories"),
    ("empty_recent_history_jobs", "Empty recent histories"),
    ("capped_recent_history_jobs", "Histories at match cap"),
    ("selected_recent_history_jobs", "Selected histories · all cohorts"),
    ("pending_recent_history_jobs", "Queued histories · all cohorts"),
    ("in_progress_recent_history_jobs", "Active histories · all cohorts"),
)
PATCH_COLUMNS = GROUP_COLUMNS[:3] + (("patch", "Patch"),) + tuple(
    pair for pair in GROUP_COLUMNS[3:] if not pair[0].endswith("_history_jobs"))
FRACTIONS = {"repeat_appearance_fraction", "within_slice_repeat_fraction", "observed_share", "configured_share", "share", "target", "empty_rate", "post_empty_rate", "known_role_fraction", "top_one_percent_appearance_share"}

# Plain-language labels and definitions shared by every detailed table.
LABELS = {
    "seed_tier": "Seed player's tier", "stream": "Sampling stream",
    "new_matches": "Newly saved games", "unique_players": "Distinct players",
    "participant_appearances": "Player slots across games",
    "repeat_appearance_fraction": "Repeated player slots · %",
    "median_lag_hours": "Typical game age when saved · hours",
    "p90_lag_hours": "90% of games were younger than · hours",
    "discovery_routes": "Recorded game discoveries",
    "distinct_discovered_matches": "Different games discovered",
    "within_slice_repeat_fraction": "Repeated game discoveries · %",
    "already_stored_routes": "Discoveries of games already saved",
    "undownloaded_discovered_matches": "Discovered games not saved by cutoff",
    "completed_recent_history_jobs": "Successful player-history checks",
    "empty_recent_history_jobs": "Checks finding no qualifying games",
    "capped_recent_history_jobs": "Checks reaching the game limit",
    "selected_recent_history_jobs": "All retained history checks · lifetime",
    "pending_recent_history_jobs": "Queued history checks · all cohorts",
    "in_progress_recent_history_jobs": "Active history checks · all cohorts",
}
DEFINITIONS = {
    "seed_tier": "Rank observed for the seed player used to find games. Not the rank of every player in a game or their rank when it was played.",
    "stream": "recent: recent games from ladder seeds; participant: recent games from players discovered in saved games; backfill: older history; seed: ladder maintenance.",
    "new_matches": "Games first saved during the selected report window, regardless of when they were played.",
    "unique_players": "Different players in this row's saved games. Do not add player counts across rows: players can appear in several groups.",
    "participant_appearances": "Player slots across all saved games in this row; normally 10 per game.",
    "repeat_appearance_fraction": "1 minus distinct players divided by player slots. Example: 700 players in 1,000 slots means 30% repeated slots. Not a duplicate-game rate.",
    "median_lag_hours": "Hours from game start to collection. Half the games were this age or younger when saved.",
    "p90_lag_hours": "Hours from game start to collection; 90% were this age or younger when saved.",
    "discovery_routes": "One recorded history-check/game pair. The same game found through different checks counts more than once.",
    "distinct_discovered_matches": "Different game IDs discovered in this row. May overlap other rows and may still await download.",
    "within_slice_repeat_fraction": "1 minus different games discovered divided by recorded discoveries. Not an API-call duplication rate.",
    "already_stored_routes": "Discoveries that pointed to a game already saved at the moment of discovery.",
    "undownloaded_discovered_matches": "Games discovered in the window but not saved by its end. Pending, failed and skipped breakdowns use current job status.",
    "pending_discovered_matches": "Discovered in the window, not saved by its end, and currently queued or being downloaded.",
    "failed_discovered_matches": "Discovered in the window, not saved by its end, and currently marked failed. Inspect collector logs for the cause.",
    "skipped_discovered_matches": "Discovered in the window, not saved by its end, and currently marked skipped by collection rules.",
    "unknown_status_discovered_matches": "Discovered in the window and not saved by its end, with no recognized unresolved job status.",
    "completed_recent_history_jobs": "Successful recent-history checks completed in the window, including empty results.",
    "empty_recent_history_jobs": "Successful recent-history checks with zero qualifying games in the requested history window.",
    "capped_recent_history_jobs": "Checks returning the requested maximum. This does not prove additional games existed.",
    "selected_recent_history_jobs": "All retained recent-history jobs at report creation, including old completed cohorts. Not limited to the report window.",
    "pending_recent_history_jobs": "Queued recent-history jobs across all retained cohorts at report creation.",
    "in_progress_recent_history_jobs": "Active recent-history jobs across all retained cohorts at report creation.",
    "share": "Share of recent seed-attributed newly saved games. Tier shares use their whole region as denominator, even when filtering.",
    "status": "Balance views use equal-coverage review thresholds. Seed coverage uses scan completion and post-scan checks. Neither proves population representativeness; see each view's rules.",
    "division": "Division I–IV at the exact rank observation saved with the history check. UNKNOWN means that observation is missing or does not match the seed.",
    "known_checks": "Successful checks completed in the report window with a known returned-game count; this is the empty-rate denominator, not a count of unique players.",
    "empty_rate": "Successful checks returning zero games divided by successful checks with a known outcome. Errors and unknown outcomes are excluded.",
    "post_known_checks": "Known-outcome checks completed in the report window whose cohorts were selected after the latest full four-division ladder scan finished.",
    "post_empty_rate": "Empty rate restricted to checks selected after the latest full scan, using known outcomes only.",
    "latest_scan_divisions_finished": "Number of division pagination chains that reached their end in the newest scan, out of four. Not a percentage of players discovered.",
    "last_full_scan_utc": "Completion time of the latest scan that finished all four divisions. A newer partial refresh may also be running.",
    "latest_cohort_candidates": "Size of the locally observed tier pool when the latest cohort was selected. Not the live tier population or the current pool size.",
    "divisions_with_enough_checks": "Divisions with at least 20 known-outcome post-scan checks in this report window, out of four. A descriptive review threshold, not statistical confidence.",
    "completed_pages": "Completed page jobs in this scan and division. Includes the final empty page when the chain ends; the total number of pages is not known in advance.",
}
EXTRA_COLUMNS = {key for key, _ in GROUP_COLUMNS} - {
    "region", "seed_tier", "stream", "new_matches", "unique_players",
    "repeat_appearance_fraction", "median_lag_hours",
}


def _table(rows, columns, table_id):
    header = "".join(
        f'<th scope="col" class="{"extra-metric" if key in EXTRA_COLUMNS else ""}"><span title="{html.escape(DEFINITIONS.get(key, label), quote=True)}">'
        f'{html.escape(LABELS.get(key, label))}</span></th>' for key, label in columns)
    body = []
    for row in rows:
        attributes = " ".join(
            f'data-{key}="{html.escape(str(row.get(key, "")), quote=True)}"'
            for key in ("region", "seed_tier", "stream", "patch", "division")
        )
        cells = "".join(
            f'<td class="{"number" if isinstance(row.get(key), (int, float)) else ""} {"extra-metric" if key in EXTRA_COLUMNS else ""}">'
            f'{_cell_value(key, row.get(key))}</td>'
            for key, _ in columns
        )
        body.append(f"<tr {attributes}>{cells}</tr>")
    if not body:
        body.append(f'<tr><td colspan="{len(columns)}">No observations available for this view.</td></tr>')
    return f'<div class="table-scroll"><table id="{table_id}"><thead><tr>{header}</tr></thead><tbody>{"".join(body)}</tbody></table></div>'


def _cell_value(key, value):
    text = html.escape(_display(value, fraction=key in FRACTIONS))
    if key == "status":
        tone = "good" if value in ("Within target range", "Ready to reassess", "End reached (inferred)") else "review" if value in ("Above target", "Below target", "Needs attention", "Page sequence incomplete", "Review failed or skipped checks", "Review missing division or outcome evidence") else "waiting"
        return f'<span class="badge {tone}">{text}</span>'
    return text


def _budget_rows(report):
    totals = {}
    for row in report.get("budget_service", []):
        key = (row["region"], row["stream"])
        totals[key] = totals.get(key, 0) + row["claims"]
    weights = report.get("configured_budgets", {}).get("weights", {})
    weight_sum = sum(weights.values())
    result = []
    for (region, stream), claims in sorted(totals.items()):
        denominator = sum(count for (r, s), count in totals.items() if r == region and s != "seed")
        result.append({"region": region, "stream": stream, "claims": claims,
                       "observed_share": claims / denominator if denominator and stream != "seed" else None,
                       "configured_share": weights.get(stream, 0) / weight_sum if weight_sum and stream != "seed" else None})
    return result


def _balance_html(report):
    assessment = assess_balance(report)
    rows = assessment["rows"]
    table = _table(rows, (("region", "Region"), ("seed_tier", "Seed tier"),
                         ("new_matches", "New games"), ("share", "Share within region · target 20%"),
                         ("status", "Assessment")), "balance-table")
    region_table = _table(assessment["region_rows"], (("region", "Region"), ("new_matches", "New games"),
                         ("share", "Share across regions"), ("target", "Equal-share reference"),
                         ("status", "Assessment")), "region-balance-table")
    items = assessment["diagnostics"]
    diagnostics = "".join(f'<li>{html.escape(item)}</li>' for item in items[:6])
    if not diagnostics:
        diagnostics = "<li>No diagnostic thresholds were triggered. This does not prove budgets are optimal; missing or small samples cannot establish efficiency.</li>"
    more = (f'<details><summary>Show {len(items) - 6} more diagnostic signals</summary><ul>'
            + "".join(f'<li>{html.escape(item)}</li>' for item in items[6:]) + '</ul></details>') if len(items) > 6 else ''
    verdict = html.escape(assessment["verdict"])
    advice = html.escape(assessment["advice"])
    return f'''<section id="balance" class="view">
<div class="assessment"><h2>{verdict}</h2><p>{advice}</p>
<p><strong>Scope:</strong> Recent ladder-seed downloads during the selected window only. Balanced seed sources do not establish historical lobby ranks or representativeness of all players.</p></div>
<p>Before adjusting Emerald or Diamond sampling, check the <strong>Seed coverage</strong> tab: early cohorts may have been selected before their ladder scan finished.</p>
<h2>1. Are all five tiers getting similar coverage?</h2>
<p>Working goal: <strong>20% per tier within each region</strong>. We flag shares outside 15–25% only after a window of at least 24 hours and 250 recent seed-attributed games in that region. These are review thresholds, not statistical confidence limits.</p>
<p class="row-count" aria-live="polite"></p>{table}
<h2>2. Are regions getting similar coverage?</h2>
<p>Reference: equal coverage across configured regions, allowing ±25% of that reference share. This is a comparison for your balanced-coverage goal, not population weighting or an automatic regional budget. We need at least 24 hours and 250 games × number of regions overall.</p>
{region_table}
<h2>3. What might need adjusting?</h2>
<p>Evidence below uses the full report window and does not change with filters. It suggests checks, not automatic budget changes. Confirm persistent patterns across comparable windows.</p>
<ul class="diagnostics">{diagnostics}</ul>
{more}
<details><summary>How these flags are calculated</summary>
<p>Empty or capped checks: at least 20 completed histories with known outcomes and 50% or more meeting the condition. Already-saved discoveries: at least 100 recorded discoveries and 50% overlap. Pending downloads: at least 100; any failed download is called out. Counts are not matched cohorts, so we do not calculate a download conversion rate or matches per API call.</p>
<p>Missing configured regions and tiers remain visible as zero games. Unattributed downloads prevent a positive balance verdict. Some regional rows can be judged before the overall report has enough data. Region or tier filters hide rows only; shares and assessments retain their original denominators.</p></details>
</section>'''


def _seed_html(report):
    seed = report.get("seed_coverage", {})
    readiness = seed.get("readiness", [])
    progress = seed.get("ladder_progress", [])
    latest = [row for row in progress if row.get("is_latest_scan")]
    older = [row for row in progress if not row.get("is_latest_scan")]
    ready = sum(row["status"] == "Ready to reassess" for row in readiness)
    columns = (("region", "Region"), ("seed_tier", "Seed tier"), ("status", "Ready to review?"),
               ("latest_scan_divisions_finished", "Newest scan: divisions finished / 4"),
               ("last_full_scan_utc", "Last full scan finished · UTC"),
               ("divisions_with_enough_checks", "Post-scan divisions with ≥20 checks / 4"),
               ("post_scan_unfinished_checks", "Post-scan checks still running"),
               ("post_scan_failed_or_skipped", "Post-scan failed / skipped checks"),
               ("unfinished_scans", "Unfinished ladder scans"),
               ("latest_cohort_candidates", "Observed pool at latest cohort selection"))
    ladder_columns = (("region", "Region"), ("seed_tier", "Seed tier"), ("division", "Division"),
                      ("scan_id", "Scan ID"), ("completed_pages", "Pages completed"),
                      ("next_page", "Next unfinished page"), ("status", "Ladder progress"),
                      ("failed_pages", "Failed pages"), ("skipped_pages", "Skipped pages"))
    history_columns = (("region", "Region"), ("seed_tier", "Seed tier"), ("division", "Division"),
                       ("known_checks", "Checks with known outcomes"), ("empty_checks", "Empty checks"),
                       ("empty_rate", "Empty rate"), ("unknown_outcomes", "Checks missing outcomes"),
                       ("post_known_checks", "Post-scan known checks"), ("post_empty_checks", "Post-scan empty checks"),
                       ("post_empty_rate", "Post-scan empty rate"))
    notes = "".join(f'<li>{html.escape(note)}</li>' for note in seed.get("notes", []))
    message = (f"{ready} of {len(readiness)} region / tier groups ready to reassess" if readiness
               else "Seed evidence unavailable — generate a new report with the updated reporter")
    older_html = (f'<details><summary>Earlier ladder scans · {len(older)} division rows</summary>'
                  + _table(older, ladder_columns, "older-ladder-table") + '</details>') if older else ''
    return f'''<section id="seeds" class="view" hidden>
<div class="assessment"><h2>{html.escape(message)}</h2>
<p>Reassess after a full ladder scan and new completed cohorts, not just after 24 hours. “Ready” means there is enough post-scan evidence to review; it does not mean the sample is unbiased or settings need changing.</p>
<p>This summary covers all regions and tiers in the report; filters below only hide rows.</p></div>
<div class="section-title"><h2>1. Coverage checkpoints · current snapshot</h2><a href="seed-readiness.csv" download>Download checkpoints</a></div>
<p>All four divisions must finish within the same scan. Then require at least 20 successful checks with known outcomes per division, selected <strong>after</strong> that full scan and completed <strong>inside the report window</strong>. Missing labels or outcomes and unfinished, failed or skipped post-scan checks hold the checkpoint open. A narrow report window may need widening.</p>
<p class="row-count" aria-live="polite"></p>{_table(readiness, columns, 'seed-readiness-table')}
<div class="section-title seed-heading"><h2>2. Latest ladder scan · by division</h2><a href="ladder-progress.csv" download>Download all scans</a></div>
<p>“End reached” is inferred from completed pages 1 through the last recorded page, with no next page queued. Total pages and total players are unknown while discovery runs, so no population-completion percentage is shown. Multiple unfinished scans indicate overlapping refresh work. An older full scan can support reassessment while a newer refresh runs; check its date above.</p>
{_table(latest, ladder_columns, 'ladder-table')}{older_html}
<div class="section-title seed-heading"><h2>3. Empty history checks · by division, in this window</h2><a href="division-history.csv" download>Download division rates</a></div>
<p>Empty means a successful lookup returned no qualifying games in its requested history window. Rates use <strong>known successful outcomes only</strong>; failed requests are not empty results. The post-scan columns separate the newer evidence from earlier cohorts selected during ladder discovery. — means no denominator or unavailable evidence. Division labels come from the saved observation used at selection, never from current rank.</p>
{_table(seed.get('division_history', []), history_columns, 'division-history-table')}
<details><summary>Readiness rules and limitations</summary><ul>{notes}</ul></details>
</section>'''


def _diversity_html(report):
    data = report.get("diversity", {})
    total, concentration = data.get("summary", {}), data.get("concentration", {})
    started = data.get("started_at")
    started_text = datetime.fromtimestamp(started, timezone.utc).isoformat() if started is not None else "not yet available"
    notes = "".join(f'<li>{html.escape(note)}</li>' for note in data.get("notes", []))
    cards = (("New participant accounts / 100 measured games", total.get("new_accounts_per_100_games"), False),
             ("Top 1% of accounts: share of player appearances · full window", concentration.get("top_one_percent_appearance_share"), True),
             ("Champion–role combinations observed · measured games", total.get("champion_role_combinations"), False),
             ("Role-aligned matchups observed · measured games", total.get("matchup_combinations"), False))
    cards_html = "".join(f'<div class="card"><div>{html.escape(label)}</div><strong>{_display(value, fraction=fraction)}</strong></div>' for label, value, fraction in cards)
    columns = (("region", "Region"), ("seed_tier", "Seed tier"), ("stream", "Stream"),
               ("measured_games", "Games with new telemetry"), ("new_accounts", "Previously unseen participant accounts"),
               ("new_accounts_per_100_games", "New accounts / 100 games"),
               ("request_attempts", "HTTP dispatch attempts"), ("new_accounts_per_100_requests", "New accounts / 100 requests"),
               ("new_champion_roles", "New champion–role combinations since tracking began"),
               ("new_roles_per_100_requests", "New champion–role combinations / 100 requests"),
               ("new_matchups", "New matchups since tracking began"),
               ("new_matchups_per_100_requests", "New matchups / 100 requests"),
               ("known_role_fraction", "Player slots with usable champion / role"))
    rare = sorted(data.get("combinations", []), key=lambda x: (x["games"], x["role"], x["champion_a"]))[:30]
    return f'''<section id="diversity" class="view" hidden>
<h2>Are we adding different accounts and game situations?</h2>
<p>New collection telemetry began <strong>{html.escape(started_text)}</strong> (UTC). Old raw files are preserved and are not rescanned. New accounts are checked against the <strong>entire participant archive</strong>; new gameplay combinations are compared only with data recorded since tracking began. The earliest combinations therefore start a new baseline.</p>
<div class="cards">{cards_html}</div>
<p>Top 1% concentration covers all games in the report window, rounds up the number of accounts, and counts account identifiers rather than distinct humans. Other cards cover only games with the new telemetry. Cards retain whole-report scope when filters hide rows.</p>
<div class="section-title"><h2>Breadth and collection cost</h2><a href="diversity.csv" download>Download metrics</a></div>
<p>Request counts include retries and ladder/history/download work. Per-request values are window throughput, <strong>not</strong> matched-cohort conversion rates: a discovery and its later download can cross windows or groups. Keep Stream = All to include maintenance and backfill costs. — means no denominator or unavailable evidence.</p>
<p class="row-count" aria-live="polite"></p>{_table(data.get('groups', []), columns, 'diversity-table')}
<div class="section-title seed-heading"><h2>How players were selected · completed checks in this window</h2><a href="selection-rules.csv" download>Download selection mix</a></div>
<p>uniform_random keeps every observed account eligible regardless of past activity. least_recently_selected rotates through less recently sampled accounts, with randomized ties. Selection history is durable across restarts. Neither rule requires a previous nonempty result. Legacy checks have no recorded rule. These are selection mechanisms, not exact overall inclusion probabilities.</p>
{_table(report.get('selection_rules', []), (('region', 'Region'), ('seed_tier', 'Tier'), ('stream', 'Stream'), ('selection_rule', 'Selection rule'), ('checks', 'Completed checks')), 'selection-rules-table')}
<details><summary>30 least frequent observed combinations · full measured window</summary>
<p>Champion numbers are Riot champion IDs. Matchups are unordered champion pairs with the same reported role on opposing teams; they are not observed lane encounters. Missing roles produce no matchup. These are observed combinations only, not a list of everything absent. This table is not affected by region or tier filters. <a href="gameplay-combinations.csv" download>Download all combination counts</a>.</p>
{_table(rare, (('kind', 'Combination type'), ('role', 'Role'), ('champion_a', 'Champion ID'), ('champion_b', 'Opponent champion ID'), ('games', 'Measured games')), 'gameplay-combinations-table')}</details>
<details><summary>Definitions, baselines and limitations</summary><ul>{notes}</ul></details>
</section>'''


def render_html(report):
    """Escape all data; filtering is local and loads no external resources."""
    summary = report["summary"]
    period = report["window"]
    groups = report.get("group_yield", [])
    patches = report.get("yield_rows", [])
    coverage = report.get("coverage", [])
    balance_rows = assess_balance(report)["rows"]
    seed_rows = report.get("seed_coverage", {}).get("division_history", [])
    target_groups = {(r["region"], r["seed_tier"]) for r in groups
                     if r["stream"] == "recent" and r.get("new_matches", 0) > 0}
    cards = (("All saved games · entire archive", summary.get("total_matches")),
             ("New games saved · this window, all streams", summary.get("window_matches")),
             ("Distinct players · new games, all streams", summary.get("window_unique_players")),
             ("Region × tier groups with new recent games", len(target_groups)))
    cards_html = "".join(f'<div class="card"><div>{html.escape(label)}</div><strong>{_display(value)}</strong></div>'
                         for label, value in cards)
    filters = []
    for key, label in (("region", "Region"), ("seed_tier", "Seed tier"), ("stream", "Stream"), ("patch", "Patch"), ("division", "Division")):
        values = sorted({str(row[key]) for row in groups + patches + coverage + balance_rows + seed_rows if row.get(key) is not None})
        options = "".join(f'<option value="{html.escape(value, quote=True)}" {"selected" if key == "stream" and value == "recent" else ""}>{html.escape(value)}</option>' for value in values)
        filters.append(f'<label>{label}<select data-filter="{key}"><option value="">All</option>{options}</select></label>')
    notes = "".join(f'<li>{html.escape(str(note))}</li>' for note in report.get("notes", []))
    sections = (
        ("groups", "Collection details · this window", "group-yield.csv", groups, GROUP_COLUMNS),
        ("patches", "Game patches · this window", "yield.csv", patches, PATCH_COLUMNS),
        ("inventory", "Saved archive · all time", "coverage.csv", coverage,
         (("region", "Region"), ("seed_tier", "Original seed tier"), ("patch", "Patch"), ("matches", "Stored matches"))),
        ("budgets", "Work allocation · lifetime", "service.csv", _budget_rows(report),
         (("region", "Region"), ("stream", "Stream"), ("claims", "Lifetime job claims"),
          ("observed_share", "Observed non-maintenance share"), ("configured_share", "Configured weight share"))),
    )
    tabs = '<button type="button" data-tab="balance" aria-pressed="true">Sampling balance · start here</button><button type="button" data-tab="seeds" aria-pressed="false">Seed coverage · ready to reassess?</button><button type="button" data-tab="diversity" aria-pressed="false">Player and gameplay variety</button>' + "".join(f'<button type="button" data-tab="{key}" aria-pressed="false">{title}</button>'
                   for i, (key, title, *_rest) in enumerate(sections))
    explanations = {
        "groups": "New saved matches use their download job’s scheduling tier and stream. Player counts and lag percentiles are recomputed across patches, not added or averaged. Completed recent histories use the report window and include empty pages; selected, queued and active history counts cover all retained cohorts in the current snapshot.",
        "patches": "The same yield measures split by actual match patch. PENDING means the match was not stored by the window end, so its patch is unavailable. History jobs can span patches; no per-patch request efficiency is inferred.",
        "inventory": "All stored matches in the database read snapshot, including data collected before balanced sampling. These additive counts use original first-discovery seed tiers, which may differ from later scheduling attribution.",
        "budgets": "The default 8:1:1 means 80% of available job turns for recent ladder seeds, 10% for discovered participants, and 10% for older history. These weights split streams, not tiers. Counts here are lifetime job turns since telemetry began, not API calls, games saved or this window's work. Retries count too. Seed maintenance is excluded from shares. Empty streams donate capacity, so a difference from the configured share is not automatically a fault. The configuration shown was loaded when generating this report and may differ from the running collector. Set Stream to All to compare every stream.",
    }
    sections_html = "".join(
        f'<section id="{key}" class="view" hidden><div class="section-title"><h2>{title}</h2>'
        f'<a href="{filename}" download>Download CSV</a></div><p class="explanation">{explanations[key]}</p>'
        f'<p class="row-count" aria-live="polite"></p>{_table(rows, columns, key + "-table")}</section>'
        for i, (key, title, filename, rows, columns) in enumerate(sections)
    )
    glossary = "".join(f'<dt>{html.escape(LABELS.get(key, key.replace("_", " ").title()))}</dt><dd>{html.escape(value)}</dd>' for key, value in DEFINITIONS.items())
    return f'''<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Collector coverage and yield</title>
<style>
:root{{color-scheme:light;--ink:#172b3a;--muted:#526675;--line:#d9e3e8;--accent:#176d70;--paper:#f3f6f8}}
*{{box-sizing:border-box}}body{{margin:0;background:var(--paper);color:var(--ink);font:15px/1.5 system-ui,Segoe UI,sans-serif}}
main{{max-width:1600px;margin:auto;padding:36px 28px}}h1{{font-size:32px;letter-spacing:-.6px;margin:0 0 8px}}h2{{font-size:19px;margin:0}}p{{margin:8px 0}}a{{color:var(--accent)}}.eyebrow{{text-transform:uppercase;letter-spacing:.13em;font-size:12px;font-weight:700;color:var(--accent)}}
.meta,.explanation,.row-count{{color:var(--muted)}}.cards{{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin:26px 0}}.card{{background:white;border:1px solid var(--line);border-radius:10px;padding:20px}}.card div{{color:var(--muted);font-size:13px}}.card strong{{display:block;font-size:30px;margin-top:7px}}
.controls{{display:flex;gap:14px;flex-wrap:wrap;align-items:end;margin:22px 0}}label{{display:grid;gap:4px;color:var(--muted);font-size:12px}}select{{font:inherit;font-size:14px;min-width:140px;padding:9px;background:white;border:1px solid var(--line);border-radius:6px}}
nav{{display:flex;gap:7px;flex-wrap:wrap;border-bottom:1px solid var(--line);padding-bottom:12px}}button{{cursor:pointer;font:inherit;border:1px solid var(--line);border-radius:6px;background:white;padding:9px 13px;color:var(--ink)}}button[aria-pressed=true]{{background:var(--accent);color:white;border-color:var(--accent)}}
section{{margin-top:24px}}section[hidden]{{display:none}}.section-title{{display:flex;align-items:center;justify-content:space-between;gap:16px}}.table-scroll{{max-height:66vh;overflow:auto;border:1px solid var(--line);border-radius:8px;background:white}}table{{border-collapse:separate;border-spacing:0;width:100%;font-size:13px}}th,td{{padding:11px 13px;text-align:left;border-bottom:1px solid #edf1f3}}th{{position:sticky;top:0;background:#eaf0f3;z-index:1;white-space:normal;min-width:105px;line-height:1.3}}td{{white-space:nowrap}}td.number{{text-align:right;font-variant-numeric:tabular-nums}}tbody tr:nth-child(even){{background:#fafcfd}}tr[hidden]{{display:none}}
details{{margin-top:28px;background:white;border:1px solid var(--line);border-radius:8px;padding:16px 20px}}summary{{cursor:pointer;font-weight:600}}li{{margin:7px 0}}footer{{margin-top:24px;color:var(--muted);font-size:13px}}
.assessment{{background:#e5f1f0;border-left:5px solid var(--accent);padding:18px 22px;margin-bottom:24px}}#balance h2{{margin-top:24px}}#balance .table-scroll{{max-height:none}}dt{{font-weight:650;margin-top:12px}}dd{{margin:3px 0 12px;color:var(--muted)}}.guide{{max-width:1000px}}.diagnostics{{padding-left:24px}}th span[title]{{cursor:help}}
.extra-metric{{display:none}}.show-diagnostics .extra-metric{{display:table-cell}}[hidden]{{display:none!important}}
.badge{{display:inline-block;padding:3px 8px;border-radius:5px;font-weight:600}}.good{{background:#dff3e5;color:#185c35}}.review{{background:#fff0c5;color:#734c00}}.waiting{{background:#edf0f3;color:#526675}}
.seed-heading{{margin-top:28px}}#seeds td{{white-space:normal;min-width:80px}}#seeds td.number{{white-space:nowrap}}#seeds .table-scroll{{max-height:70vh}}
@media(max-width:800px){{main{{padding:22px 14px}}.cards{{grid-template-columns:repeat(2,1fr)}}h1{{font-size:26px}}}}
@media print{{nav,.controls,.row-count{{display:none}}section[hidden]{{display:block!important}}.table-scroll{{max-height:none;overflow:visible}}th{{position:static}}body{{background:white}}main{{padding:0}}}}
</style></head><body><main>
<div class="eyebrow">League research · sampling audit</div><h1>Collector coverage and yield</h1>
<p class="meta">Yield window: {html.escape(str(period['since_utc']))} → {html.escape(str(period['until_utc']))} · UTC</p>
<p class="meta">Read-only snapshot. Seed tiers describe discovery or scheduling; they are not historical lobby ranks.</p>
<p class="guide"><strong>Start with Sampling balance</strong> to compare recent coverage, then use the detail tabs to investigate. This saved file does not update live; generate a new report to see progress. No budgets are changed by this report.</p>
<div class="cards">{cards_html}</div><nav aria-label="Report views">{tabs}</nav>
<p class="meta">Summary cards, balance verdict and adjustment suggestions always describe the full report. Filters below only hide table rows.</p>
<div class="controls">{"".join(filters)}<button type="button" id="reset">Reset filters</button></div>
<label id="detail-control" hidden><span><input type="checkbox" id="show-diagnostics"> Show discovery, queue and history-check columns in detail tables</span></label>
{_balance_html(report)}
{_seed_html(report)}
{_diversity_html(report)}
{sections_html}
<details><summary>Plain-language glossary · what do these columns mean?</summary><dl>{glossary}</dl></details>
<details><summary>Technical definitions and data limitations</summary>
<p>Repeat appearances = 1 − unique players / participant appearances in the selected match set. Repeated routes = 1 − distinct discovered matches / recorded routes within a row. Already-stored routes identify matches saved before that discovery; they are not exact API-cost measurements. Missing measurements display —.</p>
<ul>{notes}</ul></details>
<footer>Generated {html.escape(str(report.get('generated_at_utc', '')))} · <a href="report.json" download>Full JSON with definitions</a> · Aggregate values only; no player or match identifiers.</footer>
</main><script>
const filters = [...document.querySelectorAll('[data-filter]')];
function enableFilters(viewId) {{
  const dimensions = {{balance: ['region', 'seed_tier'], seeds: ['region', 'seed_tier', 'division'], diversity: ['region', 'seed_tier', 'stream'], groups: ['region', 'seed_tier', 'stream'], patches: ['region', 'seed_tier', 'stream', 'patch'], inventory: ['region', 'seed_tier', 'patch'], budgets: ['region', 'stream']}};
  for (const select of filters) select.disabled = !dimensions[viewId].includes(select.dataset.filter);
  document.getElementById('detail-control').hidden = !['groups', 'patches'].includes(viewId);
}}
function applyFilters() {{
  for (const view of document.querySelectorAll('.view')) {{
    let shown = 0, total = 0;
    for (const row of view.querySelectorAll('tbody tr[data-region]')) {{
      total++;
      row.hidden = filters.some(select => !select.disabled && select.value && row.dataset[select.dataset.filter] && row.dataset[select.dataset.filter] !== select.value);
      if (!row.hidden) shown++;
    }}
    view.querySelector('.row-count').textContent = `${{shown}} of ${{total}} table rows shown. Filters hide rows; totals, shares and assessments remain for the full report.`;
  }}
}}
for (const select of filters) select.addEventListener('change', applyFilters);
document.getElementById('show-diagnostics').addEventListener('change', event => document.body.classList.toggle('show-diagnostics', event.target.checked));
document.getElementById('reset').addEventListener('click', () => {{ for (const select of filters) select.value = ''; applyFilters(); }});
for (const tab of document.querySelectorAll('[data-tab]')) tab.addEventListener('click', () => {{
  for (const button of document.querySelectorAll('[data-tab]')) button.setAttribute('aria-pressed', String(button === tab));
  for (const view of document.querySelectorAll('.view')) view.hidden = view.id !== tab.dataset.tab;
  enableFilters(tab.dataset.tab); applyFilters();
}});
enableFilters('balance');
applyFilters();
</script></body></html>'''


def _write_csv(path, rows):
    columns = list(dict.fromkeys(key for row in rows for key in row))
    with path.open("x", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            # Prevent spreadsheet formula interpretation of untrusted text labels.
            safe = {key: ("'" + value if isinstance(value, str) and value.lstrip()[:1] in ("=", "+", "-", "@") else value)
                    for key, value in row.items()}
            writer.writerow(safe)


def write_coverage_report(report, data_dir: Path, output_dir: Path | None = None) -> Path:
    """Write only a fresh report directory under the dataset's reports folder."""
    root = Path(data_dir).resolve()
    reports_root = (root / "reports").resolve()
    if reports_root != root / "reports":
        raise ValueError("Reports directory must not redirect into raw data or another directory")
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S-%fZ")
    target = (Path(output_dir).resolve() if output_dir else reports_root / f"coverage-{stamp}")
    if target == reports_root or not target.is_relative_to(reports_root):
        raise ValueError("Output must be a new subdirectory of data_dir/reports; existing data is protected")
    # Render before creating files, so data/renderer errors do not leave partial output.
    rendered = render_html(report)
    encoded = json.dumps(report, indent=2, ensure_ascii=False, allow_nan=False)
    target.mkdir(parents=True, exist_ok=False)
    (target / "report.json").write_text(encoded, encoding="utf-8")
    (target / "report.html").write_text(rendered, encoding="utf-8")
    for name, key in (("coverage.csv", "coverage"), ("yield.csv", "yield_rows"),
                      ("group-yield.csv", "group_yield"), ("service.csv", "budget_service")):
        _write_csv(target / name, report.get(key, []))
    if "seed_coverage" in report:
        for name, key in (("seed-readiness.csv", "readiness"), ("ladder-progress.csv", "ladder_progress"),
                          ("division-history.csv", "division_history")):
            _write_csv(target / name, report["seed_coverage"].get(key, []))
    if "diversity" in report:
        _write_csv(target / "diversity.csv", report["diversity"].get("groups", []))
        _write_csv(target / "gameplay-combinations.csv", report["diversity"].get("combinations", []))
        _write_csv(target / "selection-rules.csv", report.get("selection_rules", []))
    return target
