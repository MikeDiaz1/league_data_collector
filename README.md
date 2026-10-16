# League Match Data Collector

A resumable Python collector for League of Legends research. It uses official Riot APIs to collect **Ranked Solo/Duo (queue 420)** matches from **NA1, EUW1, and KR**.

Collection targets balanced opportunities across Emerald–Challenger seed groups, with bounded recent histories, supplementary participant discovery, and a separate historical backfill. Complete, original Match-V5 responses are saved as gzip JSON; SQLite tracks discovery, ranks, and global deduplication. Collection respects Riot's rate limits, Retry-after, and resumes after interruption. **Requires your own [Riot API key](https://developer.riotgames.com/).**

## Setup

Requires **Python 3.11+**, **uv**, and your own Riot API key. From the repository folder in PowerShell:

```powershell
uv sync --locked
$riotSecret = Read-Host 'RIOT_API_KEY' -AsSecureString
$env:RIOT_API_KEY = [System.Net.NetworkCredential]::new('', $riotSecret).Password
Remove-Variable riotSecret
```

Alternatively, load an existing Windows user key with `$env:RIOT_API_KEY = [Environment]::GetEnvironmentVariable('RIOT_API_KEY', 'User')`. Keys are sent in headers and never saved in the dataset or logs.

## Commands

| Action | PowerShell command |
| --- | --- |
| Start / resume | `uv run --locked riot-collector collect` |
| Status | `uv run --locked riot-collector status` |
| Coverage and yield report (safe while collecting) | `uv run --locked riot-collector coverage-report` |
| Stop from another terminal | `uv run --locked riot-collector request-stop` |
| Follow logs | `Get-Content .\data\logs\collector.log -Tail 20 -Wait` |
| Check saved data | `uv run --locked riot-collector validate` |
| Export manifest | `uv run --locked riot-collector export-manifest` |
| Sample current participant ranks (while collection is stopped) | `uv run --locked riot-collector refresh-ranks --limit 25` |
| Backfill timelines | `uv run --locked riot-collector backfill-timelines` |
| Small live/resume test | `uv run --locked riot-collector smoke --per-region 1 --max-seconds 600` |
| Offline tests | `uv run --locked pytest -q` |

Run tests from the project root. Pytest keeps its cache and temporary fixtures in ignored `.tools/pytest/`; the default `tmp` directory is replaced on each run. For simultaneous pytest runs, give each a distinct `--basetemp=.tools/pytest/<run-name>` directory.

**Keep the collection terminal and its hosting application open.** Stop with **Ctrl+C once**, then wait for shutdown before restarting. Windows idle sleep is prevented while collecting; manual Sleep and shutdown still take precedence. Hidden launching does not guarantee survival after the parent application closes.

Add `--max-seconds 28800` after `collect` for an eight-hour run. Run collection and backfill separately.

## Configuration and data

Edit `collector.toml`; private overrides can use `--config .\collector.local.toml` **before** the command. Paths are relative to the config file.

Git-ignored `data/` contains raw matches/timelines, SQLite queues, quota state, logs, and manifests. Preserve the **entire directory** when resuming or moving it; back up while stopped. Never delete quota state to bypass cooldowns or use rotating keys to evade limits. Collection stops on invalid credentials or low disk space (**10 GiB reserve** by default).

Rank observations describe current rank when observed, **not historical match rank**. Manifest skill estimates are not MMR; sampling is not uniform.

## Collection rationale and policy

The goal is varied training data for a general representation model, with enough high-rank examples despite their smaller player populations. Prefer steady collection and review the evidence before adjusting settings; neither population-proportional sampling nor equal game counts is guaranteed.

- **Coverage:** NA1, EUW1 and KR, with scheduling opportunities across Emerald–Challenger. Unfinished ladder scans resume without starting overlapping scans for the same region/tier.
- **Breadth:** every six hours, select up to **200 players × 10 games** for each Emerald/Diamond group and **100 × 20** for each Master/Grandmaster/Challenger group, per region. Histories cover the preceding 14 days.
- **Player diversity:** half each cohort is uniformly sampled from discovered players; half favors players selected least recently. Empty-history players remain eligible, and rotation persists across restarts.
- **Work allocation:** **8:1:1** for recent ladder seeds, participant exploration and older-history backfill. These are job-turn weights, not game or API-request percentages; unused capacity goes to other work. Participant cohorts use 100 players × 20 games per region.
- **Preservation:** duplicate games are stored once. Existing queued work retains its saved limits and progress. Incomplete ladders and shared participants can still bias coverage; seed tier is not historical lobby rank.

## Coverage report

Run `uv run --locked riot-collector coverage-report --hours 24` for a fresh HTML/JSON/CSV bundle under `data/reports/`. Reporting is read-only and safe while collecting. It shows regional/tier/patch coverage, ladder readiness, empty histories, player concentration, new accounts, gameplay variety and request yield. Labels explain each metric and its limitations; balance flags are review heuristics. Reports never change settings. Gameplay-combination tracking starts at telemetry activation; new-account detection uses the full participant archive.

## Updating a running collector

Run `request-stop`, then wait for the log's finished-run message and the owning process to exit. Back up the stopped SQLite state and preserve the entire data directory, including raw files and quota state. Install/update the code and run the offline tests before starting collection again with the same data directory and configuration. Additive state migrations run on open; do not point collection at an immutable model snapshot. Confirm progress and the sampling summary after restart. Do not delete quota state or forcibly reset in-progress work to accelerate resumption.

## License

[MIT](LICENSE) covers the source code, not Riot data, assets, or trademarks. Keep credentials and collected data out of Git. API use remains subject to [Riot's developer policies](https://developer.riotgames.com/policies/general); this repository does not establish Riot approval.

League Data Collection isn't endorsed by Riot Games and doesn't reflect the views or opinions of Riot Games or anyone officially involved in producing or managing Riot Games properties. Riot Games, and all associated properties are trademarks or registered trademarks of Riot Games, Inc.
