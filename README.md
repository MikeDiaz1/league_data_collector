# Riot research collector

An unattended Python collector for complete, original Riot Match-V5 responses. NA1, EUW1 and KR workers run concurrently, routing matches through AMERICAS, EUROPE and ASIA. The 100,000-match setting is a **milestone, never a stop limit**. Collection continues until interrupted, explicitly time limited, discovery exhausted, or stopped by a safety/error condition. Timelines are opt-in through a separate backfill command.

## Setup (Windows PowerShell)

Python 3.11+ is required. Install project-local uv and the locked environment:

```powershell
# Run from the repository folder.
python -m pip install --target .tools uv
.\.tools\bin\uv.exe sync --locked
```

If uv is already installed, `uv sync --locked` works. On other platforms use the installed `uv` command. Enter the key without putting it in shell history or a file:

```powershell
$riotSecret = Read-Host 'RIOT_API_KEY' -AsSecureString
$env:RIOT_API_KEY = [System.Net.NetworkCredential]::new('', $riotSecret).Password
Remove-Variable riotSecret
```

Only `RIOT_API_KEY` is used, exclusively as the `X-Riot-Token` header. The key is never saved in SQLite, logs, configuration, or subprocess arguments. Development keys expire after 24 hours; the collector stops on HTTP 401/403 and retains all progress. Resume with a valid key from the same registered application. Do not use other keys/applications to bypass quotas.

## Smoke test, start, and stop

Run a small live test first:

```powershell
.\.venv\Scripts\python.exe -m riot_collector smoke --per-region 1 --max-seconds 600
```

This collects at least one new match per platform, closes and reopens the database and HTTP client, then repeats. The second pass rejects any HTTP download of a previously completed match, checks retained checksums, and validates the dataset. Smoke data stays in the dataset. Success requires both passes to reach every platform; a timeout is reported as a failure, with progress retained.

Launch independently of Codex and the current terminal, with persistent logs:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\Start-Collector.ps1
```

The launcher uses `Start-Process -WindowStyle Hidden`, inherits the key in the environment, and verifies that the collector PID remains alive after startup. If the current shell has no key, it reads `RIOT_API_KEY` from the Windows user/machine environment. It does not schedule a reboot restart. Closing Codex or PowerShell does not stop it; keep the computer connected to the internet. Do not launch against the same SQLite dataset from multiple machines or synced copies at once. Only one collector/backfill/rank-refresh process may own the dataset lock.

On Windows, collection, smoke tests and timeline backfill automatically hold a [system execution request](https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-setthreadexecutionstate) to prevent idle sleep while running. The display can still turn off. The request is released on normal shutdown and automatically disappears if the process exits or crashes; your power-plan settings are unchanged. This does not override manual Sleep, shutdown, laptop lid actions or critical-battery protection. Keep laptops plugged in with the lid open. `powercfg /requests` can show the active Python system request (an elevated terminal may be required).

Check progress and follow the log from another terminal:

```powershell
.\.venv\Scripts\python.exe -m riot_collector status
Get-Content .\data\logs\collector.log -Tail 20 -Wait
```

Request a graceful stop (works for both collection and timelines):

```powershell
.\.venv\Scripts\python.exe -m riot_collector request-stop
```

Confirm shutdown in the log; `data/collector.pid` disappears after a normal exit. In the foreground, Ctrl+C also stops safely. If forcibly terminated, interrupted jobs are recovered on the next run. Resume using the same startup command and data directory. A stop request left by an earlier run is honored once; issue startup again if you intentionally stopped before the process began.

Foreground and explicitly time-limited examples:

```powershell
.\.venv\Scripts\python.exe -m riot_collector collect
.\.venv\Scripts\python.exe -m riot_collector collect --max-seconds 28800
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\Start-Collector.ps1 -MaxSeconds 28800
```

Periodic logs include unique matches, regional counts, requests in the past minute, retries, collection speed, compressed storage, patch coverage, discovery queues and estimated remaining capacity. `status` includes the last heartbeat timestamp/age; an old heartbeat alone does not establish that a process is running.

## Configuration and discovery

Edit `collector.toml`, or copy it to Git-ignored `collector.local.toml` and use `--config .\collector.local.toml` **before** the subcommand. Relative data paths resolve against the config file's directory. To use a dedicated disk, set `data_dir = 'D:\riot-data'`. All raw data and state belong to that directory; never delete it to resume.

League-V4 seeds Challenger, Grandmaster, Master, and every Diamond/Emerald division, with persistent ladder pagination. Current official League DTOs provide PUUIDs. Downloads recursively add all participant PUUIDs. High-rank history is preferred, with regular turns for unknown-rank participants. History requests filter `queue=420`; match bodies are verified again before acceptance. Unexpected other queues are retained separately in quarantine and excluded from completed matches.

Each history crawl fixes its `endTime` on discovery, pages backward without a mandatory date floor, and saves its offset with discovered match IDs in one transaction. It yields after every page to diversify players and regions. Seed refreshes revisit completed seed-player crawls; active historical crawls retain their anchors. Different players naturally cover overlapping patches; there is no promise of uniform patch/rank sampling. Global match IDs deduplicate every discovery path. A backlog threshold favors downloads over further discovery.

## Quotas and recovery

The client follows [Riot's regional application, method and service limits](https://developer.riotgames.com/docs/portal#rate-limiting) and [official routing](https://developer.riotgames.com/docs/lol#routing-values). Conservative quota groups share NA1+AMERICAS, EUW1+EUROPE, and KR+ASIA accounting; hostnames do not create extra application allowances. Stable endpoint identifiers share method limits across players/pages/tiers. Service cooldowns span every endpoint of that service in the group. There is one in-flight request per group, with independent workers across groups.

Every attempt reserves quota on disk **before** sending. App/method response limits and counts update the limiter; service headers are honored when provided. HTTP 429 suspends its indicated application, method, or service scope for the full `Retry-After` (including fractional seconds or HTTP dates); missing/unknown scope suspends the entire group. Missing/invalid delay uses a conservative fallback. Server/network errors use bounded exponential backoff with jitter, then durable delayed queue retries. HTTP 400 and other permanent errors stop collection for inspection; 404 is recorded as unavailable. Auth failure stops all workers. Rate state and cooldowns survive restarts. A corrupted rate-state file causes a safe stop; it is never silently reset.

Configuration caps default to 20 requests/second and 100/120 seconds, with 10% headroom and smooth pacing. Header limits can reduce these further. An interrupted in-flight request can impose a full largest-known regional window of waiting after restart. Defaults intentionally favor compliance over maximum throughput. Do not use the same key in another uncoordinated collector or a separate data directory: this dataset coordinates only its own processes. Higher production-key quotas require a deliberate config change; observed headers still apply.

## Storage, validation and export

`data/` is Git-ignored. Its layout is:

```text
data/
  state.sqlite3                 # WAL, synchronous FULL; queues, cursors, ranks, metadata
  rate_state.json               # durable request reservations and cooldowns; no key
  raw/matches/NA1/123/NA1_....json.gz
  raw/timelines/...              # independent backfill
  raw/quarantine/...            # unexpected queue responses, retained unchanged
  logs/collector.log            # rotating 20 MB files, ten backups
  logs/launch-*.stderr.log      # startup errors only; background console logging disabled
  manifests/matches.jsonl       # replaceable offline characterization
```

Gzip files contain the **exact complete JSON response bytes** (all original and future fields), without projecting participant/team/statistics fields into a modeling schema. SHA-256 covers the uncompressed original bytes. Files are flushed and atomically published before transactional completion. Existing raw files are never overwritten. A crash after publication but before database commit leaves a valid orphan that is adopted without another API request. Interrupted temporary files are never treated as completed data. Windows publication uses a write-through atomic move; SQLite uses durable transactions. Preserve both raw files and SQLite when backing up; copy a consistent backup while stopped.

The default free-space reserve is **10 GiB** (`min_free_gb`). Checks occur before jobs and before publishing files. Capacity estimates use measured compressed bytes/match; SQLite, logs, quarantine and timelines also consume space. The estimate is advisory and the live free-space check controls stopping. Corrupt or missing raw files cause validation failure and are preserved for inspection.

```powershell
.\.venv\Scripts\python.exe -m riot_collector validate
.\.venv\Scripts\python.exe -m riot_collector export-manifest
.\.venv\Scripts\python.exe -m riot_collector backfill-timelines --max-seconds 28800
```

Validation checks SQLite integrity and every completed raw file's gzip, checksum, identity, participant/team structure and queue. Timeline backfill has its own transactional jobs and completion table; collect never requests timelines. Backfill is a snapshot of completed matches at startup; rerun it to include matches collected later. Offline commands do not need a key and can inspect a running collector through a read snapshot.

The JSONL manifest includes original timestamps, date, patch, duration, platform, discovery source/seed tier, participant rank observations, known-rank fraction, tier distribution, and an explicitly **estimated** match population. Rank is the latest observed current solo rank on that platform, **never historical match rank**. The estimate rounds the mean ordinal tier among known participants; it is not MMR and can be biased by sparse coverage and observation dates. Rank observation timestamps and source remain available for stricter future analysis. Raw responses remain independent of this replaceable export.

Seeding supplies cached rank observations at no extra per-player lookup cost. Optional rank refresh is separate, bounded, and cached for seven days by default:

```powershell
.\.venv\Scripts\python.exe -m riot_collector refresh-ranks --limit 25
```

It runs only while collection is stopped, uses [League-V4's PUUID rank endpoint](https://developer.riotgames.com/apis#league-v4), and caches negative/unranked results too. It is never automatically run at the expense of match collection.

## Tests

```powershell
.\.tools\bin\uv.exe run --locked pytest -q --basetemp=.pytest-tmp
```

Offline tests cover routing, fractional/date `Retry-After`, quota scopes and restart persistence, pagination, recursive discovery, deduplication, actual abrupt process exit, atomic publication interruption, orphan recovery, rank interpretation, error recovery and independent timeline backfill. No real key is needed for tests. Mocked integration cannot establish live key validity; use `smoke` for that.
