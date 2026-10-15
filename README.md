# League Data Collection

A resumable Python collector for League of Legends research. It uses official Riot APIs to collect **Ranked Solo/Duo (queue 420)** matches from **NA1, EUW1, and KR**.

Emerald–Challenger seeds and recursive participant discovery prioritize high ranks. Complete, original Match-V5 responses are saved as gzip JSON; SQLite tracks discovery, ranks, and global deduplication. Collection respects Riot's rate limits, Retry-after, and resumes after interruption. **Requires your own [Riot API key](https://developer.riotgames.com/).**

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
| Stop from another terminal | `uv run --locked riot-collector request-stop` |
| Follow logs | `Get-Content .\data\logs\collector.log -Tail 20 -Wait` |
| Check saved data | `uv run --locked riot-collector validate` |
| Export manifest | `uv run --locked riot-collector export-manifest` |
| Backfill timelines | `uv run --locked riot-collector backfill-timelines` |
| Small live/resume test | `uv run --locked riot-collector smoke --per-region 1 --max-seconds 600` |
| Offline tests | `uv run --locked pytest -q` |

**Keep the collection terminal and its hosting application open.** Stop with **Ctrl+C once**, then wait for shutdown before restarting. Windows idle sleep is prevented while collecting; manual Sleep and shutdown still take precedence. Hidden launching does not guarantee survival after the parent application closes.

Add `--max-seconds 28800` after `collect` for an eight-hour run. Run collection and backfill separately.

## Configuration and data

Edit `collector.toml`; private overrides can use `--config .\collector.local.toml` **before** the command. Paths are relative to the config file.

Git-ignored `data/` contains raw matches/timelines, SQLite queues, quota state, logs, and manifests. Preserve the **entire directory** when resuming or moving it; back up while stopped. Never delete quota state to bypass cooldowns or use rotating keys to evade limits. Collection stops on invalid credentials or low disk space (**10 GiB reserve** by default).

Rank observations describe current rank when observed, **not historical match rank**. Manifest skill estimates are not MMR; sampling is not uniform.

## License

[MIT](LICENSE) covers the source code, not Riot data, assets, or trademarks. Keep credentials and collected data out of Git. API use remains subject to [Riot's developer policies](https://developer.riotgames.com/policies/general); this repository does not establish Riot approval.

League Data Collection isn't endorsed by Riot Games and doesn't reflect the views or opinions of Riot Games or anyone officially involved in producing or managing Riot Games properties. Riot Games, and all associated properties are trademarks or registered trademarks of Riot Games, Inc.
