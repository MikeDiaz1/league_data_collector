[CmdletBinding()]
param(
    [string]$Config = '',
    [double]$MaxSeconds = 0
)
$ErrorActionPreference = 'Stop'
$repo = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
if (-not $Config) { $Config = Join-Path $repo 'collector.toml' }
$configPath = (Resolve-Path -LiteralPath $Config).Path
$python = Join-Path $repo '.venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $python)) { throw 'Run uv sync --locked first.' }
if (-not $env:RIOT_API_KEY) { $env:RIOT_API_KEY = [Environment]::GetEnvironmentVariable('RIOT_API_KEY', 'User') }
if (-not $env:RIOT_API_KEY) { $env:RIOT_API_KEY = [Environment]::GetEnvironmentVariable('RIOT_API_KEY', 'Machine') }
if (-not $env:RIOT_API_KEY) { throw 'Set RIOT_API_KEY in the environment before starting. Never put it in a command argument.' }
if ($MaxSeconds -lt 0 -or [double]::IsInfinity($MaxSeconds) -or [double]::IsNaN($MaxSeconds)) { throw 'MaxSeconds must be finite and nonnegative.' }
# Resolve TOML using the same Python parser as the collector, including relative paths.
$dataDir = & $python -c 'from pathlib import Path; from riot_collector.config import load_config; import sys; print(load_config(Path(sys.argv[1])).data_dir)' $configPath
if ($LASTEXITCODE -ne 0) { throw 'Could not read collector configuration.' }
$logs = Join-Path $dataDir 'logs'
New-Item -ItemType Directory -Path $logs -Force | Out-Null
$stamp = Get-Date -Format 'yyyyMMdd-HHmmss-fff'
$stdout = Join-Path $logs "launch-$stamp.stdout.log"
$stderr = Join-Path $logs "launch-$stamp.stderr.log"
$arguments = @('-m', 'riot_collector', '--no-console', '--config', ('"' + $configPath + '"'), 'collect')
if ($MaxSeconds -gt 0) { $arguments += @('--max-seconds', $MaxSeconds.ToString([Globalization.CultureInfo]::InvariantCulture)) }
$started = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() / 1000.0
$process = Start-Process -FilePath $python -ArgumentList $arguments -WorkingDirectory $repo -WindowStyle Hidden -RedirectStandardOutput $stdout -RedirectStandardError $stderr -PassThru
# Start-Process hides the window, but a hosting application's Windows job may
# still terminate its children. This is not a guarantee of independent lifetime.
# Check the PID written after obtaining the dataset lock.
$pidFile = Join-Path $dataDir 'collector.pid'
for ($attempt = 0; $attempt -lt 20; $attempt++) {
    Start-Sleep -Milliseconds 500
    $process.Refresh()
    if ($process.HasExited) { throw "Collector exited with code $($process.ExitCode). Inspect $stderr and $(Join-Path $logs 'collector.log')." }
    if (Test-Path -LiteralPath $pidFile) {
        try { $owner = Get-Content -LiteralPath $pidFile -Raw | ConvertFrom-Json } catch { continue }
        # Windows venv python.exe is a redirector with a child Python PID.
        $belongsToLaunch = ($owner.pid -eq $process.Id -or $owner.parent_pid -eq $process.Id)
        if ($belongsToLaunch -and $owner.started_at -ge $started -and $attempt -ge 9) {
            $running = Get-Process -Id $owner.pid -ErrorAction SilentlyContinue
            if ($running) {
                Write-Output "Collector process is running (PID $($owner.pid))."
                Write-Output "Persistent log: $(Join-Path $logs 'collector.log')"
                Write-Output "Startup stderr: $stderr"
                Write-Output 'Use riot-collector status for progress and request-stop for graceful shutdown.'
                exit 0
            }
        }
    }
}
throw "The process started but its collector PID was not verified. Inspect $stderr before launching again."
