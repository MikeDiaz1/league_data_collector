from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
import re
import signal
import sys

from .config import Config, load_config
from .dataset import export_manifest, status, validate
from .locking import DatasetBusyError, DatasetLock
from .power import keep_awake


LOG = logging.getLogger(__name__)


def configure_logging(data_dir: Path, *, console: bool = True):
    logs = data_dir / "logs"
    logs.mkdir(parents=True, exist_ok=True)
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    disk = RotatingFileHandler(logs / "collector.log", maxBytes=20_000_000, backupCount=10, encoding="utf-8")
    handlers = [disk]
    if console:
        handlers.append(logging.StreamHandler())
    for handler in handlers:
        handler.setFormatter(formatter)
    logging.basicConfig(level=logging.INFO, handlers=handlers, force=True)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


def parser():
    p = argparse.ArgumentParser(description="Resumable official Riot Match-V5 collector")
    p.add_argument("--config", type=Path, default=Path("collector.toml"))
    p.add_argument("--no-console", action="store_true", help="Write runtime logs only to rotating files (for detached launch)")
    subs = p.add_subparsers(dest="command", required=True)
    for command in ("collect", "backfill-timelines", "smoke"):
        child = subs.add_parser(command)
        child.add_argument("--max-seconds", type=float, help="Explicit wall-clock limit; otherwise collection continues")
        if command == "smoke":
            child.add_argument("--per-region", type=int, default=1, help="New matches per platform on EACH of two runs")
    subs.add_parser("status")
    subs.add_parser("validate")
    export = subs.add_parser("export-manifest", help="Offline JSONL export with explicitly estimated rank characterization")
    export.add_argument("--output", type=Path)
    subs.add_parser("request-stop", help="Request a graceful stop of the process owning this dataset")
    ranks = subs.add_parser("refresh-ranks", help="Optional explicitly budgeted current-rank enrichment")
    ranks.add_argument("--limit", type=int, default=25, help="Maximum uncached players; default 25")
    return p


async def run_collection(cfg: Config, args, api_key: str):
    from .api import RiotClient
    from .engine import Collector
    from .state import State
    from .storage import RawStorage

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    old_handlers = {}
    for sig in (signal.SIGINT, signal.SIGTERM):
        old_handlers[sig] = signal.getsignal(sig)
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, RuntimeError):
            signal.signal(sig, lambda signum, frame: loop.call_soon_threadsafe(stop.set))

    async def watch_stop():
        while not stop.is_set():
            sentinel = cfg.data_dir / "stop.request"
            if sentinel.exists():
                stop.set()
                sentinel.unlink(missing_ok=True)
                LOG.info("Graceful stop requested")
                break
            await asyncio.sleep(1)

    watcher = asyncio.create_task(watch_stop())
    try:
        protected = {}
        # Reopen both database and HTTP/rate client on the resume check.
        for pass_number in range(2 if args.command == "smoke" else 1):
            if pass_number:
                stop.clear()
                if watcher.done():
                    watcher = asyncio.create_task(watch_stop())
            state = State(cfg.data_dir)
            try:
                state.recover()
                storage = RawStorage(cfg.data_dir, cfg.min_free_gb)
                storage.check_disk()
                async with RiotClient(
                    api_key, cfg.data_dir, timeout=cfg.timeout_seconds, max_retries=cfg.max_retries,
                    app_limits=cfg.app_limits, safety_margin=cfg.safety_margin,
                ) as client:
                    if protected:
                        original_get = client.get

                        async def checked_get(route, path, method, service, params=None):
                            found = re.fullmatch(r"/lol/match/v5/matches/([A-Z0-9]+_[0-9]+)", path)
                            if found and found[1] in protected:
                                raise RuntimeError("Smoke resume check attempted to download an already completed match")
                            return await original_get(route, path, method, service, params)

                        client.get = checked_get
                    collector = Collector(cfg, state, storage, client)
                    result = await collector.run(
                        stop, mode="timelines" if args.command == "backfill-timelines" else "collect",
                        max_seconds=args.max_seconds if args.max_seconds is not None else (600 if args.command == "smoke" else None),
                        smoke_per_region=args.per_region if args.command == "smoke" else None,
                    )
                    LOG.info("Run finished: %s", json.dumps(result))
                    if args.command == "smoke":
                        counts = result.get("downloaded", {})
                        if any(counts.get(p, 0) < args.per_region for p in cfg.platforms):
                            raise RuntimeError("Smoke did not complete its new-match target on every platform")
                        if pass_number == 0:
                            for row in state.conn.execute("SELECT match_id,raw_path,sha256 FROM matches"):
                                protected[row["match_id"]] = (row["raw_path"], row["sha256"])
                        else:
                            for path, digest in protected.values():
                                if hashlib.sha256(storage.read(path)).hexdigest() != digest:
                                    raise RuntimeError("A completed raw file changed during smoke resume verification")
            finally:
                state.close()
        if args.command == "smoke":
            report = validate(cfg.data_dir)
            if not report["ok"]:
                raise RuntimeError(f"Smoke validation failed: {report['errors'][:5]}")
            LOG.info("Live smoke passed twice across %s; existing matches were not requested again", ", ".join(cfg.platforms))
    finally:
        watcher.cancel()
        await asyncio.gather(watcher, return_exceptions=True)
        for sig, handler in old_handlers.items():
            try:
                loop.remove_signal_handler(sig)
            except NotImplementedError:
                pass
            signal.signal(sig, handler)


async def refresh_ranks(cfg: Config, api_key: str, limit: int):
    """A separate opt-in request budget; never competes with match collection."""
    from .api import NotFound, RiotClient
    from .state import State
    from .storage import RawStorage
    import time
    from urllib.parse import quote

    state = State(cfg.data_dir)
    try:
        RawStorage(cfg.data_dir, cfg.min_free_gb).check_disk()
        cutoff = time.time() - cfg.rank_cache_hours * 3600
        # Missing rank has lower priority than high Elo, but remains eligible.
        rows = list(state.conn.execute("""
            SELECT puuid,platform FROM players
            WHERE rank_checked_at IS NULL OR rank_checked_at<?
            ORDER BY priority DESC,discovered_at LIMIT ?
        """, (cutoff, limit)))
        async with RiotClient(
            api_key, cfg.data_dir, timeout=cfg.timeout_seconds, max_retries=cfg.max_retries,
            app_limits=cfg.app_limits, safety_margin=cfg.safety_margin,
        ) as client:
            for row in rows:
                try:
                    raw = await client.get(
                        row["platform"].lower(),
                        f"/lol/league/v4/entries/by-puuid/{quote(row['puuid'], safe='')}",
                        "league.entries.by-puuid", "league-v4",
                    )
                    observations = json.loads(raw)
                    if not isinstance(observations, list):
                        raise ValueError("Unexpected League-V4 rank response shape")
                except NotFound:
                    observations = []
                now = time.time()
                solo = [r for r in observations if r.get("queueType") == "RANKED_SOLO_5x5"]
                # A negative lookup is cached and invalidates stale current-rank labels.
                if not solo:
                    solo = [{"queueType": "RANKED_SOLO_5x5", "tier": None}]
                with state.transaction():
                    for rank in solo:
                        state.conn.execute("""
                            INSERT INTO rank_observations
                            (puuid,platform,tier,division,lp,queue,observed_at,source,raw_json)
                            VALUES (?,?,?,?,?,?,?,?,?)
                        """, (row["puuid"], row["platform"], rank.get("tier"), rank.get("rank"),
                              rank.get("leaguePoints"), "RANKED_SOLO_5x5", now, "rank_refresh", json.dumps(rank)))
                    state.conn.execute("UPDATE players SET rank_checked_at=? WHERE puuid=? AND platform=?",
                                       (now, row["puuid"], row["platform"]))
            LOG.info("Current-rank refresh complete: %d players (budget %d)", len(rows), limit)
    finally:
        state.close()


def main(argv=None) -> int:
    args = parser().parse_args(argv)
    try:
        cfg = load_config(args.config)
        if hasattr(args, "max_seconds") and args.max_seconds is not None:
            import math
            if not math.isfinite(args.max_seconds) or args.max_seconds <= 0:
                raise ValueError("--max-seconds must be finite and positive")
        if args.command == "smoke" and args.per_region < 1:
            raise ValueError("--per-region must be positive")
        if args.command == "refresh-ranks" and args.limit < 1:
            raise ValueError("--limit must be positive")
        if args.command == "status":
            print(json.dumps(status(cfg.data_dir, cfg.min_free_gb), indent=2))
        elif args.command == "validate":
            report = validate(cfg.data_dir)
            print(json.dumps(report, indent=2))
            return 0 if report["ok"] else 1
        elif args.command == "export-manifest":
            output = args.output or cfg.data_dir / "manifests" / "matches.jsonl"
            count = export_manifest(cfg.data_dir, output)
            print(json.dumps({"matches_exported": count, "output": str(output.resolve())}))
        elif args.command == "request-stop":
            cfg.data_dir.mkdir(parents=True, exist_ok=True)
            (cfg.data_dir / "stop.request").touch()
            print("Graceful stop requested. Check collector logs for shutdown confirmation.")
        else:
            key = os.environ.get("RIOT_API_KEY", "")
            if not key:
                raise ValueError("RIOT_API_KEY is not set; set it in the environment and rerun. No progress has been discarded.")
            with DatasetLock(cfg.data_dir):
                configure_logging(cfg.data_dir, console=not args.no_console)
                if args.command == "refresh-ranks":
                    asyncio.run(refresh_ranks(cfg, key, args.limit))
                else:
                    with keep_awake():
                        asyncio.run(run_collection(cfg, args, key))
        return 0
    except KeyboardInterrupt:
        LOG.warning("Interrupted; durable queues will resume on the next run")
        return 130
    except (OSError, ValueError, DatasetBusyError) as exc:
        print(str(exc), file=sys.stderr)
        return 2
    except Exception as exc:
        # API and storage errors are intentionally sanitized; never dump HTTP objects.
        from .api import APIError
        from .storage import StorageError
        if isinstance(exc, (APIError, StorageError, RuntimeError)):
            LOG.error("Stopped safely: %s", exc)
            return 1
        LOG.error("Stopped after unexpected %s; persistent work is retained", type(exc).__name__)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
