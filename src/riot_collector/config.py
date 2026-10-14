from __future__ import annotations

from dataclasses import dataclass, fields
from pathlib import Path
import math
import tomllib


ROUTING = {"NA1": "americas", "EUW1": "europe", "KR": "asia"}


@dataclass(frozen=True)
class Config:
    data_dir: Path = Path("data")
    platforms: tuple[str, ...] = ("NA1", "EUW1", "KR")
    milestone: int = 100_000
    min_free_gb: float = 10.0
    progress_seconds: float = 30.0
    history_page_size: int = 100
    discovery_backlog: int = 2000
    seed_refresh_hours: float = 24.0
    rank_cache_hours: float = 168.0
    job_retry_seconds: float = 60.0
    app_limits: tuple[tuple[int, float], ...] = ((20, 1), (100, 120))
    safety_margin: float = 0.1
    timeout_seconds: float = 30.0
    max_retries: int = 5


def load_config(path: Path) -> Config:
    path = path.resolve()
    with path.open("rb") as stream:
        raw = tomllib.load(stream)
    unknown_sections = set(raw) - {"collector", "rate_limits"}
    if unknown_sections:
        raise ValueError(f"Unknown configuration sections: {sorted(unknown_sections)}")
    values = dict(raw.get("collector", {}))
    allowed = {f.name for f in fields(Config)} - {
        "app_limits", "safety_margin", "timeout_seconds", "max_retries"
    }
    unknown = set(values) - allowed
    if unknown:
        raise ValueError(f"Unknown collector options: {sorted(unknown)}")
    values["data_dir"] = (path.parent / values.get("data_dir", "data")).resolve()
    platforms = values.get("platforms", Config.platforms)
    if not isinstance(platforms, (tuple, list)) or not platforms or any(
        p not in ROUTING for p in platforms
    ) or len(set(platforms)) != len(platforms):
        raise ValueError("platforms must be a nonempty unique list from NA1, EUW1, KR")
    values["platforms"] = tuple(platforms)
    rate = dict(raw.get("rate_limits", {}))
    if set(rate) - {"application", "safety_margin", "timeout_seconds", "max_retries"}:
        raise ValueError("Unknown rate_limits option")
    limits = rate.pop("application", Config.app_limits)
    if not limits or any(
        not isinstance(pair, (list, tuple)) or len(pair) != 2
        or type(pair[0]) is not int or pair[0] < 1
        or not isinstance(pair[1], (float, int)) or not math.isfinite(pair[1]) or pair[1] <= 0
        for pair in limits
    ):
        raise ValueError("application must contain positive [integer requests, seconds] pairs")
    values["app_limits"] = tuple(tuple(pair) for pair in limits)
    values.update(rate)
    cfg = Config(**values)
    for name in ("milestone", "history_page_size", "discovery_backlog", "max_retries"):
        value = getattr(cfg, name)
        if type(value) is not int or value < (0 if name == "max_retries" else 1):
            raise ValueError(f"{name} must be a positive integer (max_retries may be zero)")
    if cfg.history_page_size > 100:
        raise ValueError("Match-V5 history_page_size must be <= 100")
    for name in ("min_free_gb", "progress_seconds", "seed_refresh_hours", "rank_cache_hours", "job_retry_seconds", "timeout_seconds"):
        value = getattr(cfg, name)
        if isinstance(value, bool) or not isinstance(value, (float, int)) or not math.isfinite(value) or value <= 0:
            raise ValueError(f"{name} must be finite and positive")
    if not 0 <= cfg.safety_margin < 1:
        raise ValueError("safety_margin must be in [0, 1)")
    return cfg
