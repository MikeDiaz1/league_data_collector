from __future__ import annotations

from dataclasses import dataclass, field, fields
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
    recent_window_days: float = 14.0
    recent_refresh_hours: float = 6.0
    recent_seed_players_per_tier: int = 100
    recent_participant_players_per_region: int = 100
    recent_matches_per_player: int = 20
    recent_tier_limits: dict[str, dict[str, int]] = field(default_factory=dict)
    recent_uniform_fraction: float = 0.5
    recent_sampling_weight: int = 8
    participant_sampling_weight: int = 1
    backfill_sampling_weight: int = 1
    sampling_seed: int = 42
    rank_cache_hours: float = 168.0
    job_retry_seconds: float = 60.0
    shutdown_grace_seconds: float = 60.0
    app_limits: tuple[tuple[int, float], ...] = ((20, 1), (100, 120))
    safety_margin: float = 0.1
    timeout_seconds: float = 30.0
    max_retries: int = 5

    def recent_limits(self, tier: str) -> tuple[int, int]:
        overrides = self.recent_tier_limits.get(tier, {})
        players = self.recent_participant_players_per_region if tier == "UNKNOWN" else self.recent_seed_players_per_tier
        return overrides.get("players", players), overrides.get("matches", self.recent_matches_per_player)


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
    for name in (
        "recent_seed_players_per_tier", "recent_matches_per_player",
        "recent_sampling_weight", "backfill_sampling_weight",
    ):
        if type(getattr(cfg, name)) is not int or getattr(cfg, name) < 1:
            raise ValueError(f"{name} must be a positive integer")
    for name in ("recent_participant_players_per_region", "participant_sampling_weight", "sampling_seed"):
        if type(getattr(cfg, name)) is not int or getattr(cfg, name) < 0:
            raise ValueError(f"{name} must be a nonnegative integer")
    if cfg.recent_matches_per_player > 100:
        raise ValueError("Match-V5 recent_matches_per_player must be <= 100")
    if (type(cfg.recent_uniform_fraction) not in (float, int)
            or not math.isfinite(cfg.recent_uniform_fraction) or not 0 < cfg.recent_uniform_fraction <= 1):
        raise ValueError("recent_uniform_fraction must be finite and in (0, 1]")
    if not isinstance(cfg.recent_tier_limits, dict):
        raise ValueError("recent_tier_limits must be a table of tier overrides")
    for tier, limits in cfg.recent_tier_limits.items():
        if tier not in {"EMERALD", "DIAMOND", "MASTER", "GRANDMASTER", "CHALLENGER"}:
            raise ValueError(f"Unknown recent_tier_limits tier: {tier}")
        if not isinstance(limits, dict) or set(limits) - {"players", "matches"}:
            raise ValueError("recent_tier_limits accepts only players and matches")
        for name, value in limits.items():
            if type(value) is not int or value < 1 or (name == "matches" and value > 100):
                raise ValueError(f"recent_tier_limits.{tier}.{name} must be a positive integer; matches <= 100")
    for name in ("min_free_gb", "progress_seconds", "seed_refresh_hours", "rank_cache_hours", "job_retry_seconds", "shutdown_grace_seconds", "timeout_seconds", "recent_window_days", "recent_refresh_hours"):
        value = getattr(cfg, name)
        if isinstance(value, bool) or not isinstance(value, (float, int)) or not math.isfinite(value) or value <= 0:
            raise ValueError(f"{name} must be finite and positive")
    if not 0 <= cfg.safety_margin < 1:
        raise ValueError("safety_margin must be in [0, 1)")
    return cfg
