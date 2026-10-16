import pytest

from riot_collector.config import load_config


def read_config(tmp_path, text):
    path = tmp_path / "config.toml"
    path.write_text(text, encoding="utf-8")
    return load_config(path)


def test_legacy_config_gets_balanced_sampling_defaults(tmp_path):
    cfg = read_config(tmp_path, "[collector]\ndata_dir='old-data'\nhistory_page_size=100\n")
    assert cfg.data_dir == tmp_path / "old-data"
    assert cfg.recent_window_days == 14
    assert cfg.recent_refresh_hours == 6
    assert cfg.recent_seed_players_per_tier == 100
    assert cfg.recent_participant_players_per_region == 100
    assert cfg.recent_matches_per_player == 20
    assert (cfg.recent_sampling_weight, cfg.participant_sampling_weight, cfg.backfill_sampling_weight) == (8, 1, 1)
    assert cfg.sampling_seed == 42
    assert cfg.app_limits == ((20, 1), (100, 120))


@pytest.mark.parametrize("name", ["recent_window_days", "recent_refresh_hours"])
@pytest.mark.parametrize("value", ["0", "-1.0", "nan", "inf", "true", "'six'"])
def test_window_and_refresh_must_be_finite_positive_numbers(tmp_path, name, value):
    with pytest.raises(ValueError, match=name):
        read_config(tmp_path, f"[collector]\n{name}={value}\n")


@pytest.mark.parametrize("name", [
    "recent_seed_players_per_tier", "recent_matches_per_player",
    "recent_sampling_weight", "backfill_sampling_weight",
])
@pytest.mark.parametrize("value", ["0", "-1", "1.5", "true", "'10'"])
def test_positive_sampling_counts_are_validated(tmp_path, name, value):
    with pytest.raises(ValueError, match=name):
        read_config(tmp_path, f"[collector]\n{name}={value}\n")


@pytest.mark.parametrize("name", [
    "recent_participant_players_per_region", "participant_sampling_weight", "sampling_seed",
])
@pytest.mark.parametrize("value", ["-1", "1.5", "true", "'10'"])
def test_nonnegative_sampling_counts_are_validated(tmp_path, name, value):
    with pytest.raises(ValueError, match=name):
        read_config(tmp_path, f"[collector]\n{name}={value}\n")


def test_recent_match_page_cannot_exceed_api_limit(tmp_path):
    with pytest.raises(ValueError, match="recent_matches_per_player"):
        read_config(tmp_path, "[collector]\nrecent_matches_per_player=101\n")


def test_zero_disables_optional_participant_sampling_and_seed_zero_is_valid(tmp_path):
    cfg = read_config(tmp_path, """[collector]
recent_participant_players_per_region=0
participant_sampling_weight=0
sampling_seed=0
recent_window_days=0.5
recent_refresh_hours=1
recent_matches_per_player=100
""")
    assert cfg.recent_participant_players_per_region == cfg.participant_sampling_weight == cfg.sampling_seed == 0
    assert cfg.recent_window_days == 0.5
    assert cfg.recent_refresh_hours == 1
    assert cfg.recent_matches_per_player == 100


def test_tier_limits_override_independently_and_leave_other_tiers_alone(tmp_path):
    cfg = read_config(tmp_path, """[collector]
recent_uniform_fraction=0.5
[collector.recent_tier_limits.EMERALD]
players=200
matches=10
[collector.recent_tier_limits.DIAMOND]
players=200
matches=10
""")
    assert cfg.recent_limits("EMERALD") == cfg.recent_limits("DIAMOND") == (200, 10)
    assert all(cfg.recent_limits(tier) == (100, 20) for tier in ("MASTER", "GRANDMASTER", "CHALLENGER", "UNKNOWN"))


@pytest.mark.parametrize("text", [
    "recent_uniform_fraction=0", "recent_uniform_fraction=true", "recent_uniform_fraction=nan",
    "recent_uniform_fraction=1.1", "recent_tier_limits=20",
    "[collector.recent_tier_limits.GOLD]\nplayers=200",
    "[collector.recent_tier_limits.EMERALD]\nplayers=true",
    "[collector.recent_tier_limits.EMERALD]\nmatches=101",
    "[collector.recent_tier_limits.EMERALD]\nplayers=0",
    "[collector.recent_tier_limits.EMERALD]\nother=1",
])
def test_invalid_rotation_or_tier_limits_are_rejected(tmp_path, text):
    with pytest.raises(ValueError):
        read_config(tmp_path, "[collector]\n" + text)
