import pytest

from riot_collector import power


def test_idle_sleep_request_lasts_for_context_and_restores_previous_thread_state(monkeypatch):
    monkeypatch.setattr(power, "IS_WINDOWS", True)
    calls = []

    def set_state(flags):
        calls.append(flags)
        return 0x80000002  # Preserve an earlier display requirement on exit.

    monkeypatch.setattr(power, "_set_execution_state", set_state)
    with power.keep_awake():
        assert calls == [0x80000001]  # System only; do not force the display on.
    assert calls == [0x80000001, 0x80000002]


def test_sleep_request_is_released_on_collection_failure(monkeypatch):
    monkeypatch.setattr(power, "IS_WINDOWS", True)
    calls = []
    monkeypatch.setattr(power, "_set_execution_state", lambda flags: calls.append(flags) or 0x80000000)
    with pytest.raises(RuntimeError, match="collection failed"):
        with power.keep_awake():
            raise RuntimeError("collection failed")
    assert calls == [0x80000001, 0x80000000]


def test_refused_sleep_request_prevents_collection_start(monkeypatch):
    monkeypatch.setattr(power, "IS_WINDOWS", True)
    monkeypatch.setattr(power, "_set_execution_state", lambda flags: 0)
    with pytest.raises(OSError, match="Windows refused"):
        with power.keep_awake():
            pytest.fail("must not start without requested sleep protection")


def test_other_platforms_do_not_load_windows_api(monkeypatch):
    monkeypatch.setattr(power, "IS_WINDOWS", False)
    monkeypatch.setattr(power, "_set_execution_state", lambda flags: pytest.fail("Windows API on another platform"))
    with power.keep_awake():
        pass


def test_cli_keeps_sleep_protection_until_collection_finishes(tmp_path, monkeypatch):
    from riot_collector import cli

    config = tmp_path / "collector.toml"
    config.write_text("[collector]\n", encoding="utf-8")
    calls = []
    monkeypatch.setenv("RIOT_API_KEY", "offline-test-key")
    monkeypatch.setattr(power, "IS_WINDOWS", True)
    monkeypatch.setattr(power, "_set_execution_state", lambda flags: calls.append(flags) or 0x80000000)
    monkeypatch.setattr(cli, "configure_logging", lambda *a, **kw: None)

    async def collect(*args):
        assert calls == [0x80000001]

    monkeypatch.setattr(cli, "run_collection", collect)
    assert cli.main(["--config", str(config), "collect"]) == 0
    assert calls == [0x80000001, 0x80000000]
