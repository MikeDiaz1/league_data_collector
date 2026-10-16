"""Keep pytest scratch files under the ignored tools directory."""


def pytest_configure(config):
    # Pytest's --basetemp creates its leaf directory, but not missing parents.
    (config.rootpath / ".tools" / "pytest").mkdir(parents=True, exist_ok=True)
