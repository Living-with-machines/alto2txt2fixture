import pytest


@pytest.fixture
def uncached_folder(monkeypatch, tmpdir) -> None:
    """Change local path to be fresh of cached data."""
    monkeypatch.chdir(tmpdir)
