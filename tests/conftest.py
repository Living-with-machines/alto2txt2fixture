from pathlib import Path

import pytest


@pytest.fixture()
def uncached_folder(monkeypatch, tmpdir) -> Path:
    """Change local path to be fresh of cached data."""
    return monkeypatch.chdir(tmpdir)
