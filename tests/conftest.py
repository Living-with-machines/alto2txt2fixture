from pathlib import Path

import pytest

from alto2txt2fixture.create_adjacent_tables import RemoteDataFilesType


@pytest.fixture
def uncached_folder(monkeypatch, tmpdir) -> Path:
    """Change local path to be fresh of cached data."""
    return monkeypatch.chdir(tmpdir)


@pytest.fixture
def dict_admin_counties() -> dict[str, list[str]]:
    return {"Q24826": ["Q23100", "Merseyside"]}


@pytest.fixture
def custom_admin_counties(uncached_folder, dict_admin_counties) -> RemoteDataFilesType:
    return {
        "dict_admin_counties": {
            "remote": "https://zooniversedata.blob.core.windows.net/downloads/Gazetteer-files/dict_admin_counties.json",
            "local": Path("cache/extra/path/dict_admin_counties.json"),
        }
    }
