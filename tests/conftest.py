from pathlib import Path
from typing import Generator

import pytest

from alto2txt2fixture.create_adjacent_tables import OUTPUT, run
from alto2txt2fixture.utils import load_multiple_json


@pytest.fixture
def uncached_folder(monkeypatch, tmpdir) -> Path:
    """Change local path to be fresh of cached data."""
    return monkeypatch.chdir(tmpdir)


@pytest.mark.downloaded
@pytest.fixture(scope="session")
def adjacent_data_run_results() -> None:
    """Test `create_adjacent_tables.run`, using `cached` data if available.

    This fixture provides the results of `create_adjacent_tables.run` for tests
    to compare with. Include it as a parameter for tests that need those
    files downloaded locally to run.
    """
    run()


@pytest.fixture(scope="session")
def all_create_adjacent_tables_json_results(
    adjacent_data_run_results,
) -> Generator[list, None, None]:
    """Return a list of `json` results from `adjacent_data_run_results`."""
    yield load_multiple_json(Path(OUTPUT))
