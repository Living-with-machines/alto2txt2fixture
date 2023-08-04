from pathlib import Path
from typing import Generator

import pytest
from coverage_badge.__main__ import main as gen_cov_badge

from alto2txt2fixture.create_adjacent_tables import OUTPUT, run
from alto2txt2fixture.utils import load_multiple_json

BADGE_PATH: Path = Path("docs") / "assets" / "coverage.svg"


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
def all_create_adjacent_tables_json_results() -> Generator[list, None, None]:
    """Return a list of `json` results from `adjacent_data_run_results`."""
    yield load_multiple_json(Path(OUTPUT))


def pytest_sessionfinish(session, exitstatus):
    """Generate badges for docs after tests finish."""
    if exitstatus == 0:
        gen_cov_badge(["-o", f"{BADGE_PATH}", "-f"])
