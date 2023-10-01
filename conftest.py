import json
import sys
from logging import DEBUG, INFO, WARNING
from os import PathLike
from pathlib import Path, PureWindowsPath
from pprint import pprint
from shutil import copytree, rmtree
from typing import Final, Generator

import pytest
from coverage_badge.__main__ import main as gen_cov_badge

from alto2txt2fixture.create_adjacent_tables import OUTPUT, run
from alto2txt2fixture.plaintext import (
    DEFAULT_INITIAL_PK,
    FULLTEXT_DJANGO_MODEL,
    PlainTextFixture,
    PlaintextFixtureDict,
    PlaintextFixtureFieldsDict,
)
from alto2txt2fixture.utils import load_multiple_json

MODULE_PATH: Path = Path().absolute()

BADGE_PATH: Path = Path("docs") / "img" / "coverage.svg"

LWM_PLAINTEXT_FIXTURE_FOLDER: Final[Path] = Path("bl_lwm")
LWM_PLAINTEXT_FIXTURE: Final[Path] = (
    MODULE_PATH / "tests" / LWM_PLAINTEXT_FIXTURE_FOLDER
)
# HMD_PLAINTEXT_FIXTURE: Path = (
#     Path("tests") / "bl_hmd"
# )  # "0002645_plaintext.zip"

# @pytest.fixture
# l def hmd_metadata_fixture() -> Path:
#     """Path for 0002645 1853 metadata fixture."""
#     return Path("tests") / "0002645_metadata.zip"
#
#
# @pytest.fixture
# def hmd_plaintext_fixture() -> Path:
#     """Path for 0002645 1853 plaintext fixture."""
#     return HMD_PLAINTEXT_FIXTURE


# @pytest.fixture
# def uncached_folder(monkeypatch, tmp_path) -> None:
#     """Change local path to avoid using pre-cached data."""
#     monkeypatch.chdir(tmp_path)
#
#
# @pytest.fixture(autouse=True)
# def package_path(monkeypatch) -> None:
#     monkeypatch.chdir(MODULE_PATH)


@pytest.fixture(scope="session")
def adj_test_path(tmp_path_factory) -> Path:
    """Temp path for `adjacent_data_run_results` files."""
    return tmp_path_factory.mktemp(OUTPUT.name)


@pytest.mark.downloaded
@pytest.fixture(scope="session")
def adjacent_data_run_results(adj_test_path: Path) -> Generator[PathLike, None, None]:
    """Test `create_adjacent_tables.run`, using `cached` data if available.

    This fixture provides the results of `create_adjacent_tables.run` for tests
    to compare with. Include it as a parameter for tests that need those
    files downloaded locally to run.
    """
    run(output_path=adj_test_path)
    yield adj_test_path


@pytest.mark.downloaded
@pytest.fixture(scope="session")
def all_create_adjacent_tables_json_results(
    adjacent_data_run_results,
) -> Generator[list, None, None]:
    """Return a list of `json` results from `adjacent_data_run_results`."""
    yield load_multiple_json(adjacent_data_run_results)


@pytest.fixture
def bl_lwm(tmp_path) -> Generator[Path, None, None]:
    yield copytree(LWM_PLAINTEXT_FIXTURE, tmp_path / LWM_PLAINTEXT_FIXTURE_FOLDER)
    rmtree(tmp_path / LWM_PLAINTEXT_FIXTURE_FOLDER)


@pytest.fixture
def bl_lwm_plaintext(bl_lwm) -> Generator[PlainTextFixture, None, None]:
    bl_lwm_fixture: PlainTextFixture = PlainTextFixture(
        path=bl_lwm, data_provider_code="bl_lwm"
    )
    yield bl_lwm_fixture
    bl_lwm_fixture.delete_decompressed()


@pytest.fixture
def bl_lwm_plaintext_extracted(
    bl_lwm_plaintext,
) -> Generator[PlainTextFixture, None, None]:
    bl_lwm_plaintext.extract_compressed()
    yield bl_lwm_plaintext


@pytest.fixture
def bl_lwm_plaintext_json_export(
    bl_lwm_plaintext_extracted,
    tmp_path,
) -> Generator[PlainTextFixture, None, None]:
    bl_lwm_plaintext_extracted.export_to_json_fixtures(output_path=tmp_path)
    yield bl_lwm_plaintext_extracted


@pytest.fixture
def first_lwm_plaintext_json_dict(bl_lwm) -> PlaintextFixtureDict:
    return PlaintextFixtureDict(
        pk=DEFAULT_INITIAL_PK,
        model=FULLTEXT_DJANGO_MODEL,
        fields=PlaintextFixtureFieldsDict(
            text="billel\n\nB. RANNS,\n\nDRAPER & OUTFITTER,\nSTATION ROAD,"
            "\nCHAPELTOWN,\nu NNW SWIM I • LUSA LIMIT\nOF MI\n\n' "
            "NE'TEST Gi\n\n110111 TEM SIMON.\n",
            path=bl_lwm / "extracted/0003079/1898/0107/0003079_18980107_art0001.txt",
            compressed_path=bl_lwm / "0003079-test_plaintext.zip",
            errors=None,
        ),
    )


@pytest.fixture
def win_root_shadow_path() -> PureWindowsPath:
    return PureWindowsPath("S:\\\\Standing\\in\\the\\shadows\\of\\love.")


@pytest.fixture
def correct_win_path_trunc_str() -> str:
    """Correct truncated `str` for `win_root_shadow_path`."""
    return "S:\\Standing\\*\\*\\*\\*\\love."


@pytest.fixture()
def is_platform_win() -> bool:
    """Check if `sys.platform` is windows."""
    return sys.platform.startswith("win")


@pytest.fixture()
def is_platform_darwin() -> bool:
    """Check if `sys.platform` is windows."""
    return sys.platform.startswith("darwin")


@pytest.fixture()
def tmp_json_fixtures(tmp_path: Path) -> Generator[tuple[Path, ...], None, None]:
    """Return a `tuple` of test `json` fixture paths."""
    test_paths: tuple[Path, ...] = tuple(
        tmp_path / f"test_fixture-{i}.txt" for i in range(5)
    )
    for i, path in enumerate(test_paths):
        path.write_text(json.dumps({"id": i}))
    yield test_paths
    for path in test_paths:
        path.unlink()


@pytest.fixture(autouse=True)
def doctest_auto_fixtures(
    doctest_namespace: dict, is_platform_win: bool, is_platform_darwin: bool
) -> None:
    """Elements to add to default `doctest` namespace."""
    doctest_namespace["is_platform_win"] = is_platform_win
    doctest_namespace["is_platform_darwin"] = is_platform_darwin
    doctest_namespace["pprint"] = pprint
    doctest_namespace["pytest"] = pytest
    doctest_namespace["DEBUG"] = DEBUG
    doctest_namespace["INFO"] = INFO
    doctest_namespace["WARNING"] = WARNING


def pytest_sessionfinish(session, exitstatus):
    """Generate badges for docs after tests finish."""
    if exitstatus == 0:
        BADGE_PATH.parent.mkdir(parents=True, exist_ok=True)
        gen_cov_badge(["-o", f"{BADGE_PATH}", "-f"])
