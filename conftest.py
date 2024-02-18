import json
import sys
from logging import DEBUG, INFO, WARNING
from os import PathLike, chdir
from pathlib import Path, PureWindowsPath
from pprint import pprint
from shutil import copytree, rmtree
from typing import Callable, Final, Generator

import pytest
from coverage_badge.__main__ import main as gen_cov_badge

from alto2txt2fixture.create_adjacent_tables import OUTPUT, run
from alto2txt2fixture.plaintext import (
    DEFAULT_INITIAL_PK,
    FULLTEXT_DJANGO_MODEL,
    PlainTextFixture,
    PlainTextFixtureDict,
    PlainTextFixtureFieldsDict,
)
from alto2txt2fixture.utils import load_multiple_json

MODULE_PATH: Path = Path().absolute()

BADGE_PATH: Path = Path("docs") / "img" / "coverage.svg"

LWM_PLAINTEXT_FIXTURE_FOLDER: Final[Path] = Path("bl_lwm")
HMD_FIXTURE_FOLDER: Final[Path] = Path("bl_hmd")
LWM_PLAINTEXT_FIXTURE: Final[Path] = (
    MODULE_PATH / "tests" / LWM_PLAINTEXT_FIXTURE_FOLDER
)
HMD_FIXTURE_PATH: Path = (
    MODULE_PATH / "tests" / HMD_FIXTURE_FOLDER
)  # "0002645_plaintext.zip"
LWM_FIRST_PLAINTEXT_FIXTURE_ZIP_FILE_NAME: Final[PathLike] = Path(
    "0003079-test_plaintext.zip"
)
LWM_FIRST_PLAINTEXT_FIXTURE_EXTRACTED_PATH: Final[PathLike] = Path(
    "0003079/1898/0107/0003079_18980107_art0001.txt",
)
LWM_OUTPUT_FOLDER: Final[Path] = Path("lwm_test_output")
TEST_EXTRACT_SUBDIR: Final[Path] = Path("test-extracted")
DEMOGRAPHICS_ENGLAND_WALES_1851: Final[Path] = Path(
    "census/data/demographics_england_wales_2015.csv"
)


# @pytest.fixture
@pytest.fixture
def bl_hmd(tmp_path) -> Generator[Path, None, None]:
    yield copytree(HMD_FIXTURE_PATH, tmp_path / HMD_FIXTURE_FOLDER)
    rmtree(tmp_path / HMD_FIXTURE_FOLDER)


@pytest.fixture
def bl_hmd_meta(bl_hmd) -> Generator[Path, None, None]:
    """Path for '0002645-1853' metadata fixture."""
    yield bl_hmd / "metadata" / "0002645_metadata.zip"


#
# @pytest.fixture
# def hmd_plaintext_fixture() -> Path:
#     """Path for 0002645 1853 plaintext fixture."""
#     return HMD_PLAINTEXT_FIXTURE


@pytest.fixture
def lwm_output_path() -> Path:
    """Return `LWM_OUTPUT_FOLDER` for testing."""
    return LWM_OUTPUT_FOLDER


@pytest.fixture
def json_export_filename() -> str:
    """Return default first `plaintext` `json` file name for testing."""
    return "plaintext_fixture-000001.json"


@pytest.fixture
def json_export_filename_zip(json_export_filename: str) -> str:
    """Return `LWM_OUTPUT_FOLDER` for testing."""
    return json_export_filename + ".zip"


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
    chdir(bl_lwm.parent)
    bl_lwm_fixture: PlainTextFixture = PlainTextFixture(
        path=Path(bl_lwm.name),
        data_provider_code="bl-lwm",
    )
    yield bl_lwm_fixture


@pytest.fixture
def bl_lwm_plaintext_extracted(
    bl_lwm_plaintext,
) -> Generator[PlainTextFixture, None, None]:
    bl_lwm_plaintext.extract_subdir = TEST_EXTRACT_SUBDIR
    bl_lwm_plaintext.extract_compressed()
    yield bl_lwm_plaintext
    bl_lwm_plaintext.delete_decompressed()


@pytest.fixture
def bl_lwm_plaintext_json_export(
    bl_lwm_plaintext_extracted,
) -> Generator[PlainTextFixture, None, None]:
    bl_lwm_plaintext_extracted.export_to_json_fixtures(output_path=LWM_OUTPUT_FOLDER)
    yield bl_lwm_plaintext_extracted


@pytest.fixture
def lwm_plaintext_json_dict_factory() -> (
    Callable[[int, PathLike, PathLike, str], PlainTextFixtureDict]
):
    def make_plaintext_fixture_dict(
        pk: int = DEFAULT_INITIAL_PK,
        fixture_path: PathLike = LWM_FIRST_PLAINTEXT_FIXTURE_EXTRACTED_PATH,
        fixture_compressed_path: PathLike = LWM_FIRST_PLAINTEXT_FIXTURE_ZIP_FILE_NAME,
        errors: str | None = None,
    ) -> PlainTextFixtureDict:
        return PlainTextFixtureDict(
            pk=pk,
            model=FULLTEXT_DJANGO_MODEL,
            fields=PlainTextFixtureFieldsDict(
                text="billel\n\nB. RANNS,\n\nDRAPER & OUTFITTER,\nSTATION ROAD,"
                "\nCHAPELTOWN,\nu NNW SWIM I • LUSA LIMIT\nOF MI\n\n' "
                "NE'TEST Gi\n\n110111 TEM SIMON.\n",
                text_path=str(fixture_path),
                text_compressed_path=str(fixture_compressed_path),
                errors=errors,
            ),
        )

    return make_plaintext_fixture_dict


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
    """Check if `sys.platform` is `Darwin` (macOS)."""
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


@pytest.fixture
def text_fixture_path_dict(json_export_filename: str) -> dict[str, str]:
    """Example field to add in fixture generation."""
    return {"text_fixture_path": json_export_filename}


@pytest.fixture
def demographics_1851_local_path() -> Path:
    """Test path to save example demographics data."""
    return DEMOGRAPHICS_ENGLAND_WALES_1851


@pytest.fixture(autouse=True)
def doctest_auto_fixtures(
    doctest_namespace: dict,
    is_platform_win: bool,
    is_platform_darwin: bool,
    demographics_1851_local_path: Path,
) -> None:
    """Elements to add to default `doctest` namespace."""
    if demographics_1851_local_path.exists():
        demographics_1851_local_path.unlink()
    doctest_namespace["is_platform_win"] = is_platform_win
    doctest_namespace["is_platform_darwin"] = is_platform_darwin
    doctest_namespace["pprint"] = pprint
    doctest_namespace["pytest"] = pytest
    doctest_namespace["DEBUG"] = DEBUG
    doctest_namespace["INFO"] = INFO
    doctest_namespace["WARNING"] = WARNING
    doctest_namespace["demographics_1851_local_path"] = demographics_1851_local_path


def pytest_sessionfinish(session, exitstatus):
    """Generate badges for docs after tests finish."""
    if exitstatus == 0:
        BADGE_PATH.parent.mkdir(parents=True, exist_ok=True)
        gen_cov_badge(["-o", f"{BADGE_PATH}", "-f"])
