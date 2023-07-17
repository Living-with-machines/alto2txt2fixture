from pathlib import Path
from typing import Generator

import pytest
from _pytest.capture import CaptureResult
from pandas import DataFrame, read_csv

from alto2txt2fixture.create_adjacent_tables import (
    FILES,
    GAZETTEER_OUT_FILENAMES,
    ISSUE,
    MITCHELLS_OUT_FILENAMES,
    OUTPUT,
    RemoteDataFilesType,
    TableOutputConfigType,
    csv2json_list,
    download_data,
    run,
)
from alto2txt2fixture.utils import filter_json_fields, load_json, load_multiple_json


@pytest.fixture()
def dict_admin_counties() -> dict[str, list[str]]:
    return {"Q24826": ["Q23100", "Merseyside"]}


@pytest.fixture()
def test_admin_counties_config() -> RemoteDataFilesType:
    return {
        "dict_admin_counties": {
            "remote": "https://zooniversedata.blob.core.windows.net/downloads/Gazetteer-files/dict_admin_counties.json",
            "local": Path("cache/extra/path/dict_admin_counties.json"),
        }
    }


@pytest.fixture(scope="module")
def test_run() -> None:
    """Test run, using `cached` data if available.

    Note:
        This fixture is designed to ensure tests of `run` results, and
        should be included as a parameter to ensure `run()` has concluded
        prior to testing local results.
    """
    run()


@pytest.fixture(scope="module")
def all_json_results(test_run) -> Generator[list, None, None]:
    """Return read exported `Mitchells` dataset."""
    yield load_multiple_json(Path(OUTPUT))


@pytest.mark.slow
def test_download(uncached_folder) -> None:
    """Assuming intenet connectivity, test downloading needed files."""
    download_data()


def test_download_custom_folder(
    uncached_folder, test_admin_counties_config, capsys
) -> None:
    download_data(test_admin_counties_config)
    captured: CaptureResult = capsys.readouterr()
    assert captured.out.startswith(
        f"Downloading {test_admin_counties_config['dict_admin_counties']['local']}\n100%"
    )


def test_local_result_paths(test_run) -> None:
    """Test `Mitchells` `Entry` `json` and `csv` results."""
    for data_paths in FILES.values():
        assert data_paths["local"].is_file()
    all_outfiles: TableOutputConfigType = (
        MITCHELLS_OUT_FILENAMES | GAZETTEER_OUT_FILENAMES
    )
    for paths_dict in all_outfiles.values():
        assert Path(OUTPUT / paths_dict["csv"]).is_file()
        assert Path(OUTPUT / paths_dict["json"]).is_file()


def test_csv2json_list(test_run) -> None:
    """Test converting a `csv` file to `json` `Django` `fixture`."""
    test_mitchells_write_folder: Path = Path("test_mitchells")
    mitchells_issue_csv_path: Path = (
        Path(OUTPUT) / MITCHELLS_OUT_FILENAMES[ISSUE]["csv"]
    )
    mitchells_issue_df: DataFrame = read_csv(mitchells_issue_csv_path)
    mitchells_out: list = csv2json_list(
        mitchells_issue_csv_path, output_path=test_mitchells_write_folder
    )
    mitchells_json: dict = load_json(
        test_mitchells_write_folder / MITCHELLS_OUT_FILENAMES[ISSUE]["json"]
    )
    assert len(mitchells_issue_df) == len(mitchells_out)
    assert mitchells_json == mitchells_out


def test_mitchells_entry_15_newspaper_field(all_json_results: list) -> None:
    """Test `Mitchells` `Entry` 15 `json` and `csv` results.

    The `all_json_results` fixture should provide a `list` of results in a
    consistent order using `load_multiple_json`. Ideally this is as a `dict`
    rather than a list for indexing but for now assuming the list order will
    be consistent.

    Note:
        This is to verify correct results necessary to close issue #11
    """
    mitchells_entry_15: dict = all_json_results[7][14]
    assert mitchells_entry_15["model"] == "mitchells.entry"
    assert mitchells_entry_15["fields"]["title"].startswith("LLOYD'S WEEKLY")
    assert mitchells_entry_15["fields"]["newspaper"] == 1187


@pytest.mark.xfail
def test_mitchells_empty_newspaper_field(all_json_results: list) -> None:
    """Test if any `Mitchells` `Entry` has an empty `Newspaper` field."""
    empty_newspaper_records: list | dict = filter_json_fields(
        all_json_results[7], fields=("newspaper",), value=""
    )
    # This currently fails in 878 cases
    assert len(empty_newspaper_records) == 0
