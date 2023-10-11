from pathlib import Path

import pytest
from _pytest.capture import CaptureResult
from pandas import DataFrame, read_csv

from alto2txt2fixture.create_adjacent_tables import (
    FILES,
    GAZETTEER_OUT_FILENAMES,
    ISSUE,
    MITCHELLS_OUT_FILENAMES,
    RemoteDataFilesType,
    TableOutputConfigType,
    csv2json_list,
    download_data,
)
from alto2txt2fixture.utils import filter_json_fields, load_json


@pytest.fixture()
def dict_admin_counties() -> dict[str, list[str]]:
    return {"Q24826": ["Q23100", "Merseyside"]}


@pytest.fixture()
def test_admin_counties_config(tmp_path) -> RemoteDataFilesType:
    return {
        "dict_admin_counties": {
            "remote": "https://zooniversedata.blob.core.windows.net/downloads/Gazetteer-files/dict_admin_counties.json",
            "local": tmp_path / "dict_admin_counties.json",
        }
    }


@pytest.mark.slow
@pytest.mark.download
def test_download_custom_folder(test_admin_counties_config, capsys) -> None:
    download_data(test_admin_counties_config)
    captured: CaptureResult = capsys.readouterr()
    assert captured.out.startswith(f"Downloading")
    similar_path_parts: list[tuple[str, bool]] = []
    for part in Path(test_admin_counties_config["dict_admin_counties"]["local"]).parts:
        similar_path_parts.append((part, part in captured.out))
    assert sum(int(not matches) for part, matches in similar_path_parts) < 2
    assert "100%" in captured.out


@pytest.mark.slow
@pytest.mark.download
def test_local_result_paths(adjacent_data_run_results) -> None:
    """Test `Mitchells` and `Gazetteer` `json` and `csv` results."""
    for data_paths in FILES.values():
        assert data_paths["local"].is_file()
    all_outfiles: TableOutputConfigType = (
        MITCHELLS_OUT_FILENAMES | GAZETTEER_OUT_FILENAMES
    )
    for paths_dict in all_outfiles.values():
        assert Path(adjacent_data_run_results / paths_dict["csv"]).is_file()
        assert Path(adjacent_data_run_results / paths_dict["json"]).is_file()


@pytest.mark.download
def test_csv2json_list(adjacent_data_run_results) -> None:
    """Test converting a `csv` file to `json` `Django` `fixture`."""
    test_mitchells_write_folder: Path = Path("test_mitchells")
    mitchells_issue_csv_path: Path = (
        adjacent_data_run_results / MITCHELLS_OUT_FILENAMES[ISSUE]["csv"]
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


@pytest.mark.download
def test_mitchells_entry_15_newspaper_field(
    all_create_adjacent_tables_json_results: list,
) -> None:
    """Test `Mitchells` `Entry` 15 `json` and `csv` results.

    The `all_create_adjacent_tables_json_results` fixture should provide a `list` of results in a
    consistent order using `load_multiple_json`. Ideally this is as a `dict`
    rather than a list for indexing but for now assuming the list order will
    be consistent.

    Note:
        This is to verify correct results necessary to close issue #11
    """
    mitchells_entry_15: dict = all_create_adjacent_tables_json_results[4][15]
    assert mitchells_entry_15["model"] == "mitchells.entry"
    assert mitchells_entry_15["fields"]["title"].startswith("LLOYD'S WEEKLY")
    assert mitchells_entry_15["fields"]["newspaper"] == 1187


@pytest.mark.download
def test_mitchells_empty_newspaper_field(
    all_create_adjacent_tables_json_results: list,
) -> None:
    """Test if any `Mitchells` `Entry` has an empty `Newspaper` field."""
    empty_newspaper_records: list | dict = filter_json_fields(
        all_create_adjacent_tables_json_results[4], fields=("newspaper",), value=""
    )
    # This originally failed in 878 cases
    assert len(empty_newspaper_records) == 0


@pytest.mark.download
def test_correct_gazetteer_null(all_create_adjacent_tables_json_results: list) -> None:
    """Test fixinging `Gazetteer` `AdminCounty` references."""
    empty_gazetteer_place_records: list | dict = filter_json_fields(
        all_create_adjacent_tables_json_results[3], fields=("admin_county",), value=""
    )
    # This originally failed in 1075 (all but 1) case(s)
    assert len(empty_gazetteer_place_records) == 0
