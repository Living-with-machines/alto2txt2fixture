from pathlib import Path

import pytest

from alto2txt2fixture.create_adjacent_tables import (
    GAZETTEER_OUT_FILENAMES,
    MITCHELLS_OUT_FILENAMES,
    TableOutputConfigType,
    download_data,
)
from alto2txt2fixture.utils import check_newspaper_collection_configuration


@pytest.mark.downloads
def test_json_results_ordering(all_create_adjacent_tables_json_results: list) -> None:
    """Test the ordering of `all_create_adjacent_tables_json_results`."""
    json_outfile_names: TableOutputConfigType = sorted(
        result["json"].lower()
        for result in (MITCHELLS_OUT_FILENAMES | GAZETTEER_OUT_FILENAMES).values()
    )
    for i, json_fixture in enumerate(all_create_adjacent_tables_json_results):
        assert json_fixture[0]["model"] == Path(json_outfile_names[i]).stem


@pytest.mark.slow
@pytest.mark.downloads
def test_download(uncached_folder) -> None:
    """Assuming intenet connectivity, test downloading needed files."""
    download_data()


def test_check_newspaper_collection_config(capsys) -> None:
    """Test returning unmached column names"""
    correct_log_prefix: str = (
        "Warning: 2 `collections` not in `newspaper_collections`: {"
    )
    unmatched: set[str] = check_newspaper_collection_configuration(["cat", "dog"])
    assert unmatched == {"cat", "dog"}
    assert correct_log_prefix in capsys.readouterr().out
