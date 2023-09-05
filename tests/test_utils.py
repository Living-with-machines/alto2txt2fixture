from pathlib import Path, PureWindowsPath

import pytest

from alto2txt2fixture.create_adjacent_tables import (
    GAZETTEER_OUT_FILENAMES,
    MITCHELLS_OUT_FILENAMES,
    TableOutputConfigType,
    download_data,
)
from alto2txt2fixture.utils import (
    check_newspaper_collection_configuration,
    truncate_path_str,
)


@pytest.mark.download
def test_json_results_ordering(all_create_adjacent_tables_json_results: list) -> None:
    """Test the ordering of `all_create_adjacent_tables_json_results`."""
    json_outfile_names: TableOutputConfigType = sorted(
        result["json"].lower()
        for result in (MITCHELLS_OUT_FILENAMES | GAZETTEER_OUT_FILENAMES).values()
    )
    for i, json_fixture in enumerate(all_create_adjacent_tables_json_results):
        assert json_fixture[0]["model"] == Path(json_outfile_names[i]).stem


@pytest.mark.slow
@pytest.mark.download
def test_download() -> None:
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


@pytest.mark.parametrize(
    "head_parts, tail_parts", ((500, 0), (0, 500), (-1, 50), (50, -1))
)
def test_bad_head_tail_logging(
    head_parts: int,
    tail_parts: int,
    win_root_shadow_path: PureWindowsPath,
    caplog,
) -> None:
    """Test invalid indexing options."""
    test_result: str = truncate_path_str(
        path=win_root_shadow_path,
        head_parts=head_parts,
        tail_parts=tail_parts,
        folder_filler_str="*",
        path_sep="\\",
        max_length=10,
        _force_type=PureWindowsPath,
    )
    assert PureWindowsPath(test_result) == win_root_shadow_path
    if head_parts < 0 or tail_parts < 0:
        index_params_error_log: str = (
            "Both index params for `truncate_path_str` must be >=0: "
            f"(head_parts={head_parts}, tail_parts={tail_parts})"
        )
        assert index_params_error_log in caplog.text
    else:
        drive_or_absolute_log: str = (
            f"Adding 1 to `head_parts`: {head_parts} to truncate: "
            f"'{str(win_root_shadow_path)[:10]}"
        )
        truncate_error_log: str = (
            f"Returning untruncated. Params "
            f"(head_parts={head_parts + 1}, tail_parts={tail_parts}) "
            f"not valid to truncate: '{str(win_root_shadow_path)[:10]}"
        )
        assert drive_or_absolute_log in caplog.text
        assert truncate_error_log in caplog.text


def test_windows_root_path_truncate(
    win_root_shadow_path: PureWindowsPath, correct_win_path_trunc_str: str
) -> None:
    """Test `truncate_path_str` for a root directory on `PureWindowsPath`."""
    short_root: str = truncate_path_str(
        win_root_shadow_path,
        folder_filler_str="*",
        path_sep="\\",
        max_length=10,
        _force_type=PureWindowsPath,
    )
    assert short_root == correct_win_path_trunc_str
