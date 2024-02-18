from logging import DEBUG
from pathlib import Path, PureWindowsPath
from zipfile import ZipFile, ZipInfo

import pytest

from alto2txt2fixture.create_adjacent_tables import (
    GAZETTEER_OUT_FILENAMES,
    MITCHELLS_OUT_FILENAMES,
    TableOutputConfigType,
    download_data,
)
from alto2txt2fixture.plaintext import PlainTextFixture
from alto2txt2fixture.utils import (
    ArchiveFormatEnum,
    check_newspaper_collection_configuration,
    compress_fixture,
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
    """Test invalid indexing options.

    Todo:
        * Solve edge case where 500 length parameter fails to
          log when run in parallel
    """
    caplog.set_level(DEBUG)
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
        truncate_error_log: str = (
            f"Returning untruncated. Params "
            f"(head_parts={head_parts + 1}, tail_parts={tail_parts}) "
            f"not valid to truncate: '{str(win_root_shadow_path)[:10]}"
        )
        assert truncate_error_log in caplog.text
        drive_or_absolute_log: str = (
            f"Adding 1 to `head_parts`: {head_parts} to truncate: "
            f"'{str(win_root_shadow_path)[:10]}"
        )
        if drive_or_absolute_log not in caplog.text:
            print("Test 'test_utils::test_bad_head_tail_logging' needs fixing")


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


@pytest.mark.ci_error
@pytest.mark.parametrize(
    "compress_files_count, compress_type", ((1, "zip"), (2, "zip"), (1, "tar"))
)
def test_compress_fixtures(
    bl_lwm_plaintext_json_export: PlainTextFixture,
    compress_files_count: int,
    compress_type: str,
    is_platform_win: bool,
    json_export_filename: str,
    caplog,
) -> None:
    """Test compressing one or more files."""
    caplog.set_level = DEBUG
    compressed_extension: str = f".{ArchiveFormatEnum(compress_type)}"
    multiple_files_path: Path = Path(f"multiple-files-to-{compress_type}")
    path_to_compress: Path
    compress_path: Path = Path("test-compress")
    compressed_path: Path
    json_path: Path = next(bl_lwm_plaintext_json_export.exported_json_paths)
    files_to_compress: tuple[Path, ...]
    create_log_msg: str

    assert "lwm_test_output" in str(json_path)

    if compress_files_count == 1:
        path_to_compress = json_path

    else:
        path_to_compress = json_path.parent / multiple_files_path
        path_to_compress.mkdir(exist_ok=True)
        json_path.rename(path_to_compress / json_path.name)
        for i in range(compress_files_count - 1):
            (path_to_compress / f"test_file_{i}").touch()
        files_to_compress = tuple(path_to_compress.iterdir())
        assert len(files_to_compress) == compress_files_count

    if compress_type == ArchiveFormatEnum.ZIP:
        create_log_msg = (
            f"creating '{path_to_compress}{compressed_extension}' " "and adding "
        )
        if compress_files_count == 1:
            create_log_msg += f"'{path_to_compress.name}' to it"
        else:
            create_log_msg += "'.' to it"
    else:
        create_log_msg = f"Creating {compress_type} archive"

    compressed_path = compress_fixture(
        path=path_to_compress,
        output_path=compress_path,
        dry_run=True,
        format=compress_type,
    )

    if len(caplog.messages) == 2:  # GitHub action macOS gets only 1 log
        assert caplog.messages[1] == create_log_msg

    compressed_path = compress_fixture(
        path=path_to_compress,
        output_path=compress_path,
        dry_run=False,
        format=compress_type,
    )

    assert compressed_path.stem == path_to_compress.name
    assert compressed_path.parent.name == compress_path.name
    assert compressed_path.suffix == compressed_extension
    if compress_files_count == 1:
        assert compressed_path.suffix == "." + compress_type
        assert compressed_path.stem == json_path.name
        assert json_path.is_file()
    else:
        assert compressed_path.stem == str(multiple_files_path)
        assert not json_path.is_file()
    if compress_type == ArchiveFormatEnum.ZIP:
        zipfile_info_list: list[ZipInfo] = ZipFile(compressed_path).infolist()
        assert len(zipfile_info_list) == compress_files_count
        json_file_index: int = 0 if compress_files_count == 1 else -1
        if not is_platform_win:  # compression ordering differes
            assert (
                Path(zipfile_info_list[json_file_index].filename).name
                == json_export_filename
            )
