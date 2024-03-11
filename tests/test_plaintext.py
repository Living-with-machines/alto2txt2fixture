import json
from pathlib import Path
from typing import Final

import pytest

from alto2txt2fixture.plaintext import PlainTextFixture
from alto2txt2fixture.utils import ArchiveFormatEnum

FIXTURE_FILE_NAMES: Final[tuple[str, ...]] = (
    "0003548_plaintext-OSX-utf8-error.zip",
    "0003548_19040707_art0037.txt",
    "0003548-test_plaintext.zip",
    "0003079_metadata.zip",
    "0003079-test_plaintext.zip",
    "0003079_18980121_sect0001.txt",
)
FIXTURE_REGEX_MATCHES: Final[dict[str, tuple[int, ...]]] = {
    "*test_plaintext.zip": (2, 4),
    "*.zip": (0, 2, 4),
    "*art*.txt": (1,),
    "*.txt": (1, 5),
}


def test_doctest_plaintext_process_json_export_example(
    bl_lwm_plaintext_json_export: PlainTextFixture,
) -> None:
    """Test basic json export."""
    for json_path in bl_lwm_plaintext_json_export.exported_json_paths:
        export_dict: dict = json.loads(Path(json_path).read_text())
        assert len(export_dict) == 42
        id: int | None = None
        for fixture in export_dict:
            if not id:
                id = fixture["pk"]
            else:
                id += 1
            assert fixture["pk"] == id
            assert fixture["fields"]
            assert len(fixture["fields"]["text"]) > 0
            assert fixture["fields"]["errors"] is None
            assert fixture["fields"]["text_path"] is not None
            assert fixture["fields"]["text_path"].endswith(".txt")


@pytest.mark.parametrize(
    "glob_plaintext, initial_pk, data_provider_code, compress, "
    "max_plaintext_per_fixture_file, destination_compression",
    (
        ("*test_plaintext.zip", 1, "bl-hmd", False, 100, ArchiveFormatEnum.ZIP),
        ("*test_plaintext.zip", 5, "bl-hmd", False, 10, ArchiveFormatEnum.ZIP),
        ("*test_plaintext.zip", 5, "bl-hmd", True, 10, ArchiveFormatEnum.ZIP),
    ),
)
def test_plaintext_configs(
    capsys,
    bl_lwm: Path,
    tmp_path: Path,
    # pytest.mark.parametrized vars are below
    glob_plaintext: str,
    initial_pk: int,
    data_provider_code: str,
    compress: bool,
    max_plaintext_per_fixture_file: int,
    destination_compression: ArchiveFormatEnum,
) -> None:
    """Test different `Plaintext` config options."""
    export_directory: Path = tmp_path / "json-exports"
    json_compressed_directory: Path = tmp_path / "json-compressed-exports"
    correct_fixture_file_names: tuple[str, ...] = tuple(
        FIXTURE_FILE_NAMES[index] for index in FIXTURE_REGEX_MATCHES[glob_plaintext]
    )
    fixture_files_count: int = len(correct_fixture_file_names)
    plaintext_fixture = PlainTextFixture(
        path=bl_lwm,
        plaintext_glob_regex=glob_plaintext,
        export_directory=export_directory,
        initial_pk=initial_pk,
        data_provider_code=data_provider_code,
        max_plaintext_per_fixture_file=max_plaintext_per_fixture_file,
        json_export_compression_format=destination_compression,
        json_export_compression_subdir=json_compressed_directory,
    )
    assert len(plaintext_fixture) == fixture_files_count
    plaintext_fixture.info()
    assert set(correct_fixture_file_names) == set(
        Path(file).name for file in plaintext_fixture.compressed_files
    )
    plaintext_fixture.extract_compressed()
    plaintext_fixture.export_to_json_fixtures()

    total_plaintext_files_count: int = 0
    for json_path in plaintext_fixture.exported_json_paths:
        export_dict: dict = json.loads(Path(json_path).read_text())
        id: int | None = None
        for fixture in export_dict:
            total_plaintext_files_count += 1
            if not id:
                id = fixture["pk"]
            else:
                id += 1
            assert fixture["pk"] == id
            assert fixture["fields"]
            assert len(fixture["fields"]["text"]) > 0
            assert fixture["fields"]["errors"] is None
            assert fixture["fields"]["path"] is not None
            assert fixture["fields"]["path"].endswith(".txt")

    exported_json_paths_count: int = len(tuple(plaintext_fixture.exported_json_paths))
    assert exported_json_paths_count == max(
        1, round(total_plaintext_files_count / max_plaintext_per_fixture_file)
    )

    if compress:
        export_paths: tuple[Path, ...] = plaintext_fixture.compress_json_exports()
        captured_console = capsys.readouterr()
        compressed_json_paths: tuple[Path, ...] = tuple(
            plaintext_fixture.compressed_json_export_paths
        )
        assert (
            len(compressed_json_paths) == len(export_paths) == exported_json_paths_count
        )
        for path in compressed_json_paths:
            assert Path(path).exists()
            assert Path(path).is_file()
            if path.name not in captured_console.out:
                print("{path.name} not in captured_console")

    else:
        assert len(tuple(plaintext_fixture.compressed_json_export_paths)) == 0
