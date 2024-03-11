import json
from os import PathLike, chdir
from os.path import sep
from pathlib import Path
from sys import platform, stdout
from typing import Callable

import pytest
from typer.testing import CliRunner

from alto2txt2fixture.cli import COMPRESSED_PATH_DEFAULT, cli, metadata, rename
from alto2txt2fixture.plaintext import FULLTEXT_DJANGO_MODEL, PlainTextFixtureDict
from alto2txt2fixture.types import FixtureDict
from alto2txt2fixture.utils import rename_by_0_padding

runner = CliRunner()


@pytest.mark.slow
@pytest.mark.parametrize("run", [True, False])
def test_plaintext_cli(
    tmp_path: Path,
    bl_lwm: Path,
    lwm_plaintext_json_dict_factory: Callable[
        [int, PathLike, PathLike, str], PlainTextFixtureDict
    ],
    run: bool,
) -> None:
    """Test running `plaintext` file export via `cli`."""
    chdir(bl_lwm.parent)
    extract_folder: PathLike = tmp_path / "test-cli-extract"
    save_folder: PathLike = tmp_path / "test-cli-plaintext-fixture"
    result = runner.invoke(
        cli,
        [
            "plaintext",
            bl_lwm.name,
            "--extract-path",
            extract_folder,
            "--save-path",
            save_folder,
            "--data-provider-code",
            "bl-lwm",
            "--initial-pk",
            5,
            "--log-level",
            10,
            "--run" if run else "--dry-run",
        ],
    )
    assert result.exit_code == 0

    if not run:
        assert not Path(save_folder).exists()
    else:
        # The 'Extracting:' log can fail when parallel test running
        # for message in ("Extract path:", "bl_lwm", "Extracting:"):
        for message in ("Extract path:", "bl_lwm"):
            assert message in result.stdout
        exported_json: list[FixtureDict] = json.loads(
            (Path(save_folder) / "plaintext_fixture-000001.json").read_text()
        )
        assert exported_json[0]["model"] == FULLTEXT_DJANGO_MODEL
        assert exported_json[0]["pk"] == 5
        # assert "DRAPER & OUTFITTER" in exported_json[0]["fields"]["text"]
        first_lwm_plaintext_json_dict = lwm_plaintext_json_dict_factory()
        if not platform.startswith("win"):
            assert (
                exported_json[0]["fields"]["text_path"]
                == first_lwm_plaintext_json_dict["fields"]["text_path"]
            )
        assert exported_json[0]["fields"]["text_path"] == str(
            first_lwm_plaintext_json_dict["fields"]["text_path"]
        )
        assert exported_json[0]["fields"]["text_compressed_path"] == str(
            first_lwm_plaintext_json_dict["fields"]["text_compressed_path"]
        )
        assert (
            exported_json[0]["fields"]["updated_at"]
            == exported_json[0]["fields"]["updated_at"]
        )


def test_plaintext_cli_empty_path(bl_lwm) -> None:
    """Test running `plaintext` file export via `cli`."""
    result = runner.invoke(
        cli,
        [
            "plaintext",
            str(bl_lwm / ".."),
        ],
        input="y\n..\nn\n",
    )
    assert result.exit_code == 0
    assert "'..'" in result.stdout
    assert f"'..{sep}extracted'" in result.stdout or ""


@pytest.mark.parametrize(
    "run_type, input",
    (
        ("--dry-run", "n\nn\nn\nn\n"),
        ("--dry-run", "n\ny\nn\n"),
        ("--run", "n\n"),
    ),
)
def test_rename_cli(
    tmp_json_fixtures: tuple[Path, ...],
    tmp_path: Path,
    run_type: str,
    input: str,
) -> None:
    """Test running `rename` via `cli`."""
    output_path: Path = tmp_path / "padded-file-names"
    result = runner.invoke(
        cli,
        [
            "rename",
            str(tmp_path),
            "--folder",
            str(output_path),
            "--regex",
            "*.txt",
            "--renumber",
            run_type,
        ],
        input=input,
    )
    assert result.exit_code == 0
    assert "Current" in result.stdout
    assert "New" in result.stdout
    for path in tmp_json_fixtures:
        assert str(path.name) in result.stdout
    if run_type == "--dry-run" and "n" not in input:
        assert not output_path.is_dir()
    if run_type == "--run" or "y" in input:
        assert output_path.is_dir()
        for i, path in enumerate(tmp_json_fixtures):
            original_file_name: Path = Path(Path(path).name)
            assert (
                output_path / rename_by_0_padding(original_file_name, match_int=i)
            ).is_file()


def test_rename_compress(
    tmp_json_fixtures: tuple[Path, ...],
    tmp_path: Path,
    capsys,
) -> None:
    """Test running `rename` with `zip` compression and `force=True`."""
    for path in tmp_json_fixtures:
        assert path.is_file()
    rename(tmp_path, compress=True, force=True)
    stdout: str = capsys.readouterr().out
    info_txts: tuple[str, ...] = (
        "compress_format",
        "zip",
        "compress_suffix",
        "''",
        "compress_folder",
        "compressed",
    )
    for text in info_txts:
        assert text in stdout
    for path in tmp_json_fixtures:
        assert path.name in stdout
        assert path.name + ".zip" in stdout
    for path in tmp_json_fixtures:
        zip_path: Path = path.parent / COMPRESSED_PATH_DEFAULT / (path.name + ".zip")
        assert zip_path.is_file()
        assert zip_path.name in stdout


@pytest.mark.parametrize(
    "export_fixture_tables, test_cli, run",
    (
        (True, False, False),
        (False, False, False),
        # pytest.param(True, True, False, marks=pytest.mark.xfail(reason='known issue with cli')),
        # pytest.param(False, True, False, marks=pytest.mark.xfail(reason='known issue with cli')),
    ),
)
def test_metadata_func(
    bl_hmd_meta,
    capsys,
    export_fixture_tables: bool,
    test_cli: bool,
    run: bool,
    tmp_path: Path,
) -> None:
    """Test running `metadata` `json` export as a function."""
    test_data_provider_file_name_prefix: str = "test-dataprovider-000001"
    chdir(bl_hmd_meta.parent)
    test_out: Path = (
        Path("test-func-hmd-meta-out")
        if not test_cli
        else Path("test-cli-hmd-meta-out")
    )
    if test_cli:
        result = runner.invoke(
            cli,
            [
                "metadata",
                "--mountpoint",
                bl_hmd_meta.parent,
                "--output",
                test_out,
                "--export-fixture-tables",
                export_fixture_tables,
                "--no-use-legacy-codes",
                "--run" if run else "--dry-run",
            ],
            # input="y\n..\nn\n",
        )
        assert result.exit_code == 0
    else:
        metadata(
            collections=["bl-hmd"],
            mountpoint=bl_hmd_meta.parent,
            output=test_out,
            export_fixture_tables=export_fixture_tables,
        )
    stdout: str = capsys.readouterr().out
    assert str(test_out) in stdout
    assert str("hmd") in stdout  # Currently still uses legacy_codes
    for format in ".json", ".csv":
        file_path = Path(test_out / (test_data_provider_file_name_prefix + format))
        assert file_path.exists() == export_fixture_tables
    chdir(tmp_path)


# @pytest.mark.download
# @pytest.mark.slow
# @pytest.mark.xfail("not able to test without mock data")
# def test_adjacent_cli(adj_test_path) -> None:
#     """Test using/invoking cli `adj_metadata`."""
