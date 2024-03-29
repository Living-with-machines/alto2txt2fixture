import json
from os.path import sep
from pathlib import Path
from sys import platform, stdout

import pytest
from typer.testing import CliRunner

from alto2txt2fixture.cli import COMPRESSED_PATH_DEFAULT, cli, rename
from alto2txt2fixture.types import FixtureDict
from alto2txt2fixture.utils import rename_by_0_padding

runner = CliRunner()


@pytest.mark.slow
def test_plaintext_cli(bl_lwm, first_lwm_plaintext_json_dict) -> None:
    """Test running `plaintext` file export via `cli`."""
    result = runner.invoke(
        cli,
        [
            "plaintext",
            str(bl_lwm),
            "--save-path",
            bl_lwm / "test-cli-plaintext-fixture",
            "--data-provider-code",
            "bl_lwm",
            "--initial-pk",
            5,
        ],
    )
    assert result.exit_code == 0
    for message in ("Extract path:", "bl_lwm", "extracted"):
        assert message in result.stdout
    exported_json: list[FixtureDict] = json.loads(
        (
            bl_lwm / "test-cli-plaintext-fixture" / "plaintext_fixture-000001.json"
        ).read_text()
    )
    assert exported_json[0]["model"] == "fulltext.fulltext"
    assert exported_json[0]["pk"] == 5
    # assert "DRAPER & OUTFITTER" in exported_json[0]["fields"]["text"]
    if not platform.startswith("win"):
        assert (
            exported_json[0]["fields"]["text"]
            == first_lwm_plaintext_json_dict["fields"]["text"]
        )
    assert exported_json[0]["fields"]["path"] == str(
        first_lwm_plaintext_json_dict["fields"]["path"]
    )
    assert exported_json[0]["fields"]["compressed_path"] == str(
        first_lwm_plaintext_json_dict["fields"]["compressed_path"]
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
        ("--no-dry-run", "n\n"),
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
    if run_type == "--no-dry-run" or "y" in input:
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
