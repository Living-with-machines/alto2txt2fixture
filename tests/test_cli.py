import json
from os.path import sep
from pathlib import Path
from sys import platform

import pytest
from typer.testing import CliRunner

from alto2txt2fixture.cli import cli
from alto2txt2fixture.types import FixtureDict
from alto2txt2fixture.utils import rename_by_0_padding

runner = CliRunner()


@pytest.mark.slow
def test_plaintext_cli(bl_lwm, first_lwm_plaintext_json_dict):
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


def test_plaintext_cli_empty_path(bl_lwm):
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
    (("--dry-run", "n\n"), ("--dry-run", "y\n"), ("--no-dry-run", "")),
)
def test_reindex_cli(tmp_path: Path, run_type: str, input: str):
    """Test running `reindex` via `cli`."""
    output_path: Path = tmp_path / "padded-file-names"
    test_paths: tuple[Path, ...] = tuple(
        tmp_path / f"test_fixture-{i}.txt" for i in range(5)
    )
    for path in test_paths:
        path.touch()
    result = runner.invoke(
        cli,
        [
            "reindex",
            str(tmp_path),
            "--folder",
            str(output_path),
            "--regex",
            "*.txt",
            run_type,
        ],
        input=input,
    )
    assert result.exit_code == 0
    assert "Current" in result.stdout
    assert "New" in result.stdout
    for path in test_paths:
        assert str(path.name) in result.stdout
    if run_type == "--dry-run" and "n" in input:
        assert not output_path.is_dir()
    if run_type == "--no-dry-run" or "y" in input:
        assert output_path.is_dir()
        for i, path in enumerate(test_paths):
            original_file_name: Path = Path(Path(path).name)
            assert (
                output_path / rename_by_0_padding(original_file_name, match_int=i)
            ).is_file()
