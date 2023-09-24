import json
from os.path import sep
from sys import platform

import pytest
from typer.testing import CliRunner

from alto2txt2fixture.cli import cli
from alto2txt2fixture.types import FixtureDict

runner = CliRunner()


@pytest.mark.slow
def test_plaintext_cli(bl_lwm, first_lwm_plaintext_json_dict):
    """Test running `plaintext` file export via `cli`."""
    result = runner.invoke(
        cli,
        [
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
            str(bl_lwm / ".."),
        ],
        input="y\n..\nn\n",
    )
    assert result.exit_code == 0
    assert "'..'" in result.stdout
    assert f"'..{sep}extracted'" in result.stdout or ""
