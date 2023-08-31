import json

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
        ],
    )
    assert result.exit_code == 0
    for message in ("Extract path:", "bl_lwm", "extracted"):
        assert message in result.stdout
    exported_json: list[FixtureDict] = json.loads(
        (bl_lwm / "test-cli-plaintext-fixture" / "plaintext_fixture-1.json").read_text()
    )
    assert exported_json[0]["model"] == "fulltext.fulltext"
    # assert "DRAPER & OUTFITTER" in exported_json[0]["fields"]["text"]
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
