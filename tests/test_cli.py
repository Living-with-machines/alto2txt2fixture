import json

from typer.testing import CliRunner

from alto2txt2fixture.cli import cli
from alto2txt2fixture.types import FixtureDict

runner = CliRunner()


def test_plaintext_cli(tmpdir, first_lwm_plaintext_json_dict):
    """Test running `plaintext` file export via `cli`."""
    result = runner.invoke(
        cli,
        [
            "tests/bl_lwm/",
            "--save-path",
            tmpdir / "test-cli-plaintext-fixture",
            "--data-provider-code",
            "bl_lwm",
        ],
    )
    assert result.exit_code == 0
    assert "Extract path: tests/bl_lwm/extracted" in result.stdout
    exported_json: list[FixtureDict] = json.load(
        tmpdir / "test-cli-plaintext-fixture" / "plaintext_fixture-1.json"
    )
    assert exported_json[0]["model"] == "fulltext.fulltext"
    # assert "DRAPER & OUTFITTER" in exported_json[0]["fields"]["text"]
    assert (
        exported_json[0]["fields"]["text"]
        == first_lwm_plaintext_json_dict["fields"]["text"]
    )
    assert (
        exported_json[0]["fields"]["path"]
        == first_lwm_plaintext_json_dict["fields"]["path"]
    )
    assert (
        exported_json[0]["fields"]["compressed_path"]
        == first_lwm_plaintext_json_dict["fields"]["compressed_path"]
    )
    assert (
        exported_json[0]["fields"]["updated_at"]
        == exported_json[0]["fields"]["updated_at"]
    )
