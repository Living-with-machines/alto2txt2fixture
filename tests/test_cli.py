import json

from typer.testing import CliRunner

from alto2txt2fixture.cli import cli
from alto2txt2fixture.types import FixtureDict

runner = CliRunner()


def test_plaintext_cli(tmpdir):
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
    assert "NEW TREDEGAR & BARGOED" in exported_json[0]["fields"]["text"]
    assert (
        exported_json[0]["fields"]["path"]
        == "tests/bl_lwm/extracted/0003548/1904/0630/0003548_19040630_art0002.txt"
    )
    assert (
        exported_json[0]["fields"]["compressed_path"]
        == "tests/bl_lwm/0003548-test_plaintext.zip"
    )
    assert (
        exported_json[0]["fields"]["updated_at"]
        == exported_json[0]["fields"]["updated_at"]
    )
