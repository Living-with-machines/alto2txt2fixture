import pytest
from _pytest.capture import CaptureResult

from alto2txt2fixture.__main__ import run


@pytest.mark.parametrize(
    "test_config_param, msg_included",
    [("--show-fixture-tables", True), ("--no-show-fixture-tables", False)],
)
def test_newspaper_show_tables(
    test_config_param: str,
    msg_included: bool,
    capsys: pytest.LogCaptureFixture,
) -> None:
    """Test using `test_config` to only print out run config."""
    collections_config_snippet: str = "COLLECTIONS │ ['hmd', 'lwm', 'jisc', 'bna'] │"
    fixture_config_snipit: str = "bl_hmd │ hmd"
    with pytest.raises(SystemExit) as e_info:
        run([test_config_param])
    assert e_info.traceback[1].path.name == "__main__.py"
    captured: CaptureResult = capsys.readouterr()
    assert collections_config_snippet in captured.out
    assert (fixture_config_snipit in captured.out) == msg_included


@pytest.mark.parametrize("help_param", ("-h", "--help"))
def test_newspaper_help(help_param: str, capsys: pytest.LogCaptureFixture) -> None:
    """Test using `test_config` to only print out run config."""
    help_snippet = "[-h] [-c COLLECTIONS [COLLECTIONS ...]] [-m MOUNTPOINT]"
    with pytest.raises(SystemExit) as e_info:
        run([help_param])
    assert e_info.traceback[1].path.name == "__main__.py"
    captured: CaptureResult = capsys.readouterr()
    assert help_snippet in captured.out


@pytest.mark.parametrize(
    "local_args",
    (
        None,
        [],
    ),
)
def test_run_without_local_or_blobfuse(
    local_args: list | None,
    uncached_folder: None,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.LogCaptureFixture,
) -> None:
    """Test error mesages from `alto2txt2fixture.run` cli.

    Note:
        The `monkeypatch.setattr("sys.argv", [])` line addresses
        issues running tests with ``--pdb`` following:
        https://www.valentinog.com/blog/pytest/#mocking-command-line-arguments-with-monkeypatch
    """
    monkeypatch.setattr("sys.argv", [])
    error_message: str = (
        "The mountpoint provided for alto2txt does not exist. "
        "Either create a local copy or blobfuse it"
    )
    with pytest.raises(SystemExit) as e_info:
        run(local_args)
    # Check the error is raised where expected
    assert e_info.traceback[1].path.name == "__main__.py"
    captured: CaptureResult = capsys.readouterr()
    assert error_message in captured.out
