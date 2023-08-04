import pytest
from _pytest.capture import CaptureResult

from alto2txt2fixture.__main__ import run


def test_newspaper_test_config(capsys) -> None:
    """Test using `test_config` to only print out run config."""
    collections_config = "│           COLLECTIONS │ ['hmd', 'lwm', 'jisc', 'bna'] │"
    run(["--test-config"])
    captured: CaptureResult = capsys.readouterr()
    assert collections_config in captured.out


def test_run_without_local_or_blobfuse(capsys, uncached_folder) -> None:
    """Test error mesages from `alto2txt2fixture.run` cli."""
    error_message: str = (
        "The mountpoint provided for alto2txt does not exist. "
        "Either create a local copy or blobfuse it"
    )
    with pytest.raises(SystemExit) as e_info:
        run()
    # Check the error is raised where expected
    assert e_info.traceback[1].path.name == "__main__.py"
    captured: CaptureResult = capsys.readouterr()
    assert error_message in captured.out
