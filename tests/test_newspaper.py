import pytest

from alto2txt2fixture.__main__ import run


def test_newspaper_test_config(capsys) -> None:
    """Test using `test_config` to only print out run config."""
    collections_config = "│           COLLECTIONS │ ['hmd', 'lwm', 'jisc', 'bna'] │"
    run(test_config=True)
    assert collections_config in capsys.readouterr().out


def test_run_without_local_or_blobfuse(capsys, uncached_folder) -> None:
    """Test error mesages from `alto2txt2fixture.run` cli.

    Todo:
        This currently fails if --pdb option is used because
        that alters the behaviour of `capsys`
    """
    error_message: str = (
        "The mountpoint provided for alto2txt does not exist. "
        "Either create a local copy or blobfuse it to"
    )
    with pytest.raises(SystemExit) as e_info:
        run()
    # Check the error is raised where expected
    assert e_info.traceback[1].path.name == "__main__.py"
    assert error_message in capsys.readouterr().out
