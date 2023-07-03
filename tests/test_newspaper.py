from alto2txt2fixture.run import run

import pytest


def test_run_without_local_or_blobfuse(capsys) -> None:
    """Test error mesages from `alto2txt2fixture.run` cli.

    Todo:
        This currently fails if --pdb option is used because
        that alters the behavious of `capsys`
    """
    error_message: str = (
        "The mountpoint provided for alto2txt does not exist. "
        "Either create a local copy or blobfuse it to"
    )
    with pytest.raises(SystemExit) as e_info:
        run()
    # Check the error is raised where expected
    assert e_info.traceback[1].path.name == 'run.py'
    assert error_message in capsys.readouterr().out
