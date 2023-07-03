from alto2txt2fixture.run import run

import pytest


def test_run_without_local_or_blobfuse(capsys) -> None:
    """Test running newspaper fixtures to `json`."""
    with pytest.raises(SystemExit) as e_info:
        run()
    assert e_info.value.code == 2
