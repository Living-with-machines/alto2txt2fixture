import pytest
from _pytest.capture import CaptureResult

from alto2txt2fixture import create_adjacent_tables


@pytest.mark.slow
def test_run() -> None:
    create_adjacent_tables.run()


def test_download(uncached_folder) -> None:
    create_adjacent_tables.download_data()


# @pytest.mark.parameterize()
def test_download_custom_folder(custom_admin_counties, capsys) -> None:
    create_adjacent_tables.download_data(custom_admin_counties)
    captured: CaptureResult = capsys.readouterr()
    assert captured.out.startswith(
        f"Downloading {custom_admin_counties['dict_admin_counties']['local']}\n100%"
    )
