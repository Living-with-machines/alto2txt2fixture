import pytest

from alto2txt2fixture import create_adjacent_tables


@pytest.mark.xfail(reason="str/Path error in download_data call")
def test_run() -> None:
    create_adjacent_tables.run()


@pytest.mark.xfail(reason="str/Path error")
def test_download_mitchells() -> None:
    create_adjacent_tables.download_data()
