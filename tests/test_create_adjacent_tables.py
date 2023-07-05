import pytest

from alto2txt2fixture import create_adjacent_tables


@pytest.mark.slow
def test_run(uncached_folder) -> None:
    create_adjacent_tables.run()


@pytest.mark.slow
def test_download_mitchells(uncached_folder) -> None:
    create_adjacent_tables.download_data()
