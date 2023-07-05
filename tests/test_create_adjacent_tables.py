import pytest

from alto2txt2fixture import create_adjacent_tables


def test_run() -> None:
    create_adjacent_tables.run()


def test_download_mitchells() -> None:
    create_adjacent_tables.download_data()
