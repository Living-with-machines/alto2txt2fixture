from alto2txt2fixture import create_adjacent_tables

import pytest


@pytest.mark.xfail(reason="Known syntax error")
def test_run() -> None:
    create_adjacent_tables.run()


def test_download_mitchells() -> None:
    create_adjacent_tables.download_data()
