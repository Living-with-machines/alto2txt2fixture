from alto2txt2fixture import create_adjacent_tables

import pytest


def test_run() -> None:
    create_adjacent_tables.run()


@pytest.mark.xfail(reason="str/Path error")
def test_download_mitchells() -> None:
    create_adjacent_tables.download_data()
