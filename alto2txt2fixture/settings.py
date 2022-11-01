from .types import dotdict

from rich.console import Console
from rich.table import Table

import os

# To understand the settings object, see documentation.

settings = dotdict(
    **{
        "MOUNTPOINT": "./input/alto2txt/",
        "OUTPUT": "./output/fixtures/",
        "COLLECTIONS": ["hmd", "lwm", "jisc", "bna"],
        "SKIP_FILE_SIZE": 1.5,
        "CHUNK_THRESHOLD": 1,
        "START_WITH_LARGEST": False,
        "WRITE_LOCKFILES": False,
        "CACHE_HOME": "./cache/",
        "JISC_PAPERS_CSV": "./input/JISC papers.csv",
        "MAX_ELEMENTS_PER_FILE": int(2e6),
        "REPORT_DIR": "./output/reports/",
    }
)

# Correct settings to adhere to standards
settings.SKIP_FILE_SIZE *= 1e9


def show_setup(clear: bool = True, **kwargs) -> None:
    if clear and os.name == "posix":
        os.system("clear")
    elif clear:
        os.system("cls")

    table = Table(title="alto2txt2fixture setup")

    table.add_column("Setting", justify="right", style="cyan", no_wrap=True)
    table.add_column("Value", style="magenta")
    for key, value in kwargs.items():
        table.add_row(str(key), str(value))

    console = Console()
    console.print(table)

    return
