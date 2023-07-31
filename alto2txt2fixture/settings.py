import os
from typing import Final

from rich.console import Console
from rich.table import Table

from .types import FixtureDict, dotdict

# To understand the settings object, see documentation.


NEWSPAPER_COLLECTION_METADATA: Final[list[FixtureDict]] = [
    FixtureDict(
        pk=1,
        model="newspapers.dataprovider",
        fields={
            "name": "FindMyPast",
            "code": "fmp",
            "legacy_code": "bna",
            "collection": "newspapers",
            "source_note": "FindMyPast-funded digitised newspapers provided by the British Newspaper Archive",
        },
    ),
    FixtureDict(
        pk=2,
        model="newspapers.dataprovider",
        fields={
            "name": "Heritage Made Digital",
            "code": "bl-hmd",
            "legacy_code": "hmd",
            "collection": "newspapers",
            "source_note": "British Library-funded digitised newspapers provided by the British Newspaper Archive",
        },
    ),
    FixtureDict(
        pk=3,
        model="newspapers.dataprovider",
        fields={
            "name": "Joint Information Systems Committee",
            "code": "jisc",
            "legacy_code": "jisc",
            "collection": "newspapers",
            "source_note": "JISC-funded digitised newspapers provided by the British Newspaper Archive",
        },
    ),
    FixtureDict(
        pk=3,
        model="newspapers.dataprovider",
        fields={
            "name": "Living with Machines",
            "code": "bl_lwm",
            "legacy_code": "lwm",
            "collection": "newspapers",
            "source_note": "Living with Machines-funded digitised newspapers provided by the British Newspaper Archive",
        },
    ),
]

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
        "NEWSPAPER_COLLECTION_METADATA": NEWSPAPER_COLLECTION_METADATA,
    }
)

# Correct settings to adhere to standards
settings.SKIP_FILE_SIZE *= 1e9


def show_setup(clear: bool = True, **kwargs) -> None:
    """Generate a `rich.table.Table` for printing configuration to console."""
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
