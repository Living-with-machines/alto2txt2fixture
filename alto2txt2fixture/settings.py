"""
The `settings` module provides configuration for running `alto2txt2fixture`.

Most of these are managed within the `settings` variable within this module.

!!! note
    See the command line interface [parameters documentation][optional-parameters] for means of modifying `settings` when run.

"""
from typing import Final

from .types import FixtureDict, dotdict

# To understand the settings object, see documentation.


DATA_PROVIDER_INDEX: Final[str] = "legacy_code"

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

SETUP_TITLE: str = "alto2txt2fixture setup"

settings: dotdict = dotdict(
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
        "FIXTURE_TABLES": {
            "dataprovider": NEWSPAPER_COLLECTION_METADATA,
        },
    }
)

# Correct settings to adhere to standards
settings.SKIP_FILE_SIZE *= 1e9
