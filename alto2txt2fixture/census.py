import json
from collections import UserDict
from dataclasses import dataclass, field
from enum import StrEnum, auto
from os import PathLike
from pathlib import Path
from re import findall
from typing import Any, Callable, Final, Generator, Iterable, Iterator
from zipfile import ZipFile

from pandas import DataFrame, Series, concat, read_csv
from tqdm import tqdm

from .create_adjacent_tables import OUTPUT
from .types import PRIMARY_KEY, FixtureDict
from .utils import (
    CSV_FILE_EXTENSION,
    INTERMEDIATE_PATH_TRUNCATION_STR,
    JSON_FILE_EXTENSION,
    DataSource,
    get_now,
    write_json,
)

GAZETEER_LABELS_COLUMN_NAME: Final[str] = "label"
GAZETEER_FIXTURE_PREFIX: Final[str] = "gazetteer."
# CSV_FIXTURE_PATH: Path = Path("./fixture-files/UKDA-8613-csv/")
# JSON_FIXTURE_WRITE_PATH: Path = Path("./census/fixtures/Record.json")
UKDA_8613_CSV: Path = Path("UKDA-8613-csv.zip")

URL: Final[str] = (
    "https://beta.ukdataservice.ac.uk/"
    "Umbraco/Surface/Discover/GetDownload?"
    "studyNumber=8613&"
    "fileName=8613csv_EEB7368E84B46BA0FAF00FD44204E6EF_V1.zip"
)

CENSUS_OUTPUT: Final[Path] = Path(OUTPUT / "census")

CENSUS_REGION_TYPES: Final[tuple[str, str, str]] = ("REGCNTY", "REGDIST", "SUBDIST")

RELATED_TABLE_NAMES: Final[tuple[str, ...]] = ("HistoricCounty", "AdminCounty", "Place")


class CensusTableFields(StrEnum):
    HISTORIC_COUNTIES = auto()
    ADMIN_COUNTIES = auto()  #: Final[str] = 'admin_counties'
    PLACES = auto()


@dataclass
class CensusTableFieldManager(UserDict):

    """Manage converting between related census data fields.

    Examples:
        ```pycon
        >>> census_tbl_manager = CensusTableFieldManager()
        >>> census_tbl_manager.json_fixture_files
        {'historic_counties': 'HistoricCounty.json',
         'admin_counties': 'AdminCounty.json',
         'places': 'Place.json'}
        >>> census_tbl_manager.csv_fixture_files
        {'historic_counties': 'HistoricCounty.csv',
         'admin_counties': 'AdminCounty.csv',
         'places': 'Place.csv'}

        ```
    """

    table_fields = CensusTableFields
    table_names: tuple[str, ...] = RELATED_TABLE_NAMES
    fixture_prefix: str = GAZETEER_FIXTURE_PREFIX

    def __post_init__(self) -> None:
        """Ensure attributes are correctly related."""
        self.data: dict[str, str] = dict(zip(self.table_fields, self.table_names))

    def fixture_files_gen(
        self,
        prefix: str | None = None,
        extension: str | None = None,
        extension_separator: str = INTERMEDIATE_PATH_TRUNCATION_STR,
    ) -> Iterator[tuple[str, PathLike]]:
        """`Tuple` of `json` fixture files."""
        prefix = "" if prefix is None else self.fixture_prefix
        extension = (
            "" if extension_separator is None else extension_separator + extension
        )
        for field, file_name in self.items():
            yield str(field), prefix + file_name + extension

    @property
    def json_fixture_files(self) -> dict[str, PathLike]:
        """`Tuple` of `json` fixture files."""
        return dict(self.fixture_files_gen(extension=JSON_FILE_EXTENSION))

    @property
    def csv_fixture_files(self) -> dict[str, PathLike]:
        """Related `csv` fixture file names."""
        return dict(self.fixture_files_gen(extension=CSV_FILE_EXTENSION))


UKCensus1851_1911: DataSource = DataSource(
    file_name=UKDA_8613_CSV,
    app="census",
    url=URL,
    read_func=read_csv,
    description="Demographic and socio-economic variables for "
    "Registration Sub-Districts (RSDs) in England and Wales, "
    "1851-1911",
    citation="https://doi.org/10.5255/UKDA-SN-8613-1",
    license="http://creativecommons.org/licenses/by/4.0/",
)

# class Fixture(BaseCommand):
# def write_models(self, models):
#     # lst = []
#     df = None
#
#     for x in models:
#         try:
#             df, model = x
#         except TypeError:
#             try:
#                 model = x
#             except Exception as e:
#                 exit(f"An exception occurred: {e}")
#
#         filename = f"{model._meta.label.split('.')[-1]}-fixtures.json"
#         operational_errors_occurred = 0
#
#         if isinstance(df, pd.DataFrame):
#             df_json = df.to_json(orient="index")
#
#             for pk, fields in json.loads(df_json).items():
#                 try:
#                     model.objects.update_or_create(id=pk, defaults=fields)
#                 except OperationalError:
#                     operational_errors_occurred += 1
#
#         if operational_errors_occurred:
#             self.stdout.write(
#                 self.style.WARNING(
#                     f"{operational_errors_occurred} operational error(s) occurred: rows not written to database."
#                 )
#             )
#
#         path = self.get_output_dir() / filename
#
#         model_fields = model._meta.get_fields()
#         data = serialize(
#             "json",
#             model.objects.all(),
#             fields=[
#                 x.name
#                 for x in model_fields
#                 if not x.name in ["created_at", "updated_at"]
#             ],
#         )
#
#         with open(path, "w+") as f:
#             f.write(data)
#
#     return True

# def load_fixtures(self, models=None):
#     if not models:
#         models = self.models
#
#     for model in models:
#         success = 0
#         path = (
#             self.get_output_dir()
#             / f"{model._meta.label.split('.')[-1]}-fixtures.json"
#         )
#
#         if not Path(path).exists():
#             self.stdout.write(
#                 self.style.WARNING(
#                     f"Warning: Model {model._meta.label} is missing a fixture file and will not load."
#                 )
#             )
#             continue
#
#         data = path.read_text()
#         for obj in deserialize("json", data):
#             value = timezone.now()
#             setattr(obj.object, "created_at", value)
#             setattr(obj.object, "updated_at", value)
#             obj.save()
#             success += 1
#
#         self.stdout.write(
#             self.style.SUCCESS(
#                 f"Wrote {success} objects of model {obj.object._meta.model._meta.label} to db"
#             )
#         )

# def get_output_dir(self, app_name=None):
#     if app_name:
#         return settings.BASE_DIR / Path(f"{app_name}/fixtures")
#     return settings.BASE_DIR / Path(f"{self.app_name}/fixtures")
#
# def get_input(self, msg, suggestion):
#     self.stdout.write(
#         self.style.NOTICE(f"{msg}\n[{settings.BASE_DIR / suggestion}]")
#     )
#     _ = input()
#
#     if _:
#         return _
#
#     return suggestion
#
# def try_file(self, path, return_contents=True, func=False, relative_to_django=True):
#     if relative_to_django:
#         path = settings.BASE_DIR / path
#
#     if not Path(path).exists():
#         self.stdout.write(self.style.ERROR(f"File does not exist: {path}"))
#         exit()
#
#     if return_contents:
#         if func:
#             return func(Path(path).read_text())
#
#         return Path(path).read_text()
#
#     return Path(path)
#
# def done(self):
#     self.stdout.write(self.style.SUCCESS(f"{self.app_name}: Done"))


# TO BE USED
# class Command(Fixture):
#     """Build census."""
#
#     csv_fixture_path: Path = CSV_FIXTURE_PATH
#     json_fixture_write_path: Path = JSON_FIXTURE_WRITE_PATH
#
#     def __init__(self, force=False):
#         self.force = force
#         super(Fixture, self).__init__()
#


def get_zipped_paths(
    zip_path: Path, extension: str | None = None
) -> Generator[Path, None, None]:
    """Return a list of paths from within `zip_path`.

    Args:
        zip_path: `Path` to zipped file.
        extension: String to filter zipped files on (like `csv`).

    Yields:
        `Path` of each file within `zip_path`, filtered by `extension`.

    Example:
        ```pycon
        >>> UKCensus1851_1911.download()
        <BLANKLINE>
        ...UKDA-8613-csv...
        >>> pprint(tuple(
        ...    get_zipped_paths(UKCensus1851_1911.local_path, 'csv')))
        (PosixPath('UKDA-8613-csv/csv/1851_rsd_data.csv'),
         PosixPath('UKDA-8613-csv/csv/1861_rsd_data.csv'),
         PosixPath('UKDA-8613-csv/csv/1871_rsd_data.csv'),
         PosixPath('UKDA-8613-csv/csv/1881_rsd_data.csv'),
         PosixPath('UKDA-8613-csv/csv/1891_rsd_data.csv'),
         PosixPath('UKDA-8613-csv/csv/1901_rsd_data.csv'),
         PosixPath('UKDA-8613-csv/csv/1911_rsd_data.csv'))

        ```
    """
    zip_file: ZipFile = ZipFile(zip_path)
    for path in zip_file.infolist():
        if extension:
            if str(path.filename.lower()).endswith(extension.lower()):
                yield Path(path.filename)
        else:
            yield Path(path.filename)


def concat_zipped_census_files(
    zipped_census_path: Path, extension: str = "csv"
) -> DataFrame:
    """Contact zipped census files to a `DataFrame`.

    Args:
        zipped_census_path: `Path` to zipped collection of
            England and Wales Census data, 1951-1911.
        extension: What extension to filter on.

    Returns:
        A `DataFrame` of concatenated census records.

    Example:
        ```pycon
        >>> UKCensus1851_1911.download()
        <BLANKLINE>
        ...UKDA-8613-csv...
        >>> df: DataFrame = concat_zipped_census_files(
        ...     UKCensus1851_1911.local_path)
        >>> len(df)
        14937
        >>> df.head()
          CENSUS_YEAR    CEN REGCNTY  ... M_CL_1013  F_CL_1418  M_CL_1418
        0        1851  10001  LONDON  ...  6.697248  27.837116  26.234980
        1        1851  10002  LONDON  ...  7.962213  26.649529  27.549272
        2        1851  10003  LONDON  ...  5.174892  27.249820  25.809935
        3        1851  10004  LONDON  ...  8.004510  33.761599  25.481799
        4        1851  10005  LONDON  ...  6.818182  31.151242  25.507901
        <BLANKLINE>
        [5 rows x 70 columns]

        ```
    """
    df = DataFrame()
    # now: str = get_now(as_str=True)
    for file in (
        bar1 := tqdm(get_zipped_paths(zipped_census_path, extension), leave=False)
    ):
        bar1.set_description(file.name)

        year, *_ = findall(r"\d{4}", str(file.name))
        file_df = read_csv(ZipFile(zipped_census_path).open(str(file)))
        file_df = file_df.rename({f"CEN_{year}": "CEN"}, axis=1)
        file_df["CENSUS_YEAR"] = year
        file_df = file_df[
            ["CENSUS_YEAR"] + [x for x in file_df.columns if not x == "CENSUS_YEAR"]
        ]

        df = concat([df, file_df])
    # May be unnecessary, copied from original
    df = df.reset_index(drop=True)
    # df["pk"] = df.index + 1
    # df["created_at"] = now
    # df["updated_at"] = now
    return df


@dataclass
class AdjacentFixturesManager(UserDict):

    """Class to manage linking Historic, Admin and Place records.

    Attributes:
        historic_counties: `DataFrame` (or path to file to load) of
            historic county records.
        admin_counties: `DataFrame` of (or path to file to load) of
            admin county records.
        places: `DataFrame` (or path to file to load) of `Places`
            records.
        reader: A function to call to read any of the `TABLE_FIELDS`.
            attributes when `PathLike` values are passed
        reader_kwargs: An `dict` of params to pass to the
            `reader_function` as `**kwargs`.
        table_config: A instance to manage field and fixture names.
        TABLE_QUERY_COLUMN_NAME: Column name to for indexing each
            `TABLE_FIELDS` `DataFrame`.
        PRIMARY_KEY_COLUMN_NAME: Column name for `lwmdb` table primary
            keys.

    Example:
        ```pycon
        >>> if not OUTPUT.exists():
        ...     pytest.skip(f"csv adjacent results required in '{OUTPUT}'")
        >>> related_fixtures = AdjacentFixturesManager(
        ...     historic_counties=OUTPUT / 'gazetteer.HistoricCounty.csv',
        ...     admin_counties=OUTPUT / 'gazetteer.AdminCounty.csv',
        ...     places=OUTPUT / 'gazetteer.Place.csv',
        ... )
        >>> related_fixtures
        AdjacentFixturesManager(data_fields=(<CensusTableFields.HISTORIC_COUNTIES: 'historic_counties'>, <CensusTableFields.ADMIN_COUNTIES: 'admin_counties'>, <CensusTableFields.PLACES: 'places'>))
        >>> dict(related_fixtures.related_pks('Yorkshire'))
        {'historic_counties': 5, 'admin_counties': None, 'places': None}
        >>> assert False

        ```
    """

    historic_counties: DataFrame | PathLike
    admin_counties: DataFrame | PathLike
    places: DataFrame | PathLike
    reader: Callable = read_csv
    reader_kwargs: dict[str, Any] = field(default_factory=dict)
    table_fields: CensusTableFieldManager = field(
        default_factory=CensusTableFieldManager
    )
    TABLE_QUERY_COLUMN_NAME: Final[str] = GAZETEER_LABELS_COLUMN_NAME
    # TABLE_FIELDS: Final[dict[str, str]] = field(
    #     default_factory=lambda: CENSUS_REGION_FIELDS_KEYS
    # )
    PRIMARY_KEY_COLUMN_NAME: Final[str] = PRIMARY_KEY

    def __repr__(self) -> str:
        """Return summary of `self`."""
        return f"AdjacentFixturesManager(data_fields={self.data_fields})"

    @property
    def data_fields(self) -> tuple[str, ...]:
        """Return a `tuple` of data attributes."""
        return tuple(self.table_fields.keys())

    def __post_init__(self) -> None:
        """Manage loading related data."""
        for related_field_name in self.data_fields:
            field_val: DataFrame | PathLike = getattr(self, related_field_name)
            if isinstance(field_val, PathLike):
                df: DataFrame = self.reader(field_val, **self.reader_kwargs)
                setattr(self, related_field_name, df)
        self.data = {
            field_name: getattr(self, field_name) for field_name in self.table_fields
        }

    def related_pks(
        self, region_name: str
    ) -> Generator[tuple[str, int | None], None, None]:
        """Yield `tuple` of related table and primary keys.

        Args:
            region_name: Name of region to match.

        Yields:
            `tuple`: related `field`, primary key or `None`.

        Examples:
            ```pycon
            >>> dict(census_adjacent_fixtures.related_pks('Yorkshire'))
            {'historic_counties': 5, 'admin_counties': None, 'places': None}
            >>> dict(census_adjacent_fixtures.related_pks('YORKSHIRE'))
            {'historic_counties': None, 'admin_counties': None, 'places': None}

            ```
        """
        for table_type, related_table in self.items():
            if region_name in related_table[self.TABLE_QUERY_COLUMN_NAME].values:
                pks: Series = related_table.query(
                    f"{self.TABLE_QUERY_COLUMN_NAME} == @region_name"
                )[self.PRIMARY_KEY_COLUMN_NAME]
                if pks is None or len(pks) == 0:
                    yield str(table_type), None
                elif len(pks) == 1:
                    yield str(table_type), pks.values[0]
                else:
                    raise self.DupeRelatedError(
                        f"There are {len(pks)} 'pks' = {region_name}; should be unique."
                    )
            else:
                yield str(table_type), None

    class DupeRelatedError(Exception):
        pass


def query_related_records(
    region_name: str, related_tables_manager: AdjacentFixturesManager
):
    # do some manual data wrangling - on hold until Mariona is back
    if "yorkshire" in region_name.lower():
        region_name = "YORKSHIRE"

    if "bury" in region_name and "edmund" in region_name.lower():
        region_name = "Bury St Edmunds"

    ##### first try
    related_records: dict[str, int | None] = {
        related_table: pk
        for related_table, pk in related_tables_manager.related_pks(region_name)
    }
    # history_county = related_tables_manager.related_counties
    # try:
    #     historic_county = HistoricCounty.objects.get(label__iexact=region_name).pk
    # except:
    #     historic_county = None
    #
    # try:
    #     admin_county = AdminCounty.objects.get(label__iexact=region_name).pk
    # except:
    #     admin_county = None
    #
    # try:
    #     place = Place.objects.get(label__iexact=region_name).pk
    # except:
    #     place = None

    if historic_county != None or admin_county != None or place != None:
        return (region_name, historic_county, admin_county, place)

    ##### second try: without EAST/WEST/NORTH/SOUTH/WESTERN/SOUTHEAST/FIRST/CENTRAL
    if historic_county == None and place == None:
        for word in [
            "east",
            "north",
            "west",
            "south",
            "western",
            "southeast",
            "first",
            "central",
            "south east",
        ]:
            if f"{word} " in region_name.lower() or f" {word}" in region_name.lower():
                region_name = (
                    region_name.replace(f"{word} ", " ")
                    .replace(f" {word}", " ")
                    .strip()
                )
                region_name = (
                    region_name.replace(f"{word.upper()} ", " ")
                    .replace(f" {word.upper()}", " ")
                    .strip()
                )

    try:
        historic_county = HistoricCounty.objects.get(label__iexact=region_name).pk
    except:
        historic_county = None

    try:
        admin_county = AdminCounty.objects.get(label__iexact=region_name).pk
    except:
        admin_county = None

    try:
        place = Place.objects.get(label__iexact=region_name).pk
    except:
        place = None

    return (region_name, historic_county, admin_county, place)


def build_fixture_df(
    census_df: DataFrame,
    # output_path: PathLike = CENSUS_OUTPUT,
    related_tables_manager: AdjacentFixturesManager,
    region_types: tuple[str, ...] = CENSUS_REGION_TYPES,
) -> DataFrame:
    # CSV_FILES = [x for x in self.csv_fixture_path.glob("*.csv")]
    #
    # df = pd.DataFrame()
    # # now = timezone.now()
    # now: str = get_now(as_str=True)
    #
    # for file in (bar1 := tqdm(CSV_FILES, leave=False)):
    #     bar1.set_description(file.name)
    #
    #     year, *_ = findall(r"\d{4}", str(file.name))
    #     _df = pd.read_csv(file)
    #     _df = _df.rename({f"CEN_{year}": "CEN"}, axis=1)
    #     _df["CENSUS_YEAR"] = year
    #     _df = _df[
    #         ["CENSUS_YEAR"] + [x for x in _df.columns if not x == "CENSUS_YEAR"]
    #     ]
    #
    #     df = pd.concat([df, _df])
    #
    # df = df.rename(
    #     {col: col.replace("-", "_") for col in df.columns if "-" in col},
    #     axis=1,
    # )
    # df = df.reset_index(drop=True)
    census_df["pk"] = census_df.index + 1
    census_df["created_at"] = get_now(as_str=True)
    census_df["updated_at"] = get_now(as_str=True)

    # cats = ["REGCNTY", "REGDIST", "SUBDIST"]

    for region_type in (bar1 := tqdm(region_types, leave=False)):
        bar1.set_description(f"Correcting record :: {region_type}")
        # This will create columns for each region_type, ie:
        # REGCNTY_historic_county_id, REGDIST_historic_county_id, ... SUBDIST_place_id
        census_df[f"{region_type}_rel"] = census_df[region_type].apply(
            lambda x: query_related_records(x, related_tables_manager)
        )
        census_df[f"{region_type}_historic_county_id"] = census_df[
            f"{region_type}_rel"
        ].apply(lambda x: x[1])
        census_df[f"{region_type}_admin_county_id"] = census_df[
            f"{region_type}_rel"
        ].apply(lambda x: x[2])
        census_df[f"{region_type}_place_id"] = census_df[f"{region_type}_rel"].apply(
            lambda x: x[3]
        )

    census_df.drop(
        [f"{region_type}_rel" for region_type in region_types], axis=1, inplace=True
    )
    return census_df


def main(
    zipped_census_path: Path,
    historic_counties: PathLike | DataFrame,
    admin_counties: PathLike | DataFrame,
    places: PathLike | DataFrame,
    save_path: PathLike = CENSUS_OUTPUT,
    region_types: Iterable[str] = CENSUS_REGION_TYPES,
    fixture_extension: str = "csv",
    to_json: bool = True,
) -> DataFrame:
    census_df: DataFrame = concat_zipped_census_files(
        zipped_census_path=zipped_census_path, extension=fixture_extension
    )
    # output_path: PathLike = CENSUS_OUTPUT,
    # region_types: tuple[str, ...] = CENSUS_REGION_TYPES
    related_tables_manager: AdjacentFixturesManager = AdjacentFixturesManager(
        historic_counties=historic_counties,
        admin_counties=admin_counties,
        places=places,
    )
    census_fixture_df: DataFrame = build_fixture_df(
        census_df, related_tables_manager, region_types=tuple(region_types)
    )
    if to_json:
        write_json(p=save_path, o=census_fixture_df, add_created=True)

    fixtures_json_list: list[FixtureDict] = []
    for record in json.loads(census_df.to_json(orient="records")):
        pk = record.pop("pk")
        fixtures_json_list.append(
            {"model": "census.Record", "pk": pk, "fields": record}
        )

    return lst

    # self.json_fixture_write_path.write_text(json.dumps(lst))

    # self.stdout.write(
    #     self.style.SUCCESS("Fixture file written. Now run the following command:")
    # )
    # self.stdout.write(
    #     self.style.SUCCESS(
    #         f"python manage.py loaddata {self.json_fixture_write_path}"
    #     )
    # )
