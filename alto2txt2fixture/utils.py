import datetime
import gc
import json
import logging
from collections import OrderedDict
from os import PathLike, chdir, getcwd, sep
from os.path import normpath
from pathlib import Path, PureWindowsPath
from re import findall
from shutil import disk_usage, get_unpack_formats, make_archive
from typing import (
    Any,
    Final,
    Generator,
    Hashable,
    Iterable,
    Literal,
    NamedTuple,
    Sequence,
    Type,
    TypeAlias,
    overload,
)

import pytz
from numpy import array_split
from pandas import DataFrame
from rich.console import Console
from rich.logging import RichHandler
from rich.table import Table

from .log import error, info, warning
from .settings import (
    DATA_PROVIDER_INDEX,
    EXPORT_FORMATS,
    JSON_INDENT,
    NEWSPAPER_COLLECTION_METADATA,
    settings,
)
from .types import FixtureDict

FORMAT: str = "%(message)s"

logging.basicConfig(
    level="NOTSET", format=FORMAT, datefmt="[%X]", handlers=[RichHandler()]
)

logger = logging.getLogger("rich")

console = Console()

VALID_COMPRESSION_FORMATS: Final[tuple[str, ...]] = tuple(
    [
        extension
        for format_tuple in get_unpack_formats()
        for extension in format_tuple[1]
    ]
)
BYTES_PER_GIGABYTE: Final[int] = 1024 * 1024 * 1024

NewspaperElements: Final[TypeAlias] = Literal["newspaper", "issue", "item"]
JSON_FILE_EXTENSION: str = "json"
ZIP_FILE_EXTENSION: Final[str] = "zip"

JSON_FILE_GLOB_STRING: str = f"**/*{JSON_FILE_EXTENSION}"

MAX_TRUNCATE_PATH_STR_LEN: Final[int] = 30
INTERMEDIATE_PATH_TRUNCATION_STR: Final[str] = "."

TRUNC_HEADS_PATH_DEFAULT: int = 1
TRUNC_TAILS_PATH_DEFAULT: int = 1
FILE_NAME_0_PADDING_DEFAULT: int = 6
PADDING_0_REGEX_DEFAULT: str = r"\b\d*\b"


@overload
def get_now(as_str: Literal[True]) -> str:
    ...


@overload
def get_now(as_str: Literal[False]) -> datetime.datetime:
    ...


def get_now(as_str: bool = False) -> datetime.datetime | str:
    """
    Return `datetime.now()` as either a string or `datetime` object.

    Args:
        as_str: Whether to return `now` `time` as a `str` or not, default: `False`

    Returns:
        `datetime.now()` in `pytz.UTC` time zone as a string if `as_str`, else
            as a `datetime.datetime` object.
    """
    now = datetime.datetime.now(tz=pytz.UTC)

    if as_str:
        return str(now)
    else:
        assert isinstance(now, datetime.datetime)
        return now


NOW_str = get_now(as_str=True)


def get_key(x: dict = dict(), on: list = []) -> str:
    """
    Get a string key from a dictionary using values from specified keys.

    Args:
        x: A dictionary from which the key is generated.
        on: A list of keys from the dictionary that should be used to
            generate the key.

    Returns:
        The generated string key.
    """

    return f"{'-'.join([str(x['fields'][y]) for y in on])}"


def create_lookup(lst: list = [], on: list = []) -> dict:
    """
    Create a lookup dictionary from a list of dictionaries.

    Args:
        lst: A list of dictionaries that should be used to generate the lookup.
        on: A list of keys from the dictionaries in the list that should be used as the keys in the lookup.

    Returns:
        The generated lookup dictionary.
    """
    return {get_key(x, on): x["pk"] for x in lst}


def glob_filter(p: str) -> list:
    """
    Return ordered glob, filtered out any pesky, unwanted .DS_Store from macOS.

    Args:
        p: Path to a directory to filter

    Returns:
        Sorted list of files contained in the provided path without the ones
        whose names start with a `.`
    """
    return sorted([x for x in get_path_from(p).glob("*") if not x.name.startswith(".")])


def lock(lockfile: Path) -> None:
    """
    Writes a '.' to a lockfile, after making sure the parent directory exists.

    Args:
        lockfile: The path to the lock file to be created

    Returns:
        None
    """
    lockfile.parent.mkdir(parents=True, exist_ok=True)

    lockfile.write_text("")

    return


def get_lockfile(collection: str, kind: NewspaperElements, dic: dict) -> Path:
    """
    Provides the path to any given lockfile, which controls whether any
    existing files should be overwritten or not.

    Args:
        collection: Collection folder name
        kind: Either `newspaper` or `issue` or `item`
        dic: A dictionary with required information for either `kind` passed

    Returns:
        Path to the resulting lockfile
    """

    p: Path
    base = Path(f"cache-lockfiles/{collection}")

    if kind == "newspaper":
        p = base / f"newspapers/{dic['publication_code']}"
    elif kind == "issue":
        p = base / f"issues/{dic['publication__publication_code']}/{dic['issue_code']}"
    elif kind == "item":
        try:
            if dic.get("issue_code"):
                p = base / f"items/{dic['issue_code']}/{dic['item_code']}"
            elif dic.get("issue__issue_identifier"):
                p = base / f"items/{dic['issue__issue_identifier']}/{dic['item_code']}"
        except KeyError:
            error("An unknown error occurred (in get_lockfile)")
    else:
        p = base / "lockfile"

    p.parent.mkdir(parents=True, exist_ok=True) if settings.WRITE_LOCKFILES else None

    return p


def get_chunked_zipfiles(path: Path) -> list:
    """This function takes in a `Path` object `path` and returns a list of lists
    of `zipfiles` sorted and chunked according to certain conditions defined
    in the `settings` object (see `settings.CHUNK_THRESHOLD`).

    Note: the function will also skip zip files of a certain file size, which
    can be specified in the `settings` object (see `settings.SKIP_FILE_SIZE`).

    Args:
        path: The input path where the zipfiles are located

    Returns:
        A list of lists of `zipfiles`, each inner list represents a chunk of
            zipfiles.
    """

    zipfiles = sorted(
        path.glob("*.zip"),
        key=lambda x: x.stat().st_size,
        reverse=settings.START_WITH_LARGEST,
    )

    zipfiles = [x for x in zipfiles if x.stat().st_size <= settings.SKIP_FILE_SIZE]

    if len(zipfiles) > settings.CHUNK_THRESHOLD:
        chunks = array_split(zipfiles, len(zipfiles) / settings.CHUNK_THRESHOLD)
    else:
        chunks = [zipfiles]

    return chunks


def get_path_from(p: str | Path) -> Path:
    """
    Converts an input value into a Path object if it's not already one.

    Args:
        p: The input value, which can be a string or a Path object.

    Returns:
        The input value as a Path object.
    """
    if isinstance(p, str):
        p = Path(p)

    if not isinstance(p, Path):
        raise RuntimeError(f"Unable to handle type: {type(p)}")

    return p


def clear_cache(dir: str | Path) -> None:
    """
    Clears the cache directory by removing all `.json` files in it.

    Args:
        dir: The path of the directory to be cleared.
    """

    dir = get_path_from(dir)

    y = input(
        f"Do you want to erase the cache path now that the "
        f"files have been generated ({dir.absolute()})? [y/N]"
    )

    if y.lower() == "y":
        info("Clearing up the cache directory")
        for x in dir.glob("*.json"):
            x.unlink()


def get_size_from_path(p: str | Path, raw: bool = False) -> str | float:
    """
    Returns a nice string for any given file size.

    Args:
        p: Path to read the size from
        raw: Whether to return the file size as total number of bytes or
            a human-readable MB/GB amount

    Returns:
        Return `str` followed by `MB` or `GB` for size if not `raw` otherwise `float`.
    """

    p = get_path_from(p)

    bytes = p.stat().st_size

    if raw:
        return bytes

    rel_size: float | int | str = round(bytes / 1000 / 1000 / 1000, 1)

    assert not isinstance(rel_size, str)

    if rel_size < 0.5:
        rel_size = round(bytes / 1000 / 1000, 1)
        rel_size = f"{rel_size}MB"
    else:
        rel_size = f"{rel_size}GB"

    return rel_size


def write_json(
    p: str | Path, o: dict, add_created: bool = True, json_indent: int = JSON_INDENT
) -> None:
    """
    Easier access to writing `json` files. Checks whether parent exists.

    Args:
        p: Path to write `json` to
        o: Object to write to `json` file
        add_created:
            If set to True will add `created_at` and `updated_at`
            to the dictionary's fields. If `created_at` and `updated_at`
            already exist in the fields, they will be forcefully updated.
        json_indent:
            What indetation format to write out `JSON` file in

    Returns:
        None

    Example:
        ```pycon
        >>> tmp_path: Path = getfixture('tmp_path')
        >>> path: Path = tmp_path / 'test-write-json-example.json'
        >>> write_json(p=path,
        ...            o=NEWSPAPER_COLLECTION_METADATA,
        ...            add_created=True)
        >>> imported_fixture = load_json(path)
        >>> imported_fixture[1]['pk']
        2
        >>> imported_fixture[1]['fields'][DATA_PROVIDER_INDEX]
        'hmd'

        ```
        `
    """

    p = get_path_from(p)

    if not (isinstance(o, dict) or isinstance(o, list)):
        raise RuntimeError(f"Unable to handle data of type: {type(o)}")

    def _append_created_fields(o: dict):
        """Add `created_at` and `updated_at` fields to a `dict` with `FixtureDict` values."""
        return dict(
            **{k: v for k, v in o.items() if not k == "fields"},
            fields=dict(
                **{
                    k: v
                    for k, v in o["fields"].items()
                    if not k == "created_at" and not k == "updated_at"
                },
                **{"created_at": NOW_str, "updated_at": NOW_str},
            ),
        )

    try:
        if add_created and isinstance(o, dict):
            o = _append_created_fields(o)
        elif add_created and isinstance(o, list):
            o = [_append_created_fields(x) for x in o]
    except KeyError:
        error("An unknown error occurred (in write_json)")

    p.parent.mkdir(parents=True, exist_ok=True)

    p.write_text(json.dumps(o, indent=json_indent))

    return


def load_json(p: str | Path, crash: bool = False) -> dict | list:
    """
    Easier access to reading `json` files.

    Args:
        p: Path to read `json` from
        crash: Whether the program should crash if there is a `json` decode
            error, default: ``False``

    Returns:
        The decoded `json` contents from the path, but an empty dictionary
        if the file cannot be decoded and ``crash`` is set to ``False``
    """

    p = get_path_from(p)

    try:
        return json.loads(p.read_text())
    except json.JSONDecodeError:
        msg = f"Error: {p.read_text()}"
        error(msg, crash=crash)

    return {}


def list_json_files(
    p: str | Path,
    drill: bool = False,
    exclude_names: list = [],
    include_names: list = [],
) -> Generator[Path, None, None] | list[Path]:
    """
    List `json` files under the path specified in ``p``.

    Args:
        p: The path to search for `json` files
        drill: A flag indicating whether to drill down the subdirectories
            or not. Default is ``False``
        exclude_names: A list of file names to exclude from the search
            result. Default is an empty list
        include_names: A list of file names to include in search result.
            If provided, the ``exclude_names`` argument will be ignored.
            Default is an empty list

    Returns:
        A list of `Path` objects pointing to the found `json` files
    """

    q: str = "**/*.json" if drill else "*.json"
    files = get_path_from(p).glob(q)

    if exclude_names:
        files = list({x for x in files if x.name not in exclude_names})
    elif include_names:
        files = list({x for x in files if x.name in include_names})

    return sorted(files)


def load_multiple_json(
    p: str | Path,
    drill: bool = False,
    filter_na: bool = True,
    crash: bool = False,
) -> list:
    """
    Load multiple `json` files and return a list of their content.

    Args:
        p: The path to search for `json` files
        drill: A flag indicating whether to drill down the subdirectories
            or not. Default is `False`
        filter_na: A flag indicating whether to filter out the content that
            is `None`. Default is `True`.
        crash: A flag indicating whether to raise an exception when an
            error occurs while loading a `json` file. Default is `False`.

    Returns:
        A `list` of the content of the loaded `json` files.
    """

    files = list_json_files(p, drill=drill)

    content = [load_json(x, crash=crash) for x in files]

    return [x for x in content if x] if filter_na else content


def filter_json_fields(
    json_results: list | dict | None = None,
    file_path: PathLike | None = None,
    fields: Sequence[str] = [],
    value: Hashable = "",
    **kwargs,
) -> dict | list:
    """Return `keys` and `values` from `json_dict` where any `fields` equal `value`.

    Args:
        file_path: The file `path` to load based on extension and filter
        fields: Which fields to check equal `value`
        value: Value to filter by

    Returns:
        A `dict` of records indexed by `pk` which fit filter criteria

    Raises:
        ValueError: ``file_path`` must have a `.json` `suffix`

    Example:
        ```pycon
        >>> entry_fixture: dict = [
        ...     {"pk": 4889, "model": "mitchells.entry",
        ...      "fields": {"title": "BIRMINGHAM POST .",
        ...                 "price_raw": ['2d'],
        ...                 "year": 1920,
        ...                 "date_established_raw": "1857",
        ...                 "persons": [], "newspaper": ""}},
        ...      {"pk": 9207, "model": "mitchells.entry",
        ...       "fields": {"title": "ULVERSTONE ADVERTISER .",
        ...                  "price_raw": ['2 \u00bd d', '3 \u00bd d'],
        ...                  "year": 1856,
        ...                  "date_established_raw": "1848",
        ...                  "persons": ['Stephen Soulby'],
        ...                  "newspaper": "",}},
        ...     {"pk": 15, "model": "mitchells.entry",
        ...      "fields": {"title": "LLOYD'S WEEKLY LONDON NEWSPAPER .",
        ...                 "price_raw": ['2d', '3d'],
        ...                 "year": 1857,
        ...                 "date_established_raw": "November , 1842",
        ...                 "persons": ['Mr. Douglas Jerrold', 'Edward Lloyd'],
        ...                 "newspaper": 1187}}
        ...     ]
        >>> pprint(filter_json_fields(entry_fixture,
        ...                           fields=("newspaper", "persons"),
        ...                           value=""))
        [{'fields': {'date_established_raw': '1857',
                     'newspaper': '',
                     'persons': [],
                     'price_raw': ['2d'],
                     'title': 'BIRMINGHAM POST .',
                     'year': 1920},
          'model': 'mitchells.entry',
          'pk': 4889},
         {'fields': {'date_established_raw': '1848',
                     'newspaper': '',
                     'persons': ['Stephen Soulby'],
                     'price_raw': ['2 \u00bd d', '3 \u00bd d'],
                     'title': 'ULVERSTONE ADVERTISER .',
                     'year': 1856},
          'model': 'mitchells.entry',
          'pk': 9207}]

        ```
    """
    if not json_results:
        assert file_path
        try:
            assert Path(file_path).suffix == ".json"
        except AssertionError:
            raise ValueError(f"{file_path} must be `json` format.")
        json_results = load_json(Path(file_path), **kwargs)
    assert json_results
    if isinstance(json_results, dict):
        return {
            k: v
            for k, v in json_results.items()
            if any(v["fields"][field] == value for field in fields)
        }
    else:
        return [
            v
            for v in json_results
            if any(v["fields"][field] == value for field in fields)
        ]


def dict_from_list_fixture_fields(
    fixture_list: Iterable[FixtureDict] = NEWSPAPER_COLLECTION_METADATA,
    field_name: str = DATA_PROVIDER_INDEX,
) -> dict[str, FixtureDict]:
    """Create a `dict` from ``fixture_list`` with ``attr_name`` as `key`.

    Args:
        fixture_list: `list` of `FixtureDict` with ``attr_name`` key `fields`.
        field_name: key for values within ``fixture_list`` `fields`.

    Returns:
        A `dict` where extracted `field_name` is key for related `FixtureDict` values.

    Example:
        ```pycon
        >>> fixture_dict: dict[str, FixtureDict] = dict_from_list_fixture_fields()
        >>> fixture_dict['hmd']['pk']
        2
        >>> fixture_dict['hmd']['fields'][DATA_PROVIDER_INDEX]
        'hmd'
        >>> fixture_dict['hmd']['fields']['code']
        'bl_hmd'

        ```
    """
    return {record["fields"][field_name]: record for record in fixture_list}


def fixture_or_default_dict(
    key: str,
    fixture_dict: dict[str, FixtureDict],
    default_dict: FixtureDict | dict = {},
) -> FixtureDict | dict:
    """Return a `FixtureDict` from ``fixture_list`` via ``key`` index, else ``default_dict``.

    Args:
        key:
            a `str` to query ``fixture_dict`` with
        fixture_dict:
            a `dict` of `str` to `FixtureDict`, often generated by
            ``dict_from_list_fixture_fields``
        default_dict:
            a `dict` to return if ``key`` is not in ``fixture_dict`` index

    Example:
        ```pycon
        >>> newspaper_dict: dict[str, FixtureDict] = dict_from_list_fixture_fields(
        ...     NEWSPAPER_COLLECTION_METADATA)
        >>> hmd_dict: FixtureDict = fixture_or_default_dict(
        ...     'hmd', newspaper_dict
        ... )
        >>> hmd_dict == newspaper_dict['hmd']
        True
        >>> fixture_or_default_dict(
        ...     'hmd', NEWSPAPER_COLLECTION_METADATA
        ... )
        {}
        >>> fixture_or_default_dict(
        ...     'hmd', NEWSPAPER_COLLECTION_METADATA, {'a': 'default'}
        ... )
        {'a': 'default'}

        ```
    """
    if key in fixture_dict:
        return fixture_dict[key]
    else:
        return default_dict


def check_newspaper_collection_configuration(
    collections: Iterable[str] = settings.COLLECTIONS,
    newspaper_collections: Iterable[FixtureDict] = NEWSPAPER_COLLECTION_METADATA,
    data_provider_index: str = DATA_PROVIDER_INDEX,
) -> set[str]:
    """Check the names in `collections` match the names in `newspaper_collections`.

    Arguments:
        collections:
            Names of newspaper collections, defaults to ``settings.COLLECTIONS``
        newspaper_collections:
            Newspaper collections in a list of `FixtureDict` format. Defaults
                to ``settings.FIXTURE_TABLE['dataprovider]``
        data_provider_index:
            `dict` `fields` `key` used to check matchiching `collections` name

    Returns:
        A set of ``collections`` without a matching `newspaper_collections` record.

    Example:
        ```pycon
        >>> check_newspaper_collection_configuration()
        set()
        >>> unmatched: set[str] = check_newspaper_collection_configuration(
        ...     ["cat", "dog"])
        <BLANKLINE>
        ...Warning: 2 `collections` not in `newspaper_collections`: ...
        >>> unmatched == {'dog', 'cat'}
        True

        ```

        !!! note

            Set orders are random so checking `unmatched == {'dog, 'cat'}` to
            ensure correctness irrespective of order in the example above.

    """
    newspaper_collection_names: tuple[str, ...] = tuple(
        dict_from_list_fixture_fields(
            newspaper_collections, field_name=data_provider_index
        ).keys()
    )
    collection_diff: set[str] = set(collections) - set(newspaper_collection_names)
    if collection_diff:
        warning(
            f"{len(collection_diff)} `collections` "
            f"not in `newspaper_collections`: {collection_diff}"
        )
    return collection_diff


def fixture_fields(
    fixture_dict: FixtureDict, include_pk: bool = True, as_dict: bool = False
) -> tuple[str, ...] | dict[str, Any]:
    """Generate a tuple of `FixtureDict` `field` names.

    Note:
        This is not in the `utils` module to avoid a circular import.

    Args:
        fixture_dict: A `FixtureDict` instance to extract names from `fields`
        include_pk: Whether to include the `pk` (primary key) column

    Example:
        ```pycon
        >>> fixture_fields(NEWSPAPER_COLLECTION_METADATA[0])
        ('pk', 'name', 'code', 'legacy_code', 'collection', 'source_note')
        >>> fixture_fields(NEWSPAPER_COLLECTION_METADATA[0], include_pk=False)
        ('name', 'code', 'legacy_code', 'collection', 'source_note')
        >>> hmd_dict: dict[str, Any] = fixture_fields(
        ...     NEWSPAPER_COLLECTION_METADATA[1], as_dict=True)
        >>> hmd_dict['code']
        'bl_hmd'
        >>> hmd_dict['pk']
        2
        >>> hmd_dict = fixture_fields(
        ...     NEWSPAPER_COLLECTION_METADATA[1], include_pk=False, as_dict=True)
        >>> 'pk' in hmd_dict
        False

        ```
    """
    fields: OrderedDict[str, Any] = OrderedDict(fixture_dict["fields"])
    if include_pk:
        fields["pk"] = fixture_dict["pk"]
        fields.move_to_end("pk", last=False)
    if as_dict:
        return fields
    else:
        return tuple(fields.keys())


def gen_fixture_tables(
    fixture_tables: dict[str, list[FixtureDict]] = {},
    include_fixture_pk_column: bool = True,
) -> Generator[Table, None, None]:
    """Generator of `rich.Table` instances from `FixtureDict` configuration tables.

    Args:
        fixture_tables: `dict` where `key` is for `Table` title and `value` is a `FixtureDict`
        include_fixture_pk_column: whether to include the `pk` field from `FixtureDict`

    Example:
        ```pycon
        >>> table_name: str = "data_provider"
        >>> tables = tuple(
        ...     gen_fixture_tables(
        ...         {table_name: NEWSPAPER_COLLECTION_METADATA}
        ...     ))
        >>> len(tables)
        1
        >>> assert tables[0].title == table_name
        >>> [column.header for column in tables[0].columns]
        ['pk', 'name', 'code', 'legacy_code', 'collection', 'source_note']

        ```
    """
    for name, fixture_records in fixture_tables.items():
        fixture_table: Table = Table(title=name)
        for i, fixture_dict in enumerate(fixture_records):
            if i == 0:
                [
                    fixture_table.add_column(name)
                    for name in fixture_fields(fixture_dict, include_fixture_pk_column)
                ]
            row_values: tuple[str, ...] = tuple(
                str(x) for x in (fixture_dict["pk"], *fixture_dict["fields"].values())
            )
            fixture_table.add_row(*row_values)
        yield fixture_table


def save_fixture(
    generator: Sequence | Generator = [],
    prefix: str = "",
    output_path: PathLike | str = settings.OUTPUT,
    max_elements_per_file: int = settings.MAX_ELEMENTS_PER_FILE,
    add_created: bool = True,
    json_indent: int = JSON_INDENT,
    file_name_0_padding: int = FILE_NAME_0_PADDING_DEFAULT,
) -> None:
    """Saves fixtures generated by a generator to separate JSON files.

    This function takes a generator and saves the generated fixtures to
    separate JSON files. The fixtures are saved in batches, where each batch
    is determined by the ``max_elements_per_file`` parameter.

    Args:
        generator:
            A generator that yields the fixtures to be saved.
        prefix:
            A string prefix to be added to the file names of the
            saved fixtures.
        output_path:
            Path to folder fixtures are saved to
        max_elements_per_file:
            Maximum `JSON` records saved in each file
        add_created:
            Whether to add `created_at` and `updated_at` `timestamps`
        json_indent:
            Number of indent spaces per line in saved `JSON`
        file_name_0_padding:
            Zeros to prefix the number of each fixture file name.

    Returns:
        This function saves the fixtures to files but does not return
        any value.

    Example:
        ```pycon
        >>> tmp_path: Path = getfixture('tmp_path')
        >>> save_fixture(NEWSPAPER_COLLECTION_METADATA,
        ...              prefix='test', output_path=tmp_path)
        >>> imported_fixture = load_json(tmp_path / 'test-000001.json')
        >>> imported_fixture[1]['pk']
        2
        >>> imported_fixture[1]['fields'][DATA_PROVIDER_INDEX]
        'hmd'
        >>> 'created_at' in imported_fixture[1]['fields']
        True

        ```

    """
    internal_counter = 1
    counter = 1
    lst = []
    file_name: str
    Path(output_path).mkdir(parents=True, exist_ok=True)
    for item in generator:
        lst.append(item)
        internal_counter += 1
        if internal_counter > max_elements_per_file:
            file_name = f"{prefix}-{str(counter).zfill(file_name_0_padding)}.json"
            write_json(
                p=Path(f"{output_path}/file_name"),
                o=lst,
                add_created=add_created,
                json_indent=json_indent,
            )

            # Save up some memory
            del lst
            gc.collect()

            # Re-instantiate
            lst = []
            internal_counter = 1
            counter += 1
    else:
        file_name = f"{prefix}-{str(counter).zfill(file_name_0_padding)}.json"
        write_json(
            p=Path(f"{output_path}/{file_name}"),
            o=lst,
            add_created=add_created,
            json_indent=json_indent,
        )

    return


def fixtures_dict2csv(
    fixtures: Iterable[FixtureDict] | Generator[FixtureDict, None, None],
    prefix: str = "",
    output_path: PathLike | str = settings.OUTPUT,
    index: bool = False,
    max_elements_per_file: int = settings.MAX_ELEMENTS_PER_FILE,
    file_name_0_padding: int = FILE_NAME_0_PADDING_DEFAULT,
) -> None:
    """Saves fixtures generated by a generator to separate separate `CSV` files.

    This function takes an `Iterable` or `Generator` of fixtures and saves to
    separate `CSV` files. The fixtures are saved in batches, where each batch
    is determined by the ``max_elements_per_file`` parameter.

    Args:
        fixtures:
            An `Iterable` or `Generator` of the fixtures to be saved.
        prefix:
            A string prefix to be added to the file names of the
            saved fixtures.
        output_path:
            Path to folder fixtures are saved to
        max_elements_per_file:
            Maximum `JSON` records saved in each file
        file_name_0_padding:
            Zeros to prefix the number of each fixture file name.

    Returns:
        This function saves fixtures to files and does not return a value.

    Example:
        ```pycon
        >>> tmp_path: Path = getfixture('tmp_path')
        >>> from pandas import read_csv
        >>> fixtures_dict2csv(NEWSPAPER_COLLECTION_METADATA,
        ...                   prefix='test', output_path=tmp_path)
        >>> imported_fixture = read_csv(tmp_path / 'test-000001.csv')
        >>> imported_fixture.iloc[1]['pk']
        2
        >>> imported_fixture.iloc[1][DATA_PROVIDER_INDEX]
        'hmd'

        ```

    """
    internal_counter: int = 1
    counter: int = 1
    lst: list = []
    file_name: str
    Path(output_path).mkdir(parents=True, exist_ok=True)
    for item in fixtures:
        lst.append(fixture_fields(item, as_dict=True))
        internal_counter += 1
        if internal_counter > max_elements_per_file:
            df: DataFrame = DataFrame.from_records(lst)

            file_name = f"{prefix}-{str(counter).zfill(file_name_0_padding)}.csv"
            df.to_csv(Path(output_path) / file_name, index=index)
            # Save up some memory
            del lst
            gc.collect()

            # Re-instantiate
            lst: list = []
            internal_counter = 1
            counter += 1
    else:
        df: DataFrame = DataFrame.from_records(lst)
        file_name = f"{prefix}-{str(counter).zfill(file_name_0_padding)}.csv"
        df.to_csv(Path(output_path) / file_name, index=index)


def export_fixtures(
    fixture_tables: dict[str, Sequence[FixtureDict]],
    path: str | PathLike = settings.FIXTURE_TABLES_OUTPUT,
    prefix: str = "test-",
    add_created: bool = True,
    formats: Sequence[EXPORT_FORMATS] = settings.FIXTURE_TABLES_FORMATS,
    file_name_0_padding: int = FILE_NAME_0_PADDING_DEFAULT,
) -> None:
    """Export ``fixture_tables`` in ``formats``.

    Note:
        This is still in experimental phase of development and not recommended
        for production.

    Args:
        fixture_tables:
            `dict` of table name (eg: `dataprovider`) and `FixtureDict`
        path:
            `Path` to save exports in
        prefix:
            `str` to prefix export file names with
        formats:
            `list` of `EXPORT_FORMATS` to export
        file_name_0_padding:
            Zeros to prefix the number of each fixture file name.

    Example:
        ```pycon
        >>> test_fixture_tables: dict[str, FixtureDict] = {
        ...     'test0': NEWSPAPER_COLLECTION_METADATA,
        ...     'test1': NEWSPAPER_COLLECTION_METADATA}
        >>> export_fixtures(test_fixture_tables, path='tests/')
        <BLANKLINE>
        ...Warning: Saving test0...
        ...Warning: Saving test1...
        >>> from pandas import read_csv
        >>> fixture0_json = load_json('tests/test-test0-000001.json')
        >>> fixture0_df = read_csv('tests/test-test0-000001.csv')
        >>> fixture1_json = load_json('tests/test-test1-000001.json')
        >>> fixture1_df = read_csv('tests/test-test1-000001.csv')
        >>> fixture0_json == fixture1_json
        True
        >>> all(fixture0_df == fixture1_df)
        True
        >>> all(field in fixture0_json[0]['fields']
        ...     for field in ['created_at', 'updated_at'])
        True
        >>> fixture0_json[1]['pk']
        2
        >>> fixture0_json[1]['fields'][DATA_PROVIDER_INDEX]
        'hmd'
        >>> fixture0_df[['pk', DATA_PROVIDER_INDEX]].iloc[1].to_list()
        [2, 'hmd']

        ```
    """
    for table_name, records in fixture_tables.items():
        warning(
            f"Saving {table_name} fixture in {formats} formats "
            f"to {path} *without* checks..."
        )
        if "json" in formats:
            save_fixture(
                records,
                prefix=f"{prefix}{table_name}",
                output_path=path,
                add_created=add_created,
                file_name_0_padding=file_name_0_padding,
            )
        if "csv" in formats:
            fixtures_dict2csv(records, prefix=f"{prefix}{table_name}", output_path=path)


def path_globs_to_tuple(
    path: PathLike, glob_regex_str: str = "*"
) -> tuple[PathLike, ...]:
    """Return `glob` from `path` using `glob_regex_str` as a tuple.

    Args:
        path:
            Patch to search via `glob`

        glob_regex_str:
            Regular expression to use with `glob` at `path`

    Returns:
        `tuple` of matching paths.

    Example:
        ```pycon
        >>> bl_lwm = getfixture("bl_lwm")
        >>> pprint(path_globs_to_tuple(bl_lwm, '*text.zip'))
        (...Path('...bl_lwm...0003079-test_plaintext.zip'),
         ...Path('...bl_lwm...0003548-test_plaintext.zip'))
        >>> pprint(path_globs_to_tuple(bl_lwm, '*.txt'))
        (...Path('...bl_lwm...0003079_18980121_sect0001.txt'),
         ...Path('...bl_lwm...0003548_19040707_art0037.txt'))

        ```

    """
    return tuple(sorted(Path(path).glob(glob_regex_str)))


class DiskUsageTuple(NamedTuple):

    """Type hint for `nametuple` returned from `disk_usage`."""

    total: int
    used: int
    free: int


def free_hd_space_in_GB(
    disk_usage_tuple: DiskUsageTuple | None = None, path: PathLike | None = None
) -> float:
    """Return remaing hard drive space estimate in gigabytes.

    Args:
        disk_usage_tuple:
            A `NamedTuple` normally returned from `disk_usage()` or `None`.

        path:
            A `path` to pass to `disk_usage` if `disk_usage_tuple` is `None`.

    Returns:
        A `float` from dividing the `disk_usage_tuple.free` value by `BYTES_PER_GIGABYTE`

    Example:
        ```pycon
        >>> space_in_gb = free_hd_space_in_GB()
        >>> space_in_gb > 1  # Hopefully true wherever run...
        True

        ```
    """
    if not disk_usage_tuple:
        if not path:
            path = Path(getcwd())
        disk_usage_tuple = disk_usage(path=path)
    assert disk_usage_tuple
    return disk_usage_tuple.free / BYTES_PER_GIGABYTE


def valid_compression_files(files: Sequence[PathLike]) -> list[PathLike]:
    """Return a `tuple` of valid compression paths in `files`.

    Args:
        files:
            `Sequence` of files to filter compression types from.

    Returns:
        A list of files that could be decompressed.

    Example:
        ```pycon
        >>> valid_compression_files([
        ...     'cat.tar.bz2', 'dog.tar.bz3', 'fish.tgz', 'bird.zip',
        ...     'giraffe.txt', 'frog'
        ... ])
        ['cat.tar.bz2', 'fish.tgz', 'bird.zip']

        ```
    """
    return [
        file
        for file in files
        if "".join(Path(file).suffixes) in VALID_COMPRESSION_FORMATS
    ]


def compress_fixture(
    path: PathLike,
    output_path: PathLike | str = settings.OUTPUT,
    suffix: str = "",
    format: str = ZIP_FILE_EXTENSION,
) -> None:
    """Compress exported `fixtures` files using `make_archive`.

    Args:
        path:
            `Path` to file to compress

        output_path:
            Compressed file name (without extension specified from `format`).

        format:
            A `str` of one of the registered compression formats.
            `Python` provides `zip`, `tar`, `gztar`, `bztar`, and `xztar`

        suffix:
            `str` to add to comprssed file name saved.
            For example: if `path = plaintext_fixture-1.json` and
            `suffix=_compressed`, then the saved file might be called
            `plaintext_fixture_compressed-1.json.zip`

    Example:
        ```pycon
        >>> tmp_path: Path = getfixture("tmp_path")
        >>> plaintext_bl_lwm = getfixture('bl_lwm_plaintext_json_export')
        <BLANKLINE>
        ...Compressed configs...%...[...]
        >>> compress_fixture(
        ...     path=plaintext_bl_lwm._exported_json_paths[0],
        ...     output_path=tmp_path)
        Compressing...plain...-000001.json to 'zip'
        >>> from zipfile import ZipFile, ZipInfo
        >>> zipfile_info_list: list[ZipInfo] = ZipFile(
        ...     tmp_path / 'plaintext_fixture-000001.json.zip'
        ... ).infolist()
        >>> len(zipfile_info_list)
        1
        >>> Path(zipfile_info_list[0].filename).name
        'plaintext_fixture-000001.json'

        ```
    """
    chdir(str(Path(path).parent))
    save_path: Path = Path(output_path) / f"{path}{suffix}"
    console.print(f"Compressing {path} to '{format}'")
    make_archive(str(save_path), format=format, base_dir=path)


def paths_with_newlines(
    paths: Iterable[PathLike], truncate: bool = False, **kwargs
) -> str:
    """Return a `str` of `paths` separated by a `\\n`.

    Example:
        ```pycon
        >>> plaintext_bl_lwm = getfixture('bl_lwm_plaintext')
        >>> print(paths_with_newlines(plaintext_bl_lwm.compressed_files))
        '...bl_lwm...0003079-test_plaintext.zip'
        '...bl_lwm...0003548-test_plaintext.zip'
        >>> print(
        ...     paths_with_newlines(plaintext_bl_lwm.compressed_files,
        ...                         truncate=True)
        ... )
        <BLANKLINE>
        ...Adding 1...
        '...0003079-test_plaintext.zip'
        '...0003548-test_plaintext.zip'

        ```
    """
    if truncate:
        return "\n".join(f"'{truncate_path_str(f, **kwargs)}'" for f in paths)
    else:
        return "\n".join(f"'{f}'" for f in paths)


def truncate_path_str(
    path: PathLike,
    max_length: int = MAX_TRUNCATE_PATH_STR_LEN,
    folder_filler_str: str = INTERMEDIATE_PATH_TRUNCATION_STR,
    head_parts: int = TRUNC_HEADS_PATH_DEFAULT,
    tail_parts: int = TRUNC_TAILS_PATH_DEFAULT,
    path_sep: str = sep,
    _force_type: Type[Path] | Type[PureWindowsPath] = Path,
) -> str:
    """If `len(text) > max_length` return `text` followed by `trail_str`.

    Args:
        path:
            `PathLike` object to truncate
        max_length:
            maximum length of `path` to allow, anything belond truncated
        folder_filler_str:
            what to fill intermediate path names with
        head_parts:
            how many parts of `path` from the root to keep.
            These must be `int` >= 0
        tail_parts:
            how many parts from the `path` tail the root to keep.
            These must be `int` >= 0
        path_sep:
            what `str` to replace `path` parts with if over `max_length`

    Returns:
        `text` truncated to `max_length` (if longer than `max_length`),
            with with `folder_filler_str` for intermediate folder names

    Note:
        For errors running on windows see:
        [#56](https://github.com/Living-with-machines/alto2txt2fixture/issues/56)


    Example:
        ```pycon
        >>> love_shadows: Path = (
        ...     Path('Standing') / 'in' / 'the' / 'shadows'/ 'of' / 'love.')
        >>> truncate_path_str(love_shadows)
        'Standing...love.'
        >>> truncate_path_str(love_shadows, max_length=100)
        'Standing...in...the...shadows...of...love.'
        >>> truncate_path_str(love_shadows, folder_filler_str="*")
        'Standing...*...*...*...*...love.'
        >>> root_love_shadows: Path = Path(sep) / love_shadows
        >>> truncate_path_str(root_love_shadows, folder_filler_str="*")
        <BLANKLINE>
        ...Adding 1...
        '...Standing...*...*...*...*...love.'
        >>> if is_platform_win:
        ...     pytest.skip('fails on certain Windows root paths: issue #56')
        >>> truncate_path_str(root_love_shadows,
        ...                   folder_filler_str="*", tail_parts=3)
        <BLANKLINE>
        ...Adding 1...
        '...Standing...*...*...shadows...of...love.'...

        ```
    """
    path = _force_type(normpath(path))
    if len(str(path)) > max_length:
        try:
            assert not (head_parts < 0 or tail_parts < 0)
        except AssertionError:
            logger.error(
                f"Both index params for `truncate_path_str` must be >=0: "
                f"(head_parts={head_parts}, tail_parts={tail_parts})"
            )
            return str(path)
        original_path_parts: tuple[str] = path.parts
        head_index_fix: int = 0
        if path.is_absolute() or path.drive:
            head_index_fix += 1
            for part in original_path_parts[head_parts + head_index_fix :]:
                if not part:
                    head_index_fix += 1
                else:
                    break
            logger.debug(
                f"Adding {head_index_fix} to `head_parts`: {head_parts} "
                f"to truncate: '{path}'"
            )
            head_parts += head_index_fix
        try:
            assert head_parts + tail_parts < len(str(original_path_parts))
        except AssertionError:
            logger.error(
                f"Returning untruncated. Params "
                f"(head_parts={head_parts}, tail_parts={tail_parts}) "
                f"not valid to truncate: '{path}'"
            )
            return str(path)
        tail_index: int = len(original_path_parts) - tail_parts
        replaced_path_parts: tuple[str] = tuple(
            part if (i < head_parts or i >= tail_index) else folder_filler_str
            for i, part in enumerate(original_path_parts)
        )
        replaced_start_str: str = "".join(replaced_path_parts[:head_parts])
        replaced_end_str: str = path_sep.join(
            path for path in replaced_path_parts[head_parts:]
        )
        return path_sep.join((replaced_start_str, replaced_end_str))
    else:
        return str(path)


def rename_by_0_padding(
    file_path: PathLike,
    match_index: int = -1,
    padding: int = FILE_NAME_0_PADDING_DEFAULT,
    regex: str = PADDING_0_REGEX_DEFAULT,
) -> Path:
    """Return `file_path` with `0` `padding` `Path` change.

    Params:
        file_path:
            `PathLike` to rename.

        padding:
            How many digits (0s) to pad filename integer with.

        match_index:
            Which index of number in `file_path` to pad with 0s.
            Like numbering a `list`, 0 indicates the first match
            and -1 indicates the last match.

        regex:
            Regular expression for matching numbers in `file_path` to pad.

    Example:
        ```pycon
        >>> rename_by_0_padding('a/path/to/fixture-03-05.txt')
        <BLANKLINE>
        ...Path('a/path/to/fixture-03-000005.txt')...
        >>> rename_by_0_padding('a/path/to/fixture-03-05.txt',
        ...                     match_index=0)
        <BLANKLINE>
        ...Path('a/path/to/fixture-000003-05.txt')...
        >>> rename_by_0_padding('a/path/to/fixture-03-05.txt',
        ...                     padding=0)
        <BLANKLINE>
        ...Path('a/path/to/fixture-03-5.txt')...

        ```
    """
    file_name: str = Path(file_path).name
    matches: list[str] = [s for s in findall(regex, file_name) if s]
    match_str: str = matches[match_index]
    new_file_name: str = file_name.replace(
        match_str, str(int(match_str)).zfill(padding)
    )
    return Path(file_path).parent / new_file_name
