import datetime
import json
import logging
from os import PathLike
from pathlib import Path
from typing import Final, Generator, Hashable, Literal, Sequence, TypeAlias, Union

import pytz
from numpy import array_split
from rich.logging import RichHandler

from .log import error, info
from .settings import settings

FORMAT: str = "%(message)s"
NewspaperElements: Final[TypeAlias] = Literal["newspaper", "issue", "item"]

logging.basicConfig(
    level="NOTSET", format=FORMAT, datefmt="[%X]", handlers=[RichHandler()]
)

logger = logging.getLogger("rich")


def get_now(as_str: bool = False) -> Union[datetime.datetime, str]:
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


def get_path_from(p: Union[str, Path]) -> Path:
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


def clear_cache(dir: Union[str, Path]) -> None:
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


def get_size_from_path(p: str | Path, raw: bool = False) -> str | int | float:
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


def write_json(p: Union[str, Path], o: dict, add_created: bool = True) -> None:
    """
    Easier access to writing `json` files. Checks whether parent exists.

    Args:
        p: Path to write `json` to
        o: Object to write to `json` file
        add_created: If set to True will add `created_at` and `updated_at`
            to the dictionary's fields. If `created_at` and `updated_at`
            already exist in the fields, they will be forcefully updated.

    Returns:
        None
    """

    def _append_created_fields(o):
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

    p = get_path_from(p)

    if not (isinstance(o, dict) or isinstance(o, list)):
        raise RuntimeError(f"Unable to handle data of type: {type(o)}")

    try:
        if add_created and isinstance(o, dict):
            o = _append_created_fields(o)
        elif add_created and isinstance(o, list):
            o = [_append_created_fields(x) for x in o]
    except KeyError:
        error("An unknown error occurred (in write_json)")

    p.parent.mkdir(parents=True, exist_ok=True)

    p.write_text(json.dumps(o))

    return


def load_json(p: Union[str, Path], crash: bool = False) -> dict | list:
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
    p: Union[str, Path],
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
    p: Union[str, Path],
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

    Examples:
        ```pycon
        >>> from pprint import pprint
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
