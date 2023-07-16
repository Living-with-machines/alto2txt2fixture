import datetime
import json
import logging
from pathlib import Path
from typing import Union

import pytz
from numpy import array_split
from rich.logging import RichHandler

from .log import error, info
from .settings import settings

FORMAT: str = "%(message)s"

logging.basicConfig(
    level="NOTSET", format=FORMAT, datefmt="[%X]", handlers=[RichHandler()]
)

logger = logging.getLogger("rich")


def get_now(as_str: bool = False) -> Union[datetime.datetime, str]:
    """
    Get the "now" from the datetime library, either as a string or object.

    :param as_str: Whether to return the now as a string or not, default:
        ``False``
    :return: Now, as string if ``as_str`` was passed as ``True``, otherwise as
        datetime.datetime object.
    """
    now = datetime.datetime.now(tz=pytz.UTC)

    if as_str:
        return str(now)

    return now


NOW_str = get_now(as_str=True)


def get_key(x: dict = dict(), on: list = []) -> str:
    """
    Get a string key from a dictionary using values from specified keys.

    :param x: A dictionary from which the key is generated.
    :param on: A list of keys from the dictionary that should be used to
        generate the key.
    :return: The generated string key.
    :rtype: str
    """

    return f"{'-'.join([str(x['fields'][y]) for y in on])}"


def create_lookup(lst: list = [], on: list = []) -> dict:
    """
    Create a lookup dictionary from a list of dictionaries.

    :param lst: A list of dictionaries that should be used to generate the lookup.
    :param on: A list of keys from the dictionaries in the list that should be used as the keys in the lookup.
    :return: The generated lookup dictionary.
    """
    return {get_key(x, on): x["pk"] for x in lst}


def glob_filter(p: str) -> list:
    """
    Assists Python with filtering out any pesky, unwanted .DS_Store from macOS.

    :param p: Path to a directory to filter
    :return: List of files contained in the provided path without the ones
        whose names start with a "."
    """
    return [x for x in get_path_from(p).glob("*") if not x.name.startswith(".")]


def lock(lockfile: Path) -> None:
    """
    Writes a '.' to a lockfile, after making sure the parent directory exists.

    :param lockfile: The path to the lock file to be created
    :return: None
    """
    lockfile.parent.mkdir(parents=True, exist_ok=True)

    lockfile.write_text("")

    return


def get_lockfile(collection: str, kind: str, dic: dict) -> Path:
    """
    Provides the path to any given lockfile, which controls whether any
    existing files should be overwritten or not.

    :param kind: Either "newspaper" or "issue" or "item"
    :param dic: A dictionary with required information for either `kind` passed
    :return: Path to the resulting lockfile
    """

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
    """
    This function takes in a `Path` object `path` and returns a list of lists
    of `zipfiles` sorted and chunked according to certain conditions defined
    in the ``settings`` object (see ``settings.CHUNK_THRESHOLD``).

    Note: the function will also skip zip files of a certain file size, which
    can be specified in the ``settings`` object (see
    ``settings.SKIP_FILE_SIZE``).

    :param path: The input path where the zipfiles are located
    :return: A list of lists of ``zipfiles``, each inner list represents a
        chunk of zipfiles.
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


def clear_cache(dir: Union[str, Path]) -> None:
    """
    Clears the cache directory by removing all `.json` files in it.

    :param dir: The path of the directory to be cleared.
    :return: None
    """

    dir = get_path_from(dir)

    y = input(
        f"Do you want to erase the cache path now that the files have been \
        generated ({dir.absolute()})? [y/N]"
    )

    if y.lower() == "y":
        info("Clearing up the cache directory")
        [x.unlink() for x in dir.glob("*.json")]

    return


def get_path_from(p: Union[str, Path]) -> Path:
    """
    Converts an input value into a Path object if it's not already one.

    :param p: The input value, which can be a string or a Path object.
    :return: The input value as a Path object.
    """
    if isinstance(p, str):
        p = Path(p)

    if not isinstance(p, Path):
        raise RuntimeError(f"Unable to handle type: {type(p)}")

    return p


def get_size_from_path(p: Union[str, Path], raw: bool = False) -> Union[str, int]:
    """
    Returns a nice string for any given file size.

    :param p: Path to read the size from
    :param raw: Whether to return the file size as total number of bytes or
        a human-readable MB/GB amount
    """

    p = get_path_from(p)

    bytes = p.stat().st_size

    if raw:
        return bytes

    rel_size = round(bytes / 1000 / 1000 / 1000, 1)

    if rel_size < 0.5:
        rel_size = round(bytes / 1000 / 1000, 1)
        rel_size = f"{rel_size}MB"
    else:
        rel_size = f"{rel_size}GB"

    return rel_size


def write_json(p: Union[str, Path], o: dict, add_created: bool = True) -> None:
    """
    Easier access to writing JSON files. Checks whether parent exists.

    :param p: Path to write JSON to
    :param o: Object to write to JSON file
    :param add_created: If set to True will add ``created_at`` and
        ``updated_at`` to the dictionary's fields (and if ``created_at`` and
        ``updated_at`` already exist in the fields, they will be forcefully
        updated)
    :return: None
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


def load_json(p: Union[str, Path], crash: bool = False) -> dict:
    """
    Easier access to reading JSON files.

    :param p: Path to read JSON from
    :param crash: Whether the program should crash if there is a JSON decode
        error, default: ``False``
    :return: The decoded JSON contents from the path, but an empty dictionary
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
) -> list:
    """
    List JSON files under the path specified in ``p``.

    :param p: The path to search for JSON files
    :param drill: A flag indicating whether to drill down the subdirectories
        or not. Default is ``False``
    :param exclude_names: A list of file names to exclude from the search
        result. Default is an empty list
    :param include_names: A list of file names to include in the search result
        If provided, the ``exclude_names`` argument will be ignored. Default
        is an empty list
    :return: A list of `Path` objects pointing to the found JSON files
    """

    q = "**/*.json" if drill else "*.json"
    files = get_path_from(p).glob(q)

    if exclude_names:
        return list({x for x in files if x.name not in exclude_names})
    elif include_names:
        return list({x for x in files if x.name in include_names})

    return files


def load_multiple_json(
    p: Union[str, Path],
    drill: bool = False,
    filter_na: bool = True,
    crash: bool = False,
) -> list:
    """
    Load multiple JSON files and return a list of their content.

    :param p: The path to search for JSON files
    :param drill: A flag indicating whether to drill down the subdirectories
        or not. Default is ``False``
    :param filter_na: A flag indicating whether to filter out the content that
        is ``None``. Default is ``True``.
    :param crash: A flag indicating whether to raise an exception when an
        error occurs while loading a JSON file. Default is ``False``.
    :return: A list of the content of the loaded JSON files.
    """

    files = list_json_files(p, drill=drill)

    content = [load_json(x, crash=crash) for x in files]

    return [x for x in content if x] if filter_na else content
