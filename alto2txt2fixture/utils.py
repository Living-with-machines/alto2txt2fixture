import datetime
import gc
import json
import logging
from collections import OrderedDict
from dataclasses import dataclass
from enum import StrEnum
from os import PathLike, getcwd, sep
from os.path import normpath
from pathlib import Path, PureWindowsPath
from pprint import pformat
from re import findall
from shutil import (
    copyfile,
    copyfileobj,
    disk_usage,
    get_archive_formats,
    get_unpack_formats,
    make_archive,
)
from typing import (
    Any,
    Callable,
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
from urllib.error import URLError
from urllib.request import urlopen

import pytz
from numpy import array_split
from pandas import DataFrame, Series
from rich.console import Console
from rich.logging import RichHandler
from rich.table import Table
from validators.url import url as validate_url

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

console: Console = Console()

logging.basicConfig(
    level="NOTSET",
    format=FORMAT,
    datefmt="[%X]",
    handlers=[RichHandler(rich_tracebacks=True, console=console)],
)

logger = logging.getLogger("rich")

VALID_COMPRESSION_FORMATS: Final[tuple[str, ...]] = tuple(
    [
        extension
        for format_tuple in get_unpack_formats()
        for extension in format_tuple[1]
    ]
)
BYTES_PER_GIGABYTE: Final[int] = 1024 * 1024 * 1024

NewspaperElements: Final[TypeAlias] = Literal["newspaper", "issue", "item"]

ARCHIVE_FORMATS: Final[dict[str, str]] = {k: v for k, v in get_archive_formats()}
ArchiveFormatEnum: Final = StrEnum(
    "ArchiveFormatEnum", tuple(f.upper() for f in ARCHIVE_FORMATS)
)

ZIP_FILE_EXTENSION: Final[ArchiveFormatEnum] = ArchiveFormatEnum.ZIP

COMPRESSION_TYPE_DEFAULT: Final[ArchiveFormatEnum] = ZIP_FILE_EXTENSION
COMPRESSED_PATH_DEFAULT: Final[Path] = Path("compressed")

JSON_FILE_EXTENSION: str = "json"
JSON_FILE_GLOB_STRING: str = f"**/*{JSON_FILE_EXTENSION}"

DEFAULT_MAX_LOG_STR_LENGTH: Final[int] = 30
MAX_TRUNCATE_PATH_STR_LEN: Final[int] = 30
INTERMEDIATE_PATH_TRUNCATION_STR: Final[str] = "."

TRUNC_HEADS_PATH_DEFAULT: int = 1
TRUNC_TAILS_PATH_DEFAULT: int = 1
FILE_NAME_0_PADDING_DEFAULT: int = 6
PADDING_0_REGEX_DEFAULT: str = r"\b\d*\b"

CODE_SEPERATOR_CHAR: Final[str] = "-"
FILE_NAME_SEPERATOR_CHAR: Final[str] = "_"
DEFAULT_TRUNCATION_CHARS: Final[str] = "..."
"""Default characters to trail a truncated string."""

DEFAULT_APP_DATA_FOLDER: Final[Path] = Path("data")


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


def _short_text_trunc(text: str, trail_str: str = DEFAULT_TRUNCATION_CHARS) -> str:
    """Return a `str` truncated to 15 characters followed by `trail_str`."""
    return truncate_str(
        text=text, trail_str=trail_str + path_or_str_suffix(text), max_length=15
    )


def truncate_str(
    text: str,
    max_length: int = DEFAULT_MAX_LOG_STR_LENGTH,
    trail_str: str = DEFAULT_TRUNCATION_CHARS,
) -> str:
    """If `len(text) > max_length` return `text` followed by `trail_str`.

    Args:
        text: `str` to truncate
        max_length: maximum length of `text` to allow, anything belond truncated
        trail_str: what is appended to the end of `text` if truncated

    Returns:
        `text` truncated to `max_length` (if longer than `max_length`),
        appended with `tail_str`

    Example:
        ```pycon
        >>> truncate_str('Standing in the shadows of love.', 15)
        'Standing in the...'

        ```
    """
    return text[:max_length] + trail_str if len(text) > max_length else text


def path_or_str_suffix(
    str_or_path: str | PathLike,
    max_extension_len: int = 10,
    force: bool = False,
    split_str: str = ".",
) -> str:
    """Return suffix of `str_or_path`, else `''`.

    Args:
        str_or_path: `str` or `PathLike` instance to extract `suffix` from.
        max_extension_len: Maximum `extension` allowed for `suffix` to extract.
        force: `bool` for overrised `max_extension_len` constraint.
        split_str: `str` to split `str_or_path` by, usually `.` for file path.

    Returns:
        `str` extracted from the end of `str_or_path`.

    Example:
        ```pycon
        >>> path_or_str_suffix('https://lwmd.livingwithmachines.ac.uk/file.bz2')
        'bz2'
        >>> path_or_str_suffix('https://lwmd.livingwithmachines.ac.uk/file')
        <BLANKLINE>
        ...''...
        >>> path_or_str_suffix(Path('cat') / 'dog' / 'fish.csv')
        'csv'
        >>> path_or_str_suffix(Path('cat') / 'dog' / 'fish')
        ''

        ```
    """
    suffix: str = ""
    if isinstance(str_or_path, Path):
        if str_or_path.suffix:
            suffix = str_or_path.suffix[1:]  # Skip the `.` for consistency
        else:
            """"""
    else:
        split_str_or_path: list[str] = str(str_or_path).split(split_str)
        if len(split_str_or_path) > 1:
            suffix = split_str_or_path[-1]
            if "/" in suffix:
                logger.debug(
                    f"Split via {split_str} of "
                    f"{str_or_path} has a `/` `char`. "
                    "Returning ''",
                )
                return ""
        else:
            logger.debug(
                f"Can't split via {split_str} in "
                f"{_short_text_trunc(str(str_or_path))}",
            )
            return ""
    if len(suffix) > max_extension_len:
        if force:
            console.log(
                f"Force return of suffix {suffix}",
            )
            return suffix
        else:
            console.log(
                f"suffix {_short_text_trunc(suffix)} too long "
                f"(max={max_extension_len})",
            )
            return ""
    else:
        return suffix


def download_file(
    local_path: PathLike,
    url: str,
    force: bool = False,
) -> bool:
    """If `force` or not available, download `url` to `local_path`.

    Example:
        ```pycon
        >>> jpg_url: str = "https://commons.wikimedia.org/wiki/File:Wassily_Leontief_1973.jpg"
        >>> local_path: Path = Path('test.jpg')
        >>> local_path.unlink(missing_ok=True)  # Ensure png deleted
        >>> success: bool = download_file(local_path, jpg_url)
        <BLANKLINE>
        ...'test.jpg' not found, downloading ...wiki/File:Wassily_Leonti..._1973.jpg'...
        ...Saved to 'test.jpg'...
        >>> success
        True
        >>> local_path.unlink()  # Delete downloaded jpg

        ```
    """
    local_path = Path(local_path)
    if not validate_url(url):
        console.log(
            f"'{url}' is not a valid url",  # terminal_print=terminal_print, LEVEL=ERROR
        )
        return False
    if not local_path.exists() or force:
        if force:
            console.log(
                f"Overwriting '{local_path}' by downloading from '{url}'",
            )
        else:
            console.log(
                f"'{local_path}' not found, downloading from '{url}'",
            )
        try:
            with (
                urlopen(url) as response,
                open(str(local_path), "wb") as out_file,
            ):
                copyfileobj(response, out_file)
        except IsADirectoryError:
            console.log(
                f"'{local_path}' must be a file, not a directory",
            )
            return False
        except URLError:
            console.log(
                f"Download error (likely no internet connection): '{url}'",
            )
            return False
        else:
            console.log(f"Saved to '{local_path}'")
    if not local_path.is_file():
        console.log(
            f"'{local_path}' is not a file",
        )
        return False
    if not local_path.stat().st_size > 0:
        console.log(
            f"'{local_path}' from '{url}' is empty",
        )
        return False
    else:
        logger.debug(
            f"'{url}' file available from '{local_path}'",
        )
        return True


def app_data_path(app_name: str, data_path: PathLike = DEFAULT_APP_DATA_FOLDER) -> Path:
    """Return `app_name` data `Path` and ensure exists.

    Example:
        ```pycon
        >>> tmp_path: Path = getfixture("tmp_path")
        >>> chdir(tmp_path)
        >>> app_data_path('mitchells')
        PosixPath('mitchells/data')

        ```
    """
    path = Path(app_name) / Path(data_path)
    path.mkdir(exist_ok=True, parents=True)
    return path


class DataSourceDownloadError(Exception):
    ...


@dataclass
class DataSource:
    """Class to manage storing/deleting data files.

    Attr:
        file_name: Name of file (not local path).
        app: Name of app the file is for to generate a local path.
        url: Url to dowload file from.
        read_func: Function to call on downloaded file.
        description: Text descriping the data source.
        citation: An optional link (ideally DOI) for citation.
        license: License data is available through.
        _download_exception: Exception to raise if download fails.
        _str_truncation_length: Maximum lenght of `str` to use in
            print outs.

    Example:
        ```pycon
        >>> from os import chdir
        >>> tmp_path: Path = getfixture("tmp_path")
        >>> chdir(tmp_path)
        >>> from pandas import read_csv

        >>> rsd_1851: DataSource = DataSource(
        ...     file_name=demographics_1851_local_path.name,
        ...     app="census",
        ...     url="https://reshare.ukdataservice.ac.uk/853547/4/1851_RSD_data.csv",
        ...     read_func=read_csv,
        ...     description="Demographic and socio-economic variables for "
        ...                 "Registration Sub-Districts (RSDs) in England and Wales, "
        ...                 "1851",
        ...     citation="https://dx.doi.org/10.5255/UKDA-SN-853547",
        ...     license="http://creativecommons.org/licenses/by/4.0/",
        ... )
        >>> assert rsd_1851.local_path == demographics_1851_local_path
        >>> df = rsd_1851.read()
        [...]...'census/data/demographics_england_wales_2015.csv'
        ...not found...
        >>> df.columns[:5].tolist()
        ['CEN_1851', 'REGCNTY', 'REGDIST', 'SUBDIST', 'POP_DENS']
        >>> rsd_1851.delete()
        Deleting local copy of 'de...csv'...

        ```
    """

    file_name: PathLike | str
    app: str
    url: str
    read_func: Callable[[PathLike], DataFrame | Series]
    description: str | None = None
    citation: str | None = None
    license: str | None = None
    _download_exception: DataSourceDownloadError | None = None
    _str_truncation_length: int = 15

    def __str__(self) -> str:
        """Readable description of which `file_name` from which `app`."""
        return f"'{_short_text_trunc(str(self.file_name))}' " f"for `{self.app}`"

    def __repr__(self) -> str:
        """Detailed, truncated reprt of `file_name` for `app`."""
        return (
            f"{self.__class__.__name__}({self.app!r}, "
            f"'{_short_text_trunc(str(self.file_name))}')"
        )

    @property
    def url_suffix(self) -> str:
        """Return suffix of `self.url` or None if not found."""
        return path_or_str_suffix(self.url)

    @property
    def _trunc_url_str_suffix(self) -> str:
        """Return DEFAULT_TRUNCATION_CHARS + `self.url_suffix`."""
        return DEFAULT_TRUNCATION_CHARS + self.url_suffix

    def _file_name_truncated(self) -> str:
        """Return truncated `file_name` for logging."""
        return truncate_str(
            text=Path(self.file_name).suffix,
            max_length=self._str_truncation_length,
            trail_str=self._trunc_url_str_suffix,
        )

    @property
    def local_path(self) -> Path:
        """Return path to store `self.file_name`."""
        return app_data_path(self.app) / self.file_name

    @property
    def is_empty(self) -> bool:
        """Return if `Path` to store `self.file_name` has 0 file size."""
        return self.local_path.stat().st_size == 0

    @property
    def is_file(self) -> bool:
        """Return if `self.local_path` is a file."""
        return self.local_path.is_file()

    @property
    def is_local(self) -> bool:
        """Return if `self.url` is storred locally at `self.file_name`."""
        return self.is_file and not self.is_empty

    def download(self, force: bool = False) -> bool:
        """Download `self.url` to save locally at `self.file_name`."""
        if self.is_local and not force:
            console.log(f"{self} already downloaded " f"(add `force=True` to override)")
            return True
        else:
            return download_file(self.local_path, self.url)

    def delete(self) -> None:
        """Delete local save of `self.url` at `self.file_name`.

        Note:
            No error raised if missing.
        """
        if self.is_local:
            console.log(f"Deleting local copy of {self}.")
            self.local_path.unlink(missing_ok=True)
        else:
            console.info(f"'{self.local_path}' cannot be deleted (not saved locally)")

    def read(self, force: bool = False) -> DataFrame | Series:
        """Return data in `self.local_path` processed by `self.read_func`."""
        if not self.is_local:
            success: bool = self.download(force=force)
            if not success:
                self._download_exception = DataSourceDownloadError(
                    f"Failed to access {self} data from {self.url}"
                )
                logger.error(str(self._download_exception))
        assert self.is_local
        return self.read_func(self.local_path)


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
    p: str | Path,
    o: dict,
    add_created: bool = True,
    json_indent: int = JSON_INDENT,
    extra_dict_fields: dict = {},
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
        >>> extra_fields: dict[str, str] = getfixture('text_fixture_path_dict')
        >>> path: Path = tmp_path / 'test-write-json-example.json'
        >>>
        >>> write_json(p=path,
        ...            o=NEWSPAPER_COLLECTION_METADATA,
        ...            add_created=True, extra_dict_fields=extra_fields)
        >>> imported_fixture = load_json(path)
        >>> imported_fixture[1]['pk']
        2
        >>> imported_fixture[1]['fields'][DATA_PROVIDER_INDEX]
        'hmd'
        >>> imported_fixture[1]['fields']['text_fixture_path']
        'plaintext_fixture-000001.json'

        ```
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
                **extra_dict_fields,
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
        'bl-hmd'

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
        'bl-hmd'
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
    add_fixture_name: bool = False,
    fixture_name_field: str = "",
    extra_dict_fields: dict[str, Any] = {},
    json_indent: int = JSON_INDENT,
    file_name_0_padding: int = FILE_NAME_0_PADDING_DEFAULT,
) -> None:
    """Saves fixtures generated by a generator to separate JSON files.

    This function takes a generator and saves the generated fixtures to
    separate JSON files. The fixtures are saved in batches, where each batch
    is determined by the ``max_elements_per_file`` parameter.

    Args:
        generator: A generator that yields the fixtures to be saved.
        prefix: A string prefix to be added to the file names of the
            saved fixtures.
        output_path:
            Path to folder fixtures are saved to
        max_elements_per_file:
            Maximum `JSON` records saved in each file
        add_created:
            Whether to add `created_at` and `updated_at` `timestamps`
        add_fixture_name: If `fixture_name_field` is also set, add the
            fixture name as a field within `extra_dict_fields`
        fixture_name_field: If `add_fixture_name` is also set, the
            field name as a key to the fixture file name
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
        ...              prefix='test', output_path=tmp_path,
        ...              add_fixture_name=True, fixture_name_field='fixture_path')
        >>> imported_fixture = load_json(tmp_path / 'test-000001.json')
        >>> imported_fixture[1]['pk']
        2
        >>> imported_fixture[1]['fields'][DATA_PROVIDER_INDEX]
        'hmd'
        >>> 'created_at' in imported_fixture[1]['fields']
        True
        >>> imported_fixture[1]['fields']['fixture_path']
        'test-000001.json'

        ```

    """
    internal_counter: int = 1
    counter: int = 1
    lst: list[PathLike] = []
    file_name: str
    Path(output_path).mkdir(parents=True, exist_ok=True)
    for item in generator:
        lst.append(item)
        internal_counter += 1
        if internal_counter > max_elements_per_file:
            file_name: str = f"{prefix}-{str(counter).zfill(file_name_0_padding)}.json"
            if add_fixture_name and fixture_name_field:
                extra_dict_fields[fixture_name_field] = file_name
            write_json(
                p=Path(f"{output_path}/{file_name}"),
                o=lst,
                add_created=add_created,
                json_indent=json_indent,
                extra_dict_fields=extra_dict_fields,
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
        if add_fixture_name and fixture_name_field:
            extra_dict_fields[fixture_name_field] = file_name
        write_json(
            p=Path(f"{output_path}/{file_name}"),
            o=lst,
            add_created=add_created,
            json_indent=json_indent,
            extra_dict_fields=extra_dict_fields,
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
    df: DataFrame
    Path(output_path).mkdir(parents=True, exist_ok=True)
    for item in fixtures:
        lst.append(fixture_fields(item, as_dict=True))
        internal_counter += 1
        if internal_counter > max_elements_per_file:
            df = DataFrame.from_records(lst)

            file_name = f"{prefix}-{str(counter).zfill(file_name_0_padding)}.csv"
            df.to_csv(Path(output_path) / file_name, index=index)
            # Save up some memory
            del lst
            gc.collect()

            # Re-instantiate
            lst = []
            internal_counter = 1
            counter += 1
    else:
        df = DataFrame.from_records(lst)
        file_name = f"{prefix}-{str(counter).zfill(file_name_0_padding)}.csv"
        df.to_csv(Path(output_path) / file_name, index=index)


def export_fixtures(
    fixture_tables: dict[str, Sequence[FixtureDict]],
    path: str | PathLike = settings.FIXTURE_TABLES_OUTPUT,
    prefix: str = "test-",
    add_created: bool = True,
    add_fixutre_name: bool = False,
    fixture_name_field: str = "",
    formats: Sequence[EXPORT_FORMATS] = settings.FIXTURE_TABLES_FORMATS,
    file_name_0_padding: int = FILE_NAME_0_PADDING_DEFAULT,
) -> None:
    """Export `fixture_tables` in `formats`.

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
        >>> tmp_path = getfixture('tmp_path')
        >>> test_fixture_tables: dict[str, FixtureDict] = {
        ...     'test0': NEWSPAPER_COLLECTION_METADATA,
        ...     'test1': NEWSPAPER_COLLECTION_METADATA}
        >>> export_fixtures(test_fixture_tables, path=tmp_path / 'exports')
        <BLANKLINE>
        ...Warning: Saving test0...
        ...Warning: Saving test1...
        >>> from pandas import read_csv
        >>> fixture0_json = load_json(tmp_path / 'exports/test-test0-000001.json')
        >>> fixture0_df = read_csv(tmp_path / 'exports/test-test0-000001.csv')
        >>> fixture1_json = load_json(tmp_path / 'exports/test-test1-000001.json')
        >>> fixture1_df = read_csv(tmp_path / 'exports/test-test1-000001.csv')
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
    """Return a sorted `tuple` of `Path`s in `path` using `glob_regex_str`.

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
        >>> space_in_gb > 1  # Hopefully true when run...
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
    format: str | ArchiveFormatEnum = ZIP_FILE_EXTENSION,
    # base_dir: PathLike | None = None,
    force_overwrite: bool = False,
    dry_run: bool = False,
) -> Path:
    """Compress exported `fixtures` files using `make_archive`.

    Args:
        path:
            `Path` to file to compress

        output_path:
            Compressed file name (without extension specified from `format`).

        format:
            A `str` of one of the registered compression formats. By default
            `Python` provides `zip`, `tar`, `gztar`, `bztar`, and `xztar`.
            See `ArchiveFormatEnum` variable for options checked.

        suffix:
            `str` to add to comprssed file name saved.
            For example: if `path = plaintext_fixture-1.json` and
            `suffix=_compressed`, then the saved file might be called
            `plaintext_fixture-1_compressed.json.zip`

        force_overwrite:
            Force overwriting `output_path` if it already exists.

        dry_run:
            Attempt compression without modifying any files.

    Example:
        ```pycon
        >>> logger_initial_level: int = logger.level
        >>> logger.setLevel(logging.DEBUG)
        >>> plaintext_bl_lwm = getfixture('bl_lwm_plaintext_json_export')
        <BLANKLINE>
        ...
        >>> tmp_path = getfixture('tmp_path')
        >>> json_path: Path = next(plaintext_bl_lwm.exported_json_paths)
        >>> assert 'lwm_test_output' in str(json_path)
        >>> compressed_path: Path = compress_fixture(path=json_path,
        ...                                          output_path=tmp_path,
        ...                                          dry_run=True)
        <BLANKLINE>
        ...Compressing...'...01.json'...to...'zip'...
        >>> compressed_path.exists()
        False
        >>> compressed_path: Path = compress_fixture(path=json_path,
        ...                                          output_path=tmp_path,
        ...                                          dry_run=False)
        <BLANKLINE>
        ...creating...'...01.json.zip...'...adding...
        ...'plain...01.json'...to...it...
        >>> from zipfile import ZipFile, ZipInfo
        >>> zipfile_info_list: list[ZipInfo] = ZipFile(
        ...     tmp_path / 'plaintext_fixture-000001.json.zip'
        ... ).infolist()
        >>> len(zipfile_info_list)
        1
        >>> Path(zipfile_info_list[0].filename).name
        'plaintext_fixture-000001.json'
        >>> logger.setLevel(logger_initial_level)

        ```
    """
    path = Path(path)
    absolute_path = path.absolute()
    root_dir: str | None = None
    base_dir: str | None = None
    if not path.exists():
        raise ValueError(f"Cannot compress; 'path' does not exist: {path}")
    if isinstance(format, str):
        try:
            format = ArchiveFormatEnum(format)
        except ValueError:
            raise ValueError(
                f"format '{format}' not valid, "
                f"options are:'\n{pformat(ARCHIVE_FORMATS)}"
            )

    if absolute_path.is_file():
        root_dir = str(Path(path).parent)
        base_dir = path.name
    elif absolute_path.is_dir():
        root_dir = str(absolute_path)

    else:
        raise ValueError(
            f"Path must exist and be a file or folder. " f"Not valid: '{path}'"
        )

    save_file_name: Path = Path(Path(path).stem + suffix + "".join(Path(path).suffixes))
    save_path: Path = Path(output_path) / save_file_name
    if Path(str(save_path) + f".{format}").exists():
        error_message: str = f"Path to save to already exists: '{save_path}'"
        if force_overwrite:
            logger.warn(error_message)
            logger.warn(f"Overwriting '{save_path}'")
        else:
            raise ValueError(error_message)
    logger.info(f"Compressing '{path}' to '{format}' in: '{save_path.parent}'")

    archive_path: Path = Path(
        make_archive(
            base_name=str(save_path),
            format=str(format),
            root_dir=root_dir,
            base_dir=base_dir,
            dry_run=dry_run,
            logger=logger,
        )
    )

    return archive_path


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
        'bl_lwm/0003079-test_plaintext.zip'
        'bl_lwm/0003548-test_plaintext.zip'

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
        >>> logger.setLevel(WARNING)
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
        '...Standing...*...*...*...*...love.'
        >>> if is_platform_win:
        ...     pytest.skip('fails on certain Windows root paths: issue #56')
        >>> truncate_path_str(root_love_shadows,
        ...                   folder_filler_str="*", tail_parts=3)
        '...Standing...*...*...shadows...of...love.'

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
        original_path_parts: tuple[str, ...] = path.parts
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
        replaced_path_parts: tuple[str, ...] = tuple(
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


def int_from_str(
    s: str,
    index: int = -1,
    regex: str = PADDING_0_REGEX_DEFAULT,
) -> tuple[str, int]:
    """Return matched (or None) `regex` from `s` by index `index`.

    Params:
        s:
            `str` to match and via `regex`.

        index:
            Which index of number in `s` to pad with 0s.
            Like numbering a `list`, 0 indicates the first match
            and -1 indicates the last match.

        regex:
            Regular expression for matching numbers in `s` to pad.

    Example:
        ```pycon
        >>> int_from_str('a/path/to/fixture-03-05.txt')
        ('05', 5)
        >>> int_from_str('a/path/to/fixture-03-05.txt', index=0)
        ('03', 3)

        ```
    """
    matches: list[str] = [match for match in findall(regex, s) if match]
    match_str: str = matches[index]
    return match_str, int(match_str)


def rename_by_0_padding(
    file_path: PathLike,
    match_str: str | None = None,
    match_int: int | None = None,
    padding: int = FILE_NAME_0_PADDING_DEFAULT,
    replace_count: int = 1,
    exclude_parents: bool = True,
    reverse_int_match: bool = False,
) -> Path:
    """Return `file_path` with `0` `padding` `Path` change.

    Params:
        file_path:
            `PathLike` to rename.

        match_str:
            `str` to match and replace with padded `match_int`

        match_int:
            `int` to pad and replace `match_str`

        padding:
            How many digits (0s) to pad `match_int` with.

        exclude_parents:
            Only rename parts of `Path(file_path).name`; else
            replace across `Path(file_path).parents` as well.

        reverse_int_match:
            Whether to match from the end of the `file_path`.


    Example:
        ```pycon
        >>> rename_by_0_padding('a/path/to/3/fixture-03-05.txt',
        ...                     match_str='05', match_int=5)
        <BLANKLINE>
        ...Path('a/path/to/3/fixture-03-000005.txt')...
        >>> rename_by_0_padding('a/path/to/3/fixture-03-05.txt',
        ...                     match_str='03')
        <BLANKLINE>
        ...Path('a/path/to/3/fixture-000003-05.txt')...
        >>> rename_by_0_padding('a/path/to/3/fixture-03-05.txt',
        ...                     match_str='05', padding=0)
        <BLANKLINE>
        ...Path('a/path/to/3/fixture-03-5.txt')...
        >>> rename_by_0_padding('a/path/to/3/fixture-03-05.txt',
        ...                     match_int=3)
        <BLANKLINE>
        ...Path('a/path/to/3/fixture-0000003-05.txt')...
        >>> rename_by_0_padding('a/path/to/3/f-03-05-0003.txt',
        ...                     match_int=3, padding=2,
        ...                     exclude_parents=False)
        <BLANKLINE>
        ...Path('a/path/to/03/f-03-05-0003.txt')...
        >>> rename_by_0_padding('a/path/to/3/f-03-05-0003.txt',
        ...                     match_int=3, padding=2,
        ...                     exclude_parents=False,
        ...                     replace_count=3, )
        <BLANKLINE>
        ...Path('a/path/to/03/f-003-05-00003.txt')...

        ```
    """
    if match_int is None and match_str in (None, ""):
        raise ValueError(f"At least `match_int` or `match_str` required; both None.")
    elif match_str and not match_int:
        match_int = int(match_str)
    elif match_int is not None and not match_str:
        assert str(match_int) in str(file_path)
        match_str = int_from_str(
            str(file_path),
            index=-1 if reverse_int_match else 0,
        )[0]
    assert match_int is not None and match_str is not None
    if exclude_parents:
        return Path(file_path).parent / Path(file_path).name.replace(
            match_str, str(match_int).zfill(padding), replace_count
        )
    else:
        return Path(
            str(file_path).replace(
                match_str, str(match_int).zfill(padding), replace_count
            )
        )


def glob_path_rename_by_0_padding(
    path: PathLike,
    output_path: PathLike | None = None,
    glob_regex_str: str = "*",
    padding: int | None = 0,
    match_int_regex: str = PADDING_0_REGEX_DEFAULT,
    index: int = -1,
) -> dict[PathLike, PathLike]:
    """Return an `OrderedDict` of replacement 0-padded file names from `path`.

    Params:
        path:
            `PathLike` to source files to rename.

        output_path:
            `PathLike` to save renamed files to.

        glob_regex_str:
            `str` to match files to rename within `path`.

        padding:
            How many digits (0s) to pad `match_int` with.

        match_int_regex:
            Regular expression for matching numbers in `s` to pad.
            Only rename parts of `Path(file_path).name`; else
            replace across `Path(file_path).parents` as well.

        index:
            Which index of number in `s` to pad with 0s.
            Like numbering a `list`, 0 indicates the first match
            and -1 indicates the last match.

    Example:
        ```pycon
        >>> tmp_path: Path = getfixture('tmp_path')
        >>> for i in range(4):
        ...     (tmp_path / f'test_file-{i}.txt').touch(exist_ok=True)
        >>> pprint(sorted(tmp_path.iterdir()))
        [...Path('...test_file-0.txt'),
         ...Path('...test_file-1.txt'),
         ...Path('...test_file-2.txt'),
         ...Path('...test_file-3.txt')]
        >>> pprint(glob_path_rename_by_0_padding(tmp_path))
        {...Path('...test_file-0.txt'): ...Path('...test_file-00.txt'),
         ...Path('...test_file-1.txt'): ...Path('...test_file-01.txt'),
         ...Path('...test_file-2.txt'): ...Path('...test_file-02.txt'),
         ...Path('...test_file-3.txt'): ...Path('...test_file-03.txt')}

        ```

    """
    try:
        assert Path(path).exists()
    except AssertionError:
        raise ValueError(f'path does not exist: "{Path(path)}"')
    paths_tuple: tuple[PathLike, ...] = path_globs_to_tuple(path, glob_regex_str)
    try:
        assert paths_tuple
    except AssertionError:
        raise FileNotFoundError(
            f"No files found matching 'glob_regex_str': "
            f"'{glob_regex_str}' in: '{path}'"
        )
    paths_to_index: tuple[tuple[str, int], ...] = tuple(
        int_from_str(str(matched_path), index=index, regex=match_int_regex)
        for matched_path in paths_tuple
    )
    max_index: int = max(index[1] for index in paths_to_index)
    max_index_digits: int = len(str(max_index))
    if not padding or padding < max_index_digits:
        padding = max_index_digits + 1
    new_names_dict: dict[PathLike, PathLike] = {}
    if output_path:
        if not Path(output_path).is_absolute():
            output_path = Path(path) / output_path
        logger.debug(f"Specified '{output_path}' for saving file copies")
    for i, old_path in enumerate(paths_tuple):
        match_str, match_int = paths_to_index[i]
        new_names_dict[old_path] = rename_by_0_padding(
            old_path, match_str=str(match_str), match_int=match_int, padding=padding
        )
        if output_path:
            new_names_dict[old_path] = (
                Path(output_path) / Path(new_names_dict[old_path]).name
            )
    return new_names_dict


def copy_dict_paths(copy_path_dict: dict[PathLike, PathLike]) -> None:
    """Copy files from `copy_path_dict` `keys` to `values`.

    Example:
        ```pycon
        >>> tmp_path: Path = getfixture('tmp_path')
        >>> test_files_path: Path = (tmp_path / 'copy_dict')
        >>> test_files_path.mkdir(exist_ok=True)
        >>> for i in range(4):
        ...     (test_files_path / f'test_file-{i}.txt').touch(exist_ok=True)
        >>> pprint(sorted(test_files_path.iterdir()))
        [...Path('...file-0.txt'),
         ...Path('...file-1.txt'),
         ...Path('...file-2.txt'),
         ...Path('...file-3.txt')]
        >>> output_path = test_files_path / 'save'
        >>> output_path.mkdir(exist_ok=True)
        >>> logger_initial_level: int = logger.level
        >>> logger.setLevel(logging.DEBUG)
        >>> copy_dict_paths(
        ...     glob_path_rename_by_0_padding(test_files_path,
        ...                                   glob_regex_str="*.txt",
        ...                                   output_path=output_path))
        <BLANKLINE>
        ...Specified...'...'...for...saving...file...copies...
        ...'...-0...txt'...to...'...-00...txt...'...
        ...'...-1...txt'...to...'...-01...txt...'
        ...'...-2...txt'...to...'...-02...txt...'
        ...'...-3...txt'...to...'...-03...txt...'
        >>> pprint(sorted(test_files_path.iterdir()))
        [...Path('...save'),
         ...Path('...test_file-0.txt'),
         ...Path('...test_file-1.txt'),
         ...Path('...test_file-2.txt'),
         ...Path('...test_file-3.txt')]
        >>> pprint(sorted((test_files_path / 'save').iterdir()))
         [...Path('...test_file-00.txt'),
          ...Path('...test_file-01.txt'),
          ...Path('...test_file-02.txt'),
          ...Path('...test_file-03.txt')]
        >>> logger.setLevel(logger_initial_level)

        ```
    """
    for current_path, copy_path in copy_path_dict.items():
        logger.info(f"Copying '{current_path}' to '{copy_path}'")
        Path(copy_path).parent.mkdir(exist_ok=True)
        copyfile(current_path, copy_path)


def dirs_in_path(path: PathLike) -> Generator[Path, None, None]:
    """Yield all folder paths (not recursively) in `path`.

    Args:
        path: `Path` to count subfolders in.

    Yields:
        Each folder one walk length in `path`

    Example:
        ```pycon
        >>> tmp_path = getfixture('tmp_path')
        >>> len(tuple(dir.name for dir in dirs_in_path(tmp_path)))
        0
        >>> (tmp_path / 'a_file_not_dir').touch()
        >>> len(tuple(dir.name for dir in dirs_in_path(tmp_path)))
        0
        >>> (tmp_path / 'test_dir').mkdir()
        >>> tuple(dir.name for dir in dirs_in_path(tmp_path))
        ('test_dir',)
        >>> [(tmp_path / f'new_dir_{i}').mkdir() for i in range(3)]
        [None, None, None]
        >>> tuple(dir.name for dir in dirs_in_path(tmp_path))
        ('new_dir_1', 'new_dir_0', 'test_dir', 'new_dir_2')
        >>> (tmp_path / 'test_dir' / 'another_dir').mkdir()
        >>> tuple(dir.name for dir in dirs_in_path(tmp_path))
        ('new_dir_1', 'new_dir_0', 'test_dir', 'new_dir_2')

        ```
    """
    for sub_path in Path(path).iterdir():
        if sub_path.is_dir():
            yield sub_path


def files_in_path(path: PathLike) -> Generator[Path, None, None]:
    """Yield all file paths (not recursively) in `path`.

    Args:
        path: `Path` to count files in.

    Yields:
        Each file one walk length in `path`

    Example:
        ```pycon
        >>> tmp_path = getfixture('tmp_path')
        >>> len(tuple(files_in_path(tmp_path)))
        0
        >>> (tmp_path / 'a_file_not_dir').touch()
        >>> tuple(file.name for file in files_in_path(tmp_path))
        ('a_file_not_dir',)
        >>> (tmp_path / 'test_dir').mkdir()
        >>> tuple(file.name for file in files_in_path(tmp_path))
        ('a_file_not_dir',)
        >>> [(tmp_path / f'new_dir_{i}').mkdir() for i in range(3)]
        [None, None, None]
        >>> tuple(file.name for file in files_in_path(tmp_path))
        ('a_file_not_dir',)
        >>> (tmp_path / 'test_dir' / 'another_dir').mkdir()
        >>> (tmp_path / 'test_dir' / 'another_folder_file').touch()
        >>> tuple(file.name for file in files_in_path(tmp_path))
        ('a_file_not_dir',)
        >>> (tmp_path / 'another_file_not_dir').touch()
        >>> tuple(file.name for file in files_in_path(tmp_path))
        ('a_file_not_dir', 'another_file_not_dir')

        ```
    """
    for sub_path in Path(path).iterdir():
        if sub_path.is_file():
            yield sub_path


def file_path_to_item_code(
    path: PathLike,
    separation_char: str = CODE_SEPERATOR_CHAR,
    file_name_separtion_char: str = FILE_NAME_SEPERATOR_CHAR,
) -> str:
    """Extract `lwmdb.newspapers.Item.item_code` from `path`.

    Example:
        ```pycon
        >>> file_path_to_item_code('0003548/1904/0707/0003548_19040707_art0037.txt')
        '0003548-19040707-art0037'

        ```
    """
    return Path(path).stem.replace(file_name_separtion_char, separation_char)
