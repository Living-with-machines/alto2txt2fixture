from collections import OrderedDict
from dataclasses import dataclass, field
from logging import getLogger
from os import PathLike
from pathlib import Path
from shutil import disk_usage, rmtree, unpack_archive
from typing import Final, Generator, TypedDict
from zipfile import ZipFile, ZipInfo

from tqdm.rich import tqdm

from .settings import NEWSPAPER_DATA_PROVIDER_CODE_DICT
from .types import (
    DataProviderFixtureDict,
    PlaintextFixtureDict,
    PlaintextFixtureFieldsDict,
)
from .utils import (
    DiskUsageTuple,
    console,
    free_hd_space_in_GB,
    path_globs_to_tuple,
    save_fixture,
    valid_compression_files,
)

logger = getLogger("rich")

FULLTEXT_DJANGO_MODEL: Final[str] = "fulltext.fulltext"

DEFAULT_EXTRACTED_SUBDIR: Final[PathLike] = Path("extracted")

FULLTEXT_FILE_NAME_SUFFIX: Final[str] = "_plaintext"
ZIP_FILE_EXTENSION: Final[str] = "zip"
FULLTEXT_DEFAULT_PLAINTEXT_ZIP_GLOB_REGEX: Final[
    str
] = f"*{FULLTEXT_FILE_NAME_SUFFIX}.{ZIP_FILE_EXTENSION}"
TXT_FIXTURE_FILE_EXTENSION: Final[str] = "txt"
TXT_FIXTURE_FILE_GLOB_REGEX: Final[str] = f"**/*.{TXT_FIXTURE_FILE_EXTENSION}"
DEFAULT_MAX_PLAINTEXT_PER_FIXTURE_FILE: Final[int] = 2000
DEFAULT_PLAINTEXT_FILE_NAME_PREFIX: Final[str] = "plaintext_fixture"
DEFAULT_PLAINTEXT_FIXTURE_OUTPUT: Final[PathLike] = Path("output") / "plaintext"
DEFAULT_INITIAL_PK: int = 1

SAS_ENV_VARIABLE = "FULLTEXT_SAS_TOKEN"


class FulltextPathDict(TypedDict):
    """A `dict` for storing fixture paths and primary key.

    Attributes:
        path:
            Plaintext file path.
        compressed_path:
            If `path` is within a compressed file,
            `compressed_path` is that source. Else None.
        primary_key:
            An `int >= 1` for a `SQL` table primary key (`pk`).
    """

    path: PathLike
    compressed_path: PathLike | None
    primary_key: int


@dataclass
class PlainTextFixture:

    """Convert `plaintext` results from `alto2txt` into `json` fixtures.

    Attributes:
        path:
            PathLike source for fixtures as either a folder or file.

        data_provider_code:
            A short string to uniquely identify `DataProviders`,
            primarily to match sources in `self.data_provider_code_dict`.

        files:
            A iterable `PathLike` collection of to either read as
            plaintext or decomepress to extract plaintext.

        compressed_glob_regex:
            A Regular Expression to filter plaintext files from uncompressed
            `self.files`, more specifically `self.compressed_files`.

        data_provider:
            If available a `DataProviderFixtureDict` for `DataProvider` metadata.
            By default all options are stored in `self.data_provider_code_dict`.

        model_str:
            The name of the `lwmdb` `django` model the fixture is for. This is of
            the form `app_name.model_name`. Following the default config for `lwmdb`:
            ```python
            FULLTEXT_DJANGO_MODEL: Final[str] = "fulltext.fulltext"
            ```
            the `fulltext` app has a `fulltext` `model` `class` specified in
            `lwmdb.fulltext.models.fulltext`. A `sql` table is generated from
            on that `fulltext` `class` and the `json` `fixture` structure generated
            from this class is where records will be stored.

        extract_subdir:
            Folder to extract `self.compressed_files` to.

        plaintext_extension:
            What file extension to use to filter `plaintext` files.

        data_provider_code_dict:
            A `dict` of metadata for preconfigured `DataProvider` records in `lwmdb`.

        max_plaintext_per_fixture_file:
            A maximum number of fixtures per fixture file, designed to configure
            chunking fixtures.

        saved_fixture_prefix:
            A `str` to prefix all saved `json` fixture filenames.

        export_directory:
            Directory to save all exported fixtures to.

        initial_pk:
            Default begins at 1, can be set to another number if needed to
            add to add more to pre-existing set of records up to a given `pk`

        _disk_usage:
            Available harddrive space. Designed to help mitigate decompressing too
            many files for available disk space.

        self._uncompressed_source_file_dict:
            A dictionary of extracted plaintext to compressed source file. This is
            a field in `json` fixture records.

    Example:
        ```pycon
        >>> from pprint import pprint
        >>> plaintext_bl_lwm = PlainTextFixture(
        ...     data_provider_code='bl_lwm',
        ...     path='tests/bl_lwm',
        ...     compressed_glob_regex="*_plaintext.zip",
        ...     )
        >>> plaintext_bl_lwm
        <PlainTextFixture(path='tests/bl_lwm')>
        >>> str(plaintext_bl_lwm)
        "PlainTextFixture for 2 'bl_lwm' files"
        >>> plaintext_bl_lwm.free_hd_space_in_GB > 1
        True
        >>> pprint(plaintext_bl_lwm.compressed_files)
        (PosixPath('tests/bl_lwm/0003079-test_plaintext.zip'),
         PosixPath('tests/bl_lwm/0003548-test_plaintext.zip'))
        >>> plaintext_bl_lwm.extract_compressed()
        [...] Extract path:...tests/bl_lwm/extracted...
        ...Extracting:...tests/bl_lwm/0003079-test_plaintext.zip ...
        ...Extracting:...tests/bl_lwm/0003548-test_plaintext.zip ...
        ...%...[...]
        >>> plaintext_bl_lwm.delete_decompressed()
        Deleteing all files in: tests/bl_lwm/extracted

        ```
        tests/bl_lwm/0003079-test_plaintext.zip
    Todo:
        Work through lines below to conclude `doctest`

        ```python
        plain_text_hmd.newspaper_publication_paths
        plain_text_hmd.issues_paths
        plain_text_hmd.items_paths
        plain_text_hmd.summary
        plain_text_hmd.output_paths
        plain_text_hmd.export_to_json()
        plain_text_hmd.output_paths
        plain_text_hmd.compress_json()
        ```
    """

    path: PathLike
    data_provider_code: str | None = None
    files: tuple[PathLike, ...] | None = None
    compressed_glob_regex: str = FULLTEXT_DEFAULT_PLAINTEXT_ZIP_GLOB_REGEX
    data_provider: DataProviderFixtureDict | None = None
    model_str: str = FULLTEXT_DJANGO_MODEL
    extract_subdir: PathLike = DEFAULT_EXTRACTED_SUBDIR
    plaintext_extension: str = TXT_FIXTURE_FILE_EXTENSION
    plaintext_glob_regex: str = TXT_FIXTURE_FILE_GLOB_REGEX
    data_provider_code_dict: dict[str, DataProviderFixtureDict] = field(
        default_factory=lambda: NEWSPAPER_DATA_PROVIDER_CODE_DICT
    )
    initial_pk: int = 1
    max_plaintext_per_fixture_file: int = DEFAULT_MAX_PLAINTEXT_PER_FIXTURE_FILE
    saved_fixture_prefix: str = DEFAULT_PLAINTEXT_FILE_NAME_PREFIX
    export_directory: PathLike = DEFAULT_PLAINTEXT_FIXTURE_OUTPUT

    def __post_init__(self) -> None:
        """Manage populating additional attributes if necessary."""
        self._check_and_set_files_attr(force=True)
        self._check_and_set_data_provider(force=True)
        self._disk_usage: DiskUsageTuple = disk_usage(self.path)
        self._uncompressed_source_file_dict: OrderedDict[
            PathLike, PathLike
        ] = OrderedDict()
        self._pk_plaintext_dict: OrderedDict[PathLike, int] = OrderedDict()

    def __len__(self) -> int:
        """Return the number of files to process."""
        return len(self.files) if self.files else 0

    def __str__(self) -> str:
        """Return summary with `DataProvider` if available."""
        return (
            f"{type(self).__name__} "
            f"for {len(self)} "
            f"{self._data_provider_name_quoted_with_trailing_space}files"
        )

    def __repr__(self) -> str:
        """Return summary with `DataProvider` if available."""
        return f"<{type(self).__name__}(path='{self.path}')>"

    @property
    def _data_provider_name_quoted_with_trailing_space(self) -> str | None:
        """Return `self.data_provider` `code` attributre with trailing space or `None`."""
        return f"'{self.data_provider_name}' " if self.data_provider_name else None

    @property
    def data_provider_name(self) -> str | None:
        """Return `self.data_provider` `code` attributre or `None`.

        Todo:
            * Add check without risk of recursion for `self.data_provider_code`
        """
        return self.data_provider_code if self.data_provider_code else None

    @property
    def free_hd_space_in_GB(self) -> float:
        """Return remaing hard drive space estimate in gigabytes."""
        return free_hd_space_in_GB(self._disk_usage)

    def _set_and_check_path_is_file(self, force: bool = False) -> None:
        """Test if `self.path` is a file and change if `force = True`."""
        assert Path(self.path).is_file()
        file_path_tuple: tuple[PathLike, ...] = (self.path,)
        if not self.files:
            self.files = file_path_tuple
        elif self.files == file_path_tuple:
            logger.debug(
                f"No change from running " f"{repr(self)}._set_and_check_path_is_file()"
            )
        elif force:
            self.files = file_path_tuple
            logger.debug(f"Force change to {repr(self)}\n" f"`files`: {self.files}")
        else:
            raise ValueError(
                f"{repr(self)} `path` inconsistent with `files`.\n"
                f"`path`: {self.path}\n`files`: {self.files}"
            )

    def _set_and_check_path_is_dir(self, force: bool = False) -> None:
        """Test if `self.path` is a path and change if `force = True`."""
        assert Path(self.path).is_dir()
        file_paths_tuple: tuple[PathLike, ...] = tuple(
            sorted(path_globs_to_tuple(self.path, self.compressed_glob_regex))
        )
        if self.files:
            if self.files == file_paths_tuple:
                logger.debug(
                    f"No changes from " f"{repr(self)}._set_and_check_path_is_dir()"
                )
                return
            if force:
                logger.debug(
                    f"Forcing change to {repr(self)}:\n"
                    f"`compressed_glob_regex`: {self.compressed_glob_regex}\n"
                    f"`files`: {self.files}\n"
                    f"`path`: {self.path}\n `files`: {self.files}"
                )
            else:
                raise ValueError(
                    f"{repr(self)} `path` inconsistent with `files`.\n"
                    f"`compressed_glob_regex`: {self.compressed_glob_regex}\n"
                    f"`path`: {self.path}\n`files`: {self.files}"
                )
        self.files = file_paths_tuple

    @property
    def extract_path(self) -> Path:
        """Path any compressed files would be extracted to."""
        if Path(self.path).is_file():
            return Path(self.path).parent / self.extract_subdir
        elif Path(self.path).is_dir():
            return Path(self.path) / self.extract_subdir
        else:
            raise ValueError(
                f"`extract_path` only valid if `self.path` is a "
                f"`file` or `dir`: {self.path}"
            )

    @property
    def compressed_files(self) -> tuple[PathLike, ...]:
        """Return a tuple of all `self.files` with known archive filenames."""
        return (
            tuple(sorted(valid_compression_files(files=self.files)))
            if self.files
            else ()
        )

    @property
    def plaintext_provided_uncompressed(self) -> tuple[PathLike, ...]:
        """Return a tuple of all `self.files` with `self.plaintext_extension`."""
        if self.files:
            return tuple(
                sorted(
                    file
                    for file in self.files
                    if Path(file).suffix == self.plaintext_extension
                )
            )
        else:
            return ()

    @property
    def zipinfo(self) -> Generator[list[ZipInfo], None, None]:
        """If `self.compressed_files` is in `zip`, return info, else None.

        Example:
            ```pycon
            >>> plaintext_bl_lwm = getfixture('bl_lwm_plaintext')
            >>> zipfile_info_list = list(plaintext_bl_lwm.zipinfo)
            Getting zipfile info from <PlainTextFixture(path='tests/bl_lwm')>
            >>> zipfile_info_list[0][-1].filename
            '0003079/1898/0204/0003079_18980204_sect0001.txt'
            >>> zipfile_info_list[-1][-1].filename
            '0003548/1904/0707/0003548_19040707_art0059.txt'
            >>> zipfile_info_list[0][-1].file_size
            70192
            >>> zipfile_info_list[0][-1].compress_size
            39911

            ```
        """
        if any(Path(file).suffix == ".zip" for file in self.compressed_files):
            console.print(f"Getting zipfile info from {repr(self)}")
            for compressed_file in self.compressed_files:
                if Path(compressed_file).suffix == f".{ZIP_FILE_EXTENSION}":
                    yield ZipFile(compressed_file).infolist()
        else:
            console.log(f"No `self.compressed_files` end with `.zip` for {repr(self)}.")

    # def extract_compressed(self, index: int | str | None = None) -> None:
    def extract_compressed(self) -> None:
        """Extract `self.compressed_files` to `self.extracted_subdir_name`.

        Example:
            ```pycon
            >>> plaintext_bl_lwm = getfixture('bl_lwm_plaintext')
            >>> plaintext_bl_lwm.extract_compressed()
            <BLANKLINE>
            ...Extract path:...tests/bl_lwm/extracted...
            >>> plaintext_bl_lwm._uncompressed_source_file_dict[
            ...     Path('tests/bl_lwm/extracted/0003079/1898/'
            ...          '0204/0003079_18980204_sect0001.txt')
            ...     ]
            PosixPath('tests/bl_lwm/0003079-test_plaintext.zip')
            >>> plaintext_bl_lwm.delete_decompressed()
            Deleteing all files in: tests/bl_lwm/extracted

            ```

        """
        self.extract_path.mkdir(parents=True, exist_ok=True)
        console.log(f"Extract path: {self.extract_path}")
        for compressed_file in tqdm(
            self.compressed_files,
            total=len(self.compressed_files),
        ):
            logger.info(f"Extracting: {compressed_file} ...")
            unpack_archive(compressed_file, self.extract_path)
            for path in sorted(self.extract_path.glob(self.plaintext_glob_regex)):
                if path not in self._uncompressed_source_file_dict:
                    self._uncompressed_source_file_dict[path] = compressed_file

    def plaintext_paths(
        self, reset_cache=False
    ) -> Generator[FulltextPathDict, None, None]:
        """Return a generator of all `plaintext` files for potential fixtures.

        Example:
            ```pycon
            >>> plaintext_bl_lwm = getfixture('bl_lwm_plaintext_extracted')
            <BLANKLINE>
            ...Extract path:...tests/bl_lwm/extracted...
            >>> plaintext_paths = plaintext_bl_lwm.plaintext_paths()
            >>> first_path_fixture_dict = next(iter(plaintext_paths))
            >>> first_path_fixture_dict['path'].name
            '0003079_18980107_art0001.txt'
            >>> first_path_fixture_dict['compressed_path'].name
            '0003079-test_plaintext.zip'
            >>> len(plaintext_bl_lwm._pk_plaintext_dict)
            1
            >>> plaintext_bl_lwm._pk_plaintext_dict[
            ...     first_path_fixture_dict['path']
            ... ] # This demonstrates the `pk` begins from 1 following `SQL` standards
            1

            ```
        """
        if self.compressed_files and not self.extract_path.exists():
            console.print(
                "Compressed files not yet extracted. Try `extract_compression()`."
            )
        else:
            i: int = 0
            pk: int
            if self._uncompressed_source_file_dict:
                for i, uncompressed_tuple in enumerate(
                    tqdm(
                        self._uncompressed_source_file_dict.items(),
                        desc="Compressed configs  :",
                        total=len(self._uncompressed_source_file_dict),
                    )
                ):
                    pk = i + self.initial_pk  # Most `SQL` `pk` begins at 1
                    self._pk_plaintext_dict[uncompressed_tuple[0]] = pk
                    yield FulltextPathDict(
                        path=uncompressed_tuple[0],
                        compressed_path=uncompressed_tuple[1],
                        primary_key=pk,
                    )
            if self.plaintext_provided_uncompressed:
                for j, path in enumerate(
                    tqdm(
                        self.plaintext_provided_uncompressed,
                        desc="Uncompressed configs:",
                        total=len(self.plaintext_provided_uncompressed),
                    )
                ):
                    pk = j + i + self.initial_pk
                    self._pk_plaintext_dict[path] = pk
                    yield FulltextPathDict(
                        path=path, compressed_path=None, primary_key=pk
                    )

    def plaintext_paths_to_dicts(self) -> Generator[PlaintextFixtureDict, None, None]:
        """Generate fixture dicts from `self.plaintext_paths`.

        Example:
            ```pycon
            >>> plaintext_bl_lwm = getfixture('bl_lwm_plaintext_extracted')
            <BLANKLINE>
            ...Extract path:...tests/bl_lwm/extracted...
            >>> paths_dict = list(plaintext_bl_lwm.plaintext_paths_to_dicts())
            Compressed configs  :...%.../...[ ... it/s ]
            >>> plaintext_bl_lwm.delete_decompressed()
            Deleteing all files in: tests/.../extracted

            ```
        """
        text: str
        error_str: str | None = None
        for plaintext_path_dict in self.plaintext_paths():
            error_str = None
            try:
                text = Path(plaintext_path_dict["path"]).read_text()
            except UnicodeDecodeError as err:
                logger.warning(err)
                text = ""
                error_str = str(err)
            fields: PlaintextFixtureFieldsDict = PlaintextFixtureFieldsDict(
                text=text,
                path=str(plaintext_path_dict["path"]),
                compressed_path=str(plaintext_path_dict["compressed_path"]),
                errors=error_str,
            )
            yield PlaintextFixtureDict(
                model=self.model_str,
                fields=fields,
                pk=plaintext_path_dict["primary_key"],
            )

    def export_to_json_fixtures(
        self, output_path: PathLike | None = None, prefix: str | None = None
    ) -> None:
        """Iterate over `self.plaintext_paths` exporting to `json` `django` fixtures.

        Args:
            output_path:
                Folder to save all `json` fixtures in.
            prefix:
                Any `str` prefix for saved fixture files.

        Example:
            ```pycon
            >>> tmpdir: Path = getfixture("tmpdir")
            >>> first_lwm_plaintext_json_dict: PlaintextFixtureDict = (
            ...     getfixture("first_lwm_plaintext_json_dict")
            ... )
            >>> plaintext_bl_lwm = getfixture('bl_lwm_plaintext_extracted')
            <BLANKLINE>
            ...Extract path:...tests/bl_lwm/extracted...
            >>> plaintext_bl_lwm.export_to_json_fixtures(output_path=tmpdir)
            <BLANKLINE>
            Compressed configs...%...[...]
            >>> import json
            >>> exported_json = json.load(tmpdir/'plaintext_fixture-1.json')
            >>> exported_json[0]['pk'] == first_lwm_plaintext_json_dict['pk']
            True
            >>> exported_json[0]['model'] == first_lwm_plaintext_json_dict['model']
            True
            >>> (exported_json[0]['fields']['text'] ==
            ...  first_lwm_plaintext_json_dict['fields']['text'])
            True
            >>> (exported_json[0]['fields']['path'] ==
            ...  first_lwm_plaintext_json_dict['fields']['path'])
            True
            >>> (exported_json[0]['fields']['compressed_path'] ==
            ...  first_lwm_plaintext_json_dict['fields']['compressed_path'])
            True
            >>> exported_json[0]['fields']['created_at']
            '20...'
            >>> (exported_json[0]['fields']['updated_at'] ==
            ...  exported_json[0]['fields']['updated_at'])
            True

            ```

        """
        output_path = self.export_directory if not output_path else output_path
        prefix = self.saved_fixture_prefix if not prefix else prefix
        save_fixture(
            self.plaintext_paths_to_dicts(),
            prefix=prefix,
            output_path=output_path,
            add_created=True,
        )

    # def delete_compressed(self, index: int | str | None = None) -> None:
    def delete_decompressed(self, ignore_errors: bool = True) -> None:
        """Remove all uncompressed files."""
        console.print(f"Deleteing all files in: {self.extract_path}")
        rmtree(self.extract_path, ignore_errors=ignore_errors)

    def _check_and_set_files_attr(self, force: bool = False) -> None:
        """Check and populate attributes from `self.path` and `self.files`.

        If `self.path` is a file, ensure `self.files = (self.path,)` and
        raise a ValueError if not.

        If `self.path` is a directory, then collect all `tuple` of `Paths`
        in that directory matching `self.compressed_glob_regex`, or
        just all files if `self.compressed_glob_regex` is `None`.
        If `self.files` is set check if that `tuple`

        Example:
            ```pycon
            >>> plaintext_lwm = getfixture('bl_lwm_plaintext')
            >>> len(plaintext_lwm)
            2
            >>> plaintext_lwm._check_and_set_files_attr()
            <BLANKLINE>
            ...DEBUG...No changes from...
            ...<PlainText..._set...dir...
            >>> plaintext_lwm.path = (
            ...    'tests/bl_lwm/0003079-test_plaintext.zip')
            >>> plaintext_lwm._check_and_set_files_attr()
            Traceback (most recent call last):
                ...
            ValueError:...`path` inconsistent with `files`...
            >>> len(plaintext_lwm)
            2
            >>> plaintext_lwm._check_and_set_files_attr(force=True)
            DEBUG...Force change to...<PlainText...`files`...zip...
            >>> plaintext_lwm.files
            ('tests/bl_lwm/0003079-test_plaintext.zip',)
            >>> len(plaintext_lwm)
            1

            ```
        """
        if Path(self.path).is_file():
            self._set_and_check_path_is_file(force=force)
        elif Path(self.path).is_dir():
            self._set_and_check_path_is_dir(force=force)
        else:
            raise ValueError(
                f"`self.path` must be a file or directory. " f"Currently: {self.path}"
            )

    def _check_and_set_data_provider(self, force: bool = False) -> None:
        """Set `self.data_provider` and check `self.data_provider_code`."""
        if self.data_provider_code:
            if self.data_provider:
                if self.data_provider["fields"]["code"] != self.data_provider_code:
                    raise ValueError(
                        f"`self.data_provider_code` {self.data_provider_code} "
                        f"!= {self.data_provider} (`self.data_provider`)."
                    )
                else:
                    logger.debug(
                        f"{repr(self)} `self.data_provider['fields']['code']` "
                        f"== `self.data_provider_code`"
                    )
            else:
                if self.data_provider_code in self.data_provider_code_dict:
                    self.data_provider = self.data_provider_code_dict[
                        self.data_provider_code
                    ]
                else:
                    raise ValueError(
                        f"`self.data_provider_code` {self.data_provider_code} "
                        f"not included in `self.data_provider_code_dict`."
                        f"Available `codes`: {self.data_provider_code_dict.keys()}"
                    )
        elif self.data_provider:
            self.data_provider_code = self.data_provider["fields"]["code"]
        else:
            logger.debug(
                f"Neither `self.data_provider` nor "
                f"`self.data_provider_code` provided; both are `None` for {repr(self)}"
            )
