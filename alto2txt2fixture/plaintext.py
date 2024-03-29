from collections import OrderedDict
from dataclasses import dataclass, field
from logging import getLogger
from os import PathLike
from pathlib import Path
from pprint import pformat
from shutil import disk_usage, rmtree, unpack_archive
from typing import Final, Generator, TypedDict
from zipfile import ZipFile, ZipInfo

from rich.table import Table
from tqdm.rich import tqdm

from .settings import NEWSPAPER_DATA_PROVIDER_CODE_DICT
from .types import (
    DataProviderFixtureDict,
    PlaintextFixtureDict,
    PlaintextFixtureFieldsDict,
)
from .utils import (
    COMPRESSED_PATH_DEFAULT,
    COMPRESSION_TYPE_DEFAULT,
    FILE_NAME_0_PADDING_DEFAULT,
    TRUNC_HEADS_PATH_DEFAULT,
    TRUNC_TAILS_PATH_DEFAULT,
    ZIP_FILE_EXTENSION,
    ArchiveFormatEnum,
    DiskUsageTuple,
    compress_fixture,
    console,
    free_hd_space_in_GB,
    path_globs_to_tuple,
    paths_with_newlines,
    save_fixture,
    truncate_path_str,
    valid_compression_files,
)

logger = getLogger("rich")

FULLTEXT_DJANGO_MODEL: Final[str] = "fulltext.fulltext"

DEFAULT_EXTRACTED_SUBDIR: Final[PathLike] = Path("extracted")

FULLTEXT_FILE_NAME_SUFFIX: Final[str] = "_plaintext"
FULLTEXT_DEFAULT_PLAINTEXT_ZIP_GLOB_REGEX: Final[
    str
] = f"*{FULLTEXT_FILE_NAME_SUFFIX}.{ZIP_FILE_EXTENSION}"
TXT_FIXTURE_FILE_EXTENSION: Final[str] = "txt"
TXT_FIXTURE_FILE_GLOB_REGEX: Final[str] = f"**/*.{TXT_FIXTURE_FILE_EXTENSION}"
DEFAULT_MAX_PLAINTEXT_PER_FIXTURE_FILE: Final[int] = 100
DEFAULT_PLAINTEXT_FILE_NAME_PREFIX: Final[str] = "plaintext_fixture"
DEFAULT_PLAINTEXT_FIXTURE_OUTPUT: Final[PathLike] = Path("output") / "plaintext"
DEFAULT_INITIAL_PK: int = 1
TRUNC_TAILS_SUBPATH_DEFAULT: int = 1

SAS_ENV_VARIABLE = "FULLTEXT_SAS_TOKEN"


class FulltextPathDict(TypedDict):
    """A `dict` of `lwmdb.fulltext.models.Fulltext` fixture structure.

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

    """Manage exporting `plaintext` `alto2txt` results for `lwmdb.fulltext.models.Fulltext`.

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

        json_0_file_name_padding:
            Number of `0`s to prefix file name numbering.

        _disk_usage:
            Available harddrive space. Designed to help mitigate decompressing too
            many files for available disk space.

        self._uncompressed_source_file_dict:
            A dictionary of extracted plaintext to compressed source file. This is
            a field in `json` fixture records.

    Example:
        ```pycon
        >>> path = getfixture('bl_lwm')
        >>> logger.setLevel(INFO)
        >>> plaintext_bl_lwm = PlainTextFixture(
        ...     data_provider_code='bl_lwm',
        ...     path=path,
        ...     compressed_glob_regex="*_plaintext.zip",
        ...     )
        >>> plaintext_bl_lwm
        <PlainTextFixture(path='...bl_lwm')>
        >>> plaintext_bl_lwm.info()
        <BLANKLINE>
        ...PlainTextFixture for 2 'bl_lwm' files...
        ┌─────────────────────┬────────────────────────────────...┐
        │ Path                │ '...bl_lwm'                    ...│
        │ Compressed Files    │ '...bl_lwm...0003079-test_plain...│
        │                     │ '...bl_lwm...0003548-test_plain...│
        │ Extract Path        │ '...bl_lwm...extracted'        ...│
        │ Uncompressed Files  │ None                           ...│
        │ Data Provider       │ 'Living with Machines'         ...│
        │ Initial Primary Key │ 1                              ...│
        │ Max Rows Per JSON   │ 100                            ...│
        │ JSON File Name 0s   │ 6                              ...│
        └─────────────────────┴────────────────────────────────...┘
        >>> plaintext_bl_lwm.free_hd_space_in_GB > 1
        True
        >>> plaintext_bl_lwm.extract_compressed()
        <BLANKLINE>
        ...Extract path:...'...lwm...extracted...
        ...Extracting:...'...lwm...00030...
        ...Extracting:...'...lwm...00035...
        ...%...[...]...
        >>> plaintext_bl_lwm.delete_decompressed()
        Deleting all files in:...'...bl_lwm...tracted'

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
    empty_info_default_str: str = "None"
    json_0_file_name_padding: int = FILE_NAME_0_PADDING_DEFAULT
    json_export_compression_subdir: PathLike = COMPRESSED_PATH_DEFAULT
    json_export_compression_format: ArchiveFormatEnum = COMPRESSION_TYPE_DEFAULT
    _trunc_head_paths: int = TRUNC_HEADS_PATH_DEFAULT
    _trunc_tails_paths: int = TRUNC_TAILS_PATH_DEFAULT
    _trunc_tails_sub_paths: int = TRUNC_TAILS_SUBPATH_DEFAULT

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
        """Return class name with count and `DataProvider` if available."""
        return (
            f"{type(self).__name__} "
            f"for {len(self)} "
            f"{self._data_provider_code_quoted_with_trailing_space}files"
        )

    def __repr__(self) -> str:
        """Return `class` name with `path` attribute."""
        return f"<{type(self).__name__}(path='{self.path}')>"

    @property
    def _data_provider_code_quoted_with_trailing_space(self) -> str | None:
        """Return `self.data_provider` `code` attributre with trailing space or `None`."""
        return f"'{self.data_provider_code}' " if self.data_provider_code else None

    @property
    def trunc_compressed_file_names_str(self) -> str:
        """Return truncated `self.compressed_files` file names or empty `str`."""
        return (
            self._compressed_file_names(
                truncate=True,
                tail_parts=self._trunc_tails_paths + self._trunc_tails_sub_paths,
            )
        ) or self.empty_info_default_str

    @property
    def trunc_uncompressed_file_names_str(self) -> str:
        """Return truncated `self.plaintext_provided_uncompressed` file names or empty `str`."""
        return (
            self._provided_uncompressed_file_names(
                truncate=True,
                tail_parts=self._trunc_tails_paths + self._trunc_tails_sub_paths,
            )
        ) or self.empty_info_default_str

    @property
    def trunc_extract_path_str(self) -> str:
        """Return truncated `self.extract_path` or empty `str`."""
        path_str: str = truncate_path_str(
            self.extract_path,
            tail_parts=self._trunc_tails_paths + self._trunc_tails_sub_paths,
        )
        return f"'{path_str}'" or self.empty_info_default_str

    @property
    def info_table(self) -> Table:
        """Generate a `rich.table.Table` of config information.

        Example:
            ```pycon
            >>> hmd_plaintext_fixture = PlainTextFixture(
            ...     path=".",
            ...     data_provider_code="bl_hmd")
            >>> table = hmd_plaintext_fixture.info_table
            >>> table.title
            "PlainTextFixture for 0 'bl_hmd' files"

            ```

        """
        table: Table = Table(title=str(self), show_header=False)
        table.add_row("Path", f"'{truncate_path_str(self.path)}'")
        table.add_row("Compressed Files", self.trunc_compressed_file_names_str)
        table.add_row("Extract Path", self.trunc_extract_path_str)
        table.add_row("Uncompressed Files", self.trunc_uncompressed_file_names_str)
        table.add_row("Data Provider", f"'{str(self.data_provider_name)}'")
        table.add_row("Initial Primary Key", str(self.initial_pk))
        table.add_row("Max Rows Per JSON", str(self.max_plaintext_per_fixture_file))
        table.add_row("JSON File Name 0s", str(self.json_0_file_name_padding))
        return table

    def info(self) -> None:
        """Print `self.info_table` to the `console`."""
        console.print(self.info_table)

    def _compressed_file_names(
        self, truncate: bool = False, tail_parts: int = 1
    ) -> str:
        """`self.compressed_files` `paths` separated by `\n`."""
        return paths_with_newlines(
            self.compressed_files, truncate=truncate, tail_parts=tail_parts
        )

    def _provided_uncompressed_file_names(
        self, truncate: bool = False, tail_parts: int = 1
    ) -> str:
        """`self.plaintext_provided_uncompressed` `paths` separated by `\n`."""
        return paths_with_newlines(
            self.plaintext_provided_uncompressed,
            truncate=truncate,
            tail_parts=tail_parts,
        )

    @property
    def data_provider_name(self) -> str | None:
        """Return `self.data_provider` `code` attributre or `None`.

        Todo:
            * Add check without risk of recursion for `self.data_provider_code`

        Example:
            ```pycon
            >>> bl_hmd = PlainTextFixture(
            ...     path=".",
            ...     data_provider_code="bl_hmd")
            >>> bl_hmd.data_provider_name
            'Heritage Made Digital'
            >>> bl_lwm = PlainTextFixture(
            ...     path='.',
            ...     data_provider=NEWSPAPER_DATA_PROVIDER_CODE_DICT['bl_lwm'],
            ...     )
            >>> bl_lwm.data_provider_name
            'Living with Machines'
            >>> plaintext_fixture = PlainTextFixture(
            ...     path=".")
            <BLANKLINE>
            ...`.data_provider` and `.data_provider_code`...
            ...are...'None'...in...<PlainTextFixture(path='.')>...
            >>> plaintext_fixture.data_provider_name

            ```

            `
        """
        if self.data_provider and "name" in self.data_provider["fields"]:
            return self.data_provider["fields"]["name"]
        elif self.data_provider_code:
            return self.data_provider_code
        else:
            return None

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
        else:
            return Path(self.path) / self.extract_subdir

    @property
    def compressed_files(self) -> tuple[PathLike, ...]:
        """Return a tuple of all `self.files` with known archive file names."""
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
            >>> zipfile_info_list: list[ZipInfo] = list(plaintext_bl_lwm.zipinfo)
            Getting zipfile info from <PlainTextFixture(path='...bl_lwm')>
            >>> zipfile_info_list[0][-1].filename
            '0003079...1898...0204...0003079_18980204_sect0001.txt'
            >>> zipfile_info_list[-1][-1].filename
            '0003548...1904...0707...0003548_19040707_art0059.txt'
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
            ...Extract path:...'...bl_lwm...extracted'...
            >>> filter_sect1_txt: list[str] = [txt_file for txt_file in
            ...  plaintext_bl_lwm._uncompressed_source_file_dict.keys()
            ...  if txt_file.name.endswith('204_sect0001.txt')]
            >>> len(filter_sect1_txt)
            1
            >>> plaintext_bl_lwm._uncompressed_source_file_dict[
            ...     filter_sect1_txt[0]
            ...     ]
            <BLANKLINE>
            ...Path('...bl_lwm...0003079-test_plaintext.zip')
            >>> plaintext_bl_lwm.delete_decompressed()
            Deleting all files in:...'...bl_lwm...tracted'

            ```

        """
        self.extract_path.mkdir(parents=True, exist_ok=True)
        console.log(f"Extract path: '{self.extract_path}'")
        for compressed_file in tqdm(
            self.compressed_files,
            total=len(self.compressed_files),
        ):
            logger.info(f"Extracting: '{compressed_file}' ...")
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
            ...Extract path:...bl_lwm...extracted...
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

        Note:
            For errors running on windows see:
            [#55](https://github.com/Living-with-machines/alto2txt2fixture/issues/55)

        Example:
            ```pycon
            >>> if is_platform_win:
            ...     pytest.skip('decompression fails on Windows: issue #55')
            >>> plaintext_bl_lwm = getfixture('bl_lwm_plaintext_extracted')
            <BLANKLINE>
            ...Extract path:...bl_lwm...extracted...
            >>> paths_dict = list(plaintext_bl_lwm.plaintext_paths_to_dicts())
            Compressed configs  :...%...[ ... it/s ]
            >>> plaintext_bl_lwm.delete_decompressed()
            Deleting all files in: '...tracted'

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
        self,
        output_path: PathLike | None = None,
        prefix: str | None = None,
        json_0_file_name_padding: int | None = None,
    ) -> None:
        """Iterate over `self.plaintext_paths` exporting to `json` `django` fixtures.

        Note:
            For errors running on windows see:
            [#55](https://github.com/Living-with-machines/alto2txt2fixture/issues/55)

        Args:
            output_path:
                Folder to save all `json` fixtures in.
            prefix:
                Any `str` prefix for saved fixture files.
            json_0_file_name_padding:
                Number of `0`s to prefix file name numbering.

        Example:
            ```pycon
            >>> if is_platform_win:
            ...     pytest.skip('decompression fails on Windows: issue #55')
            >>> bl_lwm: Path = getfixture("bl_lwm")
            >>> first_lwm_plaintext_json_dict: PlaintextFixtureDict = (
            ...     getfixture("first_lwm_plaintext_json_dict")
            ... )
            >>> plaintext_bl_lwm = getfixture('bl_lwm_plaintext_extracted')
            <BLANKLINE>
            ...Extract path:...bl_lwm...extracted...
            >>> plaintext_bl_lwm.export_to_json_fixtures(output_path=bl_lwm / "output")
            <BLANKLINE>
            Compressed configs...%...[...]
            >>> len(plaintext_bl_lwm._exported_json_paths)
            1
            >>> plaintext_bl_lwm._exported_json_paths
            (...Path(...plaintext_fixture-000001.json...),)
            >>> import json
            >>> exported_json = json.loads(
            ...     plaintext_bl_lwm._exported_json_paths[0].read_text()
            ... )
            >>> exported_json[0]['pk'] == first_lwm_plaintext_json_dict['pk']
            True
            >>> exported_json[0]['model'] == first_lwm_plaintext_json_dict['model']
            True
            >>> (exported_json[0]['fields']['text'] ==
            ...  first_lwm_plaintext_json_dict['fields']['text'])
            True
            >>> (exported_json[0]['fields']['path'] ==
            ...  str(first_lwm_plaintext_json_dict['fields']['path']))
            True
            >>> (exported_json[0]['fields']['compressed_path'] ==
            ...  str(first_lwm_plaintext_json_dict['fields']['compressed_path']))
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
        json_0_file_name_padding = (
            self.json_0_file_name_padding
            if not json_0_file_name_padding
            else json_0_file_name_padding
        )
        save_fixture(
            self.plaintext_paths_to_dicts(),
            prefix=prefix,
            output_path=output_path,
            add_created=True,
            max_elements_per_file=self.max_plaintext_per_fixture_file,
            file_name_0_padding=json_0_file_name_padding,
        )
        self.set_exported_json_paths(
            export_directory=output_path, saved_fixture_prefix=prefix
        )

    @property
    def exported_json_paths(self) -> Generator[Path, None, None]:
        """If `self._exported_json_paths` return `Generator` of those paths.

        Yields:
            Each path from `self._exported_json_paths`

        Example:
            ```pycon
            >>> if is_platform_win:
            ...     pytest.skip('decompression fails on Windows: issue #55')
            >>> plaintext_bl_lwm = getfixture('bl_lwm_plaintext_json_export')
            <BLANKLINE>
            ...
            >>> tuple(plaintext_bl_lwm.exported_json_paths)
            (...Path('...plaintext_fixture-000001.json'),)

            ```
        """
        if not hasattr(self, "_exported_json_paths"):
            raise ValueError(
                f"No '_exported_json_paths', "
                f"run after 'self.export_to_json_fixtures()' for {self}"
            )
        for path in self._exported_json_paths:
            yield path

    def set_exported_json_paths(
        self,
        export_directory: PathLike | None,
        saved_fixture_prefix: str | None,
        overwrite: bool = False,
    ) -> None:
        """Set `self._exported_json_paths` for use with `self.exported_json_paths`.

        Note:
            If provided `export_directory` and `saved_fixture_prefix` will
            overwite those attributes on `self.`

        Params:
            export_directory:
                `Path` to check for saved `json` files.
            saved_fixture_prefix:
                `str` to prefix each exported `json` file with.
            overwrite:
                Force replace `self._exported_json_paths` if already set.

        Example:
            ```pycon
            >>> if is_platform_win:
            ...     pytest.skip('decompression fails on Windows: issue #55')
            >>> tmp_path = getfixture('tmp_path')
            >>> plaintext_bl_lwm = getfixture('bl_lwm_plaintext_json_export')
            <BLANKLINE>
            ...
            >>> tuple(plaintext_bl_lwm.exported_json_paths)
            (...Path('...plaintext_fixture-000001.json'),)
            >>> plaintext_bl_lwm.set_exported_json_paths(tmp_path, 'check-prefix')
            Traceback (most recent call last):
                ...
            ValueError: Cannot overwrite 'self._exported_json_paths' without
            'overwrite' = True. Current 'self._exported_json_paths':
            (...Path('...plaintext_fixture-000001.json'),)
            >>> plaintext_bl_lwm.set_exported_json_paths(tmp_path,
            ...     'check-prefix', overwrite=True)
            <BLANKLINE>
            ...Force change '._exported_json_paths' in...<PlainTextFixture...
            >>> plaintext_bl_lwm.export_directory == tmp_path
            True
            >>> plaintext_bl_lwm.saved_fixture_prefix
            'check-prefix'

            ```
        """
        if hasattr(self, "_exported_json_paths"):
            if overwrite:
                logger.info(f"Force change '._exported_json_paths' in {repr(self)}")
            else:
                raise ValueError(
                    f"Cannot overwrite 'self._exported_json_paths' without "
                    f"'overwrite' = True. Current 'self._exported_json_paths':\n "
                    f"{pformat(self._exported_json_paths)}"
                )
        self.export_directory = (
            export_directory if export_directory else self.export_directory
        )
        self.saved_fixture_prefix = (
            saved_fixture_prefix if saved_fixture_prefix else self.saved_fixture_prefix
        )
        self._exported_json_paths = path_globs_to_tuple(
            self.export_directory, f"**/{self.saved_fixture_prefix}*.json"
        )

    def compress_json_exports(
        self,
        output_path: PathLike | None = None,
        format: ArchiveFormatEnum | None = None,
    ) -> tuple[Path, ...]:
        """Compress `self._exported_json_paths` to `format`.

        Args:
            output_path:
                `Path` to save compressed `json` files to. Uses
                `self.json_export_compression_subdir` if `None` is passed.
            format:
                What compression format to use from `ArchiveFormatEnum`. Uses
                `self.json_export_compression_format` if `None` is passed.

        Note:
            Neither `output_path` nor `format` overwrite the related attributes
            of `self`.

        Returns: The the `output_path` passed to save compressed `json`.

        Example:
            ```pycon
            >>> if is_platform_win:
            ...     pytest.skip('decompression fails on Windows: issue #55')
            >>> plaintext_bl_lwm = getfixture('bl_lwm_plaintext_json_export')
            <BLANKLINE>
            ...
            >>> compressed_paths: Path = plaintext_bl_lwm.compress_json_exports(
            ...     format='tar')
            <BLANKLINE>
            ...Compressing...'...01.json' to...'tar'...in:...
            >>> compressed_paths
            (...Path('.../plaintext_fixture-000001.json.tar'),)

            ```
        """
        output_path = (
            Path(self.json_export_compression_subdir)
            if not output_path
            else Path(output_path)
        )
        format = self.json_export_compression_format if not format else format
        compressed_paths: list[Path] = []
        for json_path in self.exported_json_paths:
            compressed_paths.append(
                compress_fixture(json_path, output_path=output_path, format=format)
            )
        return tuple(compressed_paths)

    # def delete_compressed(self, index: int | str | None = None) -> None:
    def delete_decompressed(self, ignore_errors: bool = True) -> None:
        """Remove all files in `self.extract_path`.

        Example:
            ```pycon
            >>> plaintext_bl_lwm = getfixture('bl_lwm_plaintext_extracted')
            <BLANKLINE>
            ...Extract path:...'...bl_lwm...extracted'...
            >>> plaintext_bl_lwm.delete_decompressed()
            Deleting all files in:...
            >>> plaintext_bl_lwm.delete_decompressed()
            <BLANKLINE>
            ...Extract path empty:...'...bl_lwm...extracted'...

            ```
        """
        if self.extract_path.exists():
            console.print(f"Deleting all files in: '{self.extract_path}'")
            rmtree(self.extract_path, ignore_errors=ignore_errors)
        else:
            console.log(f"Extract path empty: '{self.extract_path}'")

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
            ...<PlainText..._set...d...
            >>> plaintext_lwm.path = (
            ...    plaintext_lwm.path / '0003079-test_plaintext.zip')
            >>> plaintext_lwm._check_and_set_files_attr()
            Traceback (most recent call last):
                ...
            ValueError:...`path` inconsistent with `files`...
            >>> len(plaintext_lwm)
            2
            >>> plaintext_lwm._check_and_set_files_attr(force=True)
            <BLANKLINE>
            ...DEBUG...Force change to...<PlainText...`files`...zip...
            >>> plaintext_lwm.files
            (...('...bl_lwm...0003079-test_plaintext.zip'),)
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
        """Set `self.data_provider` and check `self.data_provider_code`.

        Example:
            ```pycon
            >>> logger.setLevel(DEBUG)
            >>> plaintext_fixture = PlainTextFixture(path=".")
            <BLANKLINE>
            ...`.data_provider` and `.data_provider_code`...'None' in...
            ...<PlainTextFixture(path='.')>...

            ```
        """
        if self.data_provider:
            data_provider_fields_code: str = self.data_provider["fields"]["code"]
            if not self.data_provider_code:
                self.data_provider_code = data_provider_fields_code
            elif self.data_provider_code == data_provider_fields_code:
                logger.debug(
                    f"{repr(self)} `self.data_provider['fields']['code']` "
                    f"== `self.data_provider_code`"
                )
            elif force:
                logger.warning(
                    f"Forcing {repr(self)} `data_provider_code` to "
                    f"{self.data_provider['fields']['code']}\n"
                    f"Orinal `data_provider_code`: {self.data_provider_code}"
                )
                self.data_provider_code = data_provider_fields_code
            else:
                raise ValueError(
                    f"`self.data_provider_code` {self.data_provider_code} "
                    f"!= {self.data_provider} (`self.data_provider`)."
                )
        elif self.data_provider_code:
            if self.data_provider_code in self.data_provider_code_dict:
                self.data_provider = self.data_provider_code_dict[
                    self.data_provider_code
                ]
            else:
                raise ValueError(
                    f"`self.data_provider_code` {self.data_provider_code} "
                    f"not in `self.data_provider_code_dict`."
                    f"Available `codes`: {self.data_provider_code_dict.keys()}"
                )
        else:
            logger.debug(
                f"`.data_provider` and `.data_provider_code` are 'None' in {repr(self)}"
            )
