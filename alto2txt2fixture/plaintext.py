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

HOME_DIR: PathLike = Path.home()
DOWNLOAD_DIR: PathLike = HOME_DIR / "metadata-db/"
ARCHIVE_SUBDIR: PathLike = Path("archives")
EXTRACTED_SUBDIR: PathLike = Path("extracted")
FULLTEXT_METHOD: str = "download"
FULLTEXT_CONTAINER_SUFFIX: str = "-alto2txt"
FULLTEXT_CONTAINER_PATH: PathLike = Path("plaintext/")
FULLTEXT_STORAGE_ACCOUNT_URL: str = "https://alto2txt.blob.core.windows.net"

FULLTEXT_FILE_NAME_SUFFIX: Final[str] = "_plaintext"
FULLTEXT_FILE_COMPRESSED_EXTENSION: Final[str] = "zip"
FULLTEXT_DECOMPRESSED_PATH = Path("uncompressed/")
FULLTEXT_DEFAULT_PLAINTEXT_ZIP_GLOB_REGEX: Final[
    str
] = f"*{FULLTEXT_FILE_NAME_SUFFIX}.{FULLTEXT_FILE_COMPRESSED_EXTENSION}"
DEFAULT_MAX_PLAINTEXT_PER_FIXTURE_FILE: Final[int] = 2000
DEFAULT_PLAINTEXT_FILE_NAME_PREFIX: Final[str] = "plaintext_fixture_"
DEFAULT_PLAINTEXT_FIXTURE_OUTPUT: Final[PathLike] = Path("output") / "plaintext"

SAS_ENV_VARIABLE = "FULLTEXT_SAS_TOKEN"

# def fixture_to_json(
#     fields: dict[str, Any],
#     pk: str | int,
#     model: str,
#     # model: str = FULLTEXT_DJANGO_MODEL,
#     # lwmdb_fulltext: bool = True
# ) -> FixtureDict:
#     """Read `path` and construct a `dict` for a `django` fixture.
#
#     Arguments:
#         path:
#             `PathLike` `path` to fixture
#         pk:
#             `django` record primary key (id)
#         model:
#             `django` `model` the record is saved in
#         lwmdb_fulltext:
#             Whether to include extra elements in `json` specific to `lwmdb`
#
#     Returns:
#         A `dict` of `plaintext` for `json` export as a `django` `fixture`.
#
#     Example:
#         ```pycon
#         >>> fulltext_zip_path = getfixture('hmd_plaintext_fixture')
#         >>> fixture_to_json()
#
#         ```
#     """
#     # fields: dict[str, Any] =
#     return {'pk': pk, 'model': model, 'fields': fields}


# def compressed_file_name_from_publication_code(
#         publication_code: str,
#         suffix: str = FULLTEXT_FILE_NAME_SUFFIX,
#         extension: str = FULLTEXT_FILE_COMPRESSED_EXTENSION) -> str:
#     """Filename for an Item's archive containing the full text.
#
#     Example:
#         ```pycon
#         >>> zip_file_name('0002645')
#         0002645_plaintext.zip
#         >>> zip_file_name('0002645', suffix='-diff-suff', extension='bz2')
#         0002645-diff-suff.bz2
#         ```
#     """
#     return f"{publication_code}{suffix}.{extension}"


class FulltextPathDict(TypedDict):
    """A `dict` for storing fixture paths and primary key.

    Attributes:

        path:
            Plaintext file path.

        compressed_path:
            If `path` is within a compressed file,
            `compressed_path` is that source. Else None.

        primary_key:
            An `int` >= 1 for `SQL` fixture `pk`.
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

        glob_regex_str:
            A Regular Expression to filter plaintext files from uncompressed
            `self.files`, more specifically `self.compressed_files`.

        data_provider:
            If available a `DataProviderFixtureDict` for `DataProvider` metadata.
            By default all options are stored in `self.data_provider_code_dict`.

        model:
            Name of `lwmdb` model the exported `json` `fixture` is designed for.

        extract_subdir:
            Folder to extract `self.compressed_files` to.

        plaintext_extension:
            What file extension to use to filter `plaintext` files.

        data_provider_code_dict:
            A `dict` of metadata for preconfigured `DataProvider` records in `lwmdb`.

        max_plaintext_per_fixture_file:
            A maximum number of fixtures per fixture file, designed to configure
            chunking fixtures.

        fixture_prefix:
            A `str` to prefix all saved `json` fixture filenames.

        export_directory:
            Directory to save all exported fixtures to.

        _disk_usage:
            Available harddrive space. Designed to help mitigate decompressing too
            many files for available disk space.

        self._uncompressed_source_file_dict:
            A dictionary of extracted plaintext to compressed source file. This is
            a field in `json` fixture records.

    Example:
        ```pycon
        >>> from pprint import pprint
        >>> plain_text_bl_lwm = PlainTextFixture(
        ...     data_provider_code='bl_lwm',
        ...     path='tests/test_plaintext/bl_lwm',
        ...     glob_regex_str="*_plaintext.zip",
        ...     )
        >>> plain_text_bl_lwm
        <PlainTextFixture(path='tests/test_plaintext/bl_lwm')>
        >>> str(plain_text_bl_lwm)
        "PlainTextFixture for 2 'bl_lwm' files"
        >>> plain_text_bl_lwm.free_hd_space_in_GB > 1
        True
        >>> pprint(plain_text_bl_lwm.compressed_files)
        (PosixPath('tests/test_plaintext/bl_lwm/0003079_plaintext.zip'),
         PosixPath('tests/test_plaintext/bl_lwm/0003548_plaintext.zip'))
        >>> zipfile_info_list = list(plain_text_bl_lwm.zipinfo)
        Getting zipfile info from <PlainTextFixture(path='tests/test_plaintext/bl_lwm')>
        >>> zipfile_info_list[0][-1].filename
        '0003079/1898/0114/0003079_18980114_art0041.txt'
        >>> zipfile_info_list[-1][-1].filename
        '0003548/1904/0616/0003548_19040616_art0053.txt'
        >>> zipfile_info_list[-1][-1].file_size
        2460
        >>> zipfile_info_list[-1][-1].compress_size
        1187
        >>> plain_text_bl_lwm.extract_compressed()
        [...] Extract path:...tests/test_plaintext/bl_lwm/extracted...
        ...Extracting:...tests/test_plaintext/bl_lwm/0003079_plaintext.zip ...
        ...Extracting:...tests/test_plaintext/bl_lwm/0003548_plaintext.zip ...
        >>> plain_text_bl_lwm.delete_decompressed()
        Deleteing all files in: tests/test_plaintext/bl_lwm/extracted

        ```

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
    glob_regex_str: str = FULLTEXT_DEFAULT_PLAINTEXT_ZIP_GLOB_REGEX
    # format: str
    # mount_path: PathLike | None = Path(settings.MOUNTPOINT)
    data_provider: DataProviderFixtureDict | None = None
    model: str = FULLTEXT_DJANGO_MODEL
    archive_subdir: PathLike = ARCHIVE_SUBDIR
    extract_subdir: PathLike = EXTRACTED_SUBDIR
    plaintext_extension: str = "txt"
    # decompress_subdir: PathLike = FULLTEXT_DECOMPRESSED_PATH
    download_dir: PathLike = DOWNLOAD_DIR
    fulltext_container_suffix: str = FULLTEXT_CONTAINER_SUFFIX
    data_provider_code_dict: dict[str, DataProviderFixtureDict] = field(
        default_factory=lambda: NEWSPAPER_DATA_PROVIDER_CODE_DICT
    )
    max_plaintext_per_fixture_file: int = DEFAULT_MAX_PLAINTEXT_PER_FIXTURE_FILE
    fixture_prefix: str = DEFAULT_PLAINTEXT_FILE_NAME_PREFIX
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
        file_paths_tuple: tuple[PathLike, ...] = path_globs_to_tuple(
            self.path, self.glob_regex_str
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
                    f"`glob_regex_str`: {self.glob_regex_str}\n"
                    f"`files`: {self.files}\n"
                    f"`path`: {self.path}\n `files`: {self.files}"
                )
            else:
                raise ValueError(
                    f"{repr(self)} `path` inconsistent with `files`.\n"
                    f"`glob_regex_str`: {self.glob_regex_str}\n"
                    f"`path`: {self.path}\n`files`: {self.files}"
                )
        self.files = file_paths_tuple

    @property
    def extract_path(self) -> Path:
        """Path any compressed files would be extracted to."""
        return Path(self.path) / self.extract_subdir

    @property
    def compressed_files(self) -> tuple[PathLike, ...]:
        """Return a tuple of all `self.files` with known archive filenames."""
        return tuple(valid_compression_files(files=self.files)) if self.files else ()

    # @property
    # def decompressed_paths_generator(self) -> Generator[Path, None, None]:
    #     """Return a generated of all `self.files` with known archive filenames."""
    #     if self.compressed_files and not self.extract_path.exists():
    #         console.print('Compressed files not yet extracted. Try `extract_compression()`.')
    #     else:
    #         for path in Path(self.extract_path).glob(f'*{self.plaintext_extension}'):
    #             yield path

    @property
    def plaintext_provided_uncompressed(self) -> tuple[PathLike, ...]:
        """Return a tuple of all `self.files` with `self.plaintext_extension`."""
        if self.files:
            return tuple(
                file
                for file in self.files.glob()
                if Path(file).suffix == self.plaintext_extension
            )
        else:
            return ()

    @property
    def zipinfo(self) -> Generator[list[ZipInfo], None, None]:
        """If `self.compressed_files` is in `zip`, return info, else None."""
        if any(Path(file).suffix == ".zip" for file in self.compressed_files):
            console.print(f"Getting zipfile info from {repr(self)}")
            for compressed_file in self.compressed_files:
                if Path(compressed_file).suffix == ".zip":
                    yield ZipFile(compressed_file).infolist()
        else:
            console.log(f"No `self.compressed_files` end with `.zip` for {repr(self)}.")

    # def extract_compressed(self, index: int | str | None = None) -> None:
    def extract_compressed(self) -> None:
        """Extract `self.compressed_files` to `self.extracted_subdir_name`."""
        self.extract_path.mkdir(parents=True, exist_ok=True)
        console.log(f"Extract path: {self.extract_path}")
        for compressed_file in tqdm(
            self.compressed_files,
            desc=f"Compressed files from {repr(self)}",
        ):
            console.log(f"Extracting: {compressed_file} ...")
            unpack_archive(compressed_file, self.extract_path)
            for path in self.extract_path.glob(f"*{self.plaintext_extension}"):
                if path not in self._uncompressed_source_file_dict:
                    self._uncompressed_source_file_dict[path] = compressed_file

    # @property
    # def is_likely_decompressed(self) -> bool:
    #     """Return estimate if all `self.compressed_files` are decompressed."""
    #     if self.extract_path.exists():
    #         return all( for file in self.compressed_files)
    #     return all()

    def plaintext_paths(self) -> Generator[FulltextPathDict, None, None]:
        """Return a generator of all `plaintext` files for potential fixtures."""
        if self.compressed_files and not self.extract_path.exists():
            console.print(
                "Compressed files not yet extracted. Try `extract_compression()`."
            )
        else:
            i: int = 0
            pk: int
            for i, uncompressed_tuple in enumerate(
                tqdm(
                    self._uncompressed_source_file_dict.items(),
                    desc="Configuring compressed path configs",
                )
            ):
                pk = i + 1  # Most `SQL` `pk` begins at 1
                self._pk_plaintext_dict[uncompressed_tuple[0]] = pk
                yield FulltextPathDict(
                    path=uncompressed_tuple[0],
                    compressed_path=uncompressed_tuple[1],
                    primary_key=pk,
                )
            for j, path in enumerate(
                tqdm(
                    self.plaintext_provided_uncompressed,
                    desc="Configuring uncompressed path configs",
                )
            ):
                pk = j + i + 1
                self._pk_plaintext_dict[path] = pk
                yield FulltextPathDict(path=path, compressed_path=None, primary_key=pk)

    def plaintext_paths_to_dicts(self) -> Generator[PlaintextFixtureDict, None, None]:
        """Generate fixture dicts from `self.plaintext_paths`."""
        for plaintext_path_dict in tqdm(
            self.plaintext_paths(),
            desc=f"Processing {self.plaintext_extension} files to PlaintextFixtureDict",
        ):
            fields: PlaintextFixtureFieldsDict = PlaintextFixtureFieldsDict(
                text=Path(plaintext_path_dict["path"]).read_text(),
                path=plaintext_path_dict["path"],
                compressed_path=plaintext_path_dict["compressed_path"],
            )
            yield PlaintextFixtureDict(
                model=self.model,
                fields=fields,
                pk=plaintext_path_dict["primary_key"],
            )

    def export_to_json_fixtures(self) -> None:
        """Iterate over `self.plaintext_paths` exporting to `json` `django` fixtures."""
        save_fixture(
            self.plaintext_paths_to_dicts(),
            prefix=self.fixture_prefix,
            output_path=self.export_directory,
            add_created=True,
        )

    # def delete_compressed(self, index: int | str | None = None) -> None:
    def delete_decompressed(self) -> None:
        """Remove all uncompressed files."""
        console.print(f"Deleteing all files in: {self.extract_path}")
        rmtree(self.extract_path)

    def _check_and_set_files_attr(self, force: bool = False) -> None:
        """Check and populate attributes from `self.path` and `self.files`.

        If `self.path` is a file, ensure `self.files = (self.path,)` and
        raise a ValueError if not.

        If `self.path` is a directory, then collect all `tuple` of `Paths`
        in that directory matching `self.glob_regex_str`, or just all files
        if `self.glob_regex_str` is `None`. If `self.files` is set check if
        that `tuple`

        Example:
            ```pycon
            >>> plaintext_lwm = getfixture('bl_lwm_plaintext')
            >>> len(plaintext_lwm)
            2
            >>> plaintext_lwm._check_and_set_files_attr()
            [...]...DEBUG...No changes from...
            ...<PlainText..._set...and...check...dir...
            >>> plaintext_lwm.path = (
            ...    'tests/test_plaintext/bl_lwm/0003548_plaintext.zip')
            >>> plaintext_lwm._check_and_set_files_attr()
            Traceback (most recent call last):
                ...
            ValueError:...`path` inconsistent with `files`...
            >>> len(plaintext_lwm)
            2
            >>> plaintext_lwm._check_and_set_files_attr(force=True)
            DEBUG...Force change to...<PlainText...`files`...zip...
            >>> plaintext_lwm.files
            ('tests/test_plaintext/bl_lwm/0003548_plaintext.zip',)
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

    @property
    def free_hd_space_in_GB(self) -> float:
        """Return remaing hard drive space estimate in gigabytes."""
        return free_hd_space_in_GB(self._disk_usage)

    # @property
    # def download_dir(self):
    #     """Path to the download directory for full text data.
    #
    #     The `DOWNLOAD_DIR` attribute contains the directory under
    #     which full text data will be stored. Users can change it by
    #     setting `Item.DOWNLOAD_DIR = "/path/to/wherever/"`
    #     """
    #     return Path(self.DOWNLOAD_DIR)

    @property
    def text_archive_dir(self) -> Path:
        """Path to the storage directory for full text archives."""
        return Path(self.download_dir) / self.archive_subdir

    @property
    def text_extracted_dir(self) -> Path:
        """Path to the storage directory for extracted full text files."""
        return Path(self.download_dir) / self.extracted_subdir

    # @property
    # def zip_file(self):
    #     """Filename for this Item's zip archive containing the full text."""
    #     return f"{self.issue.newspaper.publication_code}_plaintext.zip"

    @property
    def text_container(self) -> str:
        """Azure blob storage container containing the Item full text."""
        return f"{self.data_provider_name}{self.fulltext_container_suffix}"

    # @property
    # def issue_sub_paths(self, publication_code: str | None = None) -> Generator[str, None, None]:
    #     """Return `issue_sub_paths` of `publication_code`, else all `issue_sub_paths`"""
    #     if publication_code:
    #         return

    @property
    def text_paths(self):
        """Return a list of paths relative to the full text file for
        `self.data_provider_name`

        This is generated from the zip archive (once downloaded and
        extracted) from the `DOWNLOAD_DIR` and the filename.
        """
        return Path(self.issue.input_sub_path) / self.input_filename

    # @property
    # def text_path(self):
    #     """Return a path relative to the full text file for this Item.
    #
    #     This is generated from the zip archive (once downloaded and
    #     extracted) from the `DOWNLOAD_DIR` and the filename.
    #     """
    #     return Path(self.issue.input_sub_path) / self.input_filename

    # Commenting this out as it will fail with the dev on #56 (see branch kallewesterling/issue56).
    # As this will likely not be the first go-to for fulltext access, we can keep it as a method:
    # .extract_fulltext()
    #
    # @property
    # def fulltext(self):
    #     try:
    #         return self.extract_fulltext()
    #     except Exception as ex:
    #         print(ex)

    def is_downloaded(self):
        """Check whether a text archive has already been downloaded."""
        file = self.text_archive_dir / self.zip_file
        if not os.path.exists(file):
            return False
        return os.path.getsize(file) != 0

    def download_zip(self):
        """Download this Item's full text zip archive from cloud storage."""
        sas_token = os.getenv(self.SAS_ENV_VARIABLE).strip('"')
        if sas_token is None:
            raise KeyError(
                f"The environment variable {self.SAS_ENV_VARIABLE} was not found."
            )

        url = self.FULLTEXT_STORAGE_ACCOUNT_URL
        container = self.text_container
        blob_name = str(Path(self.FULLTEXT_CONTAINER_PATH) / self.zip_file)
        download_file_path = self.text_archive_dir / self.zip_file

        # Make sure the archive download directory exists.
        self.text_archive_dir.mkdir(parents=True, exist_ok=True)

        if not os.path.exists(self.text_archive_dir):
            raise RuntimeError(
                f"Failed to make archive download directory at {self.text_archive_dir}"
            )

        # Download the blob archive.
        try:
            client = BlobClient(
                url, container, blob_name=blob_name, credential=sas_token
            )

            with open(download_file_path, "wb") as download_file:
                download_file.write(client.download_blob().readall())

        except Exception as ex:
            if "status_code" in str(ex):
                print("Zip archive download failed.")
                print(
                    f"Ensure the {self.SAS_ENV_VARIABLE} env variable contains a valid SAS token"
                )

            if os.path.exists(download_file_path):
                if os.path.getsize(download_file_path) == 0:
                    os.remove(download_file_path)
                    print(f"Removing empty download: {download_file_path}")

    def extract_fulltext_file(self):
        """Extract Item's full text file from a zip archive to DOWNLOAD_DIR."""
        archive = self.text_archive_dir / self.zip_file
        with ZipFile(archive, "r") as zip_ref:
            zip_ref.extract(str(self.text_path), path=self.text_extracted_dir)

    def read_fulltext_file(self) -> list[str]:
        """Read the full text for this Item from a file."""
        with open(self.text_extracted_dir / self.text_path) as f:
            lines = f.readlines()
        return lines

    def extract_fulltext(self) -> list[str]:
        """Extract the full text of this newspaper item."""
        # If the item full text has already been extracted, read it.
        if os.path.exists(self.text_extracted_dir / self.text_path):
            return self.read_fulltext_file()

        if self.FULLTEXT_METHOD == "download":
            # If not already available locally, download the full text archive.
            if not self.is_downloaded():
                self.download_zip()

            if not self.is_downloaded():
                raise RuntimeError(
                    f"Failed to download full text archive for item {self.item_code}: Expected finished download."
                )

            # Extract the text for this item.
            self.extract_fulltext_file()

        elif self.FULLTEXT_METHOD == "blobfuse":
            raise NotImplementedError("Blobfuse access is not yet implemented.")
            blobfuse = "/mounted/blob/storage/path/"
            zip_path = blobfuse / self.zip_file

        else:
            raise RuntimeError(
                "A valid fulltext access method must be selected: options are 'download' or 'blobfuse'."
            )

        # If the item full text still hasn't been extracted, report failure.
        if not os.path.exists(self.text_extracted_dir / self.text_path):
            raise RuntimeError(
                f"Failed to extract fulltext for {self.item_code}; path does not exist: {self.text_extracted_dir / self.text_path}"
            )

        return self.read_fulltext_file()


# def unzip_plaintext(path: PathLike) -> list[FixtureDict]:
#     for
