import uuid
import zipfile
from pathlib import Path
from typing import Generator, Optional
from xml.etree import ElementTree as ET

import pandas as pd
from slugify import slugify
from tqdm import tqdm

from .jisc import get_jisc_title, setup_jisc_papers
from .log import error, warning
from .patterns import PUBLICATION_CODE
from .settings import DATA_PROVIDER_INDEX, NEWSPAPER_COLLECTION_METADATA
from .types import FixtureDict, dotdict
from .utils import (
    dict_from_list_fixture_fields,
    fixture_or_default_dict,
    get_now,
    get_size_from_path,
    write_json,
)


class Cache:
    """
    The Cache class provides a blueprint for creating and managing cache data.
    The class has several methods that help in getting the cache path,
    converting the data to a dictionary, and writing the cache data to a file.

    It is inherited by many other classes in this document.
    """

    def __init__(self):
        """
        Initializes the Cache class object.
        """
        pass

    def __str__(self) -> str:
        """
        Returns the string representation of the cache data as a dictionary.
        """
        return str(self.as_dict())

    def as_dict(self) -> dict:
        """
        Converts the cache data to a dictionary and returns it.
        """
        return {}

    def get_cache_path(self) -> Path:
        """
        Returns the cache path, which is used to store the cache data.
        The path is normally constructed using some of the object's
        properties (collection, kind, and id) but can be changed when
        inherited.
        """
        return Path(f"{CACHE_HOME}/{self.collection}/{self.kind}/{self.id}.json")

    def write_to_cache(self) -> Optional[bool]:
        """
        Writes the cache data to a file at the specified cache path. The cache
        data is first converted to a dictionary using the as_dict method. If
        the cache path already exists, the function returns True.
        """

        path = self.get_cache_path()

        try:
            if path.exists():
                return True
        except AttributeError:
            error(
                f"Error occurred when getting cache path for "
                f"{self.kind}: {path}. It was not of expected "
                f"type Path but of type {type(path)}:",
            )

        path.parent.mkdir(parents=True, exist_ok=True)

        with open(path, "w+") as f:
            f.write(json.dumps(self.as_dict()))

        return


class Newspaper(Cache):
    """The Newspaper class extends the Cache class and represents a newspaper.

    The class has several properties and methods that allow the creation of a
    newspaper object and the manipulation of its data.

    Attributes:
        root: An xml element that represents the root of the publication.
        collection: A string that represents the collection of the publication.
        meta: A dotdict object that holds metadata about the publication.
        jisc_papers: A pandas DataFrame object for JISC paper information.
    """

    kind = "newspaper"
    """A string that represents the type of the object, set to "newspaper"."""

    def __init__(
        self,
        root: ET.Element,
        collection: str = "",
        meta: dotdict = dotdict(),
        jisc_papers: Optional[pd.DataFrame] = None,
    ):
        """Constructor method."""

        if not isinstance(root, ET.Element):
            raise RuntimeError(f"Expected root to be xml.etree.Element: {type(root)}")

        self.publication = root.find("./publication")
        self.input_sub_path = root.find("./process/input_sub_path").text
        self.issue_date = self.publication.find("./issue/date").text
        self.collection = collection
        self.meta = meta
        self.jisc_papers = jisc_papers

        self._newspaper = None
        self._title = None
        self._publication_code = None

        path = str(self.get_cache_path())
        if not self.meta.newspaper_paths:
            self.meta.newspaper_paths = []
        elif path not in self.meta.newspaper_paths:
            self.meta.newspaper_paths.append(path)

        if not self.meta.publication_codes:
            self.meta.publication_codes = [self.publication_code]
        elif self.publication_code not in self.meta.publication_codes:
            self.meta.publication_codes.append(self.publication_code)

        self.zip_file = Path(meta.path).name

    @property
    def title(self) -> str:
        """
        A property that returns the title of the newspaper.

        Returns:
            The title of the newspaper
        """
        if not self._title:
            try:
                self._title = (
                    self.publication.find("./title")
                    .text.rstrip(".")
                    .strip()
                    .rstrip(":")
                    .strip()
                )
            except AttributeError:
                self._title = ""

            if self._title:
                return self._title

            # We probably have a JISC title so we will go ahead and pick from
            # filename following alto2txt convention
            if not self.zip_file:
                self._title = ""
                warning(
                    "JISC title found but zip file name was not passed so \
                    title cannot be correctly processed."
                )
                return ""

            if not isinstance(self.jisc_papers, pd.DataFrame):
                self._title = ""
                warning(
                    "JISC title found but zip file name was not passed so \
                    title cannot be correctly processed."
                )
                return ""

            abbr = self.zip_file.split("_")[0]
            self._title = get_jisc_title(
                self._title,
                self.issue_date,
                self.jisc_papers,
                self.input_sub_path,
                self.publication_code,
                abbr,
            )

        return self._title

    def as_dict(self) -> dict:
        """
        A method that returns a dictionary representation of the newspaper
        object.

        Returns:
            Dictionary representation of the Newspaper object
        """

        if not self._newspaper:
            self._newspaper = dict(
                **dict(publication_code=self.publication_code, title=self.title),
                **{
                    x.tag: x.text or ""
                    for x in self.publication.findall("*")
                    if x.tag in ["location"]
                },
            )
        return self._newspaper

    def publication_code_from_input_sub_path(self) -> str | None:
        """
        A method that returns the publication code from the input sub-path of
        the publication process.

        Returns:
            The code of the publication
        """

        g = PUBLICATION_CODE.findall(self.input_sub_path)
        if len(g) == 1:
            return g[0]
        return None

    @property
    def publication_code(self) -> str:
        """
        A property that returns the code of the publication.

        Returns:
            The code of the publication
        """
        if not self._publication_code:
            self._publication_code = self.publication.attrib.get("id")
            if len(self._publication_code) != 7:
                if self._publication_code == "NCBL1001":
                    self._publication_code = self.publication_code_from_input_sub_path()
                    if not self._publication_code:
                        # Fallback option
                        self._publication_code = "0000499"
                elif self._publication_code == "NCBL1002":
                    self._publication_code = self.publication_code_from_input_sub_path()
                    if not self._publication_code:
                        # Fallback option
                        self._publication_code = "0000499"
                elif self._publication_code == "NCBL1023":
                    self._publication_code = self.publication_code_from_input_sub_path()
                    if not self._publication_code:
                        # Fallback option
                        self._publication_code = "0000152"
                elif self._publication_code == "NCBL1024":
                    self._publication_code = self.publication_code_from_input_sub_path()
                    if not self._publication_code:
                        # Fallback option
                        self._publication_code = "0000171"
                elif self._publication_code == "NCBL1029":
                    self._publication_code = self.publication_code_from_input_sub_path()
                    if not self._publication_code:
                        # Fallback option
                        self._publication_code = "0000165"
                elif self._publication_code == "NCBL1034":
                    self._publication_code = self.publication_code_from_input_sub_path()
                    if not self._publication_code:
                        # Fallback option
                        self._publication_code = "0000160"
                elif self._publication_code == "NCBL1035":
                    self._publication_code = self.publication_code_from_input_sub_path()
                    if not self._publication_code:
                        # Fallback option
                        self._publication_code = "0000185"
                elif (
                    len(self._publication_code) == 4 or "NCBL" in self._publication_code
                ):
                    g = PUBLICATION_CODE.findall(self.input_sub_path)
                    if len(g) == 1:
                        self._publication_code = g[0]
                    else:
                        raise RuntimeError("Publication code look-up failed.")

            if not self._publication_code:
                g = PUBLICATION_CODE.findall(self.input_sub_path)
                if len(g) == 1:
                    self._publication_code = g[0]
                else:
                    raise RuntimeError("Backup failed.")

            if not len(self._publication_code) == 7:
                self._publication_code = f"{self._publication_code}".zfill(7)

            if not self._publication_code:
                raise RuntimeError("Publication code is non-existent.")

            if not len(self._publication_code) == 7:
                raise RuntimeError(
                    f"Publication code is of wrong length: \
                    {len(self._publication_code)} ({self._publication_code})."
                )

        return self._publication_code

    @property
    def number_paths(self) -> list:
        """
        Returns the nested directories in which we want to save the cache file.

        Returns:
            List of the desired directories in descending order
        """

        number_paths = [x for x in self.publication_code.lstrip("0")[:2]]

        if len(number_paths) == 1:
            number_paths = ["0"] + number_paths

        return number_paths

    def get_cache_path(self) -> Path:
        """
        Returns the path to the cache file for the newspaper object.

        Returns:
            Path to the cache file for the newspaper object
        """
        json_file = f"/{self.publication_code}/{self.publication_code}.json"

        return Path(
            f"{CACHE_HOME}/{self.collection}/" + "/".join(self.number_paths) + json_file
        )


class Item(Cache):
    """
    The Newspaper class extends the Cache class and represents a newspaper
    item, i.e. an article. The class has several properties and methods that
    allow the creation of an article object and the manipulation of its data.

    Attributes:
        root:
            An xml element that represents the root of the publication
        issue_code:
            A string that represents the issue code
        digitisation:
            TODO
        ingest:
            TODO
        collection:
            A string that represents the collection of the publication
        newspaper:
            The parent newspaper
        meta:
            TODO
    """

    kind = "item"
    """A string that represents the type of the object, set to "item"."""

    def __init__(
        self,
        root: ET.Element,
        issue_code: str = "",
        digitisation: dict = {},
        ingest: dict = {},
        collection: str = "",
        newspaper: Optional[Newspaper] = None,
        meta: dotdict = dotdict(),
    ):
        """Constructor method."""

        if not isinstance(root, ET.Element):
            raise RuntimeError(f"Expected root to be xml.etree.Element: {type(root)}")

        if not isinstance(newspaper, Newspaper):
            raise RuntimeError("Expected newspaper to be of type router.Newspaper")

        self.root: ET.Element = root
        self.issue_code: str = issue_code
        self.digitisation: dict = digitisation
        self.ingest: dict = ingest
        self.collection: str = collection
        self.newspaper: Newspaper | None = newspaper
        self.meta: dotdict = meta

        self._item_elem = None
        self._item_code = None
        self._item = None

        path: str = str(self.get_cache_path())
        if not self.meta.item_paths:
            self.meta.item_paths = [path]
        elif path not in self.meta.item_paths:
            self.meta.item_paths.append(path)

    @property
    def item_elem(self):
        """
        Sets up and saves the issue XML item for easy access as a property.
        """

        if not self._item_elem:
            self._item_elem = self.root.find("./publication/issue/item")

        return self._item_elem

    def as_dict(self) -> dict:
        """
        A method that returns a dictionary representation of the item object
        (i.e. article).

        Returns:
            Dictionary representation of the Item object
        """

        if not self._item:
            self._item = {
                f"{x.tag}": x.text or ""
                for x in self.item_elem.findall("*")
                if x.tag
                in [
                    "title",
                    "word_count",
                    "ocr_quality_mean",
                    "ocr_quality_sd",
                    "plain_text_file",
                    "item_type",
                ]
            }

            self._item["title"] = self._item.get("title", "")[:2097151]

            self._item = {
                "item_code": self.item_code,
                "word_count": self._item.get("word_count", 0),
                "title": self._item.get("title"),
                "item_type": self._item.get("item_type"),
                "input_filename": self._item.get("plain_text_file", ""),
                "ocr_quality_mean": self._item.get("ocr_quality_mean", 0),
                "ocr_quality_sd": self._item.get("ocr_quality_sd", 0),
                "digitisation__software": self.digitisation.id,
                "ingest__lwm_tool_identifier": self.ingest.id,
                "issue__issue_identifier": self.issue_code,
                "data_provider__name": self.collection,
            }

        return self._item

    @property
    def item_code(self) -> str:
        """
        Sets up and saves the item code for easy access as property.
        """
        if not self._item_code:
            self._item_code = self.issue_code + "-" + self.item_elem.attrib.get("id")

        return self._item_code

    def get_cache_path(self) -> Path:
        """
        Returns the path to the cache file for the item (article) object.

        Returns:
            Path to the cache file for the article object
        """
        return Path(
            f"{CACHE_HOME}/{self.collection}/"
            + "/".join(self.newspaper.number_paths)
            + f"/{self.newspaper.publication_code}/items.jsonl"
        )

    def write_to_cache(self) -> None:
        """
        Special cache-write function that appends rather than writes at the
        end of the process.

        Returns:
            None.
        """
        path = self.get_cache_path()

        path.parent.mkdir(parents=True, exist_ok=True)

        with open(path, "a+") as f:
            f.write(json.dumps(self.as_dict()) + "\n")

        return


class Issue(Cache):
    """
    The Issue class extends the Cache class and represents a newspaper issue.
    The class has several properties and methods that allow the creation of an
    issue object and the manipulation of its data.

    Attributes:
        root:
            An xml element that represents the root of the publication
        newspaper:
            The parent newspaper
        collection:
            A string that represents the collection of the publication
        input_sub_path:
            TODO
        meta:
            TODO
    """

    kind = "issue"
    """A string that represents the type of the object, set to "issue"."""

    def __init__(
        self,
        publication: ET.Element,
        newspaper: Optional[Newspaper] = None,
        collection: str = "",
        input_sub_path: str = "",
        meta: dotdict = dotdict(),
    ):
        """Constructor method."""

        self.publication: ET.Element = publication
        self.newspaper: Newspaper | None = newspaper
        self.collection: str = collection
        self.input_sub_path: str = input_sub_path
        self.meta: dotdict = meta

        self._issue = None
        self._issue_date = None

        path: str = str(self.get_cache_path())
        if not self.meta.issue_paths:
            self.meta.issue_paths = [path]
        elif path not in self.meta.issue_paths:
            self.meta.issue_paths.append(path)

    @property
    def issue_date(self) -> str:
        """
        Sets up and saves the issue date for easy access as property.
        """

        if not self._issue_date:
            self._issue_date = self.publication.find("./issue/date").text

        return self._issue_date

    @property
    def issue_code(self) -> str:
        """
        Sets up and saves the issue code for easy access as property.
        """
        return (
            self.newspaper.publication_code.replace("-", "")
            + "-"
            + self.issue_date.replace("-", "")
        )

    def as_dict(self) -> dict:
        """
        A method that returns a dictionary representation of the issue
        object.

        Returns:
            Dictionary representation of the Issue object
        """

        if not self._issue:
            self._issue = dict(
                issue_code=self.issue_code,
                issue_date=self.issue_date,
                publication__publication_code=self.newspaper.publication_code,
                input_sub_path=self.input_sub_path,
            )

        return self._issue

    def get_cache_path(self) -> Path:
        """
        Returns the path to the cache file for the issue object.

        Returns:
            Path to the cache file for the issue object
        """

        json_file = f"/{self.newspaper.publication_code}/issues/{self.issue_code}.json"

        return Path(
            f"{CACHE_HOME}/{self.collection}/"
            + "/".join(self.newspaper.number_paths)
            + json_file
        )


class Ingest(Cache):
    """
    The Ingest class extends the Cache class and represents a newspaper ingest.
    The class has several properties and methods that allow the creation of an
    ingest object and the manipulation of its data.

    Attributes:
        root: An xml element that represents the root of the publication
        collection: A string that represents the collection of the publication
    """

    kind = "ingest"
    """A string that represents the type of the object, set to "ingest"."""

    def __init__(self, root: ET.Element, collection: str = ""):
        """Constructor method."""

        if not isinstance(root, ET.Element):
            raise RuntimeError(f"Expected root to be xml.etree.Element: {type(root)}")

        self.root: ET.Element = root
        self.collection: str = collection

    def as_dict(self) -> dict:
        """
        A method that returns a dictionary representation of the ingest
        object.

        Returns:
            Dictionary representation of the Ingest object
        """
        return {
            f"lwm_tool_{x.tag}": x.text or ""
            for x in self.root.findall("./process/lwm_tool/*")
        }

    @property
    def id(self) -> str:
        dic = self.as_dict()
        return dic.get("lwm_tool_name") + "-" + dic.get("lwm_tool_version")


class Digitisation(Cache):
    """
    The Digitisation class extends the Cache class and represents a newspaper
    digitisation. The class has several properties and methods that allow
    creation of an digitisation object and the manipulation of its data.

    Attributes:
        root: An xml element that represents the root of the publication
        collection: A string that represents the collection of the publication
    """

    kind = "digitisation"
    """A string that represents the type of the object, set to
    "digitisation"."""

    def __init__(self, root: ET.Element, collection: str = ""):
        """Constructor method."""

        if not isinstance(root, ET.Element):
            raise RuntimeError(f"Expected root to be xml.etree.Element: {type(root)}")

        self.root: ET.Element = root
        self.collection: str = collection

    def as_dict(self) -> dict:
        """
        A method that returns a dictionary representation of the digitisation
        object.

        Returns:
            Dictionary representation of the Digitising object
        """
        dic = {
            x.tag: x.text or ""
            for x in self.root.findall("./process/*")
            if x.tag
            in [
                "xml_flavour",
                "software",
                "mets_namespace",
                "alto_namespace",
            ]
        }
        if not dic.get("software"):
            return {}

        return dic

    @property
    def id(self) -> str:
        dic = self.as_dict()
        return dic.get("software").replace("/", "---") if dic.get("software") else None


class DataProvider(Cache):
    """
    The DataProvider class extends the Cache class and represents a newspaper
    data provider. The class has several properties and methods that allow
    creation of a data provider object and the manipulation of its data.

    Attributes:
        collection: A string representing publication collection
        kind: Indication of object type, defaults to `data-provider`
        providers_meta_data: structured dict of metadata for known collection sources
        collection_type: related data sources and potential linkage source
        index_field: field name for querying existing records

    Examples:
        ```pycon
        >>> from pprint import pprint
        >>> hmd = DataProvider("hmd")
        >>> hmd.pk
        2
        >>> pprint(hmd.as_dict())
        {'code': 'bl-hmd',
         'collection': 'newspapers',
         'legacy_code': 'hmd',
         'name': 'Heritage Made Digital',
         'source_note': 'British Library-funded digitised newspapers provided by the '
                        'British Newspaper Archive'}

        ```

    """

    kind: str = "data-provider"
    providers_meta_data: list[FixtureDict] = NEWSPAPER_COLLECTION_METADATA
    collection_type: str = "newspapers"
    index_field: str = DATA_PROVIDER_INDEX

    def __init__(self, collection: str):
        """Constructor method."""
        self.collection: str = collection

    @property
    def providers_index_dict(self) -> dict[str, FixtureDict]:
        """Return all ``self.index_field`` values from `providers_meta_data`."""
        return dict_from_list_fixture_fields(self.providers_meta_data, self.index_field)

    @property
    def meta_data(self) -> FixtureDict | dict:
        """Return ``self.providers_meta_data[self.collection]`` or `{}`."""
        return fixture_or_default_dict(self.collection, self.providers_index_dict)

    @property
    def meta_data_fields(self) -> FixtureDict | dict:
        """Return ``self.providers_meta_data[self.collection]`` or `{}`."""
        if self.meta_data:
            return self.meta_data["fields"]
        else:
            return {}

    @property
    def pk(self) -> int | None:
        """Return ``pk`` if provided via ``providers_meta_data``, else `None`."""
        if self.meta_data:
            return self.meta_data["pk"]
        else:
            return None

    def as_dict(self) -> dict:
        """
        Return a `dict` of the data provider object.

        Returns:
            Dictionary representation of the DataProvider object
        """
        if self.meta_data:
            return {
                "name": self.meta_data_fields["name"],
                "code": self.meta_data_fields["code"],
                "legacy_code": self.collection,
                "source_note": self.meta_data_fields["source_note"],
                "collection": self.collection_type,
            }
        else:
            return {
                "name": self.collection,
                "code": slugify(self.collection),
                "source_note": "",
                "legacy_code": None,
                "collection": self.collection_type,
            }

    @property
    def id(self) -> str:
        return self.as_dict().get("name", "")


class Document:
    """
    The Document class is a representation of a document that contains
    information about a publication, newspaper, item, digitisation, and
    ingest. This class holds all the relevant information about a document in
    a structured manner and provides properties that can be used to access
    different aspects of the document.

    Attributes:
        collection:
            A string that represents the collection of the publication
        root:
            An xml element that represents the root of the publication
        zip_file:
            A path to a valid zip file
        jisc_papers:
            A pandas DataFrame object that holds information about the JISC
            papers
        meta:
            TODO
    """

    def __init__(self, *args, **kwargs):
        """Constructor method."""

        self.collection: str | None = kwargs.get("collection")
        if not self.collection or not isinstance(self.collection, str):
            raise RuntimeError("A valid collection must be passed")

        self.root: ET.Element | None = kwargs.get("root")
        if not self.root or not isinstance(self.root, ET.Element):
            raise RuntimeError("A valid XML root must be passed")

        self.zip_file: str | None = kwargs.get("zip_file")
        if self.zip_file and not isinstance(self.zip_file, str):
            raise RuntimeError("A valid zip file must be passed")

        self.jisc_papers: pd.DataFrame | None = kwargs.get("jisc_papers")
        if not isinstance(self.jisc_papers, pd.DataFrame):
            raise RuntimeError(
                "A valid DataFrame containing JISC papers must be passed"
            )

        self.meta: dotdict | None = kwargs.get("meta")

        self._publication_elem = None
        self._input_sub_path = None
        self._ingest = None
        self._digitisation = None
        self._item = None
        self._issue = None
        self._newspaper = None
        self._data_provider = None

    @property
    def publication(self) -> ET.Element:
        """
        This property returns an ElementTree object representing the
        publication information in the XML document.
        """
        if not self._publication_elem:
            self._publication_elem = self.root.find("./publication")
        return self._publication_elem

    @property
    def issue(self) -> Issue:
        if not self._issue:
            self._issue = Issue(
                publication=self.publication,
                newspaper=self.newspaper,
                collection=self.collection,
                input_sub_path=self.input_sub_path,
                meta=self.meta,
            )
        return self._issue

    @property
    def input_sub_path(self) -> str:
        if not self._input_sub_path:
            self._input_sub_path = self.root.find("./process/input_sub_path").text
        return self._input_sub_path

    @property
    def data_provider(self) -> DataProvider:
        if not self._data_provider:
            self._data_provider = DataProvider(collection=self.collection)

        return self._data_provider

    @property
    def ingest(self) -> Ingest:
        if not self._ingest:
            self._ingest = Ingest(root=self.root, collection=self.collection)

        return self._ingest

    @property
    def digitisation(self) -> Digitisation:
        if not self._digitisation:
            self._digitisation = Digitisation(
                root=self.root, collection=self.collection
            )

        return self._digitisation

    @property
    def item(self) -> Item:
        if not self._item:
            self._item = Item(
                root=self.root,
                issue_code=self.issue.issue_code,
                digitisation=self.digitisation,
                ingest=self.ingest,
                collection=self.collection,
                newspaper=self.newspaper,
                meta=self.meta,
            )

        return self._item

    @property
    def newspaper(self) -> Newspaper:
        if not self._newspaper:
            self._newspaper = Newspaper(
                root=self.root,
                collection=self.collection,
                meta=self.meta,
                jisc_papers=self.jisc_papers,
            )
        return self._newspaper


class Archive:
    """Manage extracting information from a ZIP archive.

    The ``Archive`` class represents a zip archive of XML files. The class is used
    to extract information from a ZIP archive, and it contains several methods
    to process the data contained in the archive.

    !!! info "`open(Archive)` context manager"

        Archive can be opened with a context manager, which creates a meta
        object, with timings for the object. When closed, it will save the
        meta JSON to the correct paths.

    Attributes:
        path: The path to the zip archive.
        collection: The collection of the XML files in the archive. Default is "".
        report: The file path of the report file for the archive.
        report_id: The report ID for the archive. If not provided, a random UUID is
            generated.
        report_parent: The parent directory of the report file for the archive.
        jisc_papers: A DataFrame of JISC papers.
        size: The size of the archive, in human-readable format.
        size_raw: The raw size of the archive, in bytes.
        roots: The root elements of the XML documents contained in the archive.
        meta: Metadata about the archive, such as its path, size, and number of contents.

    Raises:
        RuntimeError: If the ``path`` does not exist.
    """

    def __init__(
        self,
        path: str | Path,
        collection: str = "",
        report_id: str | None = None,
        jisc_papers: pd.DataFrame | None = None,
    ):
        """Constructor method."""

        self.path: Path = Path(path)

        if not self.path.exists():
            raise RuntimeError("Path does not exist.")

        self.size: str | float = get_size_from_path(self.path)
        self.size_raw: str | float = get_size_from_path(self.path, raw=True)
        self.zip_file: zipfile.ZipFile = zipfile.ZipFile(self.path)
        self.collection: str = collection
        self.roots: Generator[ET.Element, None, None] = self.get_roots()

        self.meta: dotdict = dotdict(
            path=str(self.path),
            bytes=self.size_raw,
            size=self.size,
            contents=len(self.filelist),
        )

        if not report_id:
            self.report_id: str = str(uuid.uuid4())
        else:
            self.report_id = report_id

        self.jisc_papers: pd.DataFrame = jisc_papers
        self.report_parent: Path = Path(f"{REPORT_DIR}/{self.report_id}")
        self.report: Path = (
            self.report_parent / f"{self.path.stem.replace('_metadata', '')}.json"
        )

    def __len__(self):
        """The number of files inside the zip archive."""
        return len(self.filelist)

    def __str__(self):
        return f"{self.path} ({self.size})"

    def __repr__(self):
        return f'Archive <"{self.__str__()}", {self.size}>'

    def __enter__(self):
        self.meta.start = get_now()

    def __exit__(self, exc_type, exc_value, exc_tb):
        # In the future, we might want to handle exceptions here:
        # (exc_type, exc_value, exc_tb)

        self.meta.end = get_now()
        self.meta.seconds = (self.meta.end - self.meta.start).seconds
        self.meta.microseconds = (self.meta.end - self.meta.start).microseconds
        self.meta.start = str(self.meta.start)
        self.meta.end = str(self.meta.end)

        write_json(self.report, self.meta, add_created=False)

        if self.meta.item_paths:
            for item_doc in self.meta.item_paths:
                Path(item_doc).write_text(
                    "\n".join(
                        [
                            json.dumps(x)
                            for x in [
                                json.loads(x)
                                for x in {
                                    line
                                    for line in Path(item_doc).read_text().splitlines()
                                }
                                if x
                            ]
                        ]
                    )
                    + "\n"
                )

    @property
    def filelist(self):
        """Returns the list of files in the zip file"""
        return self.zip_file.filelist

    @property
    def documents(self):
        """Property that calls the ``get_documents`` method"""
        return self.get_documents()

    def get_roots(self) -> Generator[ET.Element, None, None]:
        """
        Yields the root elements of the XML documents contained in the archive.
        """
        for xml_file in tqdm(self.filelist, leave=False, colour="blue"):
            with self.zip_file.open(xml_file) as f:
                xml = f.read()
                if xml:
                    yield ET.fromstring(xml)

    def get_documents(self) -> Generator[Document, None, None]:
        """
        A generator that yields instances of the Document class for each XML
        file in the ZIP archive.

        It uses the `tqdm` library to display a progress bar in the terminal
        while it is running.

        If the contents of the ZIP file are not empty, the method creates an
        instance of the ``Document`` class by passing the root element of the XML
        file, the collection name, meta information about the archive, and the
        JISC papers data frame (if provided) to the constructor of the
        ``Document`` class. The instance of the ``Document`` class is then
        returned by the generator.

        Yields:
            ``Document`` class instance for each unzipped `XML` file.
        """
        for xml_file in tqdm(
            self.filelist,
            desc=f"{Path(self.zip_file.filename).stem} ({self.meta.size})",
            leave=False,
            colour="green",
        ):
            with self.zip_file.open(xml_file) as f:
                xml = f.read()
                if xml:
                    yield Document(
                        root=ET.fromstring(xml),
                        collection=self.collection,
                        meta=self.meta,
                        jisc_papers=self.jisc_papers,
                    )


class Collection:
    """
    A Collection represents a group of newspaper archives from any passed
    alto2txt metadata output.

    A Collection is initialised with a name and an optional pandas DataFrame
    of JISC papers. The `archives` property returns an iterable of the
    `Archive` objects within the collection.

    Attributes:
        name (str):
            Name of the collection (default "hmd")
        jisc_papers (pandas.DataFrame, optional):
            DataFrame of JISC papers, optional
    """

    def __init__(self, name: str = "hmd", jisc_papers: Optional[pd.DataFrame] = None):
        """Constructor method."""

        self.name: str = name
        self.jisc_papers: pd.DataFrame | None = jisc_papers
        self.dir: Path = Path(f"{MNT}/{self.name}-alto2txt/metadata")
        self.zip_files: list[Path] = sorted(
            list(self.dir.glob("*.zip")), key=lambda x: x.stat().st_size
        )
        self.zip_file_count: int = sum([1 for _ in self.dir.glob("*.zip")])
        self.report_id: str = str(uuid.uuid4())
        self.empty: bool = self.zip_file_count == 0

    @property
    def archives(self):
        for zip_file in tqdm(
            self.zip_files,
            total=self.zip_file_count,
            leave=False,
            desc=f"Processing {self.name}",
            colour="magenta",
        ):
            yield Archive(
                zip_file,
                collection=self.name,
                report_id=self.report_id,
                jisc_papers=self.jisc_papers,
            )


def route(
    collections: list,
    cache_home: str,
    mountpoint: str,
    jisc_papers_path: str,
    report_dir: str,
) -> None:
    """
    This function is responsible for setting up the path for the alto2txt
    mountpoint, setting up the JISC papers and routing the collections for
    processing.

    Args:
        collections: List of collection names
        cache_home: Directory path for the cache
        mountpoint: Directory path for the alto2txt mountpoint
        jisc_papers_path: Path to the JISC papers
        report_dir: Path to the report directory

    Returns:
        None
    """

    global CACHE_HOME
    global MNT
    global REPORT_DIR

    CACHE_HOME = cache_home
    REPORT_DIR = report_dir

    MNT = Path(mountpoint) if isinstance(mountpoint, str) else mountpoint
    if not MNT.exists():
        error(
            f"The mountpoint provided for alto2txt does not exist. "
            f"Either create a local copy or blobfuse it to "
            f"`{MNT.absolute()}`."
        )

    jisc_papers = setup_jisc_papers(path=jisc_papers_path)

    for collection_name in collections:
        collection = Collection(name=collection_name, jisc_papers=jisc_papers)

        if collection.empty:
            error(
                f"It looks like {collection_name} is empty in the "
                f"alto2txt mountpoint: `{collection.dir.absolute()}`."
            )

        for archive in collection.archives:
            with archive as _:
                [
                    (
                        doc.item.write_to_cache(),
                        doc.newspaper.write_to_cache(),
                        doc.issue.write_to_cache(),
                        doc.data_provider.write_to_cache(),
                        doc.ingest.write_to_cache(),
                        doc.digitisation.write_to_cache(),
                    )
                    for doc in archive.documents
                ]

    return
