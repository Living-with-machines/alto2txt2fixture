from typing import Any, Final, Literal, NamedTuple, TypedDict

LEGACY_NEWSPAPER_OCR_FORMATS = Literal["bna", "hmd", "jisc", "lwm"]
NEWSPAPER_OCR_FORMATS = Literal["fmp", "bl-hmd", "jisc", "bl-lwm"]
PRIMARY_KEY: Final[str] = "pk"


class dotdict(dict):
    """dot.notation access to dictionary attributes"""

    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__


class FixtureDictBaseClass(TypedDict):
    """A base `dict` structure for `json` fixtures."""

    pk: int
    model: str


class FixtureDict(FixtureDictBaseClass):
    """A `dict` structure to ease use as a `json` database fixture.

    Attributes:
        pk: an id to uniquely define and query each entry
        model: what model a given record is for
        fields: a `dict` of record information conforming to `model` table
    """

    fields: dict[str, Any]


class DataProviderFieldsDict(TypedDict):
    """Fields within the `fields` portion of a `FixtureDict` to fit `lwmdb`.

    Attributes:
        name:
            The name of the collection data source. For `lwmdb` this should
            be less than 600 characters.
        code:
            A short slug-like, url-compatible (replace spaces with `-`)
            `str` to uniquely identify a data provider in `urls`, `api` calls etc.
            Designed to fit `NEWSPAPER_OCR_FORMATS` and any future slug-like codes.
        legacy_code:
            Either blank or a legacy slug-like, url-compatible (replace spaces with
            `-`) `str` originally used by `alto2txt` following
            `LEGACY_NEWSPAPER_OCR_FORMATSNEWSPAPER_OCR_FORMATS`.
        collection:
            Data Provider type.
        source_note:
            A sentence about the data provider.
    """

    name: str
    code: str | NEWSPAPER_OCR_FORMATS
    legacy_code: LEGACY_NEWSPAPER_OCR_FORMATS | None
    collection: str
    source_note: str | None


class DataProviderFixtureDict(FixtureDictBaseClass):
    """A `dict` structure for `DataProvider` sources in line with `lwmdb`.

    Attributes:
        pk: an id to uniquely define and query each entry
        model: what model a given record is for
        fields: a `DataProviderFieldsDict`
    """

    pk: int
    model: str
    fields: DataProviderFieldsDict


class TranslatorTuple(NamedTuple):
    """A named tuple of fields for translation.

    Attributes:
        start: A string representing the starting field name.
        finish: A string or list specifying the field(s) to be translated.
            If it is a string, the translated field
            will be a direct mapping of the specified field in
            each item of the input list.
            If it is a list, the translated field will be a
            hyphen-separated concatenation of the specified fields
            in each item of the input list.
        lst: A list of dictionaries representing the items to be
            translated. Each dictionary should contain the necessary
            fields for translation, with the field names specified in
            the `start` parameter.
    """

    start: str
    finish: str | list
    lst: list[dict]


class PlainTextFixtureFieldsDict(TypedDict):

    """A typed `dict` for PlainText Fixutres to match `lwmdb.newspapers.FullText` `model`

    Attributes:
        text:
            PlainText, potentially quite large newspaper articles.
            May have unusual or unreadable sequences of characters
            due to issues with Optical Character Recognition quality.
        item:
            An integer of the ForiengKey to the relate `lwmdb.Newspaper.Item` record.
        item_code:
            A unique `str` to match to `lwmdb.Newspaper.Item` record.
        text_path:
            Path of provided plaintext file. If `compressed_path` is
            `None`, this is the original relative `Path` of the `PlainText` file.
        text_compressed_path:
            The path of a compressed data source, the extraction of which provides
            access to `PlainText` files.
        text_fixture_path:
            Path to relavant generated FixtureFile (likely `json`).
        errors:
            Text to document errors in the process the text was created.
        info:
            Further information about the text, including potential OCR method.
        canonical:
            Whether this record is the default record for the related `Item`.
    """

    text: str
    item: int | None
    item_code: str | None
    text_path: str | None
    text_compressed_path: str | None
    text_fixture_path: str | None
    errors: str | None
    info: str | None
    canonical: bool


class PlainTextFixtureDict(FixtureDictBaseClass):
    """A `dict` structure for `Fulltext` sources in line with `lwmdb`.

    Attributes:
        model: `str` in `django` fixture spec to indicate what model a record is for
        fields: a `PlainTextFixtureFieldsDict` `dict` instance
        pk: `int` id for fixture record

    Note:
        No `pk` is included. By not specifying one, `django` should generate new onces during
        import.
    """

    pk: int
    model: str
    fields: PlainTextFixtureFieldsDict
