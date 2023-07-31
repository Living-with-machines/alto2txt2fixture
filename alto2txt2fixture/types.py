from typing import Any, NamedTuple, TypedDict


class dotdict(dict):
    """dot.notation access to dictionary attributes"""

    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__


class FixtureDict(TypedDict):
    """A `dict` structure to ease use as a `json` database fixture.

    Attributes:
        pk: an id to uniquely define and query each entry
        model: what model a given record is for
        fields: a `dict` of record information conforming to ``model`` table
    """

    pk: int
    model: str
    fields: dict[str, Any]


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
