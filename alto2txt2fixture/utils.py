from .settings import settings
from .log import error, info

from pathlib import Path
from numpy import array_split

import datetime
import pytz
import json


def get_now(as_str=False):
    now = datetime.datetime.now(tz=pytz.UTC)
    if as_str:
        return str(now)
    return now


NOW_str = get_now(True)


def get_key(x=dict(), on=[]):
    """See create_lookup"""
    return f"{'-'.join([str(x['fields'][y]) for y in on])}"


def create_lookup(lst=[], on=[]):
    return {get_key(x, on): x["pk"] for x in lst}


"""glob_filter is assists Python with filtering out any pesky, unwanted .DS_Store from macOS"""
glob_filter = lambda p: [
    x for x in get_path_from(p).glob("*") if not x.name.startswith(".")
]


def lock(lockfile):
    """Writes a '.' to a lockfile, after making sure the parent directory exists."""
    lockfile.parent.mkdir(parents=True, exist_ok=True)
    lockfile.write_text("")


def get_lockfile(collection, kind, dic):
    """
    Provides the path to any given lockfile, which controls whether any existing files should be overwritten or not.
    Arguments passed:
        `kind`: either "newspaper" or "issue" or "item"
        `dic`: a dictionary with required information for either `kind` passed
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


def get_chunked_zipfiles(path):
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


def clear_cache(dir):
    y = input(
        f"Do you want to erase the cache path now that the files have been generated ({Path(dir).absolute()})? [y/N]"
    )
    if y.lower() == "y":
        info("Clearing up the cache directory")
        [x.unlink() for x in Path(dir).glob("*.json")]


def get_path_from(p):
    """Guarantees that p is set to Path"""
    """TODO: This function also exists in alto2txt2fixture. Consolidate."""
    if isinstance(p, str):
        p = Path(p)

    if not isinstance(p, Path):
        raise RuntimeError(f"Unable to handle type: {type(p)}")

    return p


def get_size_from_path(p, raw=False):
    """Returns a nice string for any given file size. Accepts a string or a Path as first argument."""
    """TODO: This function also exists in alto2txt2fixture. Consolidate."""

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


def write_json(p, o, add_created=True):
    """
    Easier access to writing JSON files.
    Checks whether parent exists.
    Accepts a string or Path as first argument, and a dictionary as the second argument.
    The add_created argument will add created_at and updated_at to the dictionary's fields.
    (If created_at and updated_at already exist in the fields, they will be forcefully updated.)
    """

    _append_created_fields = lambda o: dict(
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

    return p.write_text(json.dumps(o))


def load_json(p, crash=False):
    """
    Easier access to reading JSON files.
    Accepts a string or Path as first argument.
    Returns the decoded JSON contents from the path.
    Returns an empty dictionary if file cannot be decoded and crash is set to False.
    """

    p = get_path_from(p)

    try:
        return json.loads(p.read_text())
    except json.JSONDecodeError:
        msg = f"Error: {p.read_text()}"
        error(msg, crash=crash)
        return {}


def list_json_files(p, drill=False, exclude_names=[], include_names=[]):
    """
    Easily access a list of all JSON files in a directory.
    Accepts a string or Path as first argument.
    Returns a list of JSON files.

    If drill is set to True, it will return any JSON files in child directories as well (i.e. recursive).
    If exclude_names or include_names are lists with values, the returned list will be filtered by either
        inclusive with include_names or exclusive on exclude_names.
    """

    q = "**/*.json" if drill else "*.json"
    files = get_path_from(p).glob(q)

    if exclude_names:
        return list({x for x in files if not x.name in exclude_names})
    elif include_names:
        return list({x for x in files if x.name in include_names})

    return files


def load_multiple_json(p, drill=False, filter_na=True, crash=False):
    """
    Easier loading of a bunch of JSON files.
    Accepts a string or Path as first argument.
    Returns a list of the decoded contents from the path.

    If drill is set to True, it will return any JSON files in child directories as well (i.e. recursive).
    If filter_na is set to True, it will filter out any empty elements.
    """

    files = list_json_files(p, drill=drill)
    content = [load_json(x, crash=crash) for x in files]
    return [x for x in content if x] if filter_na else content
