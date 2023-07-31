from datetime import datetime
from pathlib import Path

import pandas as pd

from .patterns import PUBLICATION_CODE
from .settings import settings


def setup_jisc_papers(path: str = settings.JISC_PAPERS_CSV) -> pd.DataFrame:
    """
    Create a `DataFrame` with information in `JISC_PAPERS_CSV` in settings.

    Returns:
        `DataFrame` with all JISC titles.
    """

    if not Path(path).exists():
        raise RuntimeError(
            f"Could not find required JISC papers file. Put {Path(path).name} in {Path(path).parent} or correct the settings with a different path."
        )

    months = {
        "Jan": 1,
        "Feb": 2,
        "Mar": 3,
        "Apr": 4,
        "May": 5,
        "Jun": 6,
        "June": 6,
        "Jul": 7,
        "July": 7,
        "Aug": 8,
        "Sep": 9,
        "Sept": 9,
        "Oct": 10,
        "Nov": 11,
        "Dec": 12,
        "Dec.": 12,
    }

    jisc_papers = pd.read_csv(
        path,
        usecols=[
            "Newspaper Title",
            "NLP",
            "Abbr",
            "StartD",
            "StartM",
            "StartY",
            "EndD",
            "EndM",
            "EndY",
        ],
    )
    jisc_papers["start_date"] = jisc_papers.apply(
        lambda x: datetime(
            year=int(x.StartY),
            month=months[x.StartM.strip(".").strip()],
            day=int(x.StartD),
        ),
        axis=1,
    )
    jisc_papers["end_date"] = jisc_papers.apply(
        lambda x: datetime(
            year=int(x.EndY), month=months[x.EndM.strip(".").strip()], day=int(x.EndD)
        ),
        axis=1,
    )
    jisc_papers.drop(
        ["StartD", "StartM", "StartY", "EndD", "EndM", "EndY"],
        axis="columns",
        inplace=True,
    )
    jisc_papers.rename(
        {"Newspaper Title": "title", "NLP": "publication_code", "Abbr": "abbr"},
        axis=1,
        inplace=True,
    )
    jisc_papers["title"] = jisc_papers["title"].apply(
        lambda x: "The " + x[:-5] if x.strip()[-5:].lower() == ", the" else x
    )
    jisc_papers["publication_code"] = jisc_papers["publication_code"].apply(
        lambda x: str(x).zfill(7)
    )

    return jisc_papers


def get_jisc_title(
    title: str,
    issue_date: str,
    jisc_papers: pd.DataFrame,
    input_sub_path: str,
    publication_code: str,
    abbr: str | None = None,
) -> str:
    """
    Match a newspaper ``title`` with ``jisc_papers`` records.

    Takes an ``input_sub_path``, a ``publication_code``, and an (optional)
    abbreviation for any newspaper to locate the ``title`` in the
    ``jisc_papers`` `DataFrame`. ``jisc_papers`` is usually loaded via the
    ``setup_jisc_papers`` function.

    Args:
        title: target newspaper title
        issue_date: target newspaper issue_date
        jisc_papers: `DataFrame` of `jisc_papers` to match
        input_sub_path: path of files to narrow down query input_sub_path
        publication_code: unique codes to match newspaper records
        abbr: an optional abbreviation of the newspaper title

    Returns:
        Matched ``title`` `str` or ``abbr``.


    Returns:
        A string estimating the JISC equivalent newspaper title
    """

    # First option, search the input_sub_path for a valid-looking publication_code
    g = PUBLICATION_CODE.findall(input_sub_path)

    if len(g) == 1:
        publication_code = g[0]
        # Let's see if we can find title:
        title = (
            jisc_papers[
                jisc_papers.publication_code == publication_code
            ].title.to_list()[0]
            if jisc_papers[
                jisc_papers.publication_code == publication_code
            ].title.count()
            == 1
            else title
        )
        return title

    # Second option, look through JISC papers for best match (on publication_code if we have it, but abbr more importantly if we have it)
    if abbr:
        _publication_code = publication_code
        publication_code = abbr

    if jisc_papers.abbr[jisc_papers.abbr == publication_code].count():
        date = datetime.strptime(issue_date, "%Y-%m-%d")
        mask = (
            (jisc_papers.abbr == publication_code)
            & (date >= jisc_papers.start_date)
            & (date <= jisc_papers.end_date)
        )
        filtered = jisc_papers.loc[mask]
        if filtered.publication_code.count() == 1:
            publication_code = filtered.publication_code.to_list()[0]
            title = filtered.title.to_list()[0]
            return title

    # Last option: let's find all the possible titles in the jisc_papers for the abbreviation, and if it's just one unique title, let's pick it!
    if abbr:
        test = list({x for x in jisc_papers[jisc_papers.abbr == abbr].title})
        if len(test) == 1:
            return test[0]
        else:
            mask1 = (jisc_papers.abbr == publication_code) & (
                jisc_papers.publication_code == _publication_code
            )
            test1 = jisc_papers.loc[mask1]
            test1 = list({x for x in jisc_papers[jisc_papers.abbr == abbr].title})
            if len(test) == 1:
                return test1[0]

    # Fallback: if abbreviation is set, we'll return that:
    if abbr:
        # For these exceptions, see issue comment:
        # https://github.com/alan-turing-institute/Living-with-Machines/issues/2453#issuecomment-1050652587
        if abbr == "IPJL":
            return "Ipswich Journal"
        elif abbr == "BHCH":
            return "Bath Chronicle"
        elif abbr == "LSIR":
            return "Leeds Intelligencer"
        elif abbr == "AGER":
            return "Lancaster Gazetter, And General Advertiser For Lancashire West"

        return abbr

    raise RuntimeError(f"Title {title} could not be found.")
