import datetime as dt
import json
from pathlib import Path

import numpy as np
import pandas as pd
import wget

OUTPUT = "./output/tables"
NOW = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f+00:00")
OVERWRITE = False

SAVED = []
NOT_FOUND_PLACES, NOT_FOUND_PAPERS, MANY_PAPERS = [], [], []


def test_place(x, rev):
    try:
        return rev.at[x, "place__pk"]
    except KeyError:
        if x not in NOT_FOUND_PLACES:
            NOT_FOUND_PLACES.append(x)
            print(f"Warning: Could not find {x} in place_table")
        return ""


def test_paper(x, rev):
    try:
        value = rev.at[x, "pk"]
        if not isinstance(value, np.int64) and len(value) > 1:
            if x not in MANY_PAPERS:
                MANY_PAPERS.append(x)
                print(f"Warning: {len(value)} newspapers found with NLP {x} -- keeping first")
                value = value.to_list()[0]
        return value
    except KeyError:
        if x not in NOT_FOUND_PAPERS:
            NOT_FOUND_PAPERS.append(x)
            print(f"Warning: Could not find NLP {x} in newspaper data")
        return ""


def correct_dict(o: dict) -> list:
    """Returns a list with corrected data from a provided dictionary."""
    return [(k, v[0], v[1]) for k, v in o.items() if not v[0].startswith("Q")] + [(k, v[1], v[0]) for k, v in o.items() if v[0].startswith("Q")]


def get_list(x):
    """Get a list from a string, which contains <SEP> as separator. If no
    string is encountered, the function returns an empty list."""
    return x.split("<SEP>") if isinstance(x, str) else []


# Set up files -- remote and local

# Note: FILES["Newspaper-1"]["remote"] has a generated access token from the
# Azure storage space. It will expire in December 2023 and will need to be
# renewed/relinked before then.
FILES = {
    "mitchells": {
        "remote": "https://bl.iro.bl.uk/downloads/da65047c-4d62-4ab7-946f-8e61e5f6f331?locale=en",
        "local": Path("cache/Mitchell_1846_1920.csv")
    },
    "dict_admin_counties": {
        "remote": "https://zooniversedata.blob.core.windows.net/downloads/Gazetteer-files/dict_admin_counties.json",
        "local": Path("cache/dict_admin_counties.json")
    },
    "dict_countries": {
        "remote": "https://zooniversedata.blob.core.windows.net/downloads/Gazetteer-files/dict_countries.json",
        "local": Path("cache/dict_countries.json")
    },
    "dict_historic_counties": {
        "remote": "https://zooniversedata.blob.core.windows.net/downloads/Gazetteer-files/dict_historic_counties.json",
        "local": Path("cache/dict_historic_counties.json")
    },
    "nlp_loc_wikidata_concat": {
        "remote": "https://zooniversedata.blob.core.windows.net/downloads/Gazetteer-files/nlp_loc_wikidata_concat.csv",
        "local": Path("cache/nlp_loc_wikidata_concat.csv")
    },
    "wikidata_gazetteer_selected_columns": {
        "remote": "https://zooniversedata.blob.core.windows.net/downloads/Gazetteer-files/wikidata_gazetteer_selected_columns.csv",
        "local": Path("cache/wikidata_gazetteer_selected_columns.csv")
    },
    "linking": {
        "remote": "https://zooniversedata.blob.core.windows.net/downloads/Mitchells/newspapers_overview_with_links_JISC_NLPs.csv",
        "local": Path("cache/linking.csv")
    },
    "Newspaper-1": {
        "remote": "https://metadatadbfixtures.blob.core.windows.net/files/json-may-2023/Newspaper-1.json?sv=2021-10-04&spr=https%2Chttp&st=2023-05-31T13%3A58%3A49Z&se=2023-12-01T14%3A58%3A00Z&sr=b&sp=r&sig=XIxiPMSEfN9IYNwiR2UBsJp1XrcBg9AZAjD6I%2BJr6O0%3D",
        "local": Path("cache/Newspaper-1.json")
    },
}


def run() -> None:
    # Create parents for local files
    [FILES[k]["local"].parent.mkdir(parents=True, exist_ok=True) for k in FILES.keys()]

    # Describe whether local file exists
    for k in FILES.keys():
        FILES[k]["exists"] = FILES[k]["local"].exists()

    # Download non-existing files
    files_to_download = [(v["remote"], v["local"], v["exists"]) for v in FILES.values() if not v["exists"] or OVERWRITE]
    for url, out, exists in files_to_download:
        Path(out).unlink() if exists else None
        print(f"Downloading {out}")
        wget.download(url=url, out=out)
        print()

    # Create the output directory (defined in OUTPUT)
    OUTPUT = Path(OUTPUT)
    OUTPUT.mkdir(exist_ok=True, parents=True)

    # Read all the Wikidata Q values from Mitchells
    mitchells_df = pd.read_csv(FILES["mitchells"]["local"], index_col=0)
    mitchell_wikidata_mentions = sorted(list(mitchells_df.PLACE_PUB_WIKI.unique()), key=lambda x: int(x.replace("Q", "")))

    # Set up wikidata_gazetteer
    gaz_cols = ["wikidata_id", "english_label", "latitude", "longitude", "geonamesIDs"]
    wikidata_gazetteer = pd.read_csv(FILES["wikidata_gazetteer_selected_columns"]["local"], usecols=gaz_cols)
    wikidata_gazetteer.rename({
                    "wikidata_id": "place_wikidata_id",
                    "english_label": "place_label",
                    "geonamesIDs": "geonames_ids",
                },
                axis=1, inplace=True
            )

    # Read in + fix all dictionaries
    dict_historic_counties = json.loads(Path(FILES["dict_historic_counties"]["local"]).read_text())
    dict_admin_counties = json.loads(Path(FILES["dict_admin_counties"]["local"]).read_text())
    dict_countries = json.loads(Path(FILES["dict_countries"]["local"]).read_text())
    dict_historic_counties = correct_dict(dict_historic_counties)
    dict_admin_counties = correct_dict(dict_admin_counties)
    dict_countries = correct_dict(dict_countries)

    # Create assisting frames
    historical_counties_df = pd.DataFrame(
        dict_historic_counties,
        columns=["place_wikidata_id", "hcounty_label", "hcounty_wikidata_id"],
    )
    admin_county_df = pd.DataFrame(
        dict_admin_counties,
        columns=[
            "place_wikidata_id",
            "admin_county_label",
            "admin_county_wikidata_id",
        ],
    )
    countries_df = pd.DataFrame(
        dict_countries,
        columns=["place_wikidata_id", "country_label", "country_wikidata_id"],
    )

    wikidata_gazetteer = wikidata_gazetteer[wikidata_gazetteer.place_wikidata_id.isin(mitchell_wikidata_mentions)].sort_values("place_wikidata_id")
    wikidata_gazetteer["place_pk"] = np.arange(1, len(wikidata_gazetteer) + 1)
    wikidata_gazetteer = wikidata_gazetteer[
                ["place_pk"] + [x for x in wikidata_gazetteer.columns if not x == "place_pk"]
            ]

    # Merge wikidata_gazetteer with all the assisting frames (and rename the
    # resulting columns)
    wikidata_gazetteer = pd.merge(
        wikidata_gazetteer, historical_counties_df, on="place_wikidata_id", how="left"
    )
    wikidata_gazetteer = pd.merge(
        wikidata_gazetteer, admin_county_df, on="place_wikidata_id", how="left"
    )
    wikidata_gazetteer = pd.merge(
        wikidata_gazetteer, countries_df, on="place_wikidata_id", how="left"
    )

    wikidata_gazetteer.rename(
        {
            "admin_county_label": "admin_county__label",
            "admin_county_wikidata_id": "admin_county__wikidata_id",
            "hcounty_label": "historic_county__label",
            "hcounty_wikidata_id": "historic_county__wikidata_id",
            "country_label": "country__label",
            "country_wikidata_id": "country__wikidata_id",
        },
        axis=1,
        inplace=True,
    )

    # Split back up into dataframes specific for the tables
    historic_county_table = (
        wikidata_gazetteer[["historic_county__label", "historic_county__wikidata_id"]]
        .drop_duplicates()
        .copy()
    )
    historic_county_table = historic_county_table.replace({"": np.nan}).dropna()
    historic_county_table["historic_county__pk"] = np.arange(
        1, len(historic_county_table) + 1
    )

    admin_county_table = (
        wikidata_gazetteer[["admin_county__label", "admin_county__wikidata_id"]]
        .drop_duplicates()
        .copy()
    )
    admin_county_table = admin_county_table.replace({"": np.nan}).dropna()
    admin_county_table["admin_county__pk"] = np.arange(
        1, len(admin_county_table) + 1
    )

    country_table = (
        wikidata_gazetteer[["country__label", "country__wikidata_id"]]
        .drop_duplicates()
        .copy()
    )
    country_table = country_table.replace({"": np.nan}).dropna()
    country_table["country__pk"] = np.arange(1, len(country_table) + 1)

    # Set up place_table from wikidata_gazetteer
    place_table = wikidata_gazetteer.copy()

    place_table = (
        pd.merge(
            place_table,
            historic_county_table,
            on=["historic_county__label", "historic_county__wikidata_id"],
            how="left",
        )
        .drop(["historic_county__label", "historic_county__wikidata_id"], axis=1)
        .rename({"historic_county__pk": "historic_county_id"}, axis=1)
    )

    place_table = (
        pd.merge(
            place_table,
            admin_county_table,
            on=["admin_county__label", "admin_county__wikidata_id"],
            how="left",
        )
        .drop(["admin_county__label", "admin_county__wikidata_id"], axis=1)
        .rename({"admin_county__pk": "admin_county_id"}, axis=1)
    )

    place_table = (
        pd.merge(
            place_table,
            country_table,
            on=["country__label", "country__wikidata_id"],
            how="left",
        )
        .drop(["country__label", "country__wikidata_id"], axis=1)
        .rename({"country__pk": "country_id"}, axis=1)
    )

    place_table.fillna("", inplace=True)
    place_table.set_index("place_pk", inplace=True)
    place_table.rename({"place_label": "label", "place_wikidata_id": "wikidata_id"}, axis=1, inplace=True)
    place_table["historic_county_id"] = place_table["historic_county_id"].replace(r'^\s*$', 0, regex=True).astype(int).replace(0, "")
    place_table["admin_county_id"] = place_table["admin_county_id"].replace(r'^\s*$', 0, regex=True).astype(int).replace(0, "")
    place_table["country_id"] = place_table["country_id"].replace(r'^\s*$', 0, regex=True).astype(int).replace(0, "")
    place_table.index.rename("pk", inplace=True)
    place_table.rename({
        "historic_county_id": "historic_county",
        "admin_county_id": "admin_county",
        "country_id": "country"
    }, axis=1, inplace=True)

    historic_county_table.set_index("historic_county__pk", inplace=True)
    historic_county_table.rename({x: x.split("__")[1] for x in historic_county_table.columns}, axis=1, inplace=True)
    historic_county_table.index.rename("pk", inplace=True)

    admin_county_table.set_index("admin_county__pk", inplace=True)
    admin_county_table.rename({x: x.split("__")[1] for x in admin_county_table.columns}, axis=1, inplace=True)
    admin_county_table.index.rename("pk", inplace=True)

    country_table.set_index("country__pk", inplace=True)
    country_table.rename({x: x.split("__")[1] for x in country_table.columns}, axis=1, inplace=True)
    country_table.index.rename("pk", inplace=True)

    # Adding created_at, updated_at to all the gazetteer tables
    place_table["created_at"] = NOW
    place_table["updated_at"] = NOW
    admin_county_table["created_at"] = NOW
    admin_county_table["updated_at"] = NOW
    historic_county_table["created_at"] = NOW
    historic_county_table["updated_at"] = NOW
    country_table["created_at"] = NOW
    country_table["updated_at"] = NOW

    # Save CSV files for gazetteer tables
    place_table.to_csv(OUTPUT / "gazetteer.Place.csv")
    admin_county_table.to_csv(OUTPUT / "gazetteer.AdminCounty.csv")
    historic_county_table.to_csv(OUTPUT / "gazetteer.HistoricCounty.csv")
    country_table.to_csv(OUTPUT / "gazetteer.Country.csv")
    SAVED.extend([OUTPUT / "gazetteer.Place.csv", OUTPUT / "gazetteer.AdminCounty.csv", OUTPUT / "gazetteer.HistoricCounty.csv", OUTPUT / "gazetteer.Country.csv"])

    # Fix up Mitchells (already loaded)
    mitchells_df["politics"] = mitchells_df.POLITICS.apply(get_list)
    mitchells_df["persons"] = mitchells_df.PERSONS.apply(get_list)
    mitchells_df["organisations"] = mitchells_df.ORGANIZATIONS.apply(get_list)
    mitchells_df["price"] = mitchells_df.PRICE.apply(get_list)

    mitchells_df.rename({
        "ID": "mpd_id",
        "TITLE": "title",
        "politics": "political_leaning_raw",
        "price": "price_raw",
        "YEAR": "year",
        "PLACE_PUB_WIKI": "place_of_publication_id",
        "ESTABLISHED_DATE": "date_established_raw",
        "PUBLISED_DATE": "day_of_publication_raw",
    }, axis=1, inplace=True)

    drop_cols = [
        "CHAIN_ID", "POLITICS", "PERSONS", "ORGANIZATIONS", "PRICE", "PLACE_PUB",
        "PLACE_PUB_COORD", "PLACES", "PLACES_TRES", "TEXT"
    ]
    mitchells_df.drop(columns=drop_cols, inplace=True)

    # Create derivative tables (from Mitchells) = political_leanings, prices,
    # issues
    political_leanings = sorted(list(set([y.strip() for x in mitchells_df.political_leaning_raw for y in x])))
    political_leanings_table = pd.DataFrame()
    political_leanings_table["political_leaning__pk"] = np.arange(
        1, len(political_leanings) + 1
    )
    political_leanings_table["political_leaning__label"] = political_leanings
    export = political_leanings_table.copy()
    export["created_at"] = NOW
    export["updated_at"] = NOW
    export.set_index("political_leaning__pk", inplace=True)
    export.index.rename("pk", inplace=True)
    export.rename({x: x.split("__")[1] if len(x.split("__")) > 1 else x for x in export.columns}, axis=1, inplace=True)
    export.to_csv(OUTPUT / "mitchells.PoliticalLeaning.csv")
    SAVED.append(OUTPUT / "mitchells.PoliticalLeaning.csv")

    prices = sorted(list(set([y.strip() for x in mitchells_df.price_raw for y in x])))
    prices_table = pd.DataFrame()
    prices_table["price__pk"] = np.arange(
        1, len(prices) + 1
    )
    prices_table["price__label"] = prices
    export = prices_table.copy()
    export["created_at"] = NOW
    export["updated_at"] = NOW
    export.set_index("price__pk", inplace=True)
    export.index.rename("pk", inplace=True)
    export.rename({x: x.split("__")[1] if len(x.split("__")) > 1 else x for x in export.columns}, axis=1, inplace=True)
    export.to_csv(OUTPUT / "mitchells.Price.csv")
    SAVED.append(OUTPUT / "mitchells.Price.csv")

    issues = sorted(list(mitchells_df.year.unique()))
    issues_table = pd.DataFrame()
    issues_table["issue__pk"] = np.arange(
        1, len(issues) + 1
    )
    issues_table["issue__year"] = issues
    export = issues_table.copy()
    export["created_at"] = NOW
    export["updated_at"] = NOW
    export.set_index("issue__pk", inplace=True)
    export.index.rename("pk", inplace=True)
    export.rename({x: x.split("__")[1] if len(x.split("__")) > 1 else x for x in export.columns}, axis=1, inplace=True)
    export.to_csv(OUTPUT / "mitchells.Issue.csv")
    SAVED.append(OUTPUT / "mitchells.Issue.csv")

    # Set up linking on Mitchells dataframe
    linking_df = pd.read_csv(FILES["linking"]["local"],
                             index_col=0,
                             dtype={"NLP": str},
                             usecols=[
                                "NLP",
                                "Title",
                                "AcquiredYears",
                                "Editions",
                                "EditionTitles",
                                "City",
                                "Publisher",
                                "UnavailableYears",
                                "Collection",
                                "UK",
                                "Complete",
                                "Notes",
                                "County",
                                "HistoricCounty",
                                "First date held",
                                "Publication title",
                                "link_to_mpd",
                            ])
    linking_df["NLP"] = linking_df.index

    linking_df.rename({"link_to_mpd": "mpd_id", "NLP": "newspaper"}, axis=1, inplace=True)

    # Link Mitchells with all the other data
    mitchells_df = pd.merge(mitchells_df, linking_df, on="mpd_id", how="inner")

    # Create entry_table
    entry_table = mitchells_df.copy()
    entry_table["place_of_circulation_raw"] = ""
    entry_table["publication_district_raw"] = ""
    entry_table["publication_county_raw"] = ""
    # TODO: What happened to the three columns above? (Check w Kaspar?)

    # Only keep relevant columns
    entry_table = entry_table[["title", "political_leaning_raw", "price_raw", "year", "date_established_raw", "day_of_publication_raw", "place_of_circulation_raw", "publication_district_raw", "publication_county_raw", "organisations", "persons", "place_of_publication_id", "newspaper"]]

    # Fix refs to political_leanings_table
    rev = political_leanings_table.set_index("political_leaning__label")
    entry_table["political_leanings"] = entry_table.political_leaning_raw.apply(lambda x: [rev.at[y, "political_leaning__pk"] for y in x])

    # Fix refs to prices_table
    rev = prices_table.set_index("price__label")
    entry_table["prices"] = entry_table.price_raw.apply(lambda x: [rev.at[y.strip(), "price__pk"] for y in x])

    # Fix refs to issues_table
    rev = issues_table.set_index("issue__year")
    entry_table["issue"] = entry_table.year.apply(lambda x: rev.at[x, "issue__pk"])

    # Fix refs to place_table
    rev = place_table.copy()
    rev["place__pk"] = rev.index
    rev.set_index("wikidata_id", inplace=True)
    entry_table["place_of_publication"] = entry_table.place_of_publication_id.apply(test_place, rev=rev)
    entry_table.drop(columns=["place_of_publication_id"], inplace=True)

    # Set up ref to newspapers
    rev = json.loads(FILES["Newspaper-1"]["local"].read_text())
    rev = [dict(pk=v["pk"], **v["fields"]) for v in rev]
    rev = pd.DataFrame(rev)
    rev.set_index("publication_code", inplace=True)
    entry_table["newspaper"] = entry_table.newspaper.str.zfill(7)
    entry_table["newspaper"] = entry_table.newspaper.apply(test_paper, rev=rev)

    # Create PK for entries
    entry_table["pk"] = np.arange(1, len(entry_table) + 1)

    # Sort columns in entries file
    entry_table = entry_table[["pk"] + [col for col in entry_table.columns if not col=="pk"]]

    # Add created_at, modified_at to entry_table
    entry_table["created_at"] = NOW
    entry_table["updated_at"] = NOW

    # Export entry_table
    entry_table.set_index("pk").to_csv(OUTPUT / "mitchells.Entry.csv")
    SAVED.append(OUTPUT / "mitchells.Entry.csv")

    # ###### NOW WE CAN EASILY CREATE JSON FILES
    for file in OUTPUT.glob("*.csv"):
        json_data = []
        df = pd.read_csv(file, index_col=0).fillna("")

        if "political_leanings" in df.columns:
            df["political_leanings"] = df["political_leanings"].apply(json.loads)
        if "prices" in df.columns:
            df["prices"] = df["prices"].apply(json.loads)

        model = file.stem.lower()

        for pk, row in df.iterrows():
            fields = row.to_dict()
            json_data.append({
                "pk": pk,
                "model": model,
                "fields": fields
            })

        Path(OUTPUT / f"{file.stem}.json").write_text(json.dumps(json_data))
        SAVED.append(OUTPUT / f"{file.stem}.json")

    print("Finished - saved files:")
    print("- " + "\n- ".join([str(x) for x in SAVED]))
