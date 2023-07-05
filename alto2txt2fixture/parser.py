import gc
import json
from pathlib import Path
from typing import Union

from tqdm import tqdm

from .utils import NOW_str


def fixtures(
    filelist: list = [],
    model: str = "",
    translate: dict = {},
    rename: dict = {},
    uniq_keys: dict = [],
) -> None:
    """
    Generates fixtures for a specified model using a list of files.

    This function takes a list of files and generates fixtures for a specified
    model.

    The fixtures can be used to populate a database or perform other
    data-related operations.

    Arguments:
        filelist (list): A list of files to process and generate fixtures from.
            model (str): The name of the model for which fixtures are
            generated.
        translate (dict): A nested dictionary representing the translation
            mapping for fields. The structure of the translator follows the
            format:

            .. code-block: json

                {
                    'part1': {
                        'part2': {
                            'translated_field': 'pk'
                        }
                    }
                }

            The translated fields will be used as keys, and their
            corresponding primary keys (obtained from the provided files) will
            be used as values in the generated fixtures.
        rename (dict): A nested dictionary representing the field renaming
            mapping. The structure of the dictionary follows the format:

            .. code-block: json

                {
                    'part1': {
                        'part2': 'new_field_name'
                    }
                }

            The fields specified in the dictionary will be renamed to the
            provided new field names in the generated fixtures.
        uniq_keys (dict): A list of fields that need to be considered for
            uniqueness in the fixtures. If specified, the fixtures will yield
            only unique items based on the combination of these fields.

    Returns:
        None: This function generates fixtures but does not return any value.
    """

    def uniq(filelist: list, keys: list = []):
        """
        Generates unique items from a list of files based on specified keys.

        This function takes a list of files and yields unique items based on a
        combination of keys. The keys are extracted from each file using the
        ``get_key_from`` function, and duplicate items are ignored.

        Arguments:
            filelist (list): A list of files from which unique items are
                generated.
            keys (list): A list of keys used for uniqueness. Each key specifies
                a field to be used for uniqueness checking in the generated
                items.

        Yields:
            item: A unique item from the filelist based on the specified keys.
        """

        def get_key_from(item: Path, x: str) -> str:
            """
            Retrieves a specific key from a file and returns its value.

            This function reads a file and extracts the value of a specified
            key. If the key is not found or an error occurs while processing
            the file, a warning is printed, and an empty string is returned.

            Arguments:
                item: The file from which the key is extracted.
                x: The key to be retrieved from the file.

            Returns:
                str: The value of the specified key from the file.
            """
            result = json.loads(item.read_text()).get(x, None)
            if not result:
                print(f"[WARN] Could not find key {x} in {item}")
                result = ""
            return result

        seen = set()
        for item in filelist:
            key = "-".join([get_key_from(item, x) for x in keys])

            if key not in seen:
                seen.add(key)
                yield item
            else:
                # Drop it if duplicate
                pass

    filelist = sorted(filelist, key=lambda x: str(x).split("/")[:-1])
    count = len(filelist)

    # Process JSONL
    if [x for x in filelist if ".jsonl" in x.name]:
        pk = 0
        # In the future, we might want to show progress here (tqdm or suchlike)
        for file in filelist:
            for line in file.read_text().splitlines():
                pk += 1
                line = json.loads(line)
                yield dict(
                    pk=pk,
                    model=model,
                    fields=dict(**get_fields(line, translate=translate, rename=rename)),
                )

        return
    else:
        # Process JSON
        pks = [x for x in range(1, count + 1)]

        if len(uniq_keys):
            uniq_files = list(uniq(filelist, uniq_keys))
            count = len(uniq_files)
            zipped = zip(uniq_files, pks)
        else:
            zipped = zip(filelist, pks)

        for x in tqdm(
            zipped, total=count, desc=f"{model} ({count:,} objs)", leave=False
        ):
            yield dict(
                pk=x[1],
                model=model,
                fields=dict(**get_fields(x[0], translate=translate, rename=rename)),
            )

        return


def reset_fixture_dir(output: str) -> None:
    """
    Resets the fixture directory by removing all JSON files inside it.

    This function takes a directory path (``output``) as input and removes all
    JSON files within the directory.

    Prior to removal, it prompts the user for confirmation to proceed. If the
    user confirms, the function clears the fixture directory by deleting the
    JSON files.

    Arguments:
        output (str): The directory path of the fixture directory to be reset.

    Raises:
        RuntimeError: If the ``output`` directory is not specified as a string.

    Returns:
        None.
    """

    if not isinstance(output, str):
        raise RuntimeError("`output` directory needs to be specified as a string.")

    output = Path(output)

    y = input(
        f"This command will automatically empty the fixture directory ({output.absolute()}). Do you want to proceed? [y/N]"
    )

    if not y.lower() == "y":
        output.mkdir(parents=True, exist_ok=True)
        return

    print("\nClearing up the fixture directory")

    # Ensure directory exists
    output.mkdir(parents=True, exist_ok=True)

    # Drop all JSON files
    [x.unlink() for x in Path(output).glob("*.json")]

    return


def get_translator(fields: list = [("", "", [])]) -> dict:
    """
    Converts a list of fields into a nested dictionary representing a
    translator.

    Arguments:
        fields (list): A list of tuples representing fields to be translated.
                       Each tuple should contain three elements:
                           - start: A string representing the starting field
                             name.
                           - finish: A string or list specifying the field(s)
                             to be translated. If it is a string, the
                             translated field will be a direct mapping of the
                             specified field in each item of the input list.
                             If it is a list, the translated field will be a
                             hyphen-separated concatenation of the specified
                             fields in each item of the input list.
                           - lst: A list of dictionaries representing the
                             items to be translated. Each dictionary should
                             contain the necessary fields for translation,
                             with the field names specified in the 'start'
                             parameter.

    Returns:
        dict: A nested dictionary representing the translator.
              The structure of the dictionary follows the format:
                  {
                      'part1': {
                          'part2': {
                              'translated_field': 'pk'
                          }
                      }
                  }

    Example:
        fields = [('start__field1', 'finish_field1', [{'fields': {'field1': 'translation1'}, 'pk': 1}])]
        translator = get_translator(fields)
        # Output: {'start': {'field1': {'translation1': 1}}}
    """
    _ = dict()
    for field in fields:
        start, finish, lst = field
        part1, part2 = start.split("__")
        if part1 not in _:
            _[part1] = {}
        if part2 not in _[part1]:
            _[part1][part2] = {}
        if isinstance(finish, str):
            _[part1][part2] = {o["fields"][finish]: o["pk"] for o in lst}
        elif isinstance(finish, list):
            _[part1][part2] = {
                "-".join([o["fields"][x] for x in finish]): o["pk"] for o in lst
            }

    return _


def get_fields(
    file: Union[Path, str, dict],
    translate: dict = {},
    rename: dict = {},
    allow_null: bool = False,
) -> dict:
    """
    Retrieves fields from a file and performs modifications and checks.

    This function takes a file (in various formats: `Path`, `str`, or `dict`)
    and processes its fields. It retrieves the fields from the file and
    performs modifications, translations, and checks on the fields.

    Arguments:
        file (Union[Path, str, dict]): The file from which the fields are
            retrieved.
        translate (dict): A nested dictionary representing the translation
            mapping for fields. The structure of the translator follows the format:

            .. code-block: json

                {
                    'part1': {
                        'part2': {
                            'translated_field': 'pk'
                        }
                    }
                }

            The translated fields will be used to replace the original fields
            in the retrieved fields.
        rename (dict): A nested dictionary representing the field renaming
            mapping. The structure of the dictionary follows the format:

            .. code-block: json

                {
                    'part1': {
                        'part2': 'new_field_name'
                    }
                }

            The fields specified in the dictionary will be renamed to the
            provided new field names in the retrieved fields.
        allow_null (bool): Determines whether to allow ``None`` values for
            relational fields. If set to ``True``, relational fields with
            missing values will be assigned ``None``. If set to ``False``, an
            error will be raised.

    Returns:
        dict: A dictionary representing the retrieved fields from the file,
        with modifications and checks applied.

    Raises:
        RuntimeError: If the file type is unsupported or if an error occurs
        during field retrieval or processing.
    """
    if isinstance(file, Path):
        try:
            fields = json.loads(file.read_text())
        except Exception as e:
            raise RuntimeError(f"Cannot interpret JSON ({e}): {file}")
    elif isinstance(file, str):
        if "\n" in file:
            raise RuntimeError("File has multiple lines.")
        try:
            fields = json.loads(file)
        except json.decoder.JSONDecodeError as e:
            raise RuntimeError(f"Cannot interpret JSON ({e}): {file}")
    elif isinstance(file, dict):
        fields = file
    else:
        raise RuntimeError(f"Cannot process type {type(file)}.")

    # Fix relational fields for any file
    for key in [key for key in fields.keys() if "__" in key]:
        parts = key.split("__")

        try:
            before = fields[key]
            if before:
                before = before.replace("---", "/")
                loc = translate.get(parts[0], {}).get(parts[1], {})
                fields[key] = loc.get(before)
                if fields[key] is None:
                    raise RuntimeError(
                        f"Cannot translate fields.{key} from {before}: {loc}"
                    )

        except AttributeError:
            if allow_null:
                fields[key] = None
            else:
                print(
                    "Content had relational fields, but something went wrong in parsing the data:"
                )
                print("file", file)
                print("fields", fields)
                print("KEY:", key)
                raise RuntimeError()

        new_name = rename.get(parts[0], {}).get(parts[1], None)
        if new_name:
            fields[new_name] = fields[key]
            del fields[key]

    fields["created_at"] = NOW_str
    fields["updated_at"] = NOW_str

    try:
        fields["item_type"] = str(fields["item_type"]).upper()
    except KeyError:
        pass

    try:
        if fields["ocr_quality_mean"] == "":
            fields["ocr_quality_mean"] = 0
    except KeyError:
        pass

    try:
        if fields["ocr_quality_sd"] == "":
            fields["ocr_quality_sd"] = 0
    except KeyError:
        pass

    return fields


def save_fixture(generator: list = [], prefix: str = "") -> None:
    """
    Saves fixtures generated by a generator to separate JSON files.

    This function takes a generator and saves the generated fixtures to
    separate JSON files. The fixtures are saved in batches, where each batch
    is determined by the ``max_elements_per_file`` parameter.

    Arguments:
        generator (list): A generator that yields the fixtures to be saved.
        prefix (str): A string prefix to be added to the file names of the
            saved fixtures.

    Returns:
        None: This function saves the fixtures to files but does not return
        any value.
    """
    internal_counter = 1
    counter = 1
    lst = []
    for item in generator:
        lst.append(item)
        internal_counter += 1
        if internal_counter > MAX_ELEMENTS_PER_FILE:
            Path(f"{OUTPUT}/{prefix}-{counter}.json").write_text(json.dumps(lst))

            # Save up some memory
            del lst
            gc.collect()

            # Re-instantiate
            lst = []
            internal_counter = 1
            counter += 1
    else:
        Path(f"{OUTPUT}/{prefix}-{counter}.json").write_text(json.dumps(lst))

    return


def parse(
    collections: list, cache_home: str, output: str, max_elements_per_file: int
) -> None:
    """
    Parses files from collections and generates fixtures for various models.

    This function processes files from the specified collections and generates
    fixtures for different models, such as ``newspapers.dataprovider``,
    ``newspapers.ingest``, ``newspapers.digitisation``,
    ``newspapers.newspaper``, ``newspapers.issue``, and ``newspapers.item``.
    It performs various steps, such as file listing, fixture generation,
    translation mapping, renaming fields, and saving fixtures to files.

    Arguments:
        collections (list): A list of collections from which files are
            processed and fixtures are generated.
        cache_home (str): The directory path where the collections are located.
        output (str): The directory path where the fixtures will be saved.
        max_elements_per_file (int): The maximum number of elements per file
            when saving fixtures.

    Returns:
        None: This function generates fixtures but does not return any value.
    """
    global CACHE_HOME
    global OUTPUT
    global MAX_ELEMENTS_PER_FILE

    CACHE_HOME = cache_home
    OUTPUT = output
    MAX_ELEMENTS_PER_FILE = max_elements_per_file

    # Set up output directory
    reset_fixture_dir(OUTPUT)

    # Get file lists
    print("\nGetting file lists...")

    def issues_in_x(x):
        return "issues" in str(x.parent).split("/")

    def newspapers_in_x(x):
        return not any(
            [
                condition
                for y in str(x.parent).split("/")
                for condition in [
                    "issues" in y,
                    "ingest" in y,
                    "digitisation" in y,
                    "data-provider" in y,
                ]
            ]
        )

    all_json = [
        x for y in collections for x in (Path(CACHE_HOME) / y).glob("**/*.json")
    ]
    all_jsonl = [
        x for y in collections for x in (Path(CACHE_HOME) / y).glob("**/*.jsonl")
    ]
    print(f"--> {len(all_json):,} JSON files altogether")
    print(f"--> {len(all_jsonl):,} JSONL files altogether")

    print("\nSetting up fixtures...")

    # Process data providers
    def data_provider_in_x(x):
        return "data-provider" in str(x.parent).split("/")

    data_provider_json = list(
        fixtures(
            model="newspapers.dataprovider",
            filelist=[x for x in all_json if data_provider_in_x(x)],
            uniq_keys=["name"],
        )
    )
    print(f"--> {len(data_provider_json):,} DataProvider fixtures")

    # Process ingest
    def ingest_in_x(x):
        return "ingest" in str(x.parent).split("/")

    ingest_json = list(
        fixtures(
            model="newspapers.ingest",
            filelist=[x for x in all_json if ingest_in_x(x)],
            uniq_keys=["lwm_tool_name", "lwm_tool_version"],
        )
    )
    print(f"--> {len(ingest_json):,} Ingest fixtures")

    # Process digitisation
    def digitisation_in_x(x):
        return "digitisation" in str(x.parent).split("/")

    digitisation_json = list(
        fixtures(
            model="newspapers.digitisation",
            filelist=[x for x in all_json if digitisation_in_x(x)],
            uniq_keys=["software"],
        )
    )
    print(f"--> {len(digitisation_json):,} Digitisation fixtures")

    # Process newspapers
    newspaper_json = list(
        fixtures(
            model="newspapers.newspaper",
            filelist=[file for file in all_json if newspapers_in_x(file)],
        )
    )
    print(f"--> {len(newspaper_json):,} Newspaper fixtures")

    # Process issue
    translate = get_translator(
        [("publication__publication_code", "publication_code", newspaper_json)]
    )
    rename = {"publication": {"publication_code": "newspaper_id"}}

    issue_json = list(
        fixtures(
            model="newspapers.issue",
            filelist=[file for file in all_json if issues_in_x(file)],
            translate=translate,
            rename=rename,
        )
    )
    print(f"--> {len(issue_json):,} Issue fixtures")

    # Create translator/clear up memory before processing items
    translate = get_translator(
        [
            ("issue__issue_identifier", "issue_code", issue_json),
            ("digitisation__software", "software", digitisation_json),
            ("data_provider__name", "name", data_provider_json),
            (
                "ingest__lwm_tool_identifier",
                ["lwm_tool_name", "lwm_tool_version"],
                ingest_json,
            ),
        ]
    )

    rename = {
        "issue": {"issue_identifier": "issue_id"},
        "digitisation": {"software": "digitisation_id"},
        "data_provider": {"name": "data_provider_id"},
        "ingest": {"lwm_tool_identifier": "ingest_id"},
    }

    save_fixture(newspaper_json, "Newspaper")
    save_fixture(issue_json, "Issue")

    del newspaper_json
    del issue_json
    gc.collect()

    print("\nSaving...")

    save_fixture(digitisation_json, "Digitisation")
    save_fixture(ingest_json, "Ingest")
    save_fixture(data_provider_json, "DataProvider")

    # Process items
    item_json = fixtures(
        model="newspapers.item",
        filelist=all_jsonl,
        translate=translate,
        rename=rename,
    )
    save_fixture(item_json, "Item")

    return
