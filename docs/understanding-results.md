## The resulting file structure

!!! attention "The examples below follow standard settings"
    If you choose other settings for when you run the program, your output directory may look different from the information on this page.

### Reports

Reports are automatically generated with a unique hash as the overarching folder structure. Inside the `reports` directory, you’ll find a JSON file for each `alto2txt` directory (organised by NLP identifier).

The report structure, thus, looks like this:

![/img/output-report-dir.png](/img/output-report-dir.png)

The JSON file has some good troubleshooting information. You’ll find that the contents are structured as a Python `dictionary` (or JavaScript `Object`). Here is an example:

![/img/output-report-json.png](/img/output-report-json.png)

Here is an explanation of each of the keys in the dictionary:

| Key                 | Explanation                                                                                            | Data type         |
| ------------------- | ------------------------------------------------------------------------------------------------------ | ----------------- |
| `path`              | The input path for the zip file that is being converted.                                               | `string`          |
| `bytes`             | The size of the input zip file represented in bytes.                                                   | `integer`         |
| `size`              | The size of the input zip file represented in a human-readable string.                                 | `string`          |
| `contents`          | #TODO                                                                                                  | `integer`         |
| `start`             | Date and time when processing started (see also `end` below).                                          | `datestring`      |
| `newspaper_paths`   | #TODO                                                                                                  | `list` (`string`) |
| `publication_codes` | A list of the NLPs that are contained in the input zip file.                                           | `list` (`string`) |
| `issue_paths`       | A list of all the issue paths that are contained in the cache directory.                               | `list` (`string`) |
| `item_paths`        | A list of all the item paths that are contained in the cache directory.                                | `list` (`string`) |
| `end`               | Date and time when processing ended (see also `start` above).                                          | `datestring`      |
| `seconds`           | Seconds that the script spent interpreting the zip file (should be added to the `microseconds` below). | `integer`         |
| `microseconds`      | Microseconds that the script spent interpreting the zip file (should be added to the `seconds` above). | `integer`         |

### Fixtures

The most important output of the script is contained in the `fixtures` directory. This directory contains JSON files for all the different columns in the corresponding Django metadata database (i.e. `DataProvider`, `Digitisation`, `Ingest`, `Issue`, `Newspaper`, and `Item`). The numbering at the end of each file indicates the order of the files as they are divided into a maximum of `2e6` elements*:

![/img/output-fixtures-dir.png](/img/output-report-dir.png)

Each JSON file contains a Python-like `list` (JavaScript `Array`) of `dictionaries` (JavaScript `Objects`), which have a primary key (`pk`), the related database model (in the example below the Django `newspapers` app’s `newspaper` table), and a nested `dictionary`/`Object` which contains all the values for the database’s table entry:

![/img/output-fixtures-json.png](/img/output-report-json.png)

----

\* The maximum elements per file can be adjusted in the `settings.py` file’s `settings` object’s `MAX_ELEMENTS_PER_FILE` value.