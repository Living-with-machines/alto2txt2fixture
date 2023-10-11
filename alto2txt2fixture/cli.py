import os
from logging import WARNING
from pathlib import Path
from typing import Any, Callable, Final, get_args, get_type_hints

import typer
from rich.prompt import Confirm, Prompt
from rich.table import Table
from typing_extensions import Annotated

from .plaintext import (
    DEFAULT_EXTRACTED_SUBDIR,
    DEFAULT_INITIAL_PK,
    DEFAULT_MAX_PLAINTEXT_PER_FIXTURE_FILE,
    DEFAULT_PLAINTEXT_FIXTURE_OUTPUT,
    PlainTextFixture,
)
from .settings import DATA_PROVIDER_INDEX, SETUP_TITLE, settings
from .types import dotdict
from .utils import (
    COMPRESSED_PATH_DEFAULT,
    COMPRESSION_TYPE_DEFAULT,
    FILE_NAME_0_PADDING_DEFAULT,
    ArchiveFormatEnum,
    check_newspaper_collection_configuration,
    compress_fixture,
    console,
    copy_dict_paths,
    free_hd_space_in_GB,
    gen_fixture_tables,
    glob_path_rename_by_0_padding,
    logger,
)

cli = typer.Typer(pretty_exceptions_show_locals=False)

FILE_RENAME_TABLE_TITLE_DEFAULT: Final[str] = "Current to New File Names"


@cli.command()
def plaintext(
    path: Annotated[Path, typer.Argument(help="Path to raw plaintext files")],
    save_path: Annotated[
        Path, typer.Option(help="Path to save json export files")
    ] = Path(DEFAULT_PLAINTEXT_FIXTURE_OUTPUT),
    data_provider_code: Annotated[
        str, typer.Option(help="Data provider code use existing config")
    ] = "",
    extract_path: Annotated[
        Path, typer.Option(help="Folder to extract compressed raw plaintext to")
    ] = Path(DEFAULT_EXTRACTED_SUBDIR),
    initial_pk: Annotated[
        int, typer.Option(help="First primary key to increment json export from")
    ] = DEFAULT_INITIAL_PK,
    records_per_json: Annotated[
        int, typer.Option(help="Max records per json fixture")
    ] = DEFAULT_MAX_PLAINTEXT_PER_FIXTURE_FILE,
    digit_padding: Annotated[
        int, typer.Option(help="Padding '0's for indexing json fixture filenames")
    ] = FILE_NAME_0_PADDING_DEFAULT,
    compress: Annotated[bool, typer.Option(help="Compress json fixtures")] = False,
    compress_path: Annotated[
        Path, typer.Option(help="Folder to compress json fixtueres to")
    ] = Path(COMPRESSED_PATH_DEFAULT),
    compress_format: Annotated[
        ArchiveFormatEnum,
        typer.Option(case_sensitive=False, help="Compression format"),
    ] = COMPRESSION_TYPE_DEFAULT,
) -> None:
    """Create a PlainTextFixture and save to `save_path`."""
    plaintext_fixture = PlainTextFixture(
        path=path,
        data_provider_code=data_provider_code,
        extract_subdir=extract_path,
        export_directory=save_path,
        initial_pk=initial_pk,
        max_plaintext_per_fixture_file=records_per_json,
        json_0_file_name_padding=digit_padding,
        json_export_compression_format=compress_format,
        json_export_compression_subdir=compress_path,
    )
    plaintext_fixture.info()
    while (
        not plaintext_fixture.compressed_files
        and not plaintext_fixture.plaintext_provided_uncompressed
    ):
        try_another_compressed_txt_source: bool = Confirm.ask(
            f"No .txt files available from extract path: "
            f"{plaintext_fixture.trunc_extract_path_str}\n"
            f"Would you like to extract fixtures from a different path?",
            default="n",
        )
        if try_another_compressed_txt_source:
            new_extract_path: str = Prompt.ask("Please enter a new extract path")
            plaintext_fixture.path = Path(new_extract_path)
        else:
            return
        plaintext_fixture.info()
    plaintext_fixture.extract_compressed()
    plaintext_fixture.export_to_json_fixtures()
    if compress:
        plaintext_fixture.compress_json_exports()


@cli.command()
def rename(
    path: Annotated[Path, typer.Argument(help="Path to files to manage")],
    folder: Annotated[
        Path, typer.Option(help="Path under `path` for new files")
    ] = Path(),
    renumber: Annotated[
        bool, typer.Option(help="Show changes without applying")
    ] = False,
    regex: Annotated[str, typer.Option(help="Regex to filter files")] = "*.txt",
    padding: Annotated[
        int, typer.Option(help="Digits to pad file name")
    ] = FILE_NAME_0_PADDING_DEFAULT,
    prefix: Annotated[str, typer.Option(help="Prefix for new file names")] = "",
    dry_run: Annotated[bool, typer.Option(help="Show changes without applying")] = True,
    compress: Annotated[bool, typer.Option(help="Whether to compress files")] = False,
    compress_format: Annotated[
        ArchiveFormatEnum,
        typer.Option(case_sensitive=False, help="Compression format"),
    ] = COMPRESSION_TYPE_DEFAULT,
    compress_suffix: Annotated[
        str, typer.Option(help="Compressed file name suffix")
    ] = "",
    compress_folder: Annotated[
        Path, typer.Option(help="Optional folder to differ from renaming")
    ] = COMPRESSED_PATH_DEFAULT,
    delete_uncompressed: Annotated[
        bool, typer.Option(help="Delete unneeded files after compression")
    ] = False,
    log_level: Annotated[
        int, typer.Option(help="Set logging level for debugging")
    ] = WARNING,
    force: Annotated[
        bool, typer.Option("--force", help="Force run without prompt")
    ] = False,
) -> None:
    """Manage file names and compression."""
    logger.level = log_level
    folder_path: Path = Path(path) / folder
    compress_path: Path = Path(path) / compress_folder

    try:
        paths_dict: dict[os.PathLike, os.PathLike] = glob_path_rename_by_0_padding(
            path=path,
            output_path=folder,
            glob_regex_str=regex,
            padding=padding,
        )
        assert paths_dict
    except (ValueError, AssertionError, IndexError) as err:
        console.print(f"Error: '{err}'\nTried reading from path: '{path}'")
        raise typer.Abort()
    files_count: int = len(paths_dict)

    if not compress and not force:
        compress = Confirm.ask(
            f"Compress all ({files_count}) output file(s)?",
            default="n",
        )
    if compress and not force:
        compress_format = Prompt.ask(
            "Compression format",
            choices=list(str(format) for format in ArchiveFormatEnum),
            default=compress_format,
        )
    extra_info_dict: dict[str, int | os.PathLike] = {
        "rename_path": folder_path,
        "compress_path": compress_path,
        "HD Space (GB)": int(free_hd_space_in_GB()),
    }
    config_table: Table = func_table(
        rename,
        values=locals(),
        extra_dict=extra_info_dict,
    )
    console.print(config_table)

    file_names_table: Table = file_rename_table(
        paths_dict,
        compress_format=compress_format,
        title=FILE_RENAME_TABLE_TITLE_DEFAULT,
        prefix=prefix,
        renumber=renumber,
    )
    console.print(file_names_table)

    if dry_run:
        if not force:
            renumber = Confirm.ask(
                f"Copy {'and compress ' if compress else ''}"
                f"{files_count} files "
                f"from:\n\t'{path}'\nto:\n\t'{folder_path}'\n"
            )
            if not delete_uncompressed:
                delete_uncompressed = Confirm.ask(
                    f"Delete all uncompressed, renamed files "
                    f"in:\n'{folder_path}'\n"
                    f"after compression to '{compress_format}' format in:"
                    f"\n'{compress_path}'\n",
                    default="n",
                )
    if renumber:
        copy_dict_paths(paths_dict)
    if compress:
        for old_path, new_path in paths_dict.items():
            file_path: Path = Path(new_path) if renumber else Path(old_path)
            compress_fixture(
                file_path,
                output_path=compress_path,
                suffix=compress_suffix,
                format=compress_format,
            )
            if delete_uncompressed and renumber:
                console.print(f"Deleting {new_path}")
                Path(new_path).unlink()


def show_setup(clear: bool = True, title: str = SETUP_TITLE, **kwargs) -> None:
    """Generate a `rich.table.Table` for printing configuration to console."""
    if clear and os.name == "posix":
        os.system("clear")
    elif clear:
        os.system("cls")

    table = Table(title=title)

    table.add_column("Setting", justify="right", style="cyan", no_wrap=True)
    table.add_column("Value", style="magenta")

    for key, value in kwargs.items():
        table.add_row(str(key), str(value))

    console.print(table)
    return


def show_fixture_tables(
    run_settings: dotdict = settings,
    print_in_call: bool = True,
    data_provider_index: str = DATA_PROVIDER_INDEX,
) -> list[Table]:
    """Print fixture tables specified in ``settings.fixture_tables`` in `rich.Table` format.

    Arguments:
        run_settings: `alto2txt2fixture` run configuration
        print_in_call: whether to print to console (will use ``console`` variable if so)
        data_provider_index: key to index `dataprovider` from ``NEWSPAPER_COLLECTION_METADATA``

    Returns:
        A `list` of `rich.Table` renders from configurations in ``run_settings.FIXTURE_TABLES``

    Example:
        ```pycon
        >>> fixture_tables: list[Table] = show_fixture_tables(
        ...     settings,
        ...     print_in_call=False)
        >>> len(fixture_tables)
        1
        >>> fixture_tables[0].title
        'dataprovider'
        >>> [column.header for column in fixture_tables[0].columns]
        ['pk', 'name', 'code', 'legacy_code', 'collection', 'source_note']
        >>> fixture_tables = show_fixture_tables(settings)
        <BLANKLINE>
        ...dataprovider...Heritage...│ bl_hmd...│ hmd...

        ```

    Note:
        It is possible for the example test to fail in different screen sizes. Try
        increasing the window or screen width of terminal used to check before
        raising an issue.
    """
    if run_settings.FIXTURE_TABLES:
        if "dataprovider" in run_settings.FIXTURE_TABLES:
            check_newspaper_collection_configuration(
                run_settings.COLLECTIONS,
                run_settings.FIXTURE_TABLES["dataprovider"],
                data_provider_index=data_provider_index,
            )
        console_tables: list[Table] = list(
            gen_fixture_tables(run_settings.FIXTURE_TABLES)
        )
        if print_in_call:
            for console_table in console_tables:
                console.print(console_table)
        return console_tables
    else:
        return []


def func_table(
    func: Callable, values: dict, title: str = "", extra_dict: dict[str, Any] = {}
) -> Table:
    """Geneate `rich` `Table` from `func` signature and `help` attr.

    Args:
        func:
            Function whose `args` and `type` hints will be converted
            to a table.

        values:
            `dict` of variables covered in `func` signature.
            `local()` often suffices.

        title:
            `str` for table title.

        extra_dict:
            A `dict` of additional rows to add to the table. For each
            `key`, `value` pair: if the `value` is a `tuple`, it will
            be expanded to match the `Type`, `Value`, and `Notes`
            columns; else the `Type` will be inferred and `Notes`
            left blank.

    Example:
        ```pycon
        >>> def test_func(
        ...     var_a: Annotated[str, typer.Option(help="Example")] = "Default"
        ... ) -> None:
        ...     test_func_table: Table = func_table(test_func, values=vars())
        ...     console.print(test_func_table)
        >>> if is_platform_win:
        ...     pytest.skip('fails on certain Windows root paths: issue #56')
        >>> test_func()
                   test_func config
        ┏━━━━━━━━━━┳━━━━━━┳━━━━━━━━━┳━━━━━━━━━┓
        ┃ Variable ┃ Type ┃ Value   ┃ Notes   ┃
        ┡━━━━━━━━━━╇━━━━━━╇━━━━━━━━━╇━━━━━━━━━┩
        │    var_a │ str  │ Default │ Example │
        └──────────┴──────┴─────────┴─────────┘

        ```
    """
    title = title if title else f"{func.__name__} config"
    func_signature: dict = get_type_hints(func, include_extras=True)
    table: Table = Table(title=title)
    table.add_column("Variable", justify="right", style="cyan")
    table.add_column("Type", style="yellow")
    table.add_column("Value", style="magenta")
    table.add_column("Notes")
    for var, info in func_signature.items():
        try:
            var_type, annotation = get_args(info)
            value: Any = values[var]
            if value in ("", ""):
                value = "''"
            table.add_row(str(var), var_type.__name__, str(value), annotation.help)
        except ValueError:
            continue
    for key, val in extra_dict.items():
        if isinstance(val, tuple):
            table.add_row(key, *val)
        else:
            table.add_row(key, type(val).__name__, str(val))
    return table


def file_rename_table(
    paths_dict: dict[os.PathLike, os.PathLike],
    compress_format: ArchiveFormatEnum = COMPRESSION_TYPE_DEFAULT,
    title: str = FILE_RENAME_TABLE_TITLE_DEFAULT,
    prefix: str = "",
    renumber: bool = True,
) -> Table:
    """Create a `rich.Table` of rename configuration.

    Args:
        paths_dict: dict[os.PathLike, os.PathLike],
            Original and renumbered `paths` `dict`
        compress_format:
            Which `ArchiveFormatEnum` for compression
        title:
            Title of returned `Table`
        prefix:
            `str` to add in front of every new path
        renumber:
            Whether an `int` in each path will be renumbered.

    """
    table: Table = Table(title=title)
    table.add_column("Current File Name", justify="right", style="cyan")
    table.add_column("New File Name", style="magenta")

    def final_file_name(name: os.PathLike) -> str:
        return (
            prefix
            + str(Path(name).name)
            + (f".{compress_format}" if compress_format else "")
        )

    for old_path, new_path in paths_dict.items():
        name: str = final_file_name(new_path if renumber else old_path)
        table.add_row(Path(old_path).name, name)
    return table
