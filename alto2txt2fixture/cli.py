import os
from pathlib import Path

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
    FILE_NAME_0_PADDING_DEFAULT,
    check_newspaper_collection_configuration,
    console,
    gen_fixture_tables,
)

cli = typer.Typer(pretty_exceptions_show_locals=False)


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
    )
    plaintext_fixture.info()
    while (
        not plaintext_fixture.compressed_files
        and not plaintext_fixture.plaintext_provided_uncompressed
    ):
        try_another_compressed_txt_source: bool = Confirm.ask(
            f"No .txt files available from extract path: "
            f"{plaintext_fixture.trunc_extract_path_str}\n"
            f"Would you like to extract fixtures from a different path?"
        )
        if try_another_compressed_txt_source:
            new_extract_path: str = Prompt.ask("Please enter a new extract path")
            plaintext_fixture.path = Path(new_extract_path)
        else:
            return
        plaintext_fixture.info()
    plaintext_fixture.extract_compressed()
    plaintext_fixture.export_to_json_fixtures()


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
