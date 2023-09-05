import os
from pathlib import Path

import typer
from rich.table import Table
from typing_extensions import Annotated

from .plaintext import (
    DEFAULT_EXTRACTED_SUBDIR,
    DEFAULT_PLAINTEXT_FIXTURE_OUTPUT,
    PlainTextFixture,
)
from .settings import DATA_PROVIDER_INDEX, SETUP_TITLE, settings
from .types import dotdict
from .utils import check_newspaper_collection_configuration, console, gen_fixture_tables

cli = typer.Typer(pretty_exceptions_show_locals=False)


@cli.command()
def plaintext(
    path: Annotated[Path, typer.Argument()],
    save_path: Annotated[Path, typer.Option()] = Path(DEFAULT_PLAINTEXT_FIXTURE_OUTPUT),
    data_provider_code: Annotated[str, typer.Option()] = "",
    extract_path: Annotated[Path, typer.Argument()] = Path(DEFAULT_EXTRACTED_SUBDIR),
) -> None:
    """Create a PlainTextFixture and save to `save_path`."""
    plaintext_fixture = PlainTextFixture(
        path=path,
        data_provider_code=data_provider_code,
        extract_subdir=extract_path,
        export_directory=save_path,
    )
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
