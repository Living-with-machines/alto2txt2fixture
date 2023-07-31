"""
Main entry point for the alto2txt2fixture module.

This module defines the run function which is the main driver for the entire
process.

It imports various functions from other modules and uses them to route and
parse alto2txt data.

The following steps are performed in the run function:

1.  Parses command line arguments using the parse_args function. If no
    arguments are provided, the default values are taken from the settings
    module.
2.  Prints a setup report to the console, showing the values of the relevant
    parameters.
3.  Calls the route function to route alto2txt data into subdirectories with
    structured files.
4.  Calls the parse function to parse the resulting JSON files.
5.  Calls the clear_cache function to clear the cache.

If the script is run as a main program (i.e., if the name of the script is
__main__), the ``run`` function is executed.
"""

from argparse import ArgumentParser

from alto2txt2fixture.parser import parse
from alto2txt2fixture.router import route
from alto2txt2fixture.settings import settings, show_setup
from alto2txt2fixture.utils import clear_cache


def parse_args(argv=None):
    parser = ArgumentParser()
    parser.add_argument(
        "-c",
        "--collections",
        nargs="+",
        help="<Optional> Set collections",
        required=False,
    )
    parser.add_argument(
        "-m",
        "--mountpoint",
        type=str,
        help="<Optional> Mountpoint",
        required=False,
    )
    parser.add_argument(
        "-o",
        "--output",
        type=str,
        help="<Optional> Set an output directory",
        required=False,
    )
    return parser.parse_args(argv)


def run() -> None:
    """
    The run function is the main function that starts the alto2txt2fixture
    process.

    It first calls parse_args to parse the command line arguments, which
    includes the ``collections``, ``output``, and ``mountpoint``. If any of
    these arguments are specified, they will be used, otherwise they will
    default to the values in the ``settings`` module.

    The ``show_setup`` function is then called to display the configurations
    being used.

    The ``route`` function is then called to route the alto2txt files into
    subdirectories with structured files.

    The ``parse`` function is then called to parse the resulting JSON files.

    Finally, the ``clear_cache`` function is called to clear the cache
    (pending the user's confirmation).
    """

    args = parse_args()

    if args.collections:
        COLLECTIONS = [x.lower() for x in args.collections]
    else:
        COLLECTIONS = settings.COLLECTIONS

    if args.output:
        OUTPUT = args.output.rstrip("/")
    else:
        OUTPUT = settings.OUTPUT

    if args.mountpoint:
        MOUNTPOINT = args.mountpoint.rstrip("/")
    else:
        MOUNTPOINT = settings.MOUNTPOINT

    show_setup(
        COLLECTIONS=COLLECTIONS,
        OUTPUT=OUTPUT,
        CACHE_HOME=settings.CACHE_HOME,
        MOUNTPOINT=MOUNTPOINT,
        JISC_PAPERS_CSV=settings.JISC_PAPERS_CSV,
        REPORT_DIR=settings.REPORT_DIR,
        MAX_ELEMENTS_PER_FILE=settings.MAX_ELEMENTS_PER_FILE,
    )

    # Routing alto2txt into subdirectories with structured files
    route(
        COLLECTIONS,
        settings.CACHE_HOME,
        MOUNTPOINT,
        settings.JISC_PAPERS_CSV,
        settings.REPORT_DIR,
    )

    # Parsing the resulting JSON files
    parse(
        COLLECTIONS,
        settings.CACHE_HOME,
        OUTPUT,
        settings.MAX_ELEMENTS_PER_FILE,
    )

    clear_cache(settings.CACHE_HOME)


if __name__ == "__main__":
    run()
