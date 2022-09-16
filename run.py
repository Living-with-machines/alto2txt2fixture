from alto2txt2fixture import route, parse, clear_cache, parse_args, settings, show_setup


def run():
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
    parse(COLLECTIONS, settings.CACHE_HOME, OUTPUT, settings.MAX_ELEMENTS_PER_FILE)

    clear_cache(settings.CACHE_HOME)


if __name__ == "__main__":
    run()
