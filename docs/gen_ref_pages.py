"""Generate the code reference pages and navigation.

Copied from https://mkdocstrings.github.io/recipes/#bind-pages-to-sections-themselves
"""

from pathlib import Path

import mkdocs_gen_files

nav = mkdocs_gen_files.Nav()

PACKAGE_PATH: str = "."
DOCS_PATH_NAME: str = "docs"
TESTS_PATH_NAME: str = "tests"

for path in sorted(Path(PACKAGE_PATH).rglob("*.py")):
    if DOCS_PATH_NAME in str(path) or TESTS_PATH_NAME in str(path):
        continue
    module_path = path.relative_to(PACKAGE_PATH).with_suffix("")
    doc_path = path.relative_to(PACKAGE_PATH).with_suffix(".md")
    full_doc_path = Path("reference", doc_path)

    parts = tuple(module_path.parts)

    if parts[-1] == "__init__":
        parts = parts[:-1]
        doc_path = doc_path.with_name("index.md")
        full_doc_path = full_doc_path.with_name("index.md")
    # Commented out default `mkdocstrings` config excluding __main__.py
    # For context, quoting creator of `mkdocstrings`:
    # https://pawamoy.github.io/posts/somewhat-modern-python-development/
    # "I urge Python developers to expose their CLI entrypoint outside of
    # __main__, and to make it accept command line parameters"
    # elif parts[-1] == "__main__":
    #     continue

    nav[parts] = doc_path.as_posix()

    with mkdocs_gen_files.open(full_doc_path, "w") as fd:
        ident = ".".join(parts)
        fd.write(f"::: {ident}")

    mkdocs_gen_files.set_edit_path(full_doc_path, path)

with mkdocs_gen_files.open("reference/SUMMARY.md", "w") as nav_file:
    nav_file.writelines(nav.build_literate_nav())
