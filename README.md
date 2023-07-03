# alto2txt2fixture

`alto2txt2fixture` is a standalone tool that converts our alto2txt metadata into JSON data with corresponding relational IDs and corrected data for easy ingestion into a relational database using, for example, Django.

## Documentation

You can find the documentation in the [`docs`](docs) directory, available [using MkDocs](https://www.mkdocs.org/getting-started/).

In short, however, the program should run automatically with the following two commands.

Install the dependencies:

```sh
$ poetry install
```

Run the tool for processing newspaper metadata:

```sh
$ poetry run a2t2f-news
```

In reality, you will need to mount the alto2txt files (or download them locally to your hard drive. The [documentation](docs) has details about how to do that.
