# alto2txt2fixture

<!--index-start-->

`alto2txt2fixture` is a standalone tool to convert [`alto2txt`](https://github.org/living-with-machines/alto2txt) `XML` output and other related datasets into `JSON` (and where feasible `CSV`) data with corresponding relational IDs to ease general use and ingestion into a relational database.

We target the the `JSON` produced for importing into [`lwmdb`](https://github.org/living-with-machines/lwmdb): a database built using the [`Django`](https://www.djangoproject.com/) `python` webframework database [`fixutre`](https://docs.djangoproject.com/en/4.2/topics/db/fixtures) structure.

## Installation and simple use

We provide a command line interface to process `alto2txt` `XML` files stored locally (or mounted via `azure` [`blobfuse`](https://github.com/Azure/azure-storage-fuse)), and for additional public data we automate a means of downloading those automatically.

### Installation

We recommend downloading a copy of the reposity or using `git clone`. From a local copy use [`poetry`](https://python-poetry.org/) to install dependencies:

```console
$ cd alto2txt2fixture
$ poetry install
```

If you would like to test, render documentation and/or contribute to the code included `dev` dependencies in a local install:

```console
$ poetry install --with dev
```

### Simple use

To processing newspaper metadata with a local copy of `alto2txt` `XML` results, it's easiest to have that data in the same folder as your `alto2txt2fixture` checkout and `poetry` installed folder. One arranged, you should be able to begin the `JSON` converstion with

```console
$ poetry run a2t2f-news
```

To generate related data in `JSON` and `CSV` form, assuming you have an internet collection and access to a `living-with-machines` `azure` account, the following will download related data into `JSON` and `CSV` files. The `JSON` results should be consistent with `lwmdb` tables for ease of import.

```console
$ poetry run a2t2f-adj
```

<!--index-end-->

## Documentation

More detailed documenation is available at https://living-with-machines.github.io/alto2txt2fixture/
