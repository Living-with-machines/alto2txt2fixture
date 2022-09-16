## Using `poetry` to run

The program should run automatically with the following command:

```sh
$ poetry run
```

Alternatively, if you want to add optional parameters and don’t want to use the standard `poetry` script to run, you can use the (somewhat convoluted) `poetry run run.py` and provide any optional parameters (see below). For example:

```sh
$ poetry run run.py --collections hmd
```

## Alternative: Run the script without poetry

If you find yourself in trouble with `poetry`, the program should run perfectly fine on its own as well. The same command, then, would be:

```sh
$ python run.py --collections hmd
```

## Optional parameters

The program has a number of optional parameters that you can choose to include or not. The table below describes each parameter, how to pass it to the program, and what its defaults are.

| Flag                  | Description                                                    | Default value        |
| --------------------- | -------------------------------------------------------------- | -------------------- |
| `-c`, `--collections` | Which collections to process in the mounted alto2txt directory | hmd, lwm, jisc, bna  |
| `-o`, `--output`      | Into which directory should the processed files be put?        | `./output/fixtures/` |
| `-m`, `--mountpoint`  | Where is the alto2txt directories mounted?                     | `./input/alto2txt/`  |

## Successfully running the program: An example

![/img/successfully-running.png](/img/successfully-running.png)
