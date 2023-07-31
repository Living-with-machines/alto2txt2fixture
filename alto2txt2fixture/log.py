from colorama import Fore, Style


def success(msg: str) -> None:
    """Print ``msg`` in `colorama` `Force.GREEN` colour."""
    print(f"{Fore.GREEN}{msg}{Style.RESET_ALL}")
    return


def info(msg: str) -> None:
    """Print ``msg`` in `colorama` `Force.CYAN` colour."""
    print(f"{Fore.CYAN}{msg}{Style.RESET_ALL}")
    return


def warning(msg: str) -> None:
    """Print ``msg`` in `colorama` `Force.YELLOW` colour."""
    print(f"{Fore.YELLOW}Warning: {msg}{Style.RESET_ALL}")
    return


def error(msg: str, crash: bool = True, silent: bool = True) -> None:
    """Print ``msg`` in `colorama` `Force.RED` and `exit()`

    If `silent` `exit()` after call, else `raise` `RuntimeError` if ``crash=True``."""
    if crash and silent:
        print(f"{Fore.RED}{msg}{Style.RESET_ALL}")
        exit()
    elif crash:
        raise RuntimeError(msg) from None
    print(f"{Fore.RED}{msg}{Style.RESET_ALL}")

    return
