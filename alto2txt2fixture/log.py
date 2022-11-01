from colorama import Fore, Style


def success(msg: str) -> None:
    print(f"{Fore.GREEN}{msg}{Style.RESET_ALL}")
    return


def info(msg: str) -> None:
    print(f"{Fore.CYAN}{msg}{Style.RESET_ALL}")
    return


def warning(msg: str) -> None:
    print(f"{Fore.YELLOW}Warning: {msg}{Style.RESET_ALL}")
    return


def error(msg: str, crash: bool = True, silent: bool = True) -> None:
    if crash and silent:
        print(f"{Fore.RED}{msg}{Style.RESET_ALL}")
        exit()
    elif crash:
        raise RuntimeError(msg) from None
    print(f"{Fore.RED}{msg}{Style.RESET_ALL}")

    return
