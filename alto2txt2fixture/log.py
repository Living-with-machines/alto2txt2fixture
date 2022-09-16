from colorama import Fore, Style


def success(msg):
    print(f"{Fore.GREEN}{msg}{Style.RESET_ALL}")


def info(msg):
    print(f"{Fore.CYAN}{msg}{Style.RESET_ALL}")


def warning(msg):
    print(f"{Fore.YELLOW}Warning: {msg}{Style.RESET_ALL}")


def error(msg, crash=True, silent=True):
    if crash and silent:
        print(f"{Fore.RED}{msg}{Style.RESET_ALL}")
        exit()
    elif crash:
        raise RuntimeError(msg) from None
    print(f"{Fore.RED}{msg}{Style.RESET_ALL}")
