import time
from typing import TypeVar, Callable


B = TypeVar("B")


def measure(name: str, f: Callable[..., B], *args, print_result: bool = True) -> B:
    start_time = time.time()
    result = f(*args)
    elapsed_time = time.time() - start_time

    time_unit = "s"
    elapsed_time_display = elapsed_time
    if elapsed_time < 1:
        time_unit = "ms"
        elapsed_time_display *= 1000

    if print_result:
        print(
            f"Running {name}: time: {elapsed_time_display:.3f}{time_unit}, result: {result}"
        )
    else:
        print(f"Running {name}: time: {elapsed_time_display:.3f}{time_unit}")

    return result
