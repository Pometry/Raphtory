import time
from typing import TypeVar, Callable

A = TypeVar('A')
B = TypeVar('B')


def measure(name: str, f: Callable[[A], B], arg: A) -> B:
    start_time = time.time()
    result = f(arg)
    elapsed_time = (time.time() - start_time) * 1000  # Convert to milliseconds

    if elapsed_time < 1000:
        print(f"Running query {name}: time: {elapsed_time:.3f}ms, result: {result}")
    else:
        elapsed_time /= 1000  # Convert to seconds
        print(f"Running query {name}: time: {elapsed_time:.3f}s, result: {result}")

    return result
