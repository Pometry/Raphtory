import json
import re
import tempfile
import time
from typing import TypeVar, Callable

import pytest

from raphtory.graphql import GraphServer

B = TypeVar("B")

PORT = 1737


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


def run_graphql_test(query, expected_output, graph):
    tmp_work_dir = tempfile.mkdtemp()
    with GraphServer(tmp_work_dir).start(PORT) as server:
        client = server.get_client()
        client.send_graph(path="g", graph=graph)

        response = client.query(query)

        # Convert response to a dictionary if needed and compare
        response_dict = json.loads(response) if isinstance(response, str) else response
        assert response_dict == expected_output


def run_graphql_error_test(query, expected_error_message, graph):
    tmp_work_dir = tempfile.mkdtemp()
    with GraphServer(tmp_work_dir).start(PORT) as server:
        client = server.get_client()
        client.send_graph(path="g", graph=graph)

        with pytest.raises(Exception) as excinfo:
            client.query(query)

        full_error_message = str(excinfo.value)
        match = re.search(r'"message":"(.*?)"', full_error_message)
        error_message = match.group(1) if match else ""

        assert (
            error_message == expected_error_message
        ), f"Expected '{expected_error_message}', but got '{error_message}'"
