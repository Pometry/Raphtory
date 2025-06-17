import json
import re
import tempfile
import time
from typing import TypeVar, Callable
import os
import pytest
from functools import wraps
from raphtory.graphql import GraphServer
from raphtory import Graph, PersistentGraph

B = TypeVar("B")

PORT = 1737


def sort_dict_recursive(d) -> dict:
    if isinstance(d, dict):
        return {key: sort_dict_recursive(d[key]) for key in sorted(d)}
    elif isinstance(d, list):
        return [sort_dict_recursive(v) for v in d]
    else:
        return d


if "DISK_TEST_MARK" in os.environ:

    def with_disk_graph(func):
        def inner(graph):
            def inner2(graph, tmpdirname):
                g = graph.to_disk_graph(tmpdirname)
                func(g)

            func(graph)
            with tempfile.TemporaryDirectory() as tmpdirname:
                inner2(graph, tmpdirname)

        return inner

else:

    def with_disk_graph(func):
        return func


def with_disk_variants(init_fn, variants=None):
    if variants is None:
        variants = [
            "graph",
            "persistent_graph",
            "event_disk_graph",
            "persistent_disk_graph",
        ]

    def decorator(func):
        @wraps(func)
        def wrapper():
            check = func()
            assert callable(
                check
            ), f"Expected test function to return a callable, got {type(check)}"

            if "graph" in variants:
                g = init_fn(Graph())
                check(g)

            if "persistent_graph" in variants:
                pg = init_fn(PersistentGraph())
                check(pg)

            if "DISK_TEST_MARK" in os.environ:
                from raphtory import DiskGraphStorage

                with tempfile.TemporaryDirectory() as tmpdir:
                    if (
                        "event_disk_graph" in variants
                        or "persistent_disk_graph" in variants
                    ):
                        g = init_fn(Graph())
                        g.to_disk_graph(tmpdir)
                        disk = DiskGraphStorage.load_from_dir(tmpdir)

                        if "event_disk_graph" in variants:
                            check(disk.to_events())
                        if "persistent_disk_graph" in variants:
                            check(disk.to_persistent())

                        del disk

        return wrapper

    return decorator


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
    with GraphServer(tmp_work_dir, create_index=True).start(PORT) as server:
        client = server.get_client()
        client.send_graph(path="g", graph=graph)
        response = client.query(query)

        # Convert response to a dictionary if needed and compare
        response_dict = json.loads(response) if isinstance(response, str) else response
        assert (
            response_dict == expected_output
        ), f"left={sort_dict_recursive(response_dict)}\nright={sort_dict_recursive(expected_output)}"


def run_group_graphql_test(queries_and_expected_outputs, graph):
    tmp_work_dir = tempfile.mkdtemp()
    with GraphServer(tmp_work_dir, create_index=True).start(PORT) as server:
        client = server.get_client()
        client.send_graph(path="g", graph=graph)

        for query, expected_output in queries_and_expected_outputs:
            response = client.query(query)
            response_dict = json.loads(response) if isinstance(response, str) else response
            assert (
                sort_dict_recursive(response_dict) == sort_dict_recursive(expected_output)
            ), f"Expected:\n{sort_dict_recursive(expected_output)}\nGot:\n{sort_dict_recursive(response_dict)}"


def run_graphql_error_test(query, expected_error_message, graph):
    tmp_work_dir = tempfile.mkdtemp()
    with GraphServer(tmp_work_dir, create_index=True).start(PORT) as server:
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


def run_group_graphql_error_test(queries_and_expected_error_messages, graph):
    tmp_work_dir = tempfile.mkdtemp()
    with GraphServer(tmp_work_dir, create_index=True).start(PORT) as server:
        client = server.get_client()
        client.send_graph(path="g", graph=graph)
        for query, expected_error_message in queries_and_expected_error_messages:
            with pytest.raises(Exception) as excinfo:
                client.query(query)

            full_error_message = str(excinfo.value)
            match = re.search(r'"message":"(.*?)"', full_error_message)
            error_message = match.group(1) if match else ""
            assert (
                error_message == expected_error_message
            ), f"Expected '{expected_error_message}', but got '{error_message}'"


def assert_set_eq(left, right):
    """Check if two lists are the same set and same length"""
    assert len(left) == len(right)
    assert set(left) == set(right)
