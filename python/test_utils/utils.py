import contextlib
import json
import os
import re
import tempfile
import time
from datetime import datetime
from functools import wraps
from typing import Callable, TypeVar

import pytest
from dateutil import parser
from raphtory import Graph, PersistentGraph
from raphtory.graphql import GraphServer

B = TypeVar("B")


def sort_dict_recursive(d) -> dict:
    if isinstance(d, dict):
        return {key: sort_dict_recursive(d[key]) for key in sorted(d)}
    elif isinstance(d, list):
        return [sort_dict_recursive(v) for v in d]
    else:
        return d


def gql_sort_key(v):
    if isinstance(v, dict):
        direct = v.get("name", v.get("id", ""))
        if direct:
            return direct
        # sort by src/dst for edges
        src = gql_sort_key(v.get("src"))
        dst = gql_sort_key(v.get("dst"))
        if src:
            if dst:
                return [src, dst]
            else:
                return src
        else:
            return dst
    else:
        return ""


def sort_by_gql_name_or_id(d):
    if isinstance(d, dict):
        output = {}
        for key, value in d.items():
            if key == "ids":
                output[key] = sorted(value)
            else:
                output[key] = sort_by_gql_name_or_id(value)
        return output
    elif isinstance(d, list):
        return sorted((sort_by_gql_name_or_id(v) for v in d), key=gql_sort_key)
    else:
        return d


def with_variants(init_fn, variants=None):
    if variants is None:
        variants = [
            "graph",
            "persistent_graph",
        ]

    if isinstance(variants, str):
        variants = (variants,)
    else:
        variants = tuple(variants)

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


@contextlib.contextmanager
def graphql_server(config=None):
    with tempfile.TemporaryDirectory() as work_dir:
        with GraphServer(work_dir, config=config).start() as server:
            yield server


@contextlib.contextmanager
def graphql_client(graph=None, path="g", config=None):
    """Start a `GraphServer` in a temporary directory (removed on exit, after
    the server has stopped) and yield its client. When `graph` is given it is
    sent to the server at `path` first, ready to query.

    The single shared way tests stand up a server — use this (directly or via
    a fixture) instead of hand-rolling `tempfile` + `GraphServer` per test.
    """
    with graphql_server(config=config) as server:
        client = server.get_client()
        if graph is not None:
            client.send_graph(path=path, graph=graph)
        yield client


@contextlib.contextmanager
def remote_graph_server(name="g", graph_type="EVENT"):
    """Start a `GraphServer` (via [`graphql_server`]), create one empty graph
    on it, and yield `(RemoteGraph, RaphtoryClient)`. Callers populate the
    yielded handle themselves — the write-path counterpart of passing a
    pre-built graph to `graphql_server`."""
    with graphql_client() as client:
        yield client.new_graph(name, graph_type), client


@contextlib.contextmanager
def remote_graph(name="g", graph_type="EVENT"):
    """As [`remote_graph_server`], yielding just the `RemoteGraph` — the
    fixture nearly every test wants."""
    with remote_graph_server(name, graph_type) as (rg, _client):
        yield rg


def run_graphql_test(query, expected_output, graph, sort_output=False):
    with graphql_client(graph) as client:
        response = client.query(query)

        # Convert response to a dictionary if needed and compare
        response_dict = json.loads(response) if isinstance(response, str) else response
        if sort_output:
            response_dict = sort_by_gql_name_or_id(response_dict)
            expected_output = sort_by_gql_name_or_id(expected_output)
        assert (
            response_dict == expected_output
        ), f"left={sort_dict_recursive(response_dict)}\nright={sort_dict_recursive(expected_output)}"


def run_group_graphql_test(queries_and_expected_outputs, graph, sort_output=False):
    with graphql_client(graph) as client:

        for query, expected_output in queries_and_expected_outputs:
            response = client.query(query)
            response_dict = (
                json.loads(response) if isinstance(response, str) else response
            )
            if sort_output:
                response_dict = sort_by_gql_name_or_id(response_dict)
                expected_output = sort_by_gql_name_or_id(expected_output)
            assert (
                response_dict == expected_output
            ), f"Expected:\n{sort_dict_recursive(expected_output)}\nGot:\n{sort_dict_recursive(response_dict)}"


def run_graphql_error_test(query, expected_error_message, graph):
    with graphql_client(graph) as client:

        with pytest.raises(Exception) as excinfo:
            client.query(query)

        full_error_message = str(excinfo.value)
        assert (
            expected_error_message in full_error_message
        ), f"Expected '{expected_error_message}' in '{full_error_message}'"


def run_group_graphql_error_test(queries_and_expected_error_messages, graph):
    with graphql_client(graph) as client:
        for query, expected_error_message in queries_and_expected_error_messages:
            with pytest.raises(Exception) as excinfo:
                client.query(query)

            full_error_message = str(excinfo.value)
            assert (
                expected_error_message in full_error_message
            ), f"Expected '{expected_error_message}' in '{full_error_message}'"


def run_graphql_error_test_contains(query, expected_substrings, graph):
    with graphql_client(graph) as client:

        with pytest.raises(Exception) as excinfo:
            client.query(query)

        full_error_message = str(excinfo.value)

        for s in expected_substrings:
            assert (
                s in full_error_message
            ), f"expected to find {s!r} in {full_error_message!r}"


def run_graphql_compare_test(query_a, query_b, graph):
    with graphql_client(graph) as client:

        resp_a = client.query(query_a)
        resp_b = client.query(query_b)

        dict_a = json.loads(resp_a) if isinstance(resp_a, str) else resp_a
        dict_b = json.loads(resp_b) if isinstance(resp_b, str) else resp_b

        assert sort_dict_recursive(dict_a) == sort_dict_recursive(dict_b), (
            f"Query A != Query B\n"
            f"A={sort_dict_recursive(dict_a)}\n"
            f"B={sort_dict_recursive(dict_b)}"
        )


def assert_set_eq(left, right):
    """Check if two lists are the same set and same length"""
    assert len(left) == len(right)
    assert set(left) == set(right)


def assert_has_properties(entity, props):
    for k, v in props.items():
        actual = entity.properties.get(k)
        # Convert PyArrow arrays and other array-like objects to lists for comparison
        if hasattr(actual, "to_pylist"):
            actual = actual.to_pylist()
        elif hasattr(actual, "tolist"):
            actual = actual.tolist()
        assert actual == v


def assert_has_metadata(entity, props):
    for k, v in props.items():
        actual = entity.metadata.get(k)
        # Convert PyArrow arrays and other array-like objects to lists for comparison
        if hasattr(actual, "to_pylist"):
            actual = actual.to_pylist()
        elif hasattr(actual, "tolist"):
            actual = actual.tolist()
        assert actual == v, f"Expected metadata {k!r} to be {v!r}, but got {actual!r}"


def expect_unify_error(fn):
    with pytest.raises(BaseException) as e:
        # check the message
        fn()
    print(e.value)
    assert "Failed to unify props" in str(e.value)


def assert_in_all(haystack: str, needles):
    for n in needles:
        assert n in haystack, f"expected to find {n!r} in {haystack!r}"


# Needed because datetimes generated using .now() have sub millisecond precision which raphtory does not support.
# Equality checks are failing because of this (in assert_has_properties and assert_has_metadata).
def truncate_dt_to_ms(dt: datetime) -> datetime:
    return dt.replace(microsecond=(dt.microsecond // 1000) * 1000)
