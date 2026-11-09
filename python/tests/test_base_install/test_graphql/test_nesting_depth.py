"""A query nested deeper than the server's limit is refused before it is parsed,
and the server keeps answering. Brackets inside strings, block strings and
comments are not nesting, so they do not count."""

import pytest
from raphtory import Graph
from utils import graphql_client

LIMIT = 128
DEEP = "{" * (LIMIT + 1)
SCORED = '{node: {presence: {op: IS_SOME, expr: {read: {property: {name: "score"}}}}}}'


def graph():
    g = Graph()
    g.add_node(1, "a", {"score": 1})
    g.add_node(1, "b")
    return g


def filtered(nots):
    """The nodes with a score, under `nots` levels of `not`."""
    tree = "{not: " * nots + SCORED + "}" * nots
    return (
        '{ graph(path: "g") { filter(expr: '
        + tree
        + ") { nodes { list { name } } } } }"
    )


def names(out):
    return sorted(n["name"] for n in out["graph"]["filter"]["nodes"]["list"])


def error_of(client, query):
    with pytest.raises(Exception) as error:
        client.query(query)
    return str(error.value)


def test_a_query_past_the_limit_is_refused_and_the_server_keeps_answering():
    with graphql_client(graph()) as client:
        assert f"nested deeper than {LIMIT} levels" in error_of(client, filtered(LIMIT))
        assert names(client.query(filtered(0))) == ["a"]
        assert names(client.query(filtered(100))) == ["a"]


@pytest.mark.parametrize(
    "path",
    [
        f'"{DEEP}"',
        f'"\\"{DEEP}"',
        f'"""{DEEP}"""',
        f'"""\\"""{DEEP}"""',
    ],
    ids=["string", "escaped quote", "block string", "escaped block quote"],
)
def test_brackets_in_strings_are_not_nesting(path):
    with graphql_client(graph()) as client:
        error = error_of(
            client, "{ graph(path: " + path + ") { nodes { list { name } } } }"
        )
        assert "nested deeper" not in error, error


def test_brackets_in_comments_are_not_nesting():
    with graphql_client(graph()) as client:
        out = client.query(
            "# " + DEEP + '\n{ graph(path: "g") { nodes { list { name } } } }'
        )
        assert sorted(n["name"] for n in out["graph"]["nodes"]["list"]) == ["a", "b"]
