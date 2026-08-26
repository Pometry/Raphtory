"""`outComponent`/`inComponent` accept an optional filter (`nodes` or `edges`) that scopes the walk
while returning full-graph nodes, so their other-layer neighbours remain queryable.

All tests are read-only over the same graph, so one module-scoped server serves them all.
Filter selects use `expr: {isValid: true}` as a pass-all edge expression.
"""

import pytest

from raphtory import Graph
from utils import graphql_client


@pytest.fixture(scope="module")
def client():
    # a --owns--> b --owns--> c   (ownership chain)
    # a --has--> x,  b --has--> y (other-layer satellites)
    g = Graph()
    g.add_edge(0, "a", "b", layer="owns")
    g.add_edge(0, "b", "c", layer="owns")
    g.add_edge(0, "a", "x", layer="has")
    g.add_edge(0, "b", "y", layer="has")
    with graphql_client(g) as c:
        yield c


def _names(client, node, field, select=""):
    arg = f"(select: {select})" if select else ""
    q = f'{{ graph(path: "g") {{ node(name: "{node}") {{ {field}{arg} {{ list {{ name }} }} }} }} }}'
    return sorted(n["name"] for n in client.query(q)["graph"]["node"][field]["list"])


def test_out_component_scoped_by_edge_layer(client):
    # unfiltered: a reaches everything downstream, across all layers
    assert _names(client, "a", "outComponent") == ["b", "c", "x", "y"]
    # scope the walk to the `owns` edge layer -> only the ownership chain
    assert _names(
        client,
        "a",
        "outComponent",
        '{edge: {layers: {names: ["owns"], expr: {isValid: true}}}}',
    ) == ["b", "c"]
    # scope to `has` -> only a's own satellite (owns edges are not followed)
    assert _names(
        client,
        "a",
        "outComponent",
        '{edge: {layers: {names: ["has"], expr: {isValid: true}}}}',
    ) == ["x"]


def test_out_component_scoped_by_node_filter(client):
    # step only through nodes b and c
    assert _names(
        client,
        "a",
        "outComponent",
        '{node: {name: {where: {isIn: {list: [{str: "b"}, {str: "c"}]}}}}}',
    ) == ["b", "c"]


def test_out_component_scoped_by_graph_layer_filter(client):
    # The graph-level layer filter (the `filter.Graph.layer(...)` equivalent).
    assert _names(
        client, "a", "outComponent", '{graph: {layers: {names: ["owns"]}}}'
    ) == ["b", "c"]
    assert _names(
        client, "a", "outComponent", '{graph: {layers: {names: ["has"]}}}'
    ) == ["x"]


def test_component_filter_and_or_combinators(client):
    # Node and edge filters compose with `and`/`or`.
    # node OR: step only through nodes named b or c
    assert _names(
        client,
        "a",
        "outComponent",
        '{node: {or: [{name: {where: {eq: {str: "b"}}}}, '
        '{name: {where: {eq: {str: "c"}}}}]}}',
    ) == ["b", "c"]
    # node AND: step through nodes that are neither x nor y
    assert _names(
        client,
        "a",
        "outComponent",
        '{node: {and: [{name: {where: {ne: {str: "x"}}}}, '
        '{name: {where: {ne: {str: "y"}}}}]}}',
    ) == ["b", "c"]
    # edge OR: follow owns OR has edges -> everything downstream
    assert _names(
        client,
        "a",
        "outComponent",
        '{edge: {or: [{layers: {names: ["owns"], expr: {isValid: true}}}, '
        '{layers: {names: ["has"], expr: {isValid: true}}}]}}',
    ) == ["b", "c", "x", "y"]
    # edge AND: owns AND valid
    assert _names(
        client,
        "a",
        "outComponent",
        '{edge: {and: [{layers: {names: ["owns"], expr: {isValid: true}}}, {isValid: true}]}}',
    ) == ["b", "c"]


def test_component_top_level_and_or_across_kinds(client):
    # Top-level `and`/`or` combining DIFFERENT filter kinds (node / edge / graph).
    # graph(owns layer) AND node(name != c) -> the owns walk, minus c
    assert _names(
        client,
        "a",
        "outComponent",
        '{and: [{graph: {layers: {names: ["owns"]}}}, '
        '{node: {name: {where: {ne: {str: "c"}}}}}]}',
    ) == ["b"]
    # graph(has layer) OR edge(owns layer) -> everything downstream
    assert _names(
        client,
        "a",
        "outComponent",
        '{or: [{graph: {layers: {names: ["has"]}}}, '
        '{edge: {layers: {names: ["owns"], expr: {isValid: true}}}}]}',
    ) == ["b", "c", "x", "y"]


def test_in_component_scoped_by_filters(client):
    assert _names(client, "c", "inComponent") == ["a", "b"]
    # node filter — step only through nodes a and b
    assert _names(
        client,
        "c",
        "inComponent",
        '{node: {name: {where: {isIn: {list: [{str: "a"}, {str: "b"}]}}}}}',
    ) == ["a", "b"]
    # edge filter
    assert _names(
        client,
        "c",
        "inComponent",
        '{edge: {layers: {names: ["owns"], expr: {isValid: true}}}}',
    ) == ["a", "b"]
    # graph (layer) filter
    assert _names(
        client, "c", "inComponent", '{graph: {layers: {names: ["owns"]}}}'
    ) == ["a", "b"]
    # no incoming `has` edges into c
    assert (
        _names(client, "c", "inComponent", '{graph: {layers: {names: ["has"]}}}') == []
    )


def test_component_respects_an_external_graph_filter(client):
    # A graph-level filter (here removing `x`) applied before the walk must be honoured — the
    # returned nodes are over that already-filtered graph.
    q = (
        '{ graph(path: "g") { filterNodes: filter(expr: {node: {name: {where: {ne: {str: "x"}}}}}) '
        '{ node(name: "a") { outComponent { list { name } } } } } }'
    )
    got = client.query(q)["graph"]["filterNodes"]["node"]["outComponent"]["list"]
    assert sorted(n["name"] for n in got) == ["b", "c", "y"]


def test_component_external_graph_filter_composed_with_select(client):
    # External graph filter (remove `c`) AND a component `select` (owns layer) compose: the
    # owns walk from `a` would reach b, c — but c is filtered out, leaving only b.
    q = (
        '{ graph(path: "g") { filterNodes: filter(expr: {node: {name: {where: {ne: {str: "c"}}}}}) '
        '{ node(name: "a") { outComponent(select: {edge: {layers: {names: ["owns"], expr: {isValid: true}}}}) '
        "{ list { name } } } } } }"
    )
    got = client.query(q)["graph"]["filterNodes"]["node"]["outComponent"]["list"]
    assert sorted(n["name"] for n in got) == ["b"]
