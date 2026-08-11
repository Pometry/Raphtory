"""`outComponent`/`inComponent` accept an optional filter (`nodes` or `edges`) that scopes the walk
while returning full-graph nodes, so their other-layer neighbours remain queryable."""

import tempfile

from raphtory import Graph
from raphtory.graphql import GraphServer

# a --owns--> b --owns--> c   (ownership chain)
# a --has--> x,  b --has--> y (other-layer satellites)
PASS_EDGE = "{isValid: true}"


def _served():
    g = Graph()
    g.add_edge(0, "a", "b", layer="owns")
    g.add_edge(0, "b", "c", layer="owns")
    g.add_edge(0, "a", "x", layer="has")
    g.add_edge(0, "b", "y", layer="has")
    work_dir = tempfile.mkdtemp()
    g.save_to_file(work_dir + "/g")
    return GraphServer(work_dir)


def _names(client, node, field, select=""):
    arg = f"(select: {select})" if select else ""
    q = '{ graph(path: "g") { node(name: "%s") { %s%s { list { name } } } } }' % (
        node,
        field,
        arg,
    )
    return sorted(n["name"] for n in client.query(q)["graph"]["node"][field]["list"])


def test_out_component_scoped_by_edge_layer():
    with _served().start() as server:
        client = server.get_client()
        # unfiltered: a reaches everything downstream, across all layers
        assert _names(client, "a", "outComponent") == ["b", "c", "x", "y"]
        # scope the walk to the `owns` edge layer -> only the ownership chain
        assert _names(
            client,
            "a",
            "outComponent",
            '{edges: {layers: {names: ["owns"], expr: %s}}}' % PASS_EDGE,
        ) == ["b", "c"]
        # scope to `has` -> only a's own satellite (owns edges are not followed)
        assert _names(
            client,
            "a",
            "outComponent",
            '{edges: {layers: {names: ["has"], expr: %s}}}' % PASS_EDGE,
        ) == ["x"]


def test_out_component_scoped_by_node_filter():
    with _served().start() as server:
        client = server.get_client()
        # step only through nodes b and c
        assert _names(
            client,
            "a",
            "outComponent",
            '{nodes: {node: {field: NODE_NAME, where: {isIn: {list: [{str: "b"}, {str: "c"}]}}}}}',
        ) == ["b", "c"]


def test_out_component_scoped_by_graph_layer_filter():
    # The graph-level layer filter (the `filter.Graph.layer(...)` equivalent).
    with _served().start() as server:
        client = server.get_client()
        assert _names(
            client, "a", "outComponent", '{graph: {layers: {names: ["owns"]}}}'
        ) == ["b", "c"]
        assert _names(
            client, "a", "outComponent", '{graph: {layers: {names: ["has"]}}}'
        ) == ["x"]


def test_component_filter_and_or_combinators():
    # Node and edge filters compose with `and`/`or`.
    with _served().start() as server:
        client = server.get_client()
        # node OR: step only through nodes named b or c
        assert _names(
            client,
            "a",
            "outComponent",
            '{nodes: {or: [{node: {field: NODE_NAME, where: {eq: {str: "b"}}}}, '
            '{node: {field: NODE_NAME, where: {eq: {str: "c"}}}}]}}',
        ) == ["b", "c"]
        # node AND: step through nodes that are neither x nor y
        assert _names(
            client,
            "a",
            "outComponent",
            '{nodes: {and: [{node: {field: NODE_NAME, where: {ne: {str: "x"}}}}, '
            '{node: {field: NODE_NAME, where: {ne: {str: "y"}}}}]}}',
        ) == ["b", "c"]
        # edge OR: follow owns OR has edges -> everything downstream
        assert _names(
            client,
            "a",
            "outComponent",
            '{edges: {or: [{layers: {names: ["owns"], expr: %s}}, {layers: {names: ["has"], expr: %s}}]}}'
            % (PASS_EDGE, PASS_EDGE),
        ) == ["b", "c", "x", "y"]
        # edge AND: owns AND valid
        assert _names(
            client,
            "a",
            "outComponent",
            '{edges: {and: [{layers: {names: ["owns"], expr: %s}}, %s]}}'
            % (PASS_EDGE, PASS_EDGE),
        ) == ["b", "c"]


def test_component_top_level_and_or_across_kinds():
    # Top-level `and`/`or` combining DIFFERENT filter kinds (node / edge / graph).
    with _served().start() as server:
        client = server.get_client()
        # graph(owns layer) AND node(name != c) -> the owns walk, minus c
        assert _names(
            client,
            "a",
            "outComponent",
            '{and: [{graph: {layers: {names: ["owns"]}}}, '
            '{nodes: {node: {field: NODE_NAME, where: {ne: {str: "c"}}}}}]}',
        ) == ["b"]
        # graph(has layer) OR edge(owns layer) -> everything downstream
        assert _names(
            client,
            "a",
            "outComponent",
            '{or: [{graph: {layers: {names: ["has"]}}}, {edges: {layers: {names: ["owns"], expr: %s}}}]}'
            % PASS_EDGE,
        ) == ["b", "c", "x", "y"]


def test_in_component_scoped_by_filters():
    with _served().start() as server:
        client = server.get_client()
        assert _names(client, "c", "inComponent") == ["a", "b"]
        # node filter — step only through nodes a and b
        assert _names(
            client,
            "c",
            "inComponent",
            '{nodes: {node: {field: NODE_NAME, where: {isIn: {list: [{str: "a"}, {str: "b"}]}}}}}',
        ) == ["a", "b"]
        # edge filter
        assert _names(
            client,
            "c",
            "inComponent",
            '{edges: {layers: {names: ["owns"], expr: %s}}}' % PASS_EDGE,
        ) == ["a", "b"]
        # graph (layer) filter
        assert _names(
            client, "c", "inComponent", '{graph: {layers: {names: ["owns"]}}}'
        ) == ["a", "b"]
        # no incoming `has` edges into c
        assert (
            _names(client, "c", "inComponent", '{graph: {layers: {names: ["has"]}}}')
            == []
        )


def test_component_respects_an_external_graph_filter():
    # A graph-level filter (here removing `x`) applied before the walk must be honoured — the
    # returned nodes are over that already-filtered graph.
    with _served().start() as server:
        client = server.get_client()
        q = (
            '{ graph(path: "g") { filterNodes: filter(expr: {nodes: {node: {field: NODE_NAME, where: {ne: {str: "x"}}}}}) '
            '{ node(name: "a") { outComponent { list { name } } } } } }'
        )
        got = client.query(q)["graph"]["filterNodes"]["node"]["outComponent"]["list"]
        assert sorted(n["name"] for n in got) == ["b", "c", "y"]


def test_component_external_graph_filter_composed_with_select():
    # External graph filter (remove `c`) AND a component `select` (owns layer) compose: the
    # owns walk from `a` would reach b, c — but c is filtered out, leaving only b.
    with _served().start() as server:
        client = server.get_client()
        q = (
            '{ graph(path: "g") { filterNodes: filter(expr: {nodes: {node: {field: NODE_NAME, where: {ne: {str: "c"}}}}}) '
            '{ node(name: "a") { outComponent(select: {edges: {layers: {names: ["owns"], expr: %s}}}) '
            "{ list { name } } } } } }" % PASS_EDGE
        )
        got = client.query(q)["graph"]["filterNodes"]["node"]["outComponent"]["list"]
        assert sorted(n["name"] for n in got) == ["b"]
