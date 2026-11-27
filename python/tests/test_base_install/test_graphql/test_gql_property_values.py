import pytest
from utils import graphql_client, run_graphql_test
from raphtory import Graph, Prop


def test_decimal_as_string_is_its_value():
    graph = Graph()
    graph.add_node(1, "a")
    graph.node("a").add_metadata({"d": Prop.decimal("3.14")})
    query = '{ graph(path: "g") { node(name: "a") { metadata { get(key: "d") { value asString } } } } }'
    expected = {
        "graph": {"node": {"metadata": {"get": {"value": "3.14", "asString": "3.14"}}}}
    }
    run_graphql_test(query, expected, graph)


BIG_U64 = 2**64 - 1

FILTER_NODES = """
query($f: FilterExpr!) {
  graph(path: "g") { filter(expr: $f) { nodes { list { name } } } }
}
"""


def big_u64_graph():
    graph = Graph()
    graph.add_node(1, "a", properties={"big": Prop.u64(BIG_U64)})
    graph.add_node(1, "b", properties={"big": Prop.u64(1)})
    return graph


def test_u64_above_i64_max_reads_back_as_an_int():
    query = '{ graph(path: "g") { node(name: "a") { properties { get(key: "big") { value } } } } }'
    expected = {"graph": {"node": {"properties": {"get": {"value": BIG_U64}}}}}
    run_graphql_test(query, expected, big_u64_graph())


def test_u64_above_i64_max_is_sent_as_an_int():
    tree = {
        "node": {
            "cmp": {
                "op": "EQ",
                "lhs": {"read": {"property": {"name": "big"}}},
                "rhs": {"const": {"u64": BIG_U64}},
            }
        }
    }
    with graphql_client(big_u64_graph()) as client:
        out = client.query(FILTER_NODES, {"f": tree})
        assert out["graph"]["filter"]["nodes"]["list"] == [{"name": "a"}]

        with pytest.raises(OverflowError):
            client.query(FILTER_NODES, {"f": {"node": {"const": {"u64": 2**64}}}})
