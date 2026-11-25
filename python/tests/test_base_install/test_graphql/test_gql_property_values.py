from utils import run_graphql_test
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
