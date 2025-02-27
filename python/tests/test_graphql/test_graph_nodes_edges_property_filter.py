import tempfile

import pytest

from raphtory.graphql import GraphServer
from raphtory import Graph, PersistentGraph
import json
import re

PORT = 1737


def create_test_graph(g):
    g.add_node(
        1,
        "a",
        properties={
            "prop1": 60,
            "prop2": 31.3,
            "prop3": "abc123",
            "prop4": True,
            "prop5": [1, 2, 3],
        },
    )
    g.add_node(
        1,
        "b",
        properties={"prop1": 10, "prop2": 31.3, "prop3": "abc223", "prop4": False},
    )
    g.add_node(
        1,
        "c",
        properties={
            "prop1": 20,
            "prop2": 31.3,
            "prop3": "abc333",
            "prop4": True,
            "prop5": [5, 6, 7],
        },
    )
    g.add_node(
        1,
        "d",
        properties={"prop1": 30, "prop2": 31.3, "prop3": "abc444", "prop4": False},
    )
    g.add_edge(
        2,
        "a",
        "d",
        properties={
            "eprop1": 60,
            "eprop2": 0.4,
            "eprop3": "xyz123",
            "eprop4": True,
            "eprop5": [1, 2, 3],
        },
    )
    g.add_edge(
        2,
        "b",
        "d",
        properties={
            "eprop1": 10,
            "eprop2": 1.7,
            "eprop3": "xyz123",
            "eprop4": True,
            "eprop5": [3, 4, 5],
        },
    )
    g.add_edge(
        2,
        "c",
        "d",
        properties={
            "eprop1": 30,
            "eprop2": 6.4,
            "eprop3": "xyz123",
            "eprop4": False,
            "eprop5": [10],
        },
    )
    return g


def run_graphql_test(query, expected_output, graph):
    create_test_graph(graph)
    tmp_work_dir = tempfile.mkdtemp()
    with GraphServer(tmp_work_dir).start(PORT) as server:
        client = server.get_client()
        client.send_graph(path="g", graph=graph)

        response = client.query(query)

        # Convert response to a dictionary if needed and compare
        response_dict = json.loads(response) if isinstance(response, str) else response
        assert response_dict == expected_output


def run_graphql_error_test(query, expected_error_message, graph):
    create_test_graph(graph)
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


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_node_property_filter_equal(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          property: "prop5", 
          condition: {
            operator: EQUAL, 
            value: [1, 2, 3]
          }
        ) {
          nodes {
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_output = {"graph": {"nodeFilter": {"nodes": {"list": [{"name": "a"}]}}}}
    run_graphql_test(query, expected_output, graph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_node_property_filter_equal_no_value_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          property: "prop5", 
          condition: {
            operator: EQUAL
          }
        ) {
          nodes {
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_error_message = "Expected a value for Equal operator"
    run_graphql_error_test(query, expected_error_message, graph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_node_property_filter_equal_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          property: "prop5", 
          condition: {
            operator: EQUAL, 
            value: 1
          }
        ) {
          nodes {
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_error_message = "PropertyType Error: Wrong type for property prop5: expected List(I64) but actual type is I64"
    run_graphql_error_test(query, expected_error_message, graph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_node_property_filter_not_equal(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          property: "prop4", 
          condition: {
            operator: NOT_EQUAL, 
            value: true
          }
        ) {
          nodes {
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {"nodeFilter": {"nodes": {"list": [{"name": "b"}, {"name": "d"}]}}}
    }
    run_graphql_test(query, expected_output, graph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_node_property_filter_not_equal_no_value_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          property: "prop4", 
          condition: {
            operator: NOT_EQUAL
          }
        ) {
          nodes {
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_error_message = "Expected a value for NotEqual operator"
    run_graphql_error_test(query, expected_error_message, graph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_node_property_filter_not_equal_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          property: "prop4", 
          condition: {
            operator: NOT_EQUAL, 
            value: 1
          }
        ) {
          nodes {
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_error_message = "PropertyType Error: Wrong type for property prop4: expected Bool but actual type is I64"
    run_graphql_error_test(query, expected_error_message, graph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_node_property_filter_greater_than_or_equal(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          property: "prop1", 
          condition: {
            operator: GREATER_THAN_OR_EQUAL, 
            value: 60
          }
        ) {
          nodes {
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_output = {"graph": {"nodeFilter": {"nodes": {"list": [{"name": "a"}]}}}}
    run_graphql_test(query, expected_output, graph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_node_property_filter_greater_than_or_equal_no_value_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          property: "prop1", 
          condition: {
            operator: GREATER_THAN_OR_EQUAL
          }
        ) {
          nodes {
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_error_message = "Expected a value for GreaterThanOrEqual operator"
    run_graphql_error_test(query, expected_error_message, graph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_node_property_filter_greater_than_or_equal_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          property: "prop1", 
          condition: {
            operator: GREATER_THAN_OR_EQUAL, 
            value: true
          }
        ) {
          nodes {
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_error_message = "PropertyType Error: Wrong type for property prop1: expected I64 but actual type is Bool"
    run_graphql_error_test(query, expected_error_message, graph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_node_property_filter_less_than_or_equal(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          property: "prop1", 
          condition: {
            operator: LESS_THAN_OR_EQUAL, 
            value: 30
          }
        ) {
          nodes {
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "nodeFilter": {
                "nodes": {"list": [{"name": "b"}, {"name": "c"}, {"name": "d"}]}
            }
        }
    }
    run_graphql_test(query, expected_output, graph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_node_property_filter_less_than_or_equal_no_value_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          property: "prop1", 
          condition: {
            operator: LESS_THAN_OR_EQUAL
          }
        ) {
          nodes {
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_error_message = "Expected a value for LessThanOrEqual operator"
    run_graphql_error_test(query, expected_error_message, graph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_node_property_filter_less_than_or_equal_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          property: "prop1", 
          condition: {
            operator: LESS_THAN_OR_EQUAL, 
            value: "shivam"
          }
        ) {
          nodes {
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_error_message = "PropertyType Error: Wrong type for property prop1: expected I64 but actual type is Str"
    run_graphql_error_test(query, expected_error_message, graph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_node_property_filter_greater_than(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          property: "prop1", 
          condition: {
            operator: GREATER_THAN, 
            value: 30
          }
        ) {
          nodes {
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_output = {"graph": {"nodeFilter": {"nodes": {"list": [{"name": "a"}]}}}}
    run_graphql_test(query, expected_output, graph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_node_property_filter_greater_than_no_value_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          property: "prop1", 
          condition: {
            operator: GREATER_THAN
          }
        ) {
          nodes {
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_error_message = "Expected a value for GreaterThan operator"
    run_graphql_error_test(query, expected_error_message, graph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_node_property_filter_greater_than_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          property: "prop1", 
          condition: {
            operator: GREATER_THAN, 
            value: "shivam"
          }
        ) {
          nodes {
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_error_message = "PropertyType Error: Wrong type for property prop1: expected I64 but actual type is Str"
    run_graphql_error_test(query, expected_error_message, graph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_node_property_filter_less_than(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          property: "prop1", 
          condition: {
            operator: LESS_THAN, 
            value: 30
          }
        ) {
          nodes {
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {"nodeFilter": {"nodes": {"list": [{"name": "b"}, {"name": "c"}]}}}
    }
    run_graphql_test(query, expected_output, graph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_node_property_filter_less_than_no_value_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          property: "prop1", 
          condition: {
            operator: LESS_THAN
          }
        ) {
          nodes {
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_error_message = "Expected a value for LessThan operator"
    run_graphql_error_test(query, expected_error_message, graph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_node_property_filter_less_than_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          property: "prop1", 
          condition: {
            operator: LESS_THAN, 
            value: "shivam"
          }
        ) {
          nodes {
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_error_message = "PropertyType Error: Wrong type for property prop1: expected I64 but actual type is Str"
    run_graphql_error_test(query, expected_error_message, graph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_node_property_filter_is_none(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          property: "prop5", 
          condition: {
            operator: IS_NONE
          }
        ) {
          nodes {
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {"nodeFilter": {"nodes": {"list": [{"name": "b"}, {"name": "d"}]}}}
    }
    run_graphql_test(query, expected_output, graph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_node_property_filter_is_some(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          property: "prop5", 
          condition: {
            operator: IS_SOME
          }
        ) {
          nodes {
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {"nodeFilter": {"nodes": {"list": [{"name": "a"}, {"name": "c"}]}}}
    }
    run_graphql_test(query, expected_output, graph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_node_property_filter_any(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          property: "prop1", 
          condition: {
            operator: ANY, 
            value: [10, 30, 50, 70]
          }
        ) {
          nodes {
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {"nodeFilter": {"nodes": {"list": [{"name": "b"}, {"name": "d"}]}}}
    }
    run_graphql_test(query, expected_output, graph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_node_property_filter_any_empty_list(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          nodeFilter(
            property: "prop1", 
            condition: {
              operator: ANY, 
              value: []
            }
          ) {
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_output = {"graph": {"nodes": {"nodeFilter": {"list": []}}}}
    run_graphql_test(query, expected_output, graph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_node_property_filter_any_no_value_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          property: "prop1", 
          condition: {
            operator: ANY,
          }
        ) {
          nodes {
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_error_message = "Expected a list for Any operator"
    run_graphql_error_test(query, expected_error_message, graph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_node_property_filter_any_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          property: "prop1", 
          condition: {
            operator: ANY, 
            value: "shivam"
          }
        ) {
          nodes {
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_error_message = "Expected a list for Any operator"
    run_graphql_error_test(query, expected_error_message, graph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_node_property_filter_not_any(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          property: "prop1", 
          condition: {
            operator: NOT_ANY, 
            value: [10, 30, 50, 70]
          }
        ) {
          nodes {
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {"nodeFilter": {"nodes": {"list": [{"name": "a"}, {"name": "c"}]}}}
    }
    run_graphql_test(query, expected_output, graph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_node_property_filter_not_any_empty_list(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          nodeFilter(
            property: "prop1", 
            condition: {
              operator: NOT_ANY, 
              value: []
            }
          ) {
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "nodes": {
                "nodeFilter": {
                    "list": [{"name": "a"}, {"name": "b"}, {"name": "c"}, {"name": "d"}]
                }
            }
        }
    }
    run_graphql_test(query, expected_output, graph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_node_property_filter_not_any_no_value_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          property: "prop1", 
          condition: {
            operator: NOT_ANY,
          }
        ) {
          nodes {
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_error_message = "Expected a list for NotAny operator"
    run_graphql_error_test(query, expected_error_message, graph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_node_property_filter_not_any_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          property: "prop1", 
          condition: {
            operator: NOT_ANY, 
            value: "shivam"
          }
        ) {
          nodes {
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_error_message = "Expected a list for NotAny operator"
    run_graphql_error_test(query, expected_error_message, graph())


# Edge property filter is not supported yet for PersistentGraph
@pytest.mark.parametrize("graph", [Graph])
def test_graph_edge_property_filter_equal(graph):
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          property: "eprop5", 
          condition: {
            operator: EQUAL, 
            value: [1, 2, 3]
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "edgeFilter": {
                "edges": {"list": [{"src": {"name": "a"}, "dst": {"name": "d"}}]}
            }
        }
    }
    run_graphql_test(query, expected_output, graph())


def test_graph_edge_property_filter_equal_persistent_graph():
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          property: "eprop5", 
          condition: {
            operator: EQUAL, 
            value: [1, 2, 3]
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = "Property filtering not implemented on PersistentGraph yet"
    run_graphql_error_test(query, expected_error_message, PersistentGraph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_edge_property_filter_equal_no_value_error(graph):
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          property: "eprop5",
          condition: {
            operator: EQUAL
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = "Expected a value for Equal operator"
    run_graphql_error_test(query, expected_error_message, graph())


# Edge property filter is not supported yet for PersistentGraph
@pytest.mark.parametrize("graph", [Graph])
def test_graph_edge_property_filter_equal_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          property: "eprop5",
          condition: {
            operator: EQUAL,
            value: 1
          }
        ) {
          nodes {
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_error_message = "PropertyType Error: Wrong type for property eprop5: expected List(I64) but actual type is I64"
    run_graphql_error_test(query, expected_error_message, graph())


def test_graph_edge_property_filter_equal_type_error_persistent_graph():
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          property: "eprop5",
          condition: {
            operator: EQUAL,
            value: 1
          }
        ) {
          nodes {
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_error_message = "Property filtering not implemented on PersistentGraph yet"
    run_graphql_error_test(query, expected_error_message, PersistentGraph())


# Edge property filter is not supported yet for PersistentGraph
@pytest.mark.parametrize("graph", [Graph])
def test_graph_edge_property_filter_not_equal(graph):
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          property: "eprop4",
          condition: {
            operator: NOT_EQUAL,
            value: true
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "edgeFilter": {
                "edges": {"list": [{"src": {"name": "c"}, "dst": {"name": "d"}}]}
            }
        }
    }
    run_graphql_test(query, expected_output, graph())


def test_graph_edge_property_filter_not_equal_persistent_graph():
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          property: "eprop4",
          condition: {
            operator: NOT_EQUAL,
            value: true
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = "Property filtering not implemented on PersistentGraph yet"
    run_graphql_error_test(query, expected_error_message, PersistentGraph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_edge_property_filter_not_equal_no_value_error(graph):
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          property: "eprop4",
          condition: {
            operator: NOT_EQUAL
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = "Expected a value for NotEqual operator"
    run_graphql_error_test(query, expected_error_message, graph())


# Edge property filter is not supported yet for PersistentGraph
@pytest.mark.parametrize("graph", [Graph])
def test_graph_edge_property_filter_not_equal_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          property: "eprop4",
          condition: {
            operator: NOT_EQUAL,
            value: 1
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = "PropertyType Error: Wrong type for property eprop4: expected Bool but actual type is I64"
    run_graphql_error_test(query, expected_error_message, graph())


def test_graph_edge_property_filter_not_equal_type_error_persistent_graph():
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          property: "eprop4",
          condition: {
            operator: NOT_EQUAL,
            value: 1
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = "Property filtering not implemented on PersistentGraph yet"
    run_graphql_error_test(query, expected_error_message, PersistentGraph())


# Edge property filter is not supported yet for PersistentGraph
@pytest.mark.parametrize("graph", [Graph])
def test_graph_edge_property_filter_greater_than_or_equal(graph):
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          property: "eprop1",
          condition: {
            operator: GREATER_THAN_OR_EQUAL,
            value: 60
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "edgeFilter": {
                "edges": {"list": [{"src": {"name": "a"}, "dst": {"name": "d"}}]}
            }
        }
    }
    run_graphql_test(query, expected_output, graph())


def test_graph_edge_property_filter_greater_than_or_equal_persistent_graph():
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          property: "eprop1",
          condition: {
            operator: GREATER_THAN_OR_EQUAL,
            value: 60
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = "Property filtering not implemented on PersistentGraph yet"
    run_graphql_error_test(query, expected_error_message, PersistentGraph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_edge_property_filter_greater_than_or_equal_no_value_error(graph):
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          property: "eprop1",
          condition: {
            operator: GREATER_THAN_OR_EQUAL
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = "Expected a value for GreaterThanOrEqual operator"
    run_graphql_error_test(query, expected_error_message, graph())


# Edge property filter is not supported yet for PersistentGraph
@pytest.mark.parametrize("graph", [Graph])
def test_graph_edge_property_filter_greater_than_or_equal_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          property: "eprop1",
          condition: {
            operator: GREATER_THAN_OR_EQUAL,
            value: true
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = "PropertyType Error: Wrong type for property eprop1: expected I64 but actual type is Bool"
    run_graphql_error_test(query, expected_error_message, graph())


def test_graph_edge_property_filter_greater_than_or_equal_type_error_persistent_graph():
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          property: "eprop1",
          condition: {
            operator: GREATER_THAN_OR_EQUAL,
            value: true
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = "Property filtering not implemented on PersistentGraph yet"
    run_graphql_error_test(query, expected_error_message, PersistentGraph())


# Edge property filter is not supported yet for PersistentGraph
@pytest.mark.parametrize("graph", [Graph])
def test_graph_edge_property_filter_less_than_or_equal(graph):
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          property: "eprop1",
          condition: {
            operator: LESS_THAN_OR_EQUAL,
            value: 30
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "edgeFilter": {
                "edges": {
                    "list": [
                        {"src": {"name": "b"}, "dst": {"name": "d"}},
                        {"src": {"name": "c"}, "dst": {"name": "d"}},
                    ]
                }
            }
        }
    }
    run_graphql_test(query, expected_output, graph())


def test_graph_edge_property_filter_less_than_or_equal_persistent_graph():
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          property: "eprop1",
          condition: {
            operator: LESS_THAN_OR_EQUAL,
            value: 30
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = "Property filtering not implemented on PersistentGraph yet"
    run_graphql_error_test(query, expected_error_message, PersistentGraph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_edge_property_filter_less_than_or_equal_no_value_error(graph):
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          property: "eprop1",
          condition: {
            operator: LESS_THAN_OR_EQUAL
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = "Expected a value for LessThanOrEqual operator"
    run_graphql_error_test(query, expected_error_message, graph())


# Edge property filter is not supported yet for PersistentGraph
@pytest.mark.parametrize("graph", [Graph])
def test_graph_edge_property_filter_less_than_or_equal_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          property: "eprop1",
          condition: {
            operator: LESS_THAN_OR_EQUAL,
            value: "shivam"
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = "PropertyType Error: Wrong type for property eprop1: expected I64 but actual type is Str"
    run_graphql_error_test(query, expected_error_message, graph())


def test_graph_edge_property_filter_less_than_or_equal_type_error_persistent_graph():
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          property: "eprop1",
          condition: {
            operator: LESS_THAN_OR_EQUAL,
            value: "shivam"
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = "Property filtering not implemented on PersistentGraph yet"
    run_graphql_error_test(query, expected_error_message, PersistentGraph())


# Edge property filter is not supported yet for PersistentGraph
@pytest.mark.parametrize("graph", [Graph])
def test_graph_edge_property_filter_greater_than(graph):
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          property: "eprop1",
          condition: {
            operator: GREATER_THAN,
            value: 30
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "edgeFilter": {
                "edges": {"list": [{"src": {"name": "a"}, "dst": {"name": "d"}}]}
            }
        }
    }
    run_graphql_test(query, expected_output, graph())


def test_graph_edge_property_filter_greater_than_persistent_graph():
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          property: "eprop1",
          condition: {
            operator: GREATER_THAN,
            value: 30
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = "Property filtering not implemented on PersistentGraph yet"
    run_graphql_error_test(query, expected_error_message, PersistentGraph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_edge_property_filter_greater_than_no_value_error(graph):
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          property: "eprop1",
          condition: {
            operator: GREATER_THAN
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = "Expected a value for GreaterThan operator"
    run_graphql_error_test(query, expected_error_message, graph())


# Edge property filter is not supported yet for PersistentGraph
@pytest.mark.parametrize("graph", [Graph])
def test_graph_edge_property_filter_greater_than_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          property: "eprop1",
          condition: {
            operator: GREATER_THAN,
            value: "shivam"
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = "PropertyType Error: Wrong type for property eprop1: expected I64 but actual type is Str"
    run_graphql_error_test(query, expected_error_message, graph())


def test_graph_edge_property_filter_greater_than_type_error_persistent_graph():
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          property: "eprop1",
          condition: {
            operator: GREATER_THAN,
            value: "shivam"
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = "Property filtering not implemented on PersistentGraph yet"
    run_graphql_error_test(query, expected_error_message, PersistentGraph())


# Edge property filter is not supported yet for PersistentGraph
@pytest.mark.parametrize("graph", [Graph])
def test_graph_edge_property_filter_less_than(graph):
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          property: "eprop1",
          condition: {
            operator: LESS_THAN,
            value: 30
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "edgeFilter": {
                "edges": {"list": [{"src": {"name": "b"}, "dst": {"name": "d"}}]}
            }
        }
    }
    run_graphql_test(query, expected_output, graph())


def test_graph_edge_property_filter_less_than_persistent_graph():
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          property: "eprop1",
          condition: {
            operator: LESS_THAN,
            value: 30
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = "Property filtering not implemented on PersistentGraph yet"
    run_graphql_error_test(query, expected_error_message, PersistentGraph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_edge_property_filter_less_than_no_value_error(graph):
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          property: "eprop1",
          condition: {
            operator: LESS_THAN
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = "Expected a value for LessThan operator"
    run_graphql_error_test(query, expected_error_message, graph())


# Edge property filter is not supported yet for PersistentGraph
@pytest.mark.parametrize("graph", [Graph])
def test_graph_edge_property_filter_less_than_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          property: "eprop1",
          condition: {
            operator: LESS_THAN,
            value: "shivam"
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = "PropertyType Error: Wrong type for property eprop1: expected I64 but actual type is Str"
    run_graphql_error_test(query, expected_error_message, graph())


def test_graph_edge_property_filter_less_than_type_error_persistent_graph():
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          property: "eprop1",
          condition: {
            operator: LESS_THAN,
            value: "shivam"
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = "Property filtering not implemented on PersistentGraph yet"
    run_graphql_error_test(query, expected_error_message, PersistentGraph())


# Edge property filter is not supported yet for PersistentGraph
@pytest.mark.parametrize("graph", [Graph])
def test_graph_edge_property_filter_is_none(graph):
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          property: "eprop5",
          condition: {
            operator: IS_NONE
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_output = {"graph": {"edgeFilter": {"edges": {"list": []}}}}
    run_graphql_test(query, expected_output, graph())


def test_graph_edge_property_filter_is_none_persistent_graph():
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          property: "eprop5",
          condition: {
            operator: IS_NONE
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = "Property filtering not implemented on PersistentGraph yet"
    run_graphql_error_test(query, expected_error_message, PersistentGraph())


# Edge property filter is not supported yet for PersistentGraph
@pytest.mark.parametrize("graph", [Graph])
def test_graph_edge_property_filter_is_some(graph):
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          property: "eprop5",
          condition: {
            operator: IS_SOME
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "edgeFilter": {
                "edges": {
                    "list": [
                        {"src": {"name": "a"}, "dst": {"name": "d"}},
                        {"src": {"name": "b"}, "dst": {"name": "d"}},
                        {"src": {"name": "c"}, "dst": {"name": "d"}},
                    ]
                }
            }
        }
    }
    run_graphql_test(query, expected_output, graph())


def test_graph_edge_property_filter_is_some_persistent_graph():
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          property: "eprop5",
          condition: {
            operator: IS_SOME
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = "Property filtering not implemented on PersistentGraph yet"
    run_graphql_error_test(query, expected_error_message, PersistentGraph())


# Edge property filter is not supported yet for PersistentGraph
@pytest.mark.parametrize("graph", [Graph])
def test_graph_edge_property_filter_any(graph):
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          property: "eprop1",
          condition: {
            operator: ANY,
            value: [10, 20, 30]
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "edgeFilter": {
                "edges": {
                    "list": [
                        {"src": {"name": "b"}, "dst": {"name": "d"}},
                        {"src": {"name": "c"}, "dst": {"name": "d"}},
                    ]
                }
            }
        }
    }
    run_graphql_test(query, expected_output, graph())


def test_graph_edge_property_filter_any_persistent_graph():
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          property: "eprop1",
          condition: {
            operator: ANY,
            value: [10, 20, 30]
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = "Property filtering not implemented on PersistentGraph yet"
    run_graphql_error_test(query, expected_error_message, PersistentGraph())


# Edge property filter is not supported yet for PersistentGraph
@pytest.mark.parametrize("graph", [Graph])
def test_graph_edge_property_filter_any_empty_list(graph):
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          property: "eprop1",
          condition: {
            operator: ANY,
            value: []
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_output = {"graph": {"edgeFilter": {"edges": {"list": []}}}}
    run_graphql_test(query, expected_output, graph())


def test_graph_edge_property_filter_any_empty_list_persistent_graph():
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          property: "eprop1",
          condition: {
            operator: ANY,
            value: []
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = "Property filtering not implemented on PersistentGraph yet"
    run_graphql_error_test(query, expected_error_message, PersistentGraph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_edge_property_filter_any_no_value_error(graph):
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          property: "prop1",
          condition: {
            operator: ANY,
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = "Expected a list for Any operator"
    run_graphql_error_test(query, expected_error_message, graph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_edge_property_filter_any_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          property: "prop1",
          condition: {
            operator: ANY,
            value: "shivam"
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = "Expected a list for Any operator"
    run_graphql_error_test(query, expected_error_message, graph())


# Edge property filter is not supported yet for PersistentGraph
@pytest.mark.parametrize("graph", [Graph])
def test_graph_edge_property_filter_not_any(graph):
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          property: "eprop1",
          condition: {
            operator: NOT_ANY,
            value: [10, 20, 30]
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "edgeFilter": {
                "edges": {"list": [{"src": {"name": "a"}, "dst": {"name": "d"}}]}
            }
        }
    }
    run_graphql_test(query, expected_output, graph())


def test_graph_edge_property_filter_not_any_persistent_graph():
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          property: "eprop1",
          condition: {
            operator: NOT_ANY,
            value: [10, 20, 30]
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = "Property filtering not implemented on PersistentGraph yet"
    run_graphql_error_test(query, expected_error_message, PersistentGraph())


# Edge property filter is not supported yet for PersistentGraph
@pytest.mark.parametrize("graph", [Graph])
def test_graph_edge_property_filter_not_any_empty_list(graph):
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          property: "eprop1",
          condition: {
            operator: NOT_ANY,
            value: []
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "edgeFilter": {
                "edges": {
                    "list": [
                        {"src": {"name": "a"}, "dst": {"name": "d"}},
                        {"src": {"name": "b"}, "dst": {"name": "d"}},
                        {"src": {"name": "c"}, "dst": {"name": "d"}},
                    ]
                }
            }
        }
    }
    run_graphql_test(query, expected_output, graph())


def test_graph_edge_property_filter_not_any_empty_list_persistent_graph():
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          property: "eprop1",
          condition: {
            operator: NOT_ANY,
            value: []
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = "Property filtering not implemented on PersistentGraph yet"
    run_graphql_error_test(query, expected_error_message, PersistentGraph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_edge_property_filter_not_any_no_value_error(graph):
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          property: "eprop1",
          condition: {
            operator: NOT_ANY,
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = "Expected a list for NotAny operator"
    run_graphql_error_test(query, expected_error_message, graph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_edge_property_filter_not_any_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          property: "eprop1",
          condition: {
            operator: NOT_ANY,
            value: "shivam"
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = "Expected a list for NotAny operator"
    run_graphql_error_test(query, expected_error_message, graph())
