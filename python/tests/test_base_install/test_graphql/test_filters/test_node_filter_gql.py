import pytest
from raphtory import Graph, PersistentGraph
from filters_setup import init_graph, init_graph2, degree_graph_with_add_node_and_add_edge 
from utils import run_graphql_test, run_graphql_error_test

EVENT_GRAPH = init_graph(Graph())
PERSISTENT_GRAPH = init_graph(PersistentGraph())


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_filter_nodes_with_str_ids_for_node_id_eq_gql(graph):
    query = """
    query {
      graph(path: "g") {
        filterNodes(
          expr: {
            node: {
              field: NODE_ID
              where: { eq: { str: "1" } }
            }
          }
        ) {
          nodes {
            list { name }
          }
        }
      }
    }
    """
    expected_output = {"graph": {"filterNodes": {"nodes": {"list": [{"name": "1"}]}}}}
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_filter_nodes_with_str_ids_for_node_id_eq_gql2(graph):
    query = """
    query {
      graph(path: "g") {
        filterNodes(
          expr: {
            node: {
              field: NODE_ID
              where: { eq: { u64: 1 } }
            }
          }
        ) {
          nodes {
            list { name }
          }
        }
      }
    }
    """
    expected_error_message = "Invalid filter: Filter value type does not match node ID type. Expected Str but got \\"
    run_graphql_error_test(query, expected_error_message, graph)


EVENT_GRAPH = init_graph2(Graph())
PERSISTENT_GRAPH = init_graph2(PersistentGraph())


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_filter_nodes_with_num_ids_for_node_id_eq_gql(graph):
    query = """
    query {
      graph(path: "g") {
        filterNodes(
          expr: {
            node: {
              field: NODE_ID
              where: { eq: { u64: 1 } }
            }
          }
        ) {
          nodes {
            list { name }
          }
        }
      }
    }
    """
    expected_output = {"graph": {"filterNodes": {"nodes": {"list": [{"name": "1"}]}}}}
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_nodes_chained_selection_with_node_filter(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          select(expr: { node: { 
            field: NODE_TYPE
            where: { eq: { str: "fire_nation" } }
          } }) {
            select(expr: { property: { name: "p9", where: { eq:{ i64: 5 } } } }) {
              filter(expr:{
                property: { name: "p100", where: { gt: { i64: 30 } } }
              }) {
                list {
                  name
                }
              }
            }        
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "nodes": {"select": {"select": {"filter": {"list": [{"name": "1"}]}}}}
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [PERSISTENT_GRAPH])
def test_nodes_filter_windowed_is_active(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          select(expr: {window: {start: 1, end: 4, expr: {isActive: true}}}) {
            list {
              name
            }
          }
        }
      }
    }
    """

    expected = {
        "graph": {
            "nodes": {
                "select": {
                    "list": [{"name": "1"}, {"name": "2"}, {"name": "3"}, {"name": "4"}]
                }
            }
        }
    }
    run_graphql_test(query, expected, graph, sort_output=True)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_nodes_filter_windowed_is_not_active(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          select(expr: {window: {start: 1, end: 4, expr: {isActive: false}}}) {
            list {
              name
            }
          }
        }
      }
    }
    """

    expected = {"graph": {"nodes": {"select": {"list": []}}}}
    run_graphql_test(query, expected, graph)


GRAPH = degree_graph_with_add_node_and_add_edge(Graph())
PERSISTENT_GRAPH = GRAPH.persistent_graph()
EVENT_GRAPH = GRAPH.event_graph()


@pytest.mark.parametrize("graph", [PERSISTENT_GRAPH, EVENT_GRAPH])
@pytest.mark.parametrize(
  "direction,degree_fn",
  [
    ("OUT", lambda node: node.out_degree()),
    ("BOTH", lambda node: node.degree()),
    ("IN", lambda node: node.in_degree()),
  ],
  ids=["out", "both", "in"],
)
@pytest.mark.parametrize(
    "where_clause,predicate",
    [
        ("{ gt: { u64: 1 } }", lambda d: d > 1),
        ("{ ge: { u64: 2 } }", lambda d: d >= 2),
        ("{ le: { u64: 1 } }", lambda d: d <= 1),
        ("{ lt: { u64: 2 } }", lambda d: d < 2),
        ("{ ne: { u64: 2 } }", lambda d: d != 2),
        ("{ eq: { u64: 2 } }", lambda d: d == 2),
        (
            "{ isIn: { list: [{u64: 1}, {u64: 2}] } }",
            lambda d: d in {1, 2},
        ),
        (
            "{ isNotIn: { list: [{u64: 1}, {u64: 2}] } }",
            lambda d: d not in {1, 2},
        ),
    ],
    ids=["gt", "ge", "le", "lt", "ne", "eq", "is_in", "is_not_in"],
)
def test_filter_nodes_degree_ops_gql(graph, direction, degree_fn, where_clause, predicate):
    query = f"""
    query {{
      graph(path: "g") {{
        filterNodes(
          expr: {{
            degree: {{
              direction: {direction}
              where: {where_clause}
            }}
          }}
        ) {{
          nodes {{
             ids
          }}
        }}
      }}
    }}
    """
    expected_node_ids = [node.id for node in graph.nodes if predicate(degree_fn(node))]
    expected_output = {
        "graph": {
            "filterNodes": {
                "nodes": {
                    "ids": expected_node_ids,
                }
            }
        }
    }
    run_graphql_test(query, expected_output, graph, sort_output=True)
