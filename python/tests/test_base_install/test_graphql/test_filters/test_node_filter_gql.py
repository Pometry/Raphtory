import pytest
from raphtory import Graph, PersistentGraph
from filters_setup import (
    init_graph,
    init_graph2,
    degree_graph_with_add_node_and_add_edge,
)
from utils import (
    run_graphql_test,
    run_graphql_error_test,
    run_graphql_error_test_contains,
    run_group_graphql_test,
)

EVENT_GRAPH = init_graph(Graph())
PERSISTENT_GRAPH = init_graph(PersistentGraph())


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_filter_nodes_with_str_ids_for_node_id_eq_gql(graph):
    query = """
    query {
      graph(path: "g") {
        filterNodes: filter(expr: { nodes: {
            id: {
              where: { eq: { str: "1" } }
            }
          } }) {
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
def test_deprecated_node_field_spelling_still_accepted(graph):
    # The old enum-argument spelling ({node: {field: ..., where: ...}})
    # remains accepted for backwards compatibility; new queries should use
    # the per-field forms (id:/name:/nodeType:).
    query = """
    query {
      graph(path: "g") {
        filterNodes: filter(expr: { nodes: {
            node: {
              field: NODE_ID
              where: { eq: { str: "1" } }
            }
          } }) {
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
        filterNodes: filter(expr: { nodes: {
            id: {
              where: { eq: { u64: 1 } }
            }
          } }) {
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
        filterNodes: filter(expr: { nodes: {
            id: {
              where: { eq: { u64: 1 } }
            }
          } }) {
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
          select(expr: { nodes: { nodeType: { 
            where: { eq: { str: "fire_nation" } }
          } } }) {
            select(expr: { nodes: { property: { name: "p9", where: { eq:{ i64: 5 } } } } }) {
              filter(expr: { nodes: {
                property: { name: "p100", where: { gt: { i64: 30 } } }
              } }) {
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
          select(expr: {nodes: {window: {start: 1, end: 4, expr: {isActive: true}}}}) {
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
          select(expr: {nodes: {window: {start: 1, end: 4, expr: {isActive: false}}}}) {
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


def _degree_value(node, direction):
    if direction == "BOTH":
        return node.degree()
    if direction == "IN":
        return node.in_degree()
    if direction == "OUT":
        return node.out_degree()
    raise ValueError(f"Unsupported direction: {direction}")


def _expected_degree_names(graph, direction, predicate):
    candidate_ids = [
        node.id for node in graph.nodes if predicate(_degree_value(node, direction))
    ]
    subgraph = graph.subgraph(candidate_ids)
    return sorted(
        str(node.id) for node in subgraph.nodes if len(node.history.collect()) > 0
    )


def _expected_degree_select_names(graph, direction, predicate):
    return sorted(
        str(node.id)
        for node in graph.nodes
        if predicate(_degree_value(node, direction))
    )


def _degree_filter_nodes_query_expected_pair(expr, expected_names):
    query = f"""
  query {{
    graph(path: "g") {{
    filterNodes: filter(expr: {{ nodes: {{ {expr} }} }}) {{
      nodes {{
      list {{ name }}
      }}
    }}
    }}
  }}
  """

    expected_output = {
        "graph": {
            "filterNodes": {
                "nodes": {"list": [{"name": name} for name in expected_names]}
            }
        }
    }
    return query, expected_output


def _degree_select_nodes_query_expected_pair(expr, expected_names):
    query = f"""
  query {{
    graph(path: "g") {{
      nodes {{
        select(expr: {{ nodes: {{ {expr} }} }}) {{
          list {{ name }}
        }}
      }}
    }}
  }}
  """

    expected_output = {
        "graph": {
            "nodes": {"select": {"list": [{"name": name} for name in expected_names]}}
        }
    }
    return query, expected_output


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_filter_nodes_degree_ops_and_gql(graph):
    threshold = 4
    queries_and_expected_outputs = []

    for direction in ["BOTH", "IN", "OUT"]:
        queries_and_expected_outputs.append(
            _degree_select_nodes_query_expected_pair(
                f"degree: {{ direction: {direction}, where: {{ lt: {{ u64: {threshold} }} }} }}",
                _expected_degree_select_names(
                    graph, direction, lambda d: d < threshold
                ),
            )
        )
        queries_and_expected_outputs.append(
            _degree_filter_nodes_query_expected_pair(
                f"degree: {{ direction: {direction}, where: {{ lt: {{ u64: {threshold} }} }} }}",
                _expected_degree_names(graph, direction, lambda d: d < threshold),
            )
        )
        queries_and_expected_outputs.append(
            _degree_select_nodes_query_expected_pair(
                f"degree: {{ direction: {direction}, where: {{ le: {{ u64: {threshold} }} }} }}",
                _expected_degree_select_names(
                    graph, direction, lambda d: d <= threshold
                ),
            )
        )
        queries_and_expected_outputs.append(
            _degree_filter_nodes_query_expected_pair(
                f"degree: {{ direction: {direction}, where: {{ le: {{ u64: {threshold} }} }} }}",
                _expected_degree_names(graph, direction, lambda d: d <= threshold),
            )
        )
        queries_and_expected_outputs.append(
            _degree_select_nodes_query_expected_pair(
                f"degree: {{ direction: {direction}, where: {{ eq: {{ u64: {threshold} }} }} }}",
                _expected_degree_select_names(
                    graph, direction, lambda d: d == threshold
                ),
            )
        )
        queries_and_expected_outputs.append(
            _degree_filter_nodes_query_expected_pair(
                f"degree: {{ direction: {direction}, where: {{ eq: {{ u64: {threshold} }} }} }}",
                _expected_degree_names(graph, direction, lambda d: d == threshold),
            )
        )
        queries_and_expected_outputs.append(
            _degree_select_nodes_query_expected_pair(
                f"degree: {{ direction: {direction}, where: {{ ne: {{ u64: {threshold} }} }} }}",
                _expected_degree_select_names(
                    graph, direction, lambda d: d != threshold
                ),
            )
        )
        queries_and_expected_outputs.append(
            _degree_filter_nodes_query_expected_pair(
                f"degree: {{ direction: {direction}, where: {{ ne: {{ u64: {threshold} }} }} }}",
                _expected_degree_names(graph, direction, lambda d: d != threshold),
            )
        )
        queries_and_expected_outputs.append(
            _degree_select_nodes_query_expected_pair(
                f"degree: {{ direction: {direction}, where: {{ ge: {{ u64: {threshold} }} }} }}",
                _expected_degree_select_names(
                    graph, direction, lambda d: d >= threshold
                ),
            )
        )
        queries_and_expected_outputs.append(
            _degree_filter_nodes_query_expected_pair(
                f"degree: {{ direction: {direction}, where: {{ ge: {{ u64: {threshold} }} }} }}",
                _expected_degree_names(graph, direction, lambda d: d >= threshold),
            )
        )
        queries_and_expected_outputs.append(
            _degree_select_nodes_query_expected_pair(
                f"degree: {{ direction: {direction}, where: {{ gt: {{ u64: {threshold} }} }} }}",
                _expected_degree_select_names(
                    graph, direction, lambda d: d > threshold
                ),
            )
        )
        queries_and_expected_outputs.append(
            _degree_filter_nodes_query_expected_pair(
                f"degree: {{ direction: {direction}, where: {{ gt: {{ u64: {threshold} }} }} }}",
                _expected_degree_names(graph, direction, lambda d: d > threshold),
            )
        )

    run_group_graphql_test(queries_and_expected_outputs, graph, sort_output=True)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_filter_nodes_degree_logic_and_sets_gql(graph):
    threshold = 3
    upper = threshold + 5
    queries_and_expected_outputs = []

    for direction in ["BOTH", "IN", "OUT"]:
        queries_and_expected_outputs.append(
            _degree_select_nodes_query_expected_pair(
                "and: ["
                f"{{ degree: {{ direction: {direction}, where: {{ gt: {{ u64: {threshold} }} }} }} }},"
                f"{{ degree: {{ direction: {direction}, where: {{ lt: {{ u64: {upper} }} }} }} }}"
                "]",
                _expected_degree_select_names(
                    graph, direction, lambda d: d > threshold and d < upper
                ),
            )
        )
        queries_and_expected_outputs.append(
            _degree_filter_nodes_query_expected_pair(
                "and: ["
                f"{{ degree: {{ direction: {direction}, where: {{ gt: {{ u64: {threshold} }} }} }} }},"
                f"{{ degree: {{ direction: {direction}, where: {{ lt: {{ u64: {upper} }} }} }} }}"
                "]",
                _expected_degree_names(
                    graph, direction, lambda d: d > threshold and d < upper
                ),
            )
        )

        queries_and_expected_outputs.append(
            _degree_select_nodes_query_expected_pair(
                "or: ["
                f"{{ degree: {{ direction: {direction}, where: {{ lt: {{ u64: {threshold} }} }} }} }},"
                f"{{ degree: {{ direction: {direction}, where: {{ gt: {{ u64: {upper} }} }} }} }}"
                "]",
                _expected_degree_select_names(
                    graph, direction, lambda d: d < threshold or d > upper
                ),
            )
        )
        queries_and_expected_outputs.append(
            _degree_filter_nodes_query_expected_pair(
                "or: ["
                f"{{ degree: {{ direction: {direction}, where: {{ lt: {{ u64: {threshold} }} }} }} }},"
                f"{{ degree: {{ direction: {direction}, where: {{ gt: {{ u64: {upper} }} }} }} }}"
                "]",
                _expected_degree_names(
                    graph, direction, lambda d: d < threshold or d > upper
                ),
            )
        )

        queries_and_expected_outputs.append(
            _degree_select_nodes_query_expected_pair(
                "or: ["
                f"{{ degree: {{ direction: {direction}, where: {{ lt: {{ u64: {threshold} }} }} }} }},"
                "{ not: "
                f"{{ degree: {{ direction: {direction}, where: {{ gt: {{ u64: {upper} }} }} }} }}"
                " }"
                "]",
                _expected_degree_select_names(
                    graph, direction, lambda d: d < threshold or d <= upper
                ),
            )
        )
        queries_and_expected_outputs.append(
            _degree_filter_nodes_query_expected_pair(
                "or: ["
                f"{{ degree: {{ direction: {direction}, where: {{ lt: {{ u64: {threshold} }} }} }} }},"
                "{ not: "
                f"{{ degree: {{ direction: {direction}, where: {{ gt: {{ u64: {upper} }} }} }} }}"
                " }"
                "]",
                _expected_degree_names(
                    graph, direction, lambda d: d < threshold or d <= upper
                ),
            )
        )

        queries_and_expected_outputs.append(
            _degree_select_nodes_query_expected_pair(
                f"degree: {{ direction: {direction}, where: {{ isIn: {{ list: [{{u64: {threshold}}}, {{u64: {threshold + 1}}}] }} }} }}",
                _expected_degree_select_names(
                    graph, direction, lambda d: d in [threshold, threshold + 1]
                ),
            )
        )
        queries_and_expected_outputs.append(
            _degree_filter_nodes_query_expected_pair(
                f"degree: {{ direction: {direction}, where: {{ isIn: {{ list: [{{u64: {threshold}}}, {{u64: {threshold + 1}}}] }} }} }}",
                _expected_degree_names(
                    graph, direction, lambda d: d in [threshold, threshold + 1]
                ),
            )
        )

        queries_and_expected_outputs.append(
            _degree_select_nodes_query_expected_pair(
                f"degree: {{ direction: {direction}, where: {{ isNotIn: {{ list: [{{u64: {threshold}}}, {{u64: {threshold + 1}}}] }} }} }}",
                _expected_degree_select_names(
                    graph, direction, lambda d: d not in [threshold, threshold + 1]
                ),
            )
        )
        queries_and_expected_outputs.append(
            _degree_filter_nodes_query_expected_pair(
                f"degree: {{ direction: {direction}, where: {{ isNotIn: {{ list: [{{u64: {threshold}}}, {{u64: {threshold + 1}}}] }} }} }}",
                _expected_degree_names(
                    graph, direction, lambda d: d not in [threshold, threshold + 1]
                ),
            )
        )

    run_group_graphql_test(queries_and_expected_outputs, graph, sort_output=True)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_filter_nodes_degree_numeric_coercion_gql(graph):
    threshold_str = "4"
    threshold_float = 4.5
    queries_and_expected_outputs = []

    for direction in ["BOTH", "IN", "OUT"]:
        queries_and_expected_outputs.append(
            _degree_select_nodes_query_expected_pair(
                f'degree: {{ direction: {direction}, where: {{ eq: {{ str: "{threshold_str}" }} }} }}',
                _expected_degree_select_names(
                    graph, direction, lambda d: d == int(threshold_str)
                ),
            )
        )
        queries_and_expected_outputs.append(
            _degree_filter_nodes_query_expected_pair(
                f'degree: {{ direction: {direction}, where: {{ eq: {{ str: "{threshold_str}" }} }} }}',
                _expected_degree_names(
                    graph, direction, lambda d: d == int(threshold_str)
                ),
            )
        )

        queries_and_expected_outputs.append(
            _degree_select_nodes_query_expected_pair(
                f"degree: {{ direction: {direction}, where: {{ ge: {{ f64: {threshold_float} }} }} }}",
                _expected_degree_select_names(
                    graph, direction, lambda d: d >= int(threshold_float)
                ),
            )
        )
        queries_and_expected_outputs.append(
            _degree_filter_nodes_query_expected_pair(
                f"degree: {{ direction: {direction}, where: {{ ge: {{ f64: {threshold_float} }} }} }}",
                _expected_degree_names(
                    graph, direction, lambda d: d >= int(threshold_float)
                ),
            )
        )

        queries_and_expected_outputs.append(
            _degree_select_nodes_query_expected_pair(
                f'degree: {{ direction: {direction}, where: {{ isIn: {{ list: [{{str: "3"}}, {{f64: 4.9}}] }} }} }}',
                _expected_degree_select_names(graph, direction, lambda d: d in [3, 4]),
            )
        )
        queries_and_expected_outputs.append(
            _degree_filter_nodes_query_expected_pair(
                f'degree: {{ direction: {direction}, where: {{ isIn: {{ list: [{{str: "3"}}, {{f64: 4.9}}] }} }} }}',
                _expected_degree_names(graph, direction, lambda d: d in [3, 4]),
            )
        )

    run_group_graphql_test(queries_and_expected_outputs, graph, sort_output=True)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_filter_nodes_degree_invalid_non_numeric_string_values_gql(graph):
    invalid_exprs = [
        'degree: { direction: BOTH, where: { lt: { str: "foo" } } }',
        'degree: { direction: IN, where: { eq: { str: "bar" } } }',
        'degree: { direction: OUT, where: { isIn: { list: [{str: "a"}, {str: "b"}] } } }',
        'degree: { direction: BOTH, where: { isNotIn: { list: [{str: "x"}, {str: "y"}] } } }',
    ]

    for expr in invalid_exprs:
        filter_nodes_query = f"""
    query {{
      graph(path: "g") {{
      filterNodes: filter(expr: {{ nodes: {{ {expr} }} }}) {{
        nodes {{
        list {{ name }}
        }}
      }}
      }}
    }}
    """

        select_nodes_query = f"""
    query {{
      graph(path: "g") {{
        nodes {{
          select(expr: {{ nodes: {{ {expr} }} }}) {{
            list {{ name }}
          }}
        }}
      }}
    }}
    """

        run_graphql_error_test_contains(filter_nodes_query, ["Invalid filter"], graph)
        run_graphql_error_test_contains(select_nodes_query, ["Invalid filter"], graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_filter_nodes_degree_invalid_expressions_gql(graph):
    invalid_exprs = [
        "degree: { direction: BOTH, where: { isNone: true } }",
        "degree: { direction: IN, where: { isSome: true } }",
        'degree: { direction: OUT, where: { startsWith: { str: "1" } } }',
        'degree: { direction: BOTH, where: { endsWith: { str: "1" } } }',
        'degree: { direction: IN, where: { contains: { str: "1" } } }',
        'degree: { direction: OUT, where: { notContains: { str: "1" } } }',
        "degree: { direction: BOTH, where: { any: { eq: { u64: 1 } } } }",
        "degree: { direction: IN, where: { all: { eq: { u64: 1 } } } }",
        "degree: { direction: OUT, where: { len: { gt: { u64: 0 } } } }",
        "degree: { direction: BOTH, where: { sum: { eq: { u64: 1 } } } }",
        "degree: { direction: IN, where: { avg: { eq: { u64: 1 } } } }",
        "degree: { direction: OUT, where: { first: { eq: { u64: 1 } } } }",
        "degree: { direction: BOTH, where: { last: { eq: { u64: 1 } } } }",
    ]

    for expr in invalid_exprs:
        filter_nodes_query = f"""
    query {{
      graph(path: "g") {{
      filterNodes: filter(expr: {{ nodes: {{ {expr} }} }}) {{
        nodes {{
        list {{ name }}
        }}
      }}
      }}
    }}
    """

        select_nodes_query = f"""
    query {{
      graph(path: "g") {{
        nodes {{
          select(expr: {{ nodes: {{ {expr} }} }}) {{
            list {{ name }}
          }}
        }}
      }}
    }}
    """

        run_graphql_error_test_contains(filter_nodes_query, ["Invalid filter"], graph)
        run_graphql_error_test_contains(select_nodes_query, ["Invalid filter"], graph)
