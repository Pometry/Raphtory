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
        filterNodes: filter(expr: {
                                    node: {
                                      eq: {
                                        lhs: {
                                          field: ID
                                        }
                                        rhs: {
                                          const: {
                                            str: "1"
                                          }
                                        }
                                      }
                                    }
                                  }) {
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
def test_sort_key_with_no_or_several_fields_is_rejected(graph):
    # A sort-key entry names exactly one attribute. Setting none, or several,
    # used to be a silent no-op / silent drop of all but the first.
    for keys in ("[{}]", "[{reverse: true}]", "[{id: true, name: true}]"):
        run_graphql_error_test_contains(
            """
            query {
              graph(path: "g") {
                nodes { sorted(sortBys: %s) { list { name } } }
              }
            }
            """
            % keys,
            "exactly one",
            graph,
        )


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_filter_nodes_with_str_ids_for_node_id_eq_gql2(graph):
    query = """
    query {
      graph(path: "g") {
        filterNodes: filter(expr: {
                                    node: {
                                      eq: {
                                        lhs: {
                                          field: ID
                                        }
                                        rhs: {
                                          const: {
                                            u64: 1
                                          }
                                        }
                                      }
                                    }
                                  }) {
          nodes {
            list { name }
          }
        }
      }
    }
    """
    expected_error_message = (
        "Invalid filter: value U64(1) of type U64 cannot be compared with Str"
    )
    run_graphql_error_test(query, expected_error_message, graph)


EVENT_GRAPH = init_graph2(Graph())
PERSISTENT_GRAPH = init_graph2(PersistentGraph())


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_filter_nodes_with_num_ids_for_node_id_eq_gql(graph):
    query = """
    query {
      graph(path: "g") {
        filterNodes: filter(expr: {
                                    node: {
                                      eq: {
                                        lhs: {
                                          field: ID
                                        }
                                        rhs: {
                                          const: {
                                            u64: 1
                                          }
                                        }
                                      }
                                    }
                                  }) {
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
          select(expr: {
                         node: {
                           eq: {
                             lhs: {
                               field: NODE_TYPE
                             }
                             rhs: {
                               const: {
                                 str: "fire_nation"
                               }
                             }
                           }
                         }
                       }) {
            select(expr: {
                           node: {
                             eq: {
                               lhs: {
                                 property: "p9"
                               }
                               rhs: {
                                 const: {
                                   i64: 5
                                 }
                               }
                             }
                           }
                         }) {
              filter(expr: {
                             node: {
                               gt: {
                                 lhs: {
                                   property: "p100"
                                 }
                                 rhs: {
                                   const: {
                                     i64: 30
                                   }
                                 }
                               }
                             }
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
          select(expr: {
                         node: {
                           viewed: {
                             views: [{
                               window: {
                                 start: 1
                                 end: 4
                               }
                             }]
                             expr: {
                               isActive: true
                             }
                           }
                         }
                       }) {
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
          select(expr: {
                         not: {
                           node: {
                             viewed: {
                               views: [{
                                 window: {
                                   start: 1
                                   end: 4
                                 }
                               }]
                               expr: {
                                 isActive: true
                               }
                             }
                           }
                         }
                       }) {
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


def _degree(direction, op, value=None, over=None):
    """A degree predicate in the tree grammar: `degree(direction) <op> value`. `over` wraps
    the degree in an aggregate, or the comparison in a qualifier, so invalid chains can be
    spelled.
    """
    lhs = f"{{ degree: {direction} }}"
    if over in ("sum", "avg", "min", "max", "first", "last", "len"):
        lhs = f"{{ {over}: {lhs} }}"
    if op in ("isSome", "isNone"):
        pred = f"{{ {op}: {lhs} }}"
    elif op in ("isIn", "isNotIn"):
        pred = f"{{ {op}: {{ expr: {lhs}, values: {value} }} }}"
    else:
        pred = f"{{ {op}: {{ lhs: {lhs}, rhs: {{ const: {value} }} }} }}"
    if over in ("any", "all"):
        pred = f"{{ {over}: {pred} }}"
    return f"{{ node: {pred} }}"


def _degree_filter_nodes_query_expected_pair(expr, expected_names):
    query = f"""
  query {{
    graph(path: "g") {{
    filterNodes: filter(expr: {expr}) {{
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
        select(expr: {expr}) {{
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
                _degree(direction, "lt", f"{{ u64: {threshold} }}"),
                _expected_degree_select_names(
                    graph, direction, lambda d: d < threshold
                ),
            )
        )
        queries_and_expected_outputs.append(
            _degree_filter_nodes_query_expected_pair(
                _degree(direction, "lt", f"{{ u64: {threshold} }}"),
                _expected_degree_names(graph, direction, lambda d: d < threshold),
            )
        )
        queries_and_expected_outputs.append(
            _degree_select_nodes_query_expected_pair(
                _degree(direction, "le", f"{{ u64: {threshold} }}"),
                _expected_degree_select_names(
                    graph, direction, lambda d: d <= threshold
                ),
            )
        )
        queries_and_expected_outputs.append(
            _degree_filter_nodes_query_expected_pair(
                _degree(direction, "le", f"{{ u64: {threshold} }}"),
                _expected_degree_names(graph, direction, lambda d: d <= threshold),
            )
        )
        queries_and_expected_outputs.append(
            _degree_select_nodes_query_expected_pair(
                _degree(direction, "eq", f"{{ u64: {threshold} }}"),
                _expected_degree_select_names(
                    graph, direction, lambda d: d == threshold
                ),
            )
        )
        queries_and_expected_outputs.append(
            _degree_filter_nodes_query_expected_pair(
                _degree(direction, "eq", f"{{ u64: {threshold} }}"),
                _expected_degree_names(graph, direction, lambda d: d == threshold),
            )
        )
        queries_and_expected_outputs.append(
            _degree_select_nodes_query_expected_pair(
                _degree(direction, "ne", f"{{ u64: {threshold} }}"),
                _expected_degree_select_names(
                    graph, direction, lambda d: d != threshold
                ),
            )
        )
        queries_and_expected_outputs.append(
            _degree_filter_nodes_query_expected_pair(
                _degree(direction, "ne", f"{{ u64: {threshold} }}"),
                _expected_degree_names(graph, direction, lambda d: d != threshold),
            )
        )
        queries_and_expected_outputs.append(
            _degree_select_nodes_query_expected_pair(
                _degree(direction, "ge", f"{{ u64: {threshold} }}"),
                _expected_degree_select_names(
                    graph, direction, lambda d: d >= threshold
                ),
            )
        )
        queries_and_expected_outputs.append(
            _degree_filter_nodes_query_expected_pair(
                _degree(direction, "ge", f"{{ u64: {threshold} }}"),
                _expected_degree_names(graph, direction, lambda d: d >= threshold),
            )
        )
        queries_and_expected_outputs.append(
            _degree_select_nodes_query_expected_pair(
                _degree(direction, "gt", f"{{ u64: {threshold} }}"),
                _expected_degree_select_names(
                    graph, direction, lambda d: d > threshold
                ),
            )
        )
        queries_and_expected_outputs.append(
            _degree_filter_nodes_query_expected_pair(
                _degree(direction, "gt", f"{{ u64: {threshold} }}"),
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
        above = _degree(direction, "gt", f"{{ u64: {threshold} }}")
        below_upper = _degree(direction, "lt", f"{{ u64: {upper} }}")
        below = _degree(direction, "lt", f"{{ u64: {threshold} }}")
        above_upper = _degree(direction, "gt", f"{{ u64: {upper} }}")
        queries_and_expected_outputs.append(
            _degree_select_nodes_query_expected_pair(
                f"{{ and: [" f"{above}," f"{below_upper}" "] }",
                _expected_degree_select_names(
                    graph, direction, lambda d: d > threshold and d < upper
                ),
            )
        )
        queries_and_expected_outputs.append(
            _degree_filter_nodes_query_expected_pair(
                f"{{ and: [" f"{above}," f"{below_upper}" "] }",
                _expected_degree_names(
                    graph, direction, lambda d: d > threshold and d < upper
                ),
            )
        )

        queries_and_expected_outputs.append(
            _degree_select_nodes_query_expected_pair(
                f"{{ or: [" f"{below}," f"{above_upper}" "] }",
                _expected_degree_select_names(
                    graph, direction, lambda d: d < threshold or d > upper
                ),
            )
        )
        queries_and_expected_outputs.append(
            _degree_filter_nodes_query_expected_pair(
                f"{{ or: [" f"{below}," f"{above_upper}" "] }",
                _expected_degree_names(
                    graph, direction, lambda d: d < threshold or d > upper
                ),
            )
        )

        queries_and_expected_outputs.append(
            _degree_select_nodes_query_expected_pair(
                f"{{ or: [" f"{below}," f"{{ not: " f"{above_upper}" f" }}" "] }",
                _expected_degree_select_names(
                    graph, direction, lambda d: d < threshold or d <= upper
                ),
            )
        )
        queries_and_expected_outputs.append(
            _degree_filter_nodes_query_expected_pair(
                f"{{ or: [" f"{below}," f"{{ not: " f"{above_upper}" f" }}" "] }",
                _expected_degree_names(
                    graph, direction, lambda d: d < threshold or d <= upper
                ),
            )
        )

        queries_and_expected_outputs.append(
            _degree_select_nodes_query_expected_pair(
                _degree(
                    direction,
                    "isIn",
                    f"{{ list: [{{u64: {threshold}}}, {{u64: {threshold + 1}}}] }}",
                ),
                _expected_degree_select_names(
                    graph, direction, lambda d: d in [threshold, threshold + 1]
                ),
            )
        )
        queries_and_expected_outputs.append(
            _degree_filter_nodes_query_expected_pair(
                _degree(
                    direction,
                    "isIn",
                    f"{{ list: [{{u64: {threshold}}}, {{u64: {threshold + 1}}}] }}",
                ),
                _expected_degree_names(
                    graph, direction, lambda d: d in [threshold, threshold + 1]
                ),
            )
        )

        queries_and_expected_outputs.append(
            _degree_select_nodes_query_expected_pair(
                _degree(
                    direction,
                    "isNotIn",
                    f"{{ list: [{{u64: {threshold}}}, {{u64: {threshold + 1}}}] }}",
                ),
                _expected_degree_select_names(
                    graph, direction, lambda d: d not in [threshold, threshold + 1]
                ),
            )
        )
        queries_and_expected_outputs.append(
            _degree_filter_nodes_query_expected_pair(
                _degree(
                    direction,
                    "isNotIn",
                    f"{{ list: [{{u64: {threshold}}}, {{u64: {threshold + 1}}}] }}",
                ),
                _expected_degree_names(
                    graph, direction, lambda d: d not in [threshold, threshold + 1]
                ),
            )
        )

    run_group_graphql_test(queries_and_expected_outputs, graph, sort_output=True)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_filter_nodes_degree_float_constants_gql(graph):
    # A float constant is compared as written: 4.5 sits between 4 and 5, and a
    # fractional set member matches no degree while a whole one still does.
    queries_and_expected_outputs = []

    for direction in ["BOTH", "IN", "OUT"]:
        queries_and_expected_outputs.append(
            _degree_select_nodes_query_expected_pair(
                _degree(direction, "ge", "{ f64: 4.5 }"),
                _expected_degree_select_names(graph, direction, lambda d: d >= 4.5),
            )
        )
        queries_and_expected_outputs.append(
            _degree_filter_nodes_query_expected_pair(
                _degree(direction, "ge", "{ f64: 4.5 }"),
                _expected_degree_names(graph, direction, lambda d: d >= 4.5),
            )
        )
        queries_and_expected_outputs.append(
            _degree_select_nodes_query_expected_pair(
                _degree(direction, "eq", "{ f64: 3.0 }"),
                _expected_degree_select_names(graph, direction, lambda d: d == 3),
            )
        )
        queries_and_expected_outputs.append(
            _degree_filter_nodes_query_expected_pair(
                _degree(direction, "isIn", "{ list: [{f64: 3.0}, {f64: 4.9}] }"),
                _expected_degree_names(graph, direction, lambda d: d == 3),
            )
        )

    run_group_graphql_test(queries_and_expected_outputs, graph, sort_output=True)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_filter_nodes_degree_string_constants_gql(graph):
    # A string constant never compares with a degree, even one that spells a
    # number; in a set it is simply not a member, so the numbers still count.
    for direction in ["BOTH", "IN", "OUT"]:
        expr = _degree(direction, "eq", '{ str: "4" }')
        query = f"""
    query {{
      graph(path: "g") {{
        filterNodes: filter(expr: {expr}) {{
          nodes {{ list {{ name }} }}
        }}
      }}
    }}
    """
        run_graphql_error_test(
            query,
            'Invalid filter: value Str(ArcStr("4")) of type Str cannot be compared with U64',
            graph,
        )

    queries_and_expected_outputs = [
        _degree_filter_nodes_query_expected_pair(
            _degree("BOTH", "isIn", '{ list: [{str: "3"}, {u64: 4}] }'),
            _expected_degree_names(graph, "BOTH", lambda d: d == 4),
        ),
        _degree_filter_nodes_query_expected_pair(
            _degree("BOTH", "isNotIn", '{ list: [{str: "3"}, {u64: 4}] }'),
            _expected_degree_names(graph, "BOTH", lambda d: d != 4),
        ),
        _degree_filter_nodes_query_expected_pair(
            _degree("OUT", "isIn", '{ list: [{str: "a"}, {str: "b"}] }'),
            _expected_degree_names(graph, "OUT", lambda d: False),
        ),
        _degree_filter_nodes_query_expected_pair(
            _degree("BOTH", "isNotIn", '{ list: [{str: "x"}, {str: "y"}] }'),
            _expected_degree_names(graph, "BOTH", lambda d: True),
        ),
    ]
    run_group_graphql_test(queries_and_expected_outputs, graph, sort_output=True)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_filter_nodes_degree_invalid_non_numeric_string_values_gql(graph):
    invalid_exprs = [
        _degree("BOTH", "lt", '{ str: "foo" }'),
        _degree("IN", "eq", '{ str: "bar" }'),
    ]

    for expr in invalid_exprs:
        filter_nodes_query = f"""
    query {{
      graph(path: "g") {{
      filterNodes: filter(expr: {expr}) {{
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
          select(expr: {expr}) {{
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
        _degree("BOTH", "isNone", "true"),
        _degree("IN", "isSome", "true"),
        _degree("OUT", "startsWith", '{ str: "1" }'),
        _degree("BOTH", "endsWith", '{ str: "1" }'),
        _degree("IN", "contains", '{ str: "1" }'),
        _degree("OUT", "notContains", '{ str: "1" }'),
        _degree("BOTH", "eq", "{ u64: 1 }", over="any"),
        _degree("IN", "eq", "{ u64: 1 }", over="all"),
        _degree("OUT", "gt", "{ u64: 0 }", over="len"),
        _degree("BOTH", "eq", "{ u64: 1 }", over="sum"),
        _degree("IN", "eq", "{ u64: 1 }", over="avg"),
        _degree("OUT", "eq", "{ u64: 1 }", over="first"),
        _degree("BOTH", "eq", "{ u64: 1 }", over="last"),
    ]

    for expr in invalid_exprs:
        filter_nodes_query = f"""
    query {{
      graph(path: "g") {{
      filterNodes: filter(expr: {expr}) {{
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
          select(expr: {expr}) {{
            list {{ name }}
          }}
        }}
      }}
    }}
    """

        run_graphql_error_test_contains(filter_nodes_query, ["Invalid filter"], graph)
        run_graphql_error_test_contains(select_nodes_query, ["Invalid filter"], graph)
