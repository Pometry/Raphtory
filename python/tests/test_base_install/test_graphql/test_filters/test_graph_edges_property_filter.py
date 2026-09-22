import pytest
from raphtory import Graph, PersistentGraph
from filters_setup import create_test_graph, init_graph2
from utils import (
    run_graphql_test,
    run_graphql_error_test,
    run_graphql_error_test_contains,
    run_graphql_compare_test,
)

EVENT_GRAPH = create_test_graph(Graph())
PERSISTENT_GRAPH = create_test_graph(PersistentGraph())


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_property_filter_equal(graph):
    query = """
    query {
      graph(path: "g") {
        filterEdges: filter(expr: {
          eq: {
            lhs: { read: { entity: EDGE, target: { property: "eprop5" } } }
            rhs: { const: { list: [{ i64: 1 }, { i64: 2 }, { i64: 3 }] } }
          }
        }) {
          edges { list { src { name } dst { name } } }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "filterEdges": {
                "edges": {"list": [{"src": {"name": "a"}, "dst": {"name": "d"}}]}
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_property_filter_equal_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        filterEdges: filter(expr: {
          eq: {
            lhs: { read: { entity: EDGE, target: { property: "eprop5" } } }
            rhs: { const: { i64: 1 } }
          }
        }) {
          nodes { list { name } }
        }
      }
    }
    """
    expected_error_message = "Invalid filter: a filter needs a yes/no answer, but this comparison gives one answer per element (List<Bool>); add any() or all() to say which elements must match"
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_property_filter_not_equal(graph):
    query = """
    query {
      graph(path: "g") {
        filterEdges: filter(expr: {
          ne: {
            lhs: { read: { entity: EDGE, target: { property: "eprop4" } } }
            rhs: { const: { bool: true } }
          }
        }) {
          edges { list { src { name } dst { name } } }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "filterEdges": {
                "edges": {"list": [{"src": {"name": "c"}, "dst": {"name": "d"}}]}
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_property_filter_not_equal_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        filterEdges: filter(expr: {
          ne: {
            lhs: { read: { entity: EDGE, target: { property: "eprop4" } } }
            rhs: { const: { i64: 1 } }
          }
        }) {
          edges { list { src { name } dst { name } } }
        }
      }
    }
    """
    expected_error_message = (
        "Invalid filter: value I64(1) of type I64 cannot be compared with Bool"
    )
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_property_filter_greater_than_or_equal(graph):
    query = """
    query {
      graph(path: "g") {
        filterEdges: filter(expr: {
          ge: {
            lhs: { read: { entity: EDGE, target: { property: "eprop1" } } }
            rhs: { const: { i64: 60 } }
          }
        }) {
          edges { list { src { name } dst { name } } }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "filterEdges": {
                "edges": {"list": [{"src": {"name": "a"}, "dst": {"name": "d"}}]}
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_property_filter_greater_than_or_equal_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        filterEdges: filter(expr: {
          ge: {
            lhs: { read: { entity: EDGE, target: { property: "eprop1" } } }
            rhs: { const: { str: "shivam" } }
          }
        }) {
          edges { list { src { name } dst { name } } }
        }
      }
    }
    """
    expected_error_message = 'Invalid filter: value Str(ArcStr("shivam")) of type Str cannot be compared with I64'
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_property_filter_less_than_or_equal(graph):
    query = """
    query {
      graph(path: "g") {
        filterEdges: filter(expr: {
          le: {
            lhs: { read: { entity: EDGE, target: { property: "eprop1" } } }
            rhs: { const: { i64: 30 } }
          }
        }) {
          edges { list { src { name } dst { name } } }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "filterEdges": {
                "edges": {
                    "list": [
                        {"src": {"name": "b"}, "dst": {"name": "d"}},
                        {"src": {"name": "c"}, "dst": {"name": "d"}},
                    ]
                }
            }
        }
    }
    run_graphql_test(query, expected_output, graph, sort_output=True)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_property_filter_less_than_or_equal_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        filterEdges: filter(expr: {
          le: {
            lhs: { read: { entity: EDGE, target: { property: "eprop1" } } }
            rhs: { const: { str: "shivam" } }
          }
        }) {
          edges { list { src { name } dst { name } } }
        }
      }
    }
    """
    expected_error_message = 'Invalid filter: value Str(ArcStr("shivam")) of type Str cannot be compared with I64'
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_property_filter_greater_than(graph):
    query = """
    query {
      graph(path: "g") {
        filterEdges: filter(expr: {
          gt: {
            lhs: { read: { entity: EDGE, target: { property: "eprop1" } } }
            rhs: { const: { i64: 30 } }
          }
        }) {
          edges { list { src { name } dst { name } } }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "filterEdges": {
                "edges": {"list": [{"src": {"name": "a"}, "dst": {"name": "d"}}]}
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_property_filter_greater_than_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        filterEdges: filter(expr: {
          gt: {
            lhs: { read: { entity: EDGE, target: { property: "eprop1" } } }
            rhs: { const: { str: "shivam" } }
          }
        }) {
          edges { list { src { name } dst { name } } }
        }
      }
    }
    """
    expected_error_message = 'Invalid filter: value Str(ArcStr("shivam")) of type Str cannot be compared with I64'
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_property_filter_less_than(graph):
    query = """
    query {
      graph(path: "g") {
        filterEdges: filter(expr: {
          lt: {
            lhs: { read: { entity: EDGE, target: { property: "eprop1" } } }
            rhs: { const: { i64: 30 } }
          }
        }) {
          edges { list { src { name } dst { name } } }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "filterEdges": {
                "edges": {"list": [{"src": {"name": "b"}, "dst": {"name": "d"}}]}
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_property_filter_less_than_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        filterEdges: filter(expr: {
          lt: {
            lhs: { read: { entity: EDGE, target: { property: "eprop1" } } }
            rhs: { const: { str: "shivam" } }
          }
        }) {
          edges { list { src { name } dst { name } } }
        }
      }
    }
    """
    expected_error_message = 'Invalid filter: value Str(ArcStr("shivam")) of type Str cannot be compared with I64'
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_property_filter_is_none(graph):
    query = """
    query {
      graph(path: "g") {
        filterEdges: filter(expr: { isNone: { read: { entity: EDGE, target: { property: "eprop5" } } } }) {
          edges { list { src { name } dst { name } } }
        }
      }
    }
    """
    expected_output = {"graph": {"filterEdges": {"edges": {"list": []}}}}
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_property_filter_is_some(graph):
    query = """
    query {
      graph(path: "g") {
        filterEdges: filter(expr: { isSome: { read: { entity: EDGE, target: { property: "eprop5" } } } }) {
          edges { list { src { name } dst { name } } }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "filterEdges": {
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
    run_graphql_test(query, expected_output, graph, sort_output=True)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_property_filter_is_in(graph):
    query = """
    query {
      graph(path: "g") {
        filterEdges: filter(expr: {
          isIn: {
            expr: { read: { entity: EDGE, target: { property: "eprop1" } } }
            values: { list: [{ i64: 10 }, { i64: 20 }, { i64: 30 }] }
          }
        }) {
          edges { list { src { name } dst { name } } }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "filterEdges": {
                "edges": {
                    "list": [
                        {"src": {"name": "b"}, "dst": {"name": "d"}},
                        {"src": {"name": "c"}, "dst": {"name": "d"}},
                    ]
                }
            }
        }
    }
    run_graphql_test(query, expected_output, graph, sort_output=True)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_property_filter_is_empty_list(graph):
    query = """
    query {
      graph(path: "g") {
        filterEdges: filter(expr: {
          isIn: {
            expr: { read: { entity: EDGE, target: { property: "eprop1" } } }
            values: { list: [] }
          }
        }) {
          edges { list { src { name } dst { name } } }
        }
      }
    }
    """
    expected_output = {"graph": {"filterEdges": {"edges": {"list": []}}}}
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_property_filter_is_in_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        filterEdges: filter(expr: {
          isIn: {
            expr: { read: { entity: EDGE, target: { property: "eprop1" } } }
            values: { str: "shivam" }
          }
        }) {
          edges { list { src { name } dst { name } } }
        }
      }
    }
    """
    expected_error_message = (
        "Invalid filter: isIn requires a list value, got Str(shivam)"
    )
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_property_filter_is_not_in(graph):
    query = """
    query {
      graph(path: "g") {
        filterEdges: filter(expr: {
          isNotIn: {
            expr: { read: { entity: EDGE, target: { property: "eprop1" } } }
            values: { list: [{ i64: 10 }, { i64: 20 }, { i64: 30 }] }
          }
        }) {
          edges { list { src { name } dst { name } } }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "filterEdges": {
                "edges": {"list": [{"src": {"name": "a"}, "dst": {"name": "d"}}]}
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_property_filter_is_not_in_empty_list(graph):
    query = """
    query {
      graph(path: "g") {
        filterEdges: filter(expr: {
          isNotIn: {
            expr: { read: { entity: EDGE, target: { property: "eprop1" } } }
            values: { list: [] }
          }
        }) {
          edges { list { src { name } dst { name } } }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "filterEdges": {
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
    run_graphql_test(query, expected_output, graph, sort_output=True)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_property_filter_is_not_in_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        filterEdges: filter(expr: {
          isNotIn: {
            expr: { read: { entity: EDGE, target: { property: "eprop1" } } }
            values: { str: "shivam" }
          }
        }) {
          edges { list { src { name } dst { name } } }
        }
      }
    }
    """
    expected_error_message = (
        "Invalid filter: isNotIn requires a list value, got Str(shivam)"
    )
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_not_property_filter(graph):
    query = """
    query {
      graph(path: "g") {
        filterEdges: filter(expr: {
          not: {
            eq: {
              lhs: { read: { entity: EDGE, target: { property: "eprop5" } } }
              rhs: { const: { list: [{ i64: 1 }, { i64: 2 }] } }
            }
          }
        }) {
          edges { list { src { name } dst { name } } }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "filterEdges": {
                "edges": {
                    "list": [
                        {"dst": {"name": "d"}, "src": {"name": "a"}},
                        {"dst": {"name": "d"}, "src": {"name": "b"}},
                        {"dst": {"name": "d"}, "src": {"name": "c"}},
                    ]
                }
            }
        }
    }
    run_graphql_test(query, expected_output, graph, sort_output=True)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_edges_property_filter_starts_with(graph):
    query = """
    query {
      graph(path: "g") {
        filterEdges: filter(expr: {
          startsWith: {
            lhs: { read: { entity: EDGE, target: { property: "eprop3" } } }
            rhs: { const: { str: "xyz" } }
          }
        }) {
          edges { list { src { name } dst { name } } }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "filterEdges": {
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
    run_graphql_test(query, expected_output, graph, sort_output=True)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_edges_property_filter_ends_with(graph):
    query = """
    query {
      graph(path: "g") {
        filterEdges: filter(expr: {
          endsWith: {
            lhs: { read: { entity: EDGE, target: { property: "eprop3" } } }
            rhs: { const: { str: "123" } }
          }
        }) {
          edges { list { src { name } dst { name } } }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "filterEdges": {
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
    run_graphql_test(query, expected_output, graph, sort_output=True)


EVENT_GRAPH = init_graph2(Graph())
PERSISTENT_GRAPH = init_graph2(PersistentGraph())


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_edges_selection(graph):
    query = """
    query {
      graph(path: "g") {
        edges(select: {
          gt: {
            lhs: { read: { entity: EDGE, target: { property: "p2" } } }
            rhs: { const: { i64: 3 } }
          }
        }) {
             list { src { name } dst { name } }
          }
        }
      }
    """
    expected_output = {
        "graph": {
            "edges": {
                "list": [
                    {"dst": {"name": "2"}, "src": {"name": "1"}},
                    {"dst": {"name": "1"}, "src": {"name": "3"}},
                    {"dst": {"name": "4"}, "src": {"name": "3"}},
                    {"dst": {"name": "1"}, "src": {"name": "2"}},
                ]
            }
        }
    }
    run_graphql_test(query, expected_output, graph, sort_output=True)


# The inner edges filter has no effect on the list of edges returned from selection filter
@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_edges_selection_edges_filter_paired(graph):
    query = """
    query {
      graph(path: "g") {
        edges(select: {
          gt: {
            lhs: { read: { entity: EDGE, target: { property: "p2" } } }
            rhs: { const: { i64: 3 } }
          }
        }) {
          filter(expr: {
            eq: {
              lhs: { read: { entity: EDGE, target: { property: "p3" } } }
              rhs: { const: { i64: 5 } }
            }
          }) {
            list { src { name } dst { name } }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "edges": {
                "filter": {
                    "list": [
                        {"dst": {"name": "2"}, "src": {"name": "1"}},
                        {"dst": {"name": "1"}, "src": {"name": "3"}},
                        {"dst": {"name": "4"}, "src": {"name": "3"}},
                        {"dst": {"name": "1"}, "src": {"name": "2"}},
                    ]
                }
            }
        }
    }
    run_graphql_test(query, expected_output, graph, sort_output=True)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_edges_chained_selection_edges_filter_paired(graph):
    query = """
    query {
      graph(path: "g") {
        edges(select: {
          gt: {
            lhs: { read: { entity: EDGE, target: { property: "p2" } } }
            rhs: { const: { i64: 3 } }
          }
        }) {
          select(expr: {
            lt: {
              lhs: { read: { entity: EDGE, target: { property: "p2" } } }
              rhs: { const: { i64: 5 } }
            }
          }) {
            filter(expr: {
              eq: {
                lhs: { read: { entity: EDGE, target: { field: ID }, endpoint: DST } }
                rhs: { const: { u64: 2 } }
              }
            }) {
              list { src { name } dst { name } }
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "edges": {
                "select": {
                    "filter": {"list": [{"dst": {"name": "2"}, "src": {"name": "1"}}]}
                }
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_edges_chained_selection_edges_filter_paired_ver2(graph):
    query = """
    query {
      graph(path: "g") {
        edges {
          select(expr: {
            gt: {
              lhs: { read: { entity: EDGE, target: { property: "p2" } } }
              rhs: { const: { i64: 3 } }
            }
          }) {
            select(expr: {
              lt: {
                lhs: { read: { entity: EDGE, target: { property: "p2" } } }
                rhs: { const: { i64: 5 } }
              }
            }) {
              filter(expr: {
                eq: {
                  lhs: { read: { entity: EDGE, target: { field: ID }, endpoint: DST } }
                  rhs: { const: { u64: 2 } }
                }
              }) {
                list { src { name } dst { name } }
              }
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "edges": {
                "select": {
                    "select": {
                        "filter": {
                            "list": [{"dst": {"name": "2"}, "src": {"name": "1"}}]
                        }
                    }
                }
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_edge_temporal_property_filter_empty_layers(graph):
    query = """
    query {
      graph(path: "g") {
        filterEdges: filter(expr: {
          lt: {
            lhs: {
              avg: {
                temporal: {
                  read: { entity: EDGE, target: { property: "p2" }, views: [{ layers: [] }] }
                }
              }
            }
            rhs: { const: { f64: 1.0 } }
          }
        }) {
          edges { list { src { name } dst { name } } }
        }
      }
    }
    """

    expected = {"graph": {"filterEdges": {"edges": {"list": []}}}}
    run_graphql_test(query, expected, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_edges_temporal_property_last_with_single_layer(graph):
    query = """
    query {
      graph(path: "g") {
        filterEdges: filter(expr: {
          eq: {
            lhs: {
              last: {
                temporal: {
                  read: {
                    entity: EDGE
                    target: { property: "p10" }
                    views: [{ layers: ["air_nomads"] }]
                  }
                }
              }
            }
            rhs: { const: { str: "Paper_ship" } }
          }
        }) {
          edges { list { src { name } dst { name } } }
        }
      }
    }
    """

    # Edge (2 -> 3) in air_nomads has p10 Paper_ship at time 2
    expected = {
        "graph": {
            "filterEdges": {
                "edges": {"list": [{"src": {"name": "2"}, "dst": {"name": "3"}}]}
            }
        }
    }

    run_graphql_test(query, expected, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_edges_temporal_property_last_with_multiple_layers(graph):
    query = """
    query {
      graph(path: "g") {
        filterEdges: filter(expr: {
          eq: {
            lhs: {
              last: {
                temporal: {
                  read: {
                    entity: EDGE
                    target: { property: "p10" }
                    views: [{ layers: ["fire_nation", "air_nomads"] }]
                  }
                }
              }
            }
            rhs: { const: { str: "Paper_airplane" } }
          }
        }) {
          edges { list { src { name } dst { name } } }
        }
      }
    }
    """

    # fire_nation edge (1 -> 2) has p10 Paper_airplane at time 1
    expected = {
        "graph": {
            "filterEdges": {
                "edges": {"list": [{"src": {"name": "1"}, "dst": {"name": "2"}}]}
            }
        }
    }
    run_graphql_test(query, expected, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_edges_temporal_property_last_with_default_layer(graph):
    query = """
    query {
      graph(path: "g") {
        filterEdges: filter(expr: {
          eq: {
            lhs: {
              last: {
                temporal: {
                  read: {
                    entity: EDGE
                    target: { property: "p10" }
                    views: [{ layers: ["_default"] }]
                  }
                }
              }
            }
            rhs: { const: { str: "Paper_airplane" } }
          }
        }) {
          edges { list { src { name } dst { name } } }
        }
      }
    }
    """

    # default-layer edge (2 -> 1) has p10 Paper_airplane at time 3 (edge_type is None)
    expected = {
        "graph": {
            "filterEdges": {
                "edges": {"list": [{"src": {"name": "2"}, "dst": {"name": "1"}}]}
            }
        }
    }
    run_graphql_test(query, expected, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_edges_at_temporal_last(graph):
    query = """
    query {
      graph(path: "g") {
        filterEdges: filter(expr: {
          eq: {
            lhs: {
              last: {
                temporal: {
                  read: { entity: EDGE, target: { property: "p10" }, views: [{ at: 1 }] }
                }
              }
            }
            rhs: { const: { str: "Paper_airplane" } }
          }
        }) {
          edges { list { src { name } dst { name } } }
        }
      }
    }
    """

    expected = {
        "graph": {
            "filterEdges": {
                "edges": {"list": [{"src": {"name": "1"}, "dst": {"name": "2"}}]}
            }
        }
    }
    run_graphql_test(query, expected, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_edges_before_temporal_last(graph):
    query = """
    query {
      graph(path: "g") {
        filterEdges: filter(expr: {
          eq: {
            lhs: {
              last: {
                temporal: {
                  read: { entity: EDGE, target: { property: "p10" }, views: [{ before: 2 }] }
                }
              }
            }
            rhs: { const: { str: "Paper_airplane" } }
          }
        }) {
          edges { list { src { name } dst { name } } }
        }
      }
    }
    """

    expected = {
        "graph": {
            "filterEdges": {
                "edges": {"list": [{"src": {"name": "1"}, "dst": {"name": "2"}}]}
            }
        }
    }
    run_graphql_test(query, expected, graph)


@pytest.mark.parametrize("graph", [PERSISTENT_GRAPH])
def test_edges_after_temporal_last(graph):
    query = """
    query {
      graph(path: "g") {
        filterEdges: filter(expr: {
          eq: {
            lhs: {
              last: {
                temporal: {
                  read: { entity: EDGE, target: { property: "p10" }, views: [{ after: 2 }] }
                }
              }
            }
            rhs: { const: { str: "Paper_ship" } }
          }
        }) {
          edges { list { src { name } dst { name } } }
        }
      }
    }
    """

    expected = {
        "graph": {
            "filterEdges": {
                "edges": {"list": [{"dst": {"name": "3"}, "src": {"name": "2"}}]}
            }
        }
    }
    run_graphql_test(query, expected, graph)


@pytest.mark.parametrize("graph", [PERSISTENT_GRAPH])
def test_edges_latest_temporal_last(graph):
    query = """
    query {
      graph(path: "g") {
        filterEdges: filter(expr: {
          eq: {
            lhs: {
              last: {
                temporal: {
                  read: { entity: EDGE, target: { property: "p10" }, views: [{ latest: true }] }
                }
              }
            }
            rhs: { const: { str: "Paper_ship" } }
          }
        }) {
          edges { list { src { name } dst { name } } }
        }
      }
    }
    """
    expected = {
        "graph": {
            "filterEdges": {
                "edges": {"list": [{"src": {"name": "2"}, "dst": {"name": "3"}}]}
            }
        }
    }
    run_graphql_test(query, expected, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_edges_snapshot_at_temporal_last(graph):
    query = """
    query {
      graph(path: "g") {
        filterEdges: filter(expr: {
          eq: {
            lhs: {
              last: {
                temporal: {
                  read: {
                    entity: EDGE
                    target: { property: "p10" }
                    views: [{ snapshotAt: 2 }]
                  }
                }
              }
            }
            rhs: { const: { str: "Paper_ship" } }
          }
        }) {
          edges { list { src { name } dst { name } } }
        }
      }
    }
    """
    expected = {
        "graph": {
            "filterEdges": {
                "edges": {"list": [{"src": {"name": "2"}, "dst": {"name": "3"}}]}
            }
        }
    }
    run_graphql_test(query, expected, graph)


@pytest.mark.parametrize("graph", [PERSISTENT_GRAPH])
def test_edges_snapshot_latest_temporal_last(graph):
    query = """
    query {
      graph(path: "g") {
        filterEdges: filter(expr: {
          eq: {
            lhs: {
              last: {
                temporal: {
                  read: {
                    entity: EDGE
                    target: { property: "p10" }
                    views: [{ snapshotLatest: true }]
                  }
                }
              }
            }
            rhs: { const: { str: "Paper_ship" } }
          }
        }) {
          edges { list { src { name } dst { name } } }
        }
      }
    }
    """
    expected = {
        "graph": {
            "filterEdges": {
                "edges": {"list": [{"src": {"name": "2"}, "dst": {"name": "3"}}]}
            }
        }
    }
    run_graphql_test(query, expected, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH])
def test_edges_graph_filter_gql(graph):
    query = """
    query {
      graph(path: "g") {
        filter(expr: {
          view: [{ layers: ["fire_nation"] }, { window: { start: 1, end: 4 } }]
        })
        {
          nodes {
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
            "filter": {
                "nodes": {
                    "list": [{"name": "1"}, {"name": "2"}, {"name": "3"}, {"name": "4"}]
                }
            }
        }
    }
    run_graphql_test(query, expected, graph, sort_output=True)
