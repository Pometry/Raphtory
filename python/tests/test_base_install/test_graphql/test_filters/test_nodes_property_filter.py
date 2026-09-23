import pytest
from raphtory import Graph, PersistentGraph
from filters_setup import (
    create_test_graph,
    create_test_graph2,
    create_test_graph3,
    init_graph,
    init_graph2,
)
from utils import (
    run_graphql_test,
    run_graphql_error_test,
    run_graphql_error_test_contains,
)

EVENT_GRAPH = create_test_graph(Graph())
PERSISTENT_GRAPH = create_test_graph(PersistentGraph())


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_equal2(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          filter(expr: {
                         node: {
                           eq: {
                             lhs: {
                               property: "prop5"
                             }
                             rhs: {
                               const: {
                                 list: [{
                                   i64: 1
                                 }, {
                                   i64: 2
                                 }, {
                                   i64: 3
                                 }]
                               }
                             }
                           }
                         }
                       }) {
            list {
              name
              neighbours {
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
            "nodes": {
                "filter": {
                    "list": [
                        {"name": "a", "neighbours": {"list": []}},
                        {"name": "b", "neighbours": {"list": []}},
                        {"name": "c", "neighbours": {"list": []}},
                        {"name": "d", "neighbours": {"list": [{"name": "a"}]}},
                    ]
                }
            }
        }
    }
    run_graphql_test(query, expected_output, graph, sort_output=True)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_equal3(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          select(expr: {
                         node: {
                           eq: {
                             lhs: {
                               property: "prop5"
                             }
                             rhs: {
                               const: {
                                 list: [{
                                   i64: 1
                                 }, {
                                   i64: 2
                                 }, {
                                   i64: 3
                                 }]
                               }
                             }
                           }
                         }
                       }
          ) {
            list { name }
          }
        }
      }
    }
    """
    expected_output = {"graph": {"nodes": {"select": {"list": [{"name": "a"}]}}}}
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_equal_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          select(expr: {
                         node: {
                           eq: {
                             lhs: {
                               property: "prop5"
                             }
                             rhs: {
                               const: {
                                 i64: 1
                               }
                             }
                           }
                         }
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
    expected_error_message = "Invalid filter: a filter needs a yes/no answer, but this comparison gives one answer per element (List<Bool>); add any() or all() to say which elements must match"
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_not_equal(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          select(expr: {
                         node: {
                           ne: {
                             lhs: {
                               property: "prop4"
                             }
                             rhs: {
                               const: {
                                 bool: true
                               }
                             }
                           }
                         }
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
        "graph": {"nodes": {"select": {"list": [{"name": "b"}, {"name": "d"}]}}}
    }
    run_graphql_test(query, expected_output, graph, sort_output=True)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_not_equal_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          select(expr: {
                         node: {
                           ne: {
                             lhs: {
                               property: "prop4"
                             }
                             rhs: {
                               const: {
                                 i64: 1
                               }
                             }
                           }
                         }
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
    expected_error_message = (
        "Invalid filter: value I64(1) of type I64 cannot be compared with Bool"
    )
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_greater_than_or_equal(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          select(expr: {
                         node: {
                           ge: {
                             lhs: {
                               property: "prop1"
                             }
                             rhs: {
                               const: {
                                 i64: 60
                               }
                             }
                           }
                         }
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
    expected_output = {"graph": {"nodes": {"select": {"list": [{"name": "a"}]}}}}
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_greater_than_or_equal_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          select(expr: {
                         node: {
                           ge: {
                             lhs: {
                               property: "prop1"
                             }
                             rhs: {
                               const: {
                                 str: "shivam"
                               }
                             }
                           }
                         }
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
    expected_error_message = 'Invalid filter: value Str(ArcStr("shivam")) of type Str cannot be compared with I64'
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_less_than_or_equal(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          select(expr: {
                         node: {
                           le: {
                             lhs: {
                               property: "prop1"
                             }
                             rhs: {
                               const: {
                                 i64: 30
                               }
                             }
                           }
                         }
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
            "nodes": {"select": {"list": [{"name": "b"}, {"name": "c"}, {"name": "d"}]}}
        }
    }
    run_graphql_test(query, expected_output, graph, sort_output=True)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_less_than_or_equal_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          select(expr: {
                         node: {
                           le: {
                             lhs: {
                               property: "prop1"
                             }
                             rhs: {
                               const: {
                                 str: "shivam"
                               }
                             }
                           }
                         }
                       }
          ) {
            list { name }
          }
        }
      }
    }
    """
    expected_error_message = 'Invalid filter: value Str(ArcStr("shivam")) of type Str cannot be compared with I64'
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_greater_than(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          select(expr: {
                         node: {
                           gt: {
                             lhs: {
                               property: "prop1"
                             }
                             rhs: {
                               const: {
                                 i64: 30
                               }
                             }
                           }
                         }
                       }
          ) {
            list { name }
          }
        }
      }
    }
    """
    expected_output = {"graph": {"nodes": {"select": {"list": [{"name": "a"}]}}}}
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_greater_than_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          select(expr: {
                         node: {
                           gt: {
                             lhs: {
                               property: "prop1"
                             }
                             rhs: {
                               const: {
                                 str: "shivam"
                               }
                             }
                           }
                         }
                       }
          ) {
            list { name }
          }
        }
      }
    }
    """
    expected_error_message = 'Invalid filter: value Str(ArcStr("shivam")) of type Str cannot be compared with I64'
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_less_than(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          select(expr: {
                         node: {
                           lt: {
                             lhs: {
                               property: "prop1"
                             }
                             rhs: {
                               const: {
                                 i64: 30
                               }
                             }
                           }
                         }
                       }
          ) {
            list { name }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {"nodes": {"select": {"list": [{"name": "b"}, {"name": "c"}]}}}
    }
    run_graphql_test(query, expected_output, graph, sort_output=True)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_less_than_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          select(expr: {
                         node: {
                           lt: {
                             lhs: {
                               property: "prop1"
                             }
                             rhs: {
                               const: {
                                 str: "shivam"
                               }
                             }
                           }
                         }
                       }
          ) {
            list { name }
          }
        }
      }
    }
    """
    expected_error_message = 'Invalid filter: value Str(ArcStr("shivam")) of type Str cannot be compared with I64'
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_is_none(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          select(expr: { node: { isNone: { property: "prop5" } } }
          ) {
            list { name }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {"nodes": {"select": {"list": [{"name": "b"}, {"name": "d"}]}}}
    }
    run_graphql_test(query, expected_output, graph, sort_output=True)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_is_some(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          select(expr: { node: { isSome: { property: "prop5" } } }
          ) {
            list { name }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {"nodes": {"select": {"list": [{"name": "a"}, {"name": "c"}]}}}
    }
    run_graphql_test(query, expected_output, graph, sort_output=True)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_is_in(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          select(expr: {
                         node: {
                           isIn: {
                             expr: {
                               property: "prop1"
                             }
                             values: {
                               list: [{
                                 i64: 10
                               }, {
                                 i64: 30
                               }, {
                                 i64: 50
                               }, {
                                 i64: 70
                               }]
                             }
                           }
                         }
                       }
          ) {
            list { name }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {"nodes": {"select": {"list": [{"name": "b"}, {"name": "d"}]}}}
    }
    run_graphql_test(query, expected_output, graph, sort_output=True)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_is_in_empty_list(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          select(expr: {
                         node: {
                           isIn: {
                             expr: {
                               property: "prop1"
                             }
                             values: {
                               list: []
                             }
                           }
                         }
                       }
          ) {
            list { name }
          }
        }
      }
    }
    """
    expected_output = {"graph": {"nodes": {"select": {"list": []}}}}
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_is_in_no_value(graph):
    # Keeping semantics: value list has no matching elements
    query = """
    query {
      graph(path: "g") {
        nodes {
          select(expr: {
                         node: {
                           isIn: {
                             expr: {
                               property: "prop1"
                             }
                             values: {
                               list: [{
                                 i64: 100
                               }]
                             }
                           }
                         }
                       }
          ) {
            list { name }
          }
        }
      }
    }
    """
    expected_output = {"graph": {"nodes": {"select": {"list": []}}}}
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_is_in_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          select(expr: {
                         node: {
                           isIn: {
                             expr: {
                               property: "prop1"
                             }
                             values: {
                               str: "shivam"
                             }
                           }
                         }
                       }
          ) {
            list { name }
          }
        }
      }
    }
    """
    expected_error_message = (
        "Invalid filter: isIn requires a list value, got Str(shivam)"
    )
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_is_not_in(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          select(expr: {
                         node: {
                           isNotIn: {
                             expr: {
                               property: "prop1"
                             }
                             values: {
                               list: [{
                                 i64: 10
                               }, {
                                 i64: 30
                               }, {
                                 i64: 50
                               }, {
                                 i64: 70
                               }]
                             }
                           }
                         }
                       }
          ) {
            list { name }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {"nodes": {"select": {"list": [{"name": "a"}, {"name": "c"}]}}}
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_is_not_in_empty_list(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          select(expr: {
                         node: {
                           isNotIn: {
                             expr: {
                               property: "prop1"
                             }
                             values: {
                               list: []
                             }
                           }
                         }
                       }
          ) {
            list { name }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "nodes": {
                "select": {
                    "list": [{"name": "a"}, {"name": "b"}, {"name": "c"}, {"name": "d"}]
                }
            }
        }
    }
    run_graphql_test(query, expected_output, graph, sort_output=True)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_is_not_in_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          select(expr: {
                         node: {
                           isNotIn: {
                             expr: {
                               property: "prop1"
                             }
                             values: {
                               str: "shivam"
                             }
                           }
                         }
                       }
          ) {
            list { name }
          }
        }
      }
    }
    """
    expected_error_message = (
        "Invalid filter: isNotIn requires a list value, got Str(shivam)"
    )
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_contains_wrong_value_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        filterNodes: filter(expr: {
                                    node: {
                                      contains: {
                                        lhs: {
                                          property: "p10"
                                        }
                                        rhs: {
                                          const: {
                                            u64: 2
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
    expected_error_message = "Property p10 does not exist"
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_nodes_property_filter_starts_with(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          select(expr: {
                         node: {
                           startsWith: {
                             lhs: {
                               property: "prop3"
                             }
                             rhs: {
                               const: {
                                 str: "abc"
                               }
                             }
                           }
                         }
                       }) {
            list { name }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "nodes": {
                "select": {
                    "list": [{"name": "a"}, {"name": "b"}, {"name": "c"}, {"name": "d"}]
                }
            }
        }
    }
    run_graphql_test(query, expected_output, graph, sort_output=True)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_nodes_property_filter_ends_with(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          select(expr: {
                         node: {
                           endsWith: {
                             lhs: {
                               property: "prop3"
                             }
                             rhs: {
                               const: {
                                 str: "333"
                               }
                             }
                           }
                         }
                       }) {
            list { name }
          }
        }
      }
    }
    """
    expected_output = {"graph": {"nodes": {"select": {"list": [{"name": "c"}]}}}}
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_nodes_property_filter_temporal_first_starts_with(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          select(expr: {
                         node: {
                           startsWith: {
                             lhs: {
                               first: {
                                 temporalProperty: "prop3"
                               }
                             }
                             rhs: {
                               const: {
                                 str: "abc"
                               }
                             }
                           }
                         }
                       }) {
            list { name }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "nodes": {
                "select": {
                    "list": [{"name": "a"}, {"name": "b"}, {"name": "c"}, {"name": "d"}]
                }
            }
        }
    }
    run_graphql_test(query, expected_output, graph, sort_output=True)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_nodes_property_filter_temporal_all_starts_with(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          select(expr: {
                         node: {
                           any: {
                             startsWith: {
                               lhs: {
                                 temporalProperty: "prop3"
                               }
                               rhs: {
                                 const: {
                                   str: "abc1"
                                 }
                               }
                             }
                           }
                         }
                       }) {
            list { name }
          }
        }
      }
    }
    """
    expected_output = {"graph": {"nodes": {"select": {"list": [{"name": "a"}]}}}}
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_nodes_property_filter_list_agg(graph):
    # SUM(list(prop5)) == 6
    query = """
    query {
      graph(path: "g") {
        filterNodes: filter(expr: {
                                    node: {
                                      eq: {
                                        lhs: {
                                          sum: {
                                            property: "prop5"
                                          }
                                        }
                                        rhs: {
                                          const: {
                                            i64: 6
                                          }
                                        }
                                      }
                                    }
                                  }) {
          nodes { list { name } }
        }
      }
    }
    """
    expected_output = {"graph": {"filterNodes": {"nodes": {"list": [{"name": "a"}]}}}}
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_nodes_property_filter_list_qualifier(graph):
    query = """
    query {
      graph(path: "g") {
        filterNodes: filter(expr: {
                                    node: {
                                      any: {
                                        eq: {
                                          lhs: {
                                            property: "prop5"
                                          }
                                          rhs: {
                                            const: {
                                              i64: 6
                                            }
                                          }
                                        }
                                      }
                                    }
                                  }) {
          nodes { list { name } }
        }
      }
    }
    """
    expected_output = {"graph": {"filterNodes": {"nodes": {"list": [{"name": "c"}]}}}}
    run_graphql_test(query, expected_output, graph)


EVENT_GRAPH = init_graph(Graph())
PERSISTENT_GRAPH = init_graph(PersistentGraph())


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_nodes_temporal_property_filter_agg(graph):
    query = """
    query {
      graph(path: "g") {
        filterNodes: filter(expr: {
                                    node: {
                                      lt: {
                                        lhs: {
                                          avg: {
                                            temporalProperty: "p2"
                                          }
                                        }
                                        rhs: {
                                          const: {
                                            f64: 10.0
                                          }
                                        }
                                      }
                                    }
                                  }) {
          nodes { list { name } }
        }
      }
    }
    """
    expected_output = {
        "graph": {"filterNodes": {"nodes": {"list": [{"name": "2"}, {"name": "3"}]}}}
    }
    run_graphql_test(query, expected_output, graph, sort_output=True)


EVENT_GRAPH = create_test_graph(Graph())
PERSISTENT_GRAPH = create_test_graph(PersistentGraph())


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_nodes_temporal_property_filter_any_avg(graph):
    # ANY timepoint where AVG(list) < 10.0
    query = """
    query {
      graph(path: "g") {
        filterNodes: filter(expr: {
                                    node: {
                                      any: {
                                        lt: {
                                          lhs: {
                                            avg: {
                                              temporalProperty: "prop5"
                                            }
                                          }
                                          rhs: {
                                            const: {
                                              f64: 10.0
                                            }
                                          }
                                        }
                                      }
                                    }
                                  }) {
          nodes { list { name } }
        }
      }
    }
    """
    expected_output = {
        "graph": {"filterNodes": {"nodes": {"list": [{"name": "a"}, {"name": "c"}]}}}
    }
    run_graphql_test(query, expected_output, graph)


EVENT_GRAPH = init_graph2(Graph())
PERSISTENT_GRAPH = init_graph2(PersistentGraph())


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_nodes_neighbours_selection_with_prop_filter(graph):
    query = """
    query {
      graph(path: "g") {
        nodes(select: {
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
            neighbours {
              list {
                name
              }
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "nodes": {
                "list": [
                    {
                        "name": "1",
                        "neighbours": {"list": [{"name": "2"}, {"name": "3"}]},
                    },
                    {
                        "name": "3",
                        "neighbours": {
                            "list": [{"name": "1"}, {"name": "2"}, {"name": "4"}]
                        },
                    },
                ]
            }
        }
    }
    run_graphql_test(query, expected_output, graph, sort_output=True)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_nodes_selection(graph):
    query = """
    query {
      graph(path: "g") {
        nodes(select: {
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
    """
    expected_output = {"graph": {"nodes": {"list": [{"name": "1"}, {"name": "3"}]}}}
    run_graphql_test(query, expected_output, graph)


# The inner nodes filter has no effect on the list of nodes returned from selection filter
@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_nodes_selection_nodes_filter_paired(graph):
    query = """
    query {
      graph(path: "g") {
        nodes(select: {
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
          filter(expr: {
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
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {"nodes": {"filter": {"list": [{"name": "1"}, {"name": "3"}]}}}
    }
    run_graphql_test(query, expected_output, graph)


# The inner nodes filter has effect on the neighbours list
@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_nodes_selection_nodes_filter_paired2(graph):
    query = """
    query {
      graph(path: "g") {
        nodes(select: {
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
          filter(expr: {
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
            list {
              neighbours {
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
            "nodes": {
                "filter": {
                    "list": [
                        {"neighbours": {"list": []}},
                        {"neighbours": {"list": [{"name": "1"}]}},
                    ]
                }
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_nodes_chained_selection_node_filter_paired(graph):
    query = """
    query {
      graph(path: "g") {
        nodes(select: {
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
              list {
                name
              }
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {"nodes": {"select": {"filter": {"list": [{"name": "1"}]}}}}
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_nodes_chained_selection_node_filter_paired_ver2(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          select(expr: {
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


EVENT_GRAPH = create_test_graph(Graph())
PERSISTENT_GRAPH = create_test_graph(PersistentGraph())


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_nodes_temporal_property_filter_any_avg_with_window(graph):
    query = """
    query {
      graph(path: "g") {
        filterNodes: filter(expr: {
                                    node: {
                                      any: {
                                        lt: {
                                          lhs: {
                                            avg: {
                                              viewed: {
                                                views: [{
                                                  window: {
                                                    start: 1
                                                    end: 3
                                                  }
                                                }]
                                                expr: {
                                                  temporalProperty: "prop5"
                                                }
                                              }
                                            }
                                          }
                                          rhs: {
                                            const: {
                                              f64: 10.0
                                            }
                                          }
                                        }
                                      }
                                    }
                                  }) {
          nodes { list { name } }
        }
      }
    }
    """

    expected = {
        "graph": {"filterNodes": {"nodes": {"list": [{"name": "a"}, {"name": "c"}]}}}
    }
    run_graphql_test(query, expected, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_layer_filter_not_supported(graph):
    query = """
    query {
      graph(path: "g") {
        filterNodes: filter(expr: {
                                    node: {
                                      any: {
                                        lt: {
                                          lhs: {
                                            avg: {
                                              viewed: {
                                                views: [{
                                                  layers: ["air_nomads"]
                                                }]
                                                expr: {
                                                  temporalProperty: "prop5"
                                                }
                                              }
                                            }
                                          }
                                          rhs: {
                                            const: {
                                              f64: 10.0
                                            }
                                          }
                                        }
                                      }
                                    }
                                  }) {
          nodes { list { name } }
        }
      }
    }
    """

    expected_needles = [
        "Invalid layer: air_nomads",
        "Valid layers:",
    ]

    run_graphql_error_test_contains(query, expected_needles, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH])
def test_nodes_at_temporal_property(graph):
    query = """
    query {
      graph(path: "g") {
        filterNodes: filter(expr: {
                                    node: {
                                      any: {
                                        lt: {
                                          lhs: {
                                            avg: {
                                              viewed: {
                                                views: [{
                                                  at: 2
                                                }]
                                                expr: {
                                                  temporalProperty: "prop5"
                                                }
                                              }
                                            }
                                          }
                                          rhs: {
                                            const: {
                                              f64: 10.0
                                            }
                                          }
                                        }
                                      }
                                    }
                                  }) {
          nodes { list { name } }
        }
      }
    }
    """
    expected = {"graph": {"filterNodes": {"nodes": {"list": [{"name": "a"}]}}}}
    run_graphql_test(query, expected, graph)


@pytest.mark.parametrize("graph", [PERSISTENT_GRAPH])
def test_nodes_before_temporal_property(graph):
    query = """
    query {
      graph(path: "g") {
        filterNodes: filter(expr: {
                                    node: {
                                      any: {
                                        lt: {
                                          lhs: {
                                            avg: {
                                              viewed: {
                                                views: [{
                                                  before: 3
                                                }]
                                                expr: {
                                                  temporalProperty: "prop5"
                                                }
                                              }
                                            }
                                          }
                                          rhs: {
                                            const: {
                                              f64: 10.0
                                            }
                                          }
                                        }
                                      }
                                    }
                                  }) {
          nodes { list { name } }
        }
      }
    }
    """
    expected = {
        "graph": {"filterNodes": {"nodes": {"list": [{"name": "a"}, {"name": "c"}]}}}
    }
    run_graphql_test(query, expected, graph)


@pytest.mark.parametrize("graph", [PERSISTENT_GRAPH])
def test_nodes_after_temporal_property(graph):
    query = """
    query {
      graph(path: "g") {
        filterNodes: filter(expr: {
                                    node: {
                                      any: {
                                        lt: {
                                          lhs: {
                                            avg: {
                                              viewed: {
                                                views: [{
                                                  after: 2
                                                }]
                                                expr: {
                                                  temporalProperty: "prop5"
                                                }
                                              }
                                            }
                                          }
                                          rhs: {
                                            const: {
                                              f64: 10.0
                                            }
                                          }
                                        }
                                      }
                                    }
                                  }) {
          nodes { list { name } }
        }
      }
    }
    """
    expected = {
        "graph": {"filterNodes": {"nodes": {"list": [{"name": "a"}, {"name": "c"}]}}}
    }
    run_graphql_test(query, expected, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH])
def test_nodes_latest_temporal_property(graph):
    query = """
    query {
      graph(path: "g") {
        filterNodes: filter(expr: {
                                    node: {
                                      any: {
                                        lt: {
                                          lhs: {
                                            avg: {
                                              viewed: {
                                                views: [{
                                                  latest: true
                                                }]
                                                expr: {
                                                  temporalProperty: "prop5"
                                                }
                                              }
                                            }
                                          }
                                          rhs: {
                                            const: {
                                              f64: 10.0
                                            }
                                          }
                                        }
                                      }
                                    }
                                  }) {
          nodes { list { name } }
        }
      }
    }
    """
    expected = {"graph": {"filterNodes": {"nodes": {"list": [{"name": "a"}]}}}}
    run_graphql_test(query, expected, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_nodes_snapshot_at_temporal_property(graph):
    query = """
    query {
      graph(path: "g") {
        filterNodes: filter(expr: {
                                    node: {
                                      any: {
                                        lt: {
                                          lhs: {
                                            avg: {
                                              viewed: {
                                                views: [{
                                                  snapshotAt: 2
                                                }]
                                                expr: {
                                                  temporalProperty: "prop5"
                                                }
                                              }
                                            }
                                          }
                                          rhs: {
                                            const: {
                                              f64: 10.0
                                            }
                                          }
                                        }
                                      }
                                    }
                                  }) {
          nodes { list { name } }
        }
      }
    }
    """
    expected = {
        "graph": {"filterNodes": {"nodes": {"list": [{"name": "a"}, {"name": "c"}]}}}
    }
    run_graphql_test(query, expected, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_nodes_snapshot_latest_temporal_property(graph):
    query = """
    query {
      graph(path: "g") {
        filterNodes: filter(expr: {
                                    node: {
                                      any: {
                                        lt: {
                                          lhs: {
                                            avg: {
                                              viewed: {
                                                views: [{
                                                  snapshotLatest: true
                                                }]
                                                expr: {
                                                  temporalProperty: "prop5"
                                                }
                                              }
                                            }
                                          }
                                          rhs: {
                                            const: {
                                              f64: 10.0
                                            }
                                          }
                                        }
                                      }
                                    }
                                  }) {
          nodes { list { name } }
        }
      }
    }
    """
    expected = {
        "graph": {"filterNodes": {"nodes": {"list": [{"name": "a"}, {"name": "c"}]}}}
    }
    run_graphql_test(query, expected, graph)


# Both orders give the same answer on this fixture; the pair pins that either order is
# accepted on a read. Order itself is pinned by `window_then_latest` in
# test_filter_expr_grammar.py.
@pytest.mark.parametrize("graph", [EVENT_GRAPH])
def test_nodes_layer_then_latest(graph):
    query = """
    query {
      graph(path: "g") {
        filterNodes: filter(expr: {
                                    node: {
                                      any: {
                                        lt: {
                                          lhs: {
                                            avg: {
                                              viewed: {
                                                views: [{
                                                  layers: ["_default"]
                                                }, {
                                                  latest: true
                                                }]
                                                expr: {
                                                  temporalProperty: "prop5"
                                                }
                                              }
                                            }
                                          }
                                          rhs: {
                                            const: {
                                              f64: 10.0
                                            }
                                          }
                                        }
                                      }
                                    }
                                  }) {
          nodes { list { name } }
        }
      }
    }
    """
    expected = {"graph": {"filterNodes": {"nodes": {"list": [{"name": "a"}]}}}}
    run_graphql_test(query, expected, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH])
def test_nodes_latest_then_layer(graph):
    query = """
    query {
      graph(path: "g") {
        filterNodes: filter(expr: {
                                    node: {
                                      any: {
                                        lt: {
                                          lhs: {
                                            avg: {
                                              viewed: {
                                                views: [{
                                                  latest: true
                                                }, {
                                                  layers: ["_default"]
                                                }]
                                                expr: {
                                                  temporalProperty: "prop5"
                                                }
                                              }
                                            }
                                          }
                                          rhs: {
                                            const: {
                                              f64: 10.0
                                            }
                                          }
                                        }
                                      }
                                    }
                                  }) {
          nodes { list { name } }
        }
      }
    }
    """

    expected = {"graph": {"filterNodes": {"nodes": {"list": [{"name": "a"}]}}}}
    run_graphql_test(query, expected, graph)
