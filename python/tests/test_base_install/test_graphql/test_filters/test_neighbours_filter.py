import pytest
from raphtory import Graph, PersistentGraph
from filters_setup import create_test_graph, init_graph2
from utils import run_graphql_test, run_graphql_error_test

EVENT_GRAPH = create_test_graph(Graph())
PERSISTENT_GRAPH = create_test_graph(PersistentGraph())


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_out_neighbours_found(graph):
    query = """
        query {
          graph(path: "g") {
            node(name: "a") {
              filter(expr: {
                             and: [{
                               node: {
                                 eq: {
                                   lhs: {
                                     field: NAME
                                   }
                                   rhs: {
                                     const: {
                                       str: "d"
                                     }
                                   }
                                 }
                               }
                             }, {
                               node: {
                                 gt: {
                                   lhs: {
                                     property: "prop1"
                                   }
                                   rhs: {
                                     const: {
                                       i64: 10
                                     }
                                   }
                                 }
                               }
                             }]
                           }) {
                outNeighbours {
                  list { name }
                }
              }
            }
          }
        }
    """
    expected_output = {
        "graph": {"node": {"filter": {"outNeighbours": {"list": [{"name": "d"}]}}}}
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_out_neighbours_found_select(graph):
    query = """
        query {
          graph(path: "g") {
            node(name: "a") {
              filter(expr: {
                             and: [{
                               node: {
                                 eq: {
                                   lhs: {
                                     field: NAME
                                   }
                                   rhs: {
                                     const: {
                                       str: "d"
                                     }
                                   }
                                 }
                               }
                             }, {
                               node: {
                                 gt: {
                                   lhs: {
                                     property: "prop1"
                                   }
                                   rhs: {
                                     const: {
                                       i64: 10
                                     }
                                   }
                                 }
                               }
                             }]
                           }) {
                outNeighbours(select: {
                                        node: {
                                          eq: {
                                            lhs: {
                                              field: NAME
                                            }
                                            rhs: {
                                              const: {
                                                str: "d"
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
        }
    """
    expected_output = {
        "graph": {"node": {"filter": {"outNeighbours": {"list": [{"name": "d"}]}}}}
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_out_neighbours_not_found(graph):
    query = """
        query {
          graph(path: "g") {
            node(name: "a") {
              filter(expr: {
                             node: {
                               eq: {
                                 lhs: {
                                   field: NAME
                                 }
                                 rhs: {
                                   const: {
                                     str: "e"
                                   }
                                 }
                               }
                             }
                           }) {
                outNeighbours {
                  list { name }
                }
              }
            }
          }
        }
    """
    expected_output = {"graph": {"node": {"filter": {"outNeighbours": {"list": []}}}}}
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_in_neighbours_found(graph):
    query = """
        query {
          graph(path: "g") {
            node(name: "d") {
              filter(expr: {
                             node: {
                               gt: {
                                 lhs: {
                                   property: "prop1"
                                 }
                                 rhs: {
                                   const: {
                                     i64: 10
                                   }
                                 }
                               }
                             }
                           }) {
                inNeighbours {
                  list { name }
                }
              }
            }
          }
        }
    """
    expected_output = {
        "graph": {
            "node": {
                "filter": {"inNeighbours": {"list": [{"name": "a"}, {"name": "c"}]}}
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_in_neighbours_found_select(graph):
    query = """
        query {
          graph(path: "g") {
            node(name: "d") {
              filter(expr: {
                             node: {
                               gt: {
                                 lhs: {
                                   property: "prop1"
                                 }
                                 rhs: {
                                   const: {
                                     i64: 10
                                   }
                                 }
                               }
                             }
                           }) {
                inNeighbours(select: {
                                       node: {
                                         eq: {
                                           lhs: {
                                             field: NAME
                                           }
                                           rhs: {
                                             const: {
                                               str: "c"
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
        }
    """
    expected_output = {
        "graph": {"node": {"filter": {"inNeighbours": {"list": [{"name": "c"}]}}}}
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_in_neighbours_not_found(graph):
    query = """
        query {
          graph(path: "g") {
            node(name: "d") {
              filter(expr: {
                             node: {
                               eq: {
                                 lhs: {
                                   field: NAME
                                 }
                                 rhs: {
                                   const: {
                                     str: "e"
                                   }
                                 }
                               }
                             }
                           }) {
                inNeighbours {
                  list { name }
                }
              }
            }
          }
        }
    """
    expected_output = {"graph": {"node": {"filter": {"inNeighbours": {"list": []}}}}}
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_neighbours_found(graph):
    query = """
        query {
          graph(path: "g") {
            node(name: "d") {
              filter(expr: {
                             node: {
                               ne: {
                                 lhs: {
                                   field: NAME
                                 }
                                 rhs: {
                                   const: {
                                     str: "a"
                                   }
                                 }
                               }
                             }
                           }) {
                neighbours {
                  list { name }
                }
              }
            }
          }
        }
    """
    expected_output = {
        "graph": {
            "node": {"filter": {"neighbours": {"list": [{"name": "b"}, {"name": "c"}]}}}
        }
    }
    run_graphql_test(query, expected_output, graph, sort_output=True)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_neighbours_found_select(graph):
    query = """
        query {
          graph(path: "g") {
            node(name: "d") {
              filter(expr: {
                             node: {
                               ne: {
                                 lhs: {
                                   field: NAME
                                 }
                                 rhs: {
                                   const: {
                                     str: "a"
                                   }
                                 }
                               }
                             }
                           }) {
                neighbours(select: {
                                     node: {
                                       eq: {
                                         lhs: {
                                           field: NAME
                                         }
                                         rhs: {
                                           const: {
                                             str: "b"
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
        }
    """
    expected_output = {
        "graph": {"node": {"filter": {"neighbours": {"list": [{"name": "b"}]}}}}
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_neighbours_not_found(graph):
    query = """
        query {
          graph(path: "g") {
            node(name: "d") {
              filter(expr: {
                             node: {
                               eq: {
                                 lhs: {
                                   field: NAME
                                 }
                                 rhs: {
                                   const: {
                                     str: "e"
                                   }
                                 }
                               }
                             }
                           }) {
                neighbours {
                  list { name }
                }
              }
            }
          }
        }
    """
    expected_output = {"graph": {"node": {"filter": {"neighbours": {"list": []}}}}}
    run_graphql_test(query, expected_output, graph)


EVENT_GRAPH = init_graph2(Graph())
PERSISTENT_GRAPH = init_graph2(PersistentGraph())


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_neighbours_selection(graph):
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
                neighbours {
                  select(expr: {
                                 node: {
                                   gt: {
                                     lhs: {
                                       property: "p2"
                                     }
                                     rhs: {
                                       const: {
                                         i64: 3
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
            "nodes": {
                "list": [
                    {"neighbours": {"select": {"list": [{"name": "3"}]}}},
                    {"neighbours": {"select": {"list": []}}},
                ]
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_neighbours_neighbours_filtering(graph):
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
                  filter(expr: {
                                 node: {
                                   gt: {
                                     lhs: {
                                       property: "p2"
                                     }
                                     rhs: {
                                       const: {
                                         i64: 3
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
          }
        }
    """
    expected_output = {
        "graph": {
            "nodes": {
                "list": [
                    {
                        "name": "1",
                        "neighbours": {
                            "filter": {
                                "list": [
                                    {
                                        "name": "2",
                                        "neighbours": {"list": [{"name": "3"}]},
                                    },
                                    {"name": "3", "neighbours": {"list": []}},
                                ]
                            }
                        },
                    },
                    {
                        "name": "3",
                        "neighbours": {
                            "filter": {
                                "list": [
                                    {
                                        "name": "1",
                                        "neighbours": {"list": [{"name": "3"}]},
                                    },
                                    {
                                        "name": "2",
                                        "neighbours": {"list": [{"name": "3"}]},
                                    },
                                    {
                                        "name": "4",
                                        "neighbours": {"list": [{"name": "3"}]},
                                    },
                                ]
                            }
                        },
                    },
                ]
            }
        }
    }
    run_graphql_test(query, expected_output, graph, sort_output=True)
