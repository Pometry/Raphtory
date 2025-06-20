from raphtory import Graph
from utils import run_graphql_test, run_graphql_error_test, run_group_graphql_error_test
from datetime import datetime


def create_graph_epoch(g):
    g.add_edge(1, 1, 2)
    g.add_edge(2, 1, 2)
    g.add_edge(3, 1, 2)
    g.add_edge(2, 1, 3)
    g.add_edge(3, 1, 3)
    g.add_edge(4, 1, 3)
    g.add_edge(5, 6, 7)


def create_graph_date(g):
    dates = [
        datetime(2025, 1, 1, 0, 0),
        datetime(2025, 1, 2, 0, 0),
        datetime(2025, 1, 3, 0, 0),
        datetime(2025, 1, 4, 0, 0),
        datetime(2025, 1, 5, 0, 0),
    ]
    g.add_node(dates[0], 1, {"where": "Berlin"}, "Person")
    g.add_edge(dates[0], 1, 2, {}, "met")
    g.add_edge(dates[1], 1, 2)
    g.add_edge(dates[2], 1, 2)
    g.add_edge(dates[1], 1, 3)
    g.add_edge(dates[2], 1, 3)
    g.add_edge(dates[3], 1, 3)
    g.add_edge(dates[4], 6, 7, {"where": "fishbowl"}, "finds")


def test_apply_view_snapshot_latest():
    graph = Graph()
    create_graph_date(graph)
    query = """
 {
  graph(path: "g") {
    applyViews(views: [{snapshotLatest: true}]) {
      earliestTime
    }
    nodes {
      applyViews(views: [{snapshotLatest: true}]) {
        page(limit: 1, offset: 0) {
          history
        }
      }
    }
    node(name: "1") {
      applyViews(views: [{snapshotLatest: true}]) {
        history
      }
    }
    edges {
      applyViews(views: [{snapshotLatest: true}]) {
        page(limit: 1, offset: 0) {
          src {
            history
          }
          dst {
            history
          }
        }
      }
    }
    edge(src: "1", dst: "2") {
      applyViews(views: [{snapshotLatest: true}]) {
        src {
          history
        }
      }
    }
  }
}
    """

    correct = {
        "graph": {
            "applyViews": {"earliestTime": 1735689600000},
            "nodes": {
                "applyViews": {
                    "page": [
                        {
                            "history": [
                                1735689600000,
                                1735776000000,
                                1735862400000,
                                1735948800000,
                            ]
                        }
                    ]
                }
            },
            "node": {
                "applyViews": {
                    "history": [
                        1735689600000,
                        1735776000000,
                        1735862400000,
                        1735948800000,
                    ]
                }
            },
            "edges": {
                "applyViews": {
                    "page": [
                        {
                            "src": {
                                "history": [
                                    1735689600000,
                                    1735776000000,
                                    1735862400000,
                                    1735948800000,
                                ]
                            },
                            "dst": {
                                "history": [
                                    1735689600000,
                                    1735776000000,
                                    1735862400000,
                                ]
                            },
                        }
                    ]
                }
            },
            "edge": {
                "applyViews": {
                    "src": {
                        "history": [
                            1735689600000,
                            1735776000000,
                            1735862400000,
                            1735948800000,
                        ]
                    }
                }
            },
        }
    }

    run_graphql_test(query, correct, graph)


def test_apply_view_default_layer():
    graph = Graph()
    create_graph_date(graph)
    query = """
 {
  graph(path: "g") {
    applyViews(views: [{defaultLayer: true}]) {
      earliestTime
    }
    nodes {
      applyViews(views: [{defaultLayer: true}]) {
        page(limit: 1, offset: 0) {
          history
        }
      }
    }
    node(name: "2") {
      applyViews(views: [{defaultLayer: true}]) {
        history
      }
    }
    edges {
      applyViews(views: [{defaultLayer: true}]) {
        page(limit: 1, offset: 0) {
          src {
            history
          }
          dst {
            history
          }
        }
      }
    }
    edge(src: "6", dst: "7") {
      applyViews(views: [{defaultLayer: true}]) {
        src {
          history
        }
      }
    }
  }
}"""
    correct = {
        "graph": {
            "applyViews": {"earliestTime": 1735689600000},
            "nodes": {
                "applyViews": {
                    "page": [
                        {
                            "history": [
                                1735689600000,
                                1735776000000,
                                1735862400000,
                                1735948800000,
                            ]
                        }
                    ]
                }
            },
            "node": {"applyViews": {"history": [1735776000000, 1735862400000]}},
            "edges": {
                "applyViews": {
                    "page": [
                        {
                            "src": {
                                "history": [
                                    1735689600000,
                                    1735776000000,
                                    1735862400000,
                                    1735948800000,
                                ]
                            },
                            "dst": {
                                "history": [1735689600000, 1735776000000, 1735862400000]
                            },
                        }
                    ]
                }
            },
            "edge": {"applyViews": {"src": {"history": [1736035200000]}}},
        }
    }
    run_graphql_test(query, correct, graph)


def test_apply_view_latest():
    graph = Graph()
    create_graph_date(graph)
    query = """
{
  graph(path: "g") {
    applyViews(views: [{latest: true}]) {
      earliestTime
    }
    nodes {
      applyViews(views: [{latest: true}]) {
        page(limit: 1, offset: 0) {
          history
        }
      }
    }
    node(name: "2") {
      applyViews(views: [{latest: true}]) {
        history
      }
    }
    edges {
      applyViews(views: [{latest: true}]) {
        page(limit: 1, offset: 0) {
          src {
            history
          }
          dst {
            history
          }
        }
      }
    }
    edge(src: "6", dst: "7") {
      applyViews(views: [{latest: true}]) {
        src {
          history
        }
      }
    }
  }
}
"""
    correct = {
        "graph": {
            "applyViews": {"earliestTime": 1736035200000},
            "nodes": {"applyViews": {"page": [{"history": [1736035200000]}]}},
            "node": {"applyViews": {"history": []}},
            "edges": {
                "applyViews": {
                    "page": [
                        {
                            "src": {
                                "history": [
                                    1735689600000,
                                    1735776000000,
                                    1735862400000,
                                    1735948800000,
                                ]
                            },
                            "dst": {
                                "history": [1735689600000, 1735776000000, 1735862400000]
                            },
                        }
                    ]
                }
            },
            "edge": {"applyViews": {"src": {"history": [1736035200000]}}},
        }
    }
    run_graphql_test(query, correct, graph)


def test_apply_view_at():
    graph = Graph()
    create_graph_date(graph)
    query = """
  {
  graph(path: "g") {
    applyViews(views: [{at: 1735689600000}]) {
      earliestTime
    }
    nodes {
      applyViews(views: [{at: 1735689600000}]) {
        page(limit: 1, offset: 0) {
          history
        }
      }
    }
    node(name: "2") {
      applyViews(views: [{at: 1735689600000}]) {
        history
      }
    }
    edges {
      applyViews(views: [{at: 1735689600000}]) {
        page(limit: 1, offset: 0) {
          src {
            history
          }
          dst {
            history
          }
        }
      }
    }
    edge(src: "6", dst: "7") {
      applyViews(views: [{at: 1735689600000}]) {
        src {
          history
        }
      }
    }
  }
}
"""
    correct = {
        "graph": {
            "applyViews": {"earliestTime": 1735689600000},
            "nodes": {"applyViews": {"page": [{"history": [1735689600000]}]}},
            "node": {"applyViews": {"history": [1735689600000]}},
            "edges": {
                "applyViews": {
                    "page": [
                        {
                            "src": {
                                "history": [
                                    1735689600000,
                                    1735776000000,
                                    1735862400000,
                                    1735948800000,
                                ]
                            },
                            "dst": {
                                "history": [1735689600000, 1735776000000, 1735862400000]
                            },
                        }
                    ]
                }
            },
            "edge": {"applyViews": {"src": {"history": [1736035200000]}}},
        }
    }
    run_graphql_test(query, correct, graph)


def test_apply_view_snapshot_at():
    graph = Graph()
    create_graph_date(graph)
    query = """
{
  graph(path: "g") {
    applyViews(views: [{snapshotAt: 1740873600000}]) {
    	latestTime
    }
    nodes {
      applyViews(views: [{snapshotAt: 1735901379000}]) {
        page(limit: 1, offset: 0) {
          history
        }
      }
    }
    node(name: "2") {
      applyViews(views: [{snapshotAt: 1735901379000}]) {
        history
      }
    }
    edges {
      applyViews(views: [{snapshotAt: 1735901379000}]) {
        page(limit: 1, offset: 0) {
          src {
            history
          }
          dst {
            history
          }
        }
      }
    }
    edge(src: "6", dst: "7") {
      applyViews(views: [{snapshotAt: 1735901379000}]) {
        src {
          history
        }
      }
    }
  }
}
"""
    correct = {
        "graph": {
            "applyViews": {"latestTime": 1736035200000},
            "nodes": {
                "applyViews": {
                    "page": [{"history": [1735689600000, 1735776000000, 1735862400000]}]
                }
            },
            "node": {
                "applyViews": {"history": [1735689600000, 1735776000000, 1735862400000]}
            },
            "edges": {
                "applyViews": {
                    "page": [
                        {
                            "src": {
                                "history": [
                                    1735689600000,
                                    1735776000000,
                                    1735862400000,
                                    1735948800000,
                                ]
                            },
                            "dst": {
                                "history": [1735689600000, 1735776000000, 1735862400000]
                            },
                        }
                    ]
                }
            },
            "edge": {"applyViews": {"src": {"history": [1736035200000]}}},
        }
    }
    run_graphql_test(query, correct, graph)


def test_apply_view_window():
    graph = Graph()
    create_graph_date(graph)
    query = """
{
  graph(path: "g") {
    applyViews(views: [{window: {
      start: 1735689600000
      end: 1735862400000
    }}]) {
    	latestTime
    }
    nodes {
      applyViews(views: [{window: {
         start: 1735689600000
      end: 1735862400000
    }}]) {
        page( limit: 1,offset: 0) {
          history
        }
      }
    }
    node(name: "2") {
       applyViews(views: [{window: {
        start: 1735689600000
      end: 1735862400000
    }}]) {
        history
      }
    }
    edges {
       applyViews(views: [{window: {
        start: 1735689600000
      end: 1735862400000
    }}]) {
        page(limit: 1, offset: 0) {
          src {
            history
          }
          dst {
            history
          }
        }
      }
    }
    edge(src: "1", dst: "2") {
       applyViews(views: [{window: {
     start: 1735689600000
      end: 1735862400000
    }}]) {
        src {
          history
        }
      }
    }
  }
}
"""
    correct = {
        "graph": {
            "applyViews": {"latestTime": 1735776000000},
            "nodes": {
                "applyViews": {"page": [{"history": [1735689600000, 1735776000000]}]}
            },
            "node": {"applyViews": {"history": [1735689600000, 1735776000000]}},
            "edges": {
                "applyViews": {
                    "page": [
                        {
                            "src": {
                                "history": [
                                    1735689600000,
                                    1735776000000,
                                    1735862400000,
                                    1735948800000,
                                ]
                            },
                            "dst": {
                                "history": [1735689600000, 1735776000000, 1735862400000]
                            },
                        }
                    ]
                }
            },
            "edge": {
                "applyViews": {
                    "src": {
                        "history": [
                            1735689600000,
                            1735776000000,
                            1735862400000,
                            1735948800000,
                        ]
                    }
                }
            },
        }
    }

    run_graphql_test(query, correct, graph)


def test_apply_view_before():
    graph = Graph()
    create_graph_date(graph)
    query = """
{
  graph(path: "g") {
    applyViews(views: [{before: 1735862400000}]) {
      latestTime
    }
    nodes {
      applyViews(views: [{before: 1735862400000}]) {
        page(limit: 1, offset: 0) {
          history
        }
      }
    }
    node(name: "2") {
      applyViews(views: [{before: 1735862400000}]) {
        history
      }
    }
    edges {
      applyViews(views: [{before: 1735862400000}]) {
        page(limit: 1, offset: 0) {
          src {
            history
          }
          dst {
            history
          }
        }
      }
    }
    edge(src: "1", dst: "2") {
      applyViews(views: [{before: 1735862400000}]) {
        src {
          history
        }
      }
    }
  }
}"""

    correct = {
        "graph": {
            "applyViews": {"latestTime": 1735776000000},
            "nodes": {
                "applyViews": {"page": [{"history": [1735689600000, 1735776000000]}]}
            },
            "node": {"applyViews": {"history": [1735689600000, 1735776000000]}},
            "edges": {
                "applyViews": {
                    "page": [
                        {
                            "src": {
                                "history": [
                                    1735689600000,
                                    1735776000000,
                                    1735862400000,
                                    1735948800000,
                                ]
                            },
                            "dst": {
                                "history": [1735689600000, 1735776000000, 1735862400000]
                            },
                        }
                    ]
                }
            },
            "edge": {
                "applyViews": {
                    "src": {
                        "history": [
                            1735689600000,
                            1735776000000,
                            1735862400000,
                            1735948800000,
                        ]
                    }
                }
            },
        }
    }

    run_graphql_test(query, correct, graph)


# def test_apply_view_shrink_window():
#     graph = Graph()
#     create_graph_date(graph)
#     query = """
# {
#   graph(path: "basic") {
#     applyViews(views: [{shrinkWindow: {
#       start: 1735689600000
#       end: 1735776000000
#     }}]) {
#       latestTime
#     }
#     nodes {
#   applyViews(views: [{shrinkWindow: {
#       start: 1735689600000
#       end: 1735776000000
#     }}]) {
#         page(limit: 1, offset: 0) {
#           history
#         }
#       }
#     }
#     node(name: "2") {
#        applyViews(views: [{shrinkWindow: {
#       start: 1735689600000
#       end: 1735776000000
#     }}]) {
#         history
#       }
#     }
#     edges {
#        applyViews(views: [{shrinkWindow: {
#       start: 1735689600000
#       end: 1735776000000
#     }}]) {
#         page(limit: 1, offset: 0) {
#           src {
#            name
#             history
#           }
#           dst {
#             history
#           }
#         }
#       }
#     }
#     edge(src: "1", dst: "2") {
#         applyViews(views: [{shrinkWindow: {
#       start: 1735689600000
#       end: 1735776000000
#     }}]) {
#         src {
#           history
#         }
#       }
#     }
#   }
# }"""


def test_apply_view_exclude_nodes():
    graph = Graph()
    create_graph_date(graph)
    query = """
{
  graph(path: "g") {
    applyViews(views: [{excludeNodes: ["6", "7"]}]) {
      latestTime
    }
  }
}"""
    correct = {
        "graph": {
            "applyViews": {"latestTime": 1735948800000},
        }
    }
    run_graphql_test(query, correct, graph)


def test_apply_view_multiple_arguments():
    graph = Graph()
    create_graph_date(graph)
    queries_and_exceptions = []
    too_many_arguments_exception = "Fields \\"
    query = """
{
  graph(path: "g") {
      applyViews(views: [{layers: ["odd"]}]) {
        name
      }
      applyViews(views: [{layers: ["Person"]}]) {
        name
      }
      }
      }"""
    queries_and_exceptions.append((query, too_many_arguments_exception))
    run_group_graphql_error_test(queries_and_exceptions, graph)


def test_apply_view_nested():
    graph = Graph()
    create_graph_date(graph)
    query = """
{
   graph(path: "g") {
    applyViews(views: [{layers: ["finds"]}]) {
      earliestTime
      edges {
        applyViews(views: [{layers: ["finds"]}]) {
          list {
            history
          }
        }
      }
    }
  }
    }"""
    correct = {
        "graph": {
            "applyViews": {
                "earliestTime": 1735689600000,
                "edges": {"applyViews": {"list": [{"history": [1736035200000]}]}},
            }
        }
    }

    run_graphql_test(query, correct, graph)


def test_apply_view_invalid_argument():
    graph = Graph()
    create_graph_date(graph)
    queries_and_exceptions = []
    invalid_argument = "Invalid value for argument \\"
    query = """
{
  graph(path: "g") {
    applyViews(views: [{layers: "finds"}]) {
      earliestTime
    }
  }
  }
"""
    queries_and_exceptions.append((query, invalid_argument))
    run_group_graphql_error_test(queries_and_exceptions, graph)


def test_apply_view_node_filter():
    graph = Graph()
    create_graph_date(graph)
    query = """
{
  graph(path: "g") {
    applyViews(views: [
      {
        nodeFilter: {
            property: {
              name: "where"
              operator: EQUAL
              value: {str: "Berlin"}
            
          }
        }
      }
      
    ]) {
       nodes {
        list {
          name
        }
      }
    }
  }
  }
"""
    correct = {"graph": {"applyViews": {"nodes": {"list": [{"name": "1"}]}}}}
    run_graphql_test(query, correct, graph)


def test_apply_view_edge_filter():
    graph = Graph()
    create_graph_date(graph)
    query = """
{
  graph(path: "g") {
    applyViews(views: [
      {
        edgeFilter: {
            property: {
              name: "where"
              operator: EQUAL
              value: {str: "fishbowl"}
            
          }
        }
      }
      
    ]) {
       edges {
        list {
          history
        }
      }
    }
  }
  }
"""
    correct = {
        "graph": {"applyViews": {"edges": {"list": [{"history": [1736035200000]}]}}}
    }
    run_graphql_test(query, correct, graph)
