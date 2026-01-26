from raphtory import Graph
from utils import run_graphql_test, run_graphql_error_test, run_group_graphql_error_test
from datetime import datetime
from raphtory import PersistentGraph


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
    g.add_edge(dates[1], 1, 2, {"where": "Facebook"}, "follows")
    g.add_edge(dates[2], 1, 2)
    g.add_edge(dates[1], 1, 3)
    g.add_edge(dates[2], 1, 3)
    g.add_edge(dates[3], 1, 3)
    g.add_edge(dates[4], 6, 7, {"where": "fishbowl"}, "finds")


def create_persistent_graph_epoch(g):
    g.add_edge(1, 1, 2)
    g.add_edge(2, 1, 2)
    g.add_edge(3, 1, 2)
    g.add_edge(2, 1, 3)
    g.add_edge(3, 1, 3)
    g.add_edge(4, 1, 3)
    g.add_edge(5, 6, 7)
    g.delete_edge(6, 1, 3)
    g.delete_edge(7, 1, 2)


def test_apply_view_snapshot_latest():
    graph = Graph()
    create_graph_date(graph)
    query = """
 {
  graph(path: "g") {
    applyViews(views: [{snapshotLatest: true}]) {
      earliestTime {
        timestamp
      }
    }
    nodes {
      applyViews(views: [{snapshotLatest: true}]) {
        page(limit: 1, offset: 0) {
          history {
            timestamps {
              list
            }
          }
        }
      }
    }
    node(name: "1") {
      applyViews(views: [{snapshotLatest: true}]) {
        history {
          timestamps {
            list
          }
        }
      }
    }
    edges {
      applyViews(views: [{snapshotLatest: true}]) {
        page(limit: 1, offset: 0) {
          src {
            history {
              timestamps {
                list
              }
            }
          }
          dst {
            history {
              timestamps {
                list
              }
            }
          }
        }
      }
    }
    edge(src: "1", dst: "2") {
      applyViews(views: [{snapshotLatest: true}]) {
        src {
          history {
            timestamps {
              list
            }
          }
        }
      }
    }
  }
}
    """

    correct = {
        "graph": {
            "applyViews": {"earliestTime": {"timestamp": 1735689600000}},
            "nodes": {
                "applyViews": {
                    "page": [
                        {
                            "history": {
                                "timestamps": {
                                    "list": [
                                        1735689600000,
                                        1735689600000,
                                        1735776000000,
                                        1735776000000,
                                        1735862400000,
                                        1735862400000,
                                        1735948800000,
                                    ]
                                }
                            }
                        }
                    ]
                }
            },
            "node": {
                "applyViews": {
                    "history": {
                        "timestamps": {
                            "list": [
                                1735689600000,
                                1735689600000,
                                1735776000000,
                                1735776000000,
                                1735862400000,
                                1735862400000,
                                1735948800000,
                            ]
                        }
                    }
                }
            },
            "edges": {
                "applyViews": {
                    "page": [
                        {
                            "src": {
                                "history": {
                                    "timestamps": {
                                        "list": [
                                            1735689600000,
                                            1735689600000,
                                            1735776000000,
                                            1735776000000,
                                            1735862400000,
                                            1735862400000,
                                            1735948800000,
                                        ]
                                    }
                                }
                            },
                            "dst": {
                                "history": {
                                    "timestamps": {
                                        "list": [
                                            1735689600000,
                                            1735776000000,
                                            1735862400000,
                                        ]
                                    }
                                }
                            },
                        }
                    ]
                }
            },
            "edge": {
                "applyViews": {
                    "src": {
                        "history": {
                            "timestamps": {
                                "list": [
                                    1735689600000,
                                    1735689600000,
                                    1735776000000,
                                    1735776000000,
                                    1735862400000,
                                    1735862400000,
                                    1735948800000,
                                ]
                            }
                        }
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
      earliestTime {
        timestamp
      }
    }
    nodes {
      applyViews(views: [{defaultLayer: true}]) {
        page(limit: 1, offset: 0) {
          history {
            timestamps {
              list
            }
          }
        }
      }
    }
    node(name: "2") {
      applyViews(views: [{defaultLayer: true}]) {
        history {
          timestamps {
            list
          }
        }
      }
    }
    edges {
      applyViews(views: [{defaultLayer: true}]) {
        page(limit: 1, offset: 0) {
          src {
            history {
              timestamps {
                list
              }
            }
          }
          dst {
            history {
              timestamps {
                list
              }
            }
          }
        }
      }
    }
    edge(src: "6", dst: "7") {
      applyViews(views: [{defaultLayer: true}]) {
        src {
          history {
            timestamps {
              list
            }
          }
        }
      }
    }
  }
}"""
    correct = {
        "graph": {
            "applyViews": {"earliestTime": {"timestamp": 1735689600000}},
            "nodes": {
                "applyViews": {
                    "page": [
                        {
                            "history": {
                                "timestamps": {
                                    "list": [
                                        1735689600000,
                                        1735776000000,
                                        1735862400000,
                                        1735862400000,
                                        1735948800000,
                                    ]
                                }
                            }
                        }
                    ]
                }
            },
            "node": {
                "applyViews": {"history": {"timestamps": {"list": [1735862400000]}}}
            },
            "edges": {
                "applyViews": {
                    "page": [
                        {
                            "src": {
                                "history": {
                                    "timestamps": {
                                        "list": [
                                            1735689600000,
                                            1735776000000,
                                            1735862400000,
                                            1735862400000,
                                            1735948800000,
                                        ]
                                    }
                                }
                            },
                            "dst": {
                                "history": {
                                    "timestamps": {
                                        "list": [
                                            1735862400000,
                                        ]
                                    }
                                }
                            },
                        }
                    ]
                }
            },
            "edge": {"applyViews": {"src": {"history": {"timestamps": {"list": []}}}}},
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
      earliestTime {
        timestamp
      }
    }
    nodes {
      applyViews(views: [{latest: true}]) {
        page(limit: 1, offset: 0) {
          history {
            timestamps {
              list
            }
          }
        }
      }
    }
    node(name: "2") {
      applyViews(views: [{latest: true}]) {
        history {
          timestamps {
            list
          }
        }
      }
    }
    edges {
      applyViews(views: [{latest: true}]) {
        page(limit: 1, offset: 0) {
          src {
            history {
              timestamps {
                list
              }
            }
          }
          dst {
            history {
              timestamps {
                list
              }
            }
          }
        }
      }
    }
    edge(src: "6", dst: "7") {
      applyViews(views: [{latest: true}]) {
        src {
          history {
            timestamps {
              list
            }
          }
        }
      }
    }
  }
}
"""
    correct = {
        "graph": {
            "applyViews": {"earliestTime": {"timestamp": 1736035200000}},
            "nodes": {
                "applyViews": {"page": [{"history": {"timestamps": {"list": []}}}]}
            },
            "node": {"applyViews": {"history": {"timestamps": {"list": []}}}},
            "edges": {
                "applyViews": {
                    "page": [
                        {
                            "src": {"history": {"timestamps": {"list": []}}},
                            "dst": {"history": {"timestamps": {"list": []}}},
                        }
                    ]
                }
            },
            "edge": {
                "applyViews": {
                    "src": {"history": {"timestamps": {"list": [1736035200000]}}}
                }
            },
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
      earliestTime {
        timestamp
      }
    }
    nodes {
      applyViews(views: [{at: 1735689600000}]) {
        page(limit: 1, offset: 0) {
          history {
            timestamps {
              list
            }
          }
        }
      }
    }
    node(name: "2") {
      applyViews(views: [{at: 1735689600000}]) {
        history {
          timestamps {
            list
          }
        }
      }
    }
    edges {
      applyViews(views: [{at: 1735689600000}]) {
        page(limit: 1, offset: 0) {
          src {
            history {
              timestamps {
                list
              }
            }
          }
          dst {
            history {
              timestamps {
                list
              }
            }
          }
        }
      }
    }
    edge(src: "6", dst: "7") {
      applyViews(views: [{at: 1735689600000}]) {
        src {
          history {
            timestamps {
              list
            }
          }
        }
      }
    }
  }
}
"""
    correct = {
        "graph": {
            "applyViews": {"earliestTime": {"timestamp": 1735689600000}},
            "nodes": {
                "applyViews": {
                    "page": [
                        {
                            "history": {
                                "timestamps": {"list": [1735689600000, 1735689600000]}
                            }
                        }
                    ]
                }
            },
            "node": {
                "applyViews": {"history": {"timestamps": {"list": [1735689600000]}}}
            },
            "edges": {
                "applyViews": {
                    "page": [
                        {
                            "src": {
                                "history": {
                                    "timestamps": {
                                        "list": [
                                            1735689600000,
                                            1735689600000,
                                        ]
                                    }
                                }
                            },
                            "dst": {
                                "history": {"timestamps": {"list": [1735689600000]}}
                            },
                        }
                    ]
                }
            },
            "edge": {"applyViews": {"src": {"history": {"timestamps": {"list": []}}}}},
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
    	latestTime {
        timestamp
      }
    }
    nodes {
      applyViews(views: [{snapshotAt: 1735901379000}]) {
        page(limit: 1, offset: 0) {
          history {
            timestamps {
              list
            }
          }
        }
      }
    }
    node(name: "2") {
      applyViews(views: [{snapshotAt: 1735901379000}]) {
        history {
          timestamps {
            list
          }
        }
      }
    }
    edges {
      applyViews(views: [{snapshotAt: 1735901379000}]) {
        page(limit: 1, offset: 0) {
          src {
            history {
              timestamps {
                list
              }
            }
          }
          dst {
            history {
              timestamps {
                list
              }
            }
          }
        }
      }
    }
    edge(src: "6", dst: "7") {
      applyViews(views: [{snapshotAt: 1735901379000}]) {
        src {
          history {
            timestamps {
              list
            }
          }
        }
      }
    }
  }
}
"""
    correct = {
        "graph": {
            "applyViews": {"latestTime": {"timestamp": 1736035200000}},
            "nodes": {
                "applyViews": {
                    "page": [
                        {
                            "history": {
                                "timestamps": {
                                    "list": [
                                        1735689600000,
                                        1735689600000,
                                        1735776000000,
                                        1735776000000,
                                        1735862400000,
                                        1735862400000,
                                    ]
                                }
                            }
                        }
                    ]
                }
            },
            "node": {
                "applyViews": {
                    "history": {
                        "timestamps": {
                            "list": [1735689600000, 1735776000000, 1735862400000]
                        }
                    }
                }
            },
            "edges": {
                "applyViews": {
                    "page": [
                        {
                            "src": {
                                "history": {
                                    "timestamps": {
                                        "list": [
                                            1735689600000,
                                            1735689600000,
                                            1735776000000,
                                            1735776000000,
                                            1735862400000,
                                            1735862400000,
                                        ]
                                    }
                                }
                            },
                            "dst": {
                                "history": {
                                    "timestamps": {
                                        "list": [
                                            1735689600000,
                                            1735776000000,
                                            1735862400000,
                                        ]
                                    }
                                }
                            },
                        }
                    ]
                }
            },
            "edge": {"applyViews": {"src": {"history": {"timestamps": {"list": []}}}}},
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
    	latestTime {
        timestamp
      }
    }
    nodes {
      applyViews(views: [{window: {
         start: 1735689600000
      end: 1735862400000
    }}]) {
        page( limit: 1,offset: 0) {
          history {
            timestamps {
              list
            }
          }
        }
      }
    }
    node(name: "2") {
       applyViews(views: [{window: {
        start: 1735689600000
      end: 1735862400000
    }}]) {
        history {
          timestamps {
            list
          }
        }
      }
    }
    edges {
       applyViews(views: [{window: {
        start: 1735689600000
      end: 1735862400000
    }}]) {
        page(limit: 1, offset: 0) {
          src {
            history {
              timestamps {
                list
              }
            }
          }
          dst {
            history {
              timestamps {
                list
              }
            }
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
          history {
            timestamps {
              list
            }
          }
        }
      }
    }
  }
}
"""
    correct = {
        "graph": {
            "applyViews": {"latestTime": {"timestamp": 1735776000000}},
            "nodes": {
                "applyViews": {
                    "page": [
                        {
                            "history": {
                                "timestamps": {
                                    "list": [
                                        1735689600000,
                                        1735689600000,
                                        1735776000000,
                                        1735776000000,
                                    ]
                                }
                            }
                        }
                    ]
                }
            },
            "node": {
                "applyViews": {
                    "history": {"timestamps": {"list": [1735689600000, 1735776000000]}}
                }
            },
            "edges": {
                "applyViews": {
                    "page": [
                        {
                            "src": {
                                "history": {
                                    "timestamps": {
                                        "list": [
                                            1735689600000,
                                            1735689600000,
                                            1735776000000,
                                            1735776000000,
                                        ]
                                    }
                                }
                            },
                            "dst": {
                                "history": {
                                    "timestamps": {
                                        "list": [
                                            1735689600000,
                                            1735776000000,
                                        ]
                                    }
                                }
                            },
                        }
                    ]
                }
            },
            "edge": {
                "applyViews": {
                    "src": {
                        "history": {
                            "timestamps": {
                                "list": [
                                    1735689600000,
                                    1735689600000,
                                    1735776000000,
                                    1735776000000,
                                ]
                            }
                        }
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
      latestTime {
        timestamp
      }
    }
    nodes {
      applyViews(views: [{before: 1735862400000}]) {
        page(limit: 1, offset: 0) {
          history {
            timestamps {
              list
            }
          }
        }
      }
    }
    node(name: "2") {
      applyViews(views: [{before: 1735862400000}]) {
        history {
          timestamps {
            list
          }
        }
      }
    }
    edges {
      applyViews(views: [{before: 1735862400000}]) {
        page(limit: 1, offset: 0) {
          src {
            history {
              timestamps {
                list
              }
            }
          }
          dst {
            history {
              timestamps {
                list
              }
            }
          }
        }
      }
    }
    edge(src: "1", dst: "2") {
      applyViews(views: [{before: 1735862400000}]) {
        src {
          history {
            timestamps {
              list
            }
          }
        }
      }
    }
  }
}"""

    correct = {
        "graph": {
            "applyViews": {"latestTime": {"timestamp": 1735776000000}},
            "nodes": {
                "applyViews": {
                    "page": [
                        {
                            "history": {
                                "timestamps": {
                                    "list": [
                                        1735689600000,
                                        1735689600000,
                                        1735776000000,
                                        1735776000000,
                                    ]
                                }
                            }
                        }
                    ]
                }
            },
            "node": {
                "applyViews": {
                    "history": {"timestamps": {"list": [1735689600000, 1735776000000]}}
                }
            },
            "edges": {
                "applyViews": {
                    "page": [
                        {
                            "src": {
                                "history": {
                                    "timestamps": {
                                        "list": [
                                            1735689600000,
                                            1735689600000,
                                            1735776000000,
                                            1735776000000,
                                        ]
                                    }
                                }
                            },
                            "dst": {
                                "history": {
                                    "timestamps": {
                                        "list": [
                                            1735689600000,
                                            1735776000000,
                                        ]
                                    }
                                }
                            },
                        }
                    ]
                }
            },
            "edge": {
                "applyViews": {
                    "src": {
                        "history": {
                            "timestamps": {
                                "list": [
                                    1735689600000,
                                    1735689600000,
                                    1735776000000,
                                    1735776000000,
                                ]
                            }
                        }
                    }
                }
            },
        }
    }

    run_graphql_test(query, correct, graph)


def test_apply_view_after():
    graph = Graph()
    create_graph_epoch(graph)
    query = """
{
  graph(path: "g") {
    applyViews(views: [{after: 6}]) {
      latestTime {
        timestamp
      }
    }
    nodes {
      applyViews(views: [{after: 6}]) {
        list {
          history {
            timestamps {
              list
            }
          }
        }
      }
    }
    node(name: "2") {
      applyViews(views: [{after: 3}]) {
        history {
          timestamps {
            list
          }
        }
      }
    }
    edges {
      applyViews(views: [{after: 6}]) {
        list {
        history {
          timestamps {
            list
          }
        }
          src {
            name
          }
          dst {
            name
          }
        }
      }
    }
    edge(src: "1", dst: "2") {
      applyViews(views: [{after: 3}]) {
       history {
        timestamps {
          list
        }
      }
        src {
          name
        }
      }
    }
  }
}
"""
    correct = {
        "graph": {
            "applyViews": {"latestTime": {"timestamp": None}},
            "nodes": {
                "applyViews": {
                    "list": [
                        {"history": {"timestamps": {"list": []}}},
                        {"history": {"timestamps": {"list": []}}},
                        {"history": {"timestamps": {"list": []}}},
                        {"history": {"timestamps": {"list": []}}},
                        {"history": {"timestamps": {"list": []}}},
                    ]
                }
            },
            "node": {"applyViews": {"history": {"timestamps": {"list": []}}}},
            "edges": {
                "applyViews": {
                    "list": [
                        {
                            "history": {"timestamps": {"list": []}},
                            "src": {"name": "1"},
                            "dst": {"name": "2"},
                        },
                        {
                            "history": {"timestamps": {"list": []}},
                            "src": {"name": "1"},
                            "dst": {"name": "3"},
                        },
                        {
                            "history": {"timestamps": {"list": []}},
                            "src": {"name": "6"},
                            "dst": {"name": "7"},
                        },
                    ]
                }
            },
            "edge": {
                "applyViews": {
                    "history": {"timestamps": {"list": []}},
                    "src": {"name": "1"},
                }
            },
        }
    }
    run_graphql_test(query, correct, graph)


def test_apply_view_shrink_window():
    graph = Graph()
    create_graph_date(graph)
    query = """
{
  graph(path: "g") {
    applyViews(views: [{shrinkWindow: {start: 1736035200000, end: 1736121600000}}]) {
      latestTime {
        timestamp
      }
    }
    nodes {
      applyViews(views: [{shrinkWindow: {start: 1736035200000, end: 1736121600000}}]) {
        list {
          name
        }
      }
    }
    node(name: "2") {
      applyViews(views: [{shrinkWindow: {start: 1736035200000, end: 1736121600000}}]) {
        history {
          timestamps {
            list
          }
        }
      }
    }
    edges {
      applyViews(views: [{shrinkWindow: {start: 1736035200000, end: 1736121600000}}]) {
        list {
        history {
          timestamps {
            list
          }
        }
          src {
            name
          }
          dst {
            name
          }
        }
      }
    }
    edge(src: "1", dst: "2") {
      applyViews(views: [{shrinkWindow: {start: 1736035200000, end: 1736121600000}}]) {
        history {
          timestamps {
            list
          }
        }
        src {
          name
        }
        dst {
          name
        }
      }
    }
  }
}"""

    correct = {
        "graph": {
            "applyViews": {"latestTime": {"timestamp": 1736035200000}},
            "nodes": {
                "applyViews": {
                    "list": [
                        {"name": "1"},
                        {"name": "2"},
                        {"name": "3"},
                        {"name": "6"},
                        {"name": "7"},
                    ]
                }
            },
            "node": {"applyViews": {"history": {"timestamps": {"list": []}}}},
            "edges": {
                "applyViews": {
                    "list": [
                        {
                            "history": {"timestamps": {"list": []}},
                            "src": {"name": "1"},
                            "dst": {"name": "2"},
                        },
                        {
                            "history": {"timestamps": {"list": []}},
                            "src": {"name": "1"},
                            "dst": {"name": "3"},
                        },
                        {
                            "history": {"timestamps": {"list": [1736035200000]}},
                            "src": {"name": "6"},
                            "dst": {"name": "7"},
                        },
                    ]
                }
            },
            "edge": {
                "applyViews": {
                    "history": {"timestamps": {"list": []}},
                    "src": {"name": "1"},
                    "dst": {"name": "2"},
                }
            },
        }
    }
    run_graphql_test(query, correct, graph)


def test_apply_view_shrink_start():
    graph = Graph()
    create_graph_date(graph)
    query = """
{
  graph(path: "g") {
    applyViews(views: [{shrinkStart:  1736035200000}]) {
      latestTime {
        timestamp
      }
    }
    nodes {
     applyViews(views: [{shrinkStart:  1736035200000}]) {
        list {
          name
        }
      }
    }
    node(name: "2") {
     applyViews(views: [{shrinkStart:  1736035200000}]) {
        history {
          timestamps {
            list
          }
        }
      }
    }
    edges {
   applyViews(views: [{shrinkStart:  1736035200000}]) {
        list {
           history {
            timestamps {
              list
            }
          }
          src {
            name
          }
          dst {
            name
          }
        }
      }
    }
    edge(src: "1", dst: "2") {
    applyViews(views: [{shrinkStart:  1736035200000}]) {
    history {
      timestamps {
        list
      }
    }
        src {
          name
        }
        dst {
          name
        }
      }
    }
  }
}
"""
    correct = {
        "graph": {
            "applyViews": {"latestTime": {"timestamp": 1736035200000}},
            "nodes": {
                "applyViews": {
                    "list": [
                        {"name": "1"},
                        {"name": "2"},
                        {"name": "3"},
                        {"name": "6"},
                        {"name": "7"},
                    ]
                }
            },
            "node": {"applyViews": {"history": {"timestamps": {"list": []}}}},
            "edges": {
                "applyViews": {
                    "list": [
                        {
                            "history": {"timestamps": {"list": []}},
                            "src": {"name": "1"},
                            "dst": {"name": "2"},
                        },
                        {
                            "history": {"timestamps": {"list": []}},
                            "src": {"name": "1"},
                            "dst": {"name": "3"},
                        },
                        {
                            "history": {"timestamps": {"list": [1736035200000]}},
                            "src": {"name": "6"},
                            "dst": {"name": "7"},
                        },
                    ]
                }
            },
            "edge": {
                "applyViews": {
                    "history": {"timestamps": {"list": []}},
                    "src": {"name": "1"},
                    "dst": {"name": "2"},
                }
            },
        }
    }
    run_graphql_test(query, correct, graph)


def test_apply_view_shrink_end():
    graph = Graph()
    create_graph_date(graph)
    query = """
{
  graph(path: "g") {
    applyViews(views: [{shrinkEnd:  1735776000000}]) {
      latestTime {
        timestamp
      }
    }
    nodes {
     applyViews(views: [{shrinkEnd:  1735776000000}]) {
        list {
          name
        }
      }
    }
    node(name: "2") {
     applyViews(views: [{shrinkEnd:  1735776000000}]) {
        history {
          timestamps {
            list
          }
        }
      }
    }
    edges {
   applyViews(views: [{shrinkEnd:  1735776000000}]) {
        list {
        history {
          timestamps {
            list
          }
        }
          src {
            name
          }
          dst {
            name
          }
        }
      }
    }
    edge(src: "1", dst: "2") {
    applyViews(views: [{shrinkEnd:  1735776000000}]) {
    history {
      timestamps {
        list
      }
    }
        src {
          name
        }
        dst {
          name
        }
      }
    }
  }
}
"""
    correct = {
        "graph": {
            "applyViews": {"latestTime": {"timestamp": 1735689600000}},
            "nodes": {
                "applyViews": {
                    "list": [
                        {"name": "1"},
                        {"name": "2"},
                        {"name": "3"},
                        {"name": "6"},
                        {"name": "7"},
                    ]
                }
            },
            "node": {
                "applyViews": {"history": {"timestamps": {"list": [1735689600000]}}}
            },
            "edges": {
                "applyViews": {
                    "list": [
                        {
                            "history": {"timestamps": {"list": [1735689600000]}},
                            "src": {"name": "1"},
                            "dst": {"name": "2"},
                        },
                        {
                            "history": {"timestamps": {"list": []}},
                            "src": {"name": "1"},
                            "dst": {"name": "3"},
                        },
                        {
                            "history": {"timestamps": {"list": []}},
                            "src": {"name": "6"},
                            "dst": {"name": "7"},
                        },
                    ]
                }
            },
            "edge": {
                "applyViews": {
                    "history": {"timestamps": {"list": [1735689600000]}},
                    "src": {"name": "1"},
                    "dst": {"name": "2"},
                }
            },
        }
    }
    run_graphql_test(query, correct, graph)


def test_apply_view_layers():
    graph = Graph()
    create_graph_date(graph)
    query = """
{
  graph(path: "g") {
    applyViews(views: [{layers: ["finds", "Person"]}]) {
      earliestTime {
        timestamp
      }
    }
    nodes {
      applyViews(views: [{layers: ["finds", "Person"]}]) {
        list {
          name
          history {
            timestamps {
              list
            }
          }
        }
      }
    }
    node(name: "1") {
      applyViews(views: [{layers: ["finds"]}]) {
        history {
          timestamps {
            list
          }
        }
      }
    }
    edges {
      applyViews(views: [{layers: ["finds", "Person"]}]) {
        list {
        history {
          timestamps {
            list
          }
        }
          src {
            name
          }
          dst {
            name
          }
        }
      }
    }
    edge(src: "1", dst: "2") {
    applyViews(views: [{layers: ["finds", "met"]}]) {
    history {
      timestamps {
        list
      }
    }
        src {
          name
        }
        dst {
          name
        }
      }
    }
  }
}
"""
    correct = {
        "graph": {
            "applyViews": {"earliestTime": {"timestamp": 1735689600000}},
            "nodes": {
                "applyViews": {
                    "list": [
                        {
                            "name": "1",
                            "history": {"timestamps": {"list": [1735689600000]}},
                        },
                        {
                            "name": "2",
                            "history": {"timestamps": {"list": []}},
                        },
                        {
                            "name": "3",
                            "history": {"timestamps": {"list": []}},
                        },
                        {
                            "name": "6",
                            "history": {"timestamps": {"list": [1736035200000]}},
                        },
                        {
                            "name": "7",
                            "history": {"timestamps": {"list": [1736035200000]}},
                        },
                    ]
                }
            },
            "node": {
                "applyViews": {"history": {"timestamps": {"list": [1735689600000]}}}
            },
            "edges": {
                "applyViews": {
                    "list": [
                        {
                            "history": {"timestamps": {"list": []}},
                            "src": {"name": "1"},
                            "dst": {"name": "2"},
                        },
                        {
                            "history": {"timestamps": {"list": []}},
                            "src": {"name": "1"},
                            "dst": {"name": "3"},
                        },
                        {
                            "history": {"timestamps": {"list": [1736035200000]}},
                            "src": {"name": "6"},
                            "dst": {"name": "7"},
                        },
                    ]
                }
            },
            "edge": {
                "applyViews": {
                    "history": {"timestamps": {"list": [1735689600000]}},
                    "src": {"name": "1"},
                    "dst": {"name": "2"},
                }
            },
        }
    }
    run_graphql_test(query, correct, graph)


def test_apply_view_layer():
    graph = Graph()
    create_graph_date(graph)
    query = """
{
  graph(path: "g") {
    applyViews(views: [{layers: ["Person"]}]) {
      earliestTime {
        timestamp
      }
    }
    nodes {
     applyViews(views: [{typeFilter: ["Person"]}]) {
        list {
          history {
            timestamps {
              list
            }
          }
          name
        }
      }
    }
    node(name: "1") {
      applyViews(views: [{layers: ["finds"]}]) {
        history {
          timestamps {
            list
          }
        }
      }
    }
    edges {
      applyViews(views: [{layers: ["finds"]}]) {
        list {
        history {
          timestamps {
            list
          }
        }
          src {
            name
          }
          dst {
            name
          }
        }
      }
    }
    edge(src: "1", dst: "2") {
  applyViews(views: [{layers: ["met"]}]) {
  history {
    timestamps {
      list
    }
  }
        src {
          name
        }
        dst {
          name
        }
      }
    }
  }
}"""
    correct = {
        "graph": {
            "applyViews": {"earliestTime": {"timestamp": 1735689600000}},
            "nodes": {
                "applyViews": {
                    "list": [
                        {
                            "history": {
                                "timestamps": {
                                    "list": [
                                        1735689600000,
                                        1735689600000,
                                        1735776000000,
                                        1735776000000,
                                        1735862400000,
                                        1735862400000,
                                        1735948800000,
                                    ]
                                }
                            },
                            "name": "1",
                        }
                    ]
                }
            },
            "node": {
                "applyViews": {"history": {"timestamps": {"list": [1735689600000]}}}
            },
            "edges": {
                "applyViews": {
                    "list": [
                        {
                            "history": {"timestamps": {"list": []}},
                            "src": {"name": "1"},
                            "dst": {"name": "2"},
                        },
                        {
                            "history": {"timestamps": {"list": []}},
                            "src": {"name": "1"},
                            "dst": {"name": "3"},
                        },
                        {
                            "history": {"timestamps": {"list": [1736035200000]}},
                            "src": {"name": "6"},
                            "dst": {"name": "7"},
                        },
                    ]
                }
            },
            "edge": {
                "applyViews": {
                    "history": {"timestamps": {"list": [1735689600000]}},
                    "src": {"name": "1"},
                    "dst": {"name": "2"},
                }
            },
        }
    }
    run_graphql_test(query, correct, graph)


def test_apply_view_exclude_layer():
    graph = Graph()
    create_graph_date(graph)
    query = """
{
  graph(path: "g") {
    applyViews(views: [{excludeLayer: "Person"}]) {
      earliestTime {
        timestamp
      }
    }
    nodes {
      applyViews(views: [{excludeLayer: "Person"}]) {
        list {
        name
          history {
            timestamps {
              list
            }
          }
        }
      }
    }
    node(name: "1") {
      applyViews(views: [{excludeLayer: "Person"}]) {
        history {
          timestamps {
            list
          }
        }
      }
    }
    edges {
      applyViews(views: [{excludeLayer: "finds"}]) {
        list {
        history {
          timestamps {
            list
          }
        }
          src {
            name
          }
          dst {
            name
          }
        }
      }
    }
    edge(src: "6", dst: "7") {
      applyViews(views: [{excludeLayer: "finds"}]) {
      history {
        timestamps {
          list
        }
      }
        src {
          name
        }
        dst {
          name
        }
      }
    }
  }
}
"""
    correct = {
        "graph": {
            "applyViews": {"earliestTime": {"timestamp": 1735689600000}},
            "nodes": {
                "applyViews": {
                    "list": [
                        {
                            "name": "1",
                            "history": {
                                "timestamps": {
                                    "list": [
                                        1735689600000,
                                        1735689600000,
                                        1735776000000,
                                        1735776000000,
                                        1735862400000,
                                        1735862400000,
                                        1735948800000,
                                    ]
                                }
                            },
                        },
                        {
                            "name": "2",
                            "history": {
                                "timestamps": {
                                    "list": [
                                        1735689600000,
                                        1735776000000,
                                        1735862400000,
                                    ]
                                }
                            },
                        },
                        {
                            "name": "3",
                            "history": {
                                "timestamps": {
                                    "list": [
                                        1735776000000,
                                        1735862400000,
                                        1735948800000,
                                    ]
                                }
                            },
                        },
                        {
                            "name": "6",
                            "history": {"timestamps": {"list": [1736035200000]}},
                        },
                        {
                            "name": "7",
                            "history": {"timestamps": {"list": [1736035200000]}},
                        },
                    ]
                }
            },
            "node": {
                "applyViews": {
                    "history": {
                        "timestamps": {
                            "list": [
                                1735689600000,
                                1735689600000,
                                1735776000000,
                                1735776000000,
                                1735862400000,
                                1735862400000,
                                1735948800000,
                            ]
                        }
                    }
                }
            },
            "edges": {
                "applyViews": {
                    "list": [
                        {
                            "history": {
                                "timestamps": {
                                    "list": [
                                        1735689600000,
                                        1735776000000,
                                        1735862400000,
                                    ]
                                }
                            },
                            "src": {"name": "1"},
                            "dst": {"name": "2"},
                        },
                        {
                            "history": {
                                "timestamps": {
                                    "list": [
                                        1735776000000,
                                        1735862400000,
                                        1735948800000,
                                    ]
                                }
                            },
                            "src": {"name": "1"},
                            "dst": {"name": "3"},
                        },
                        {
                            "history": {"timestamps": {"list": []}},
                            "src": {"name": "6"},
                            "dst": {"name": "7"},
                        },
                    ]
                }
            },
            "edge": {
                "applyViews": {
                    "history": {"timestamps": {"list": []}},
                    "src": {"name": "6"},
                    "dst": {"name": "7"},
                }
            },
        }
    }
    run_graphql_test(query, correct, graph)


def test_apply_view_exclude_layers():
    graph = Graph()
    create_graph_date(graph)
    query = """
{
  graph(path: "g") {
    applyViews(views: [{excludeLayers: ["Person", "finds"]}]) {
      earliestTime {
        timestamp
      }
    }
    nodes {
      applyViews(views: [{excludeLayers: ["Person"]}]) {
        list {
        name
          history {
            timestamps {
              list
            }
          }
        }
      }
    }
    node(name: "1") {
      applyViews(views: [{excludeLayers: ["Person"]}]) {
        history {
          timestamps {
            list
          }
        }
      }
    }
    edges {
      applyViews(views: [{excludeLayers: ["finds", "met"]}]) {
        list {
        history {
          timestamps {
            list
          }
        }
          src {
            name
          }
          dst {
            name
          }
        }
      }
    }
    edge(src: "6", dst: "7") {
     applyViews(views: [{excludeLayers: ["finds"]}]) {
     history {
      timestamps {
        list
      }
    }
        src {
          name
        }
        dst {
          name
        }
      }
    }
  }
}
"""
    correct = {
        "graph": {
            "applyViews": {"earliestTime": {"timestamp": 1735689600000}},
            "nodes": {
                "applyViews": {
                    "list": [
                        {
                            "name": "1",
                            "history": {
                                "timestamps": {
                                    "list": [
                                        1735689600000,
                                        1735689600000,
                                        1735776000000,
                                        1735776000000,
                                        1735862400000,
                                        1735862400000,
                                        1735948800000,
                                    ]
                                }
                            },
                        },
                        {
                            "name": "2",
                            "history": {
                                "timestamps": {
                                    "list": [
                                        1735689600000,
                                        1735776000000,
                                        1735862400000,
                                    ]
                                }
                            },
                        },
                        {
                            "name": "3",
                            "history": {
                                "timestamps": {
                                    "list": [
                                        1735776000000,
                                        1735862400000,
                                        1735948800000,
                                    ]
                                }
                            },
                        },
                        {
                            "name": "6",
                            "history": {"timestamps": {"list": [1736035200000]}},
                        },
                        {
                            "name": "7",
                            "history": {"timestamps": {"list": [1736035200000]}},
                        },
                    ]
                }
            },
            "node": {
                "applyViews": {
                    "history": {
                        "timestamps": {
                            "list": [
                                1735689600000,
                                1735689600000,
                                1735776000000,
                                1735776000000,
                                1735862400000,
                                1735862400000,
                                1735948800000,
                            ]
                        }
                    }
                }
            },
            "edges": {
                "applyViews": {
                    "list": [
                        {
                            "history": {
                                "timestamps": {"list": [1735776000000, 1735862400000]}
                            },
                            "src": {"name": "1"},
                            "dst": {"name": "2"},
                        },
                        {
                            "history": {
                                "timestamps": {
                                    "list": [
                                        1735776000000,
                                        1735862400000,
                                        1735948800000,
                                    ]
                                }
                            },
                            "src": {"name": "1"},
                            "dst": {"name": "3"},
                        },
                        {
                            "history": {"timestamps": {"list": []}},
                            "src": {"name": "6"},
                            "dst": {"name": "7"},
                        },
                    ]
                }
            },
            "edge": {
                "applyViews": {
                    "history": {"timestamps": {"list": []}},
                    "src": {"name": "6"},
                    "dst": {"name": "7"},
                }
            },
        }
    }
    run_graphql_test(query, correct, graph)


def test_apply_view_type_filter():
    graph = Graph()
    create_graph_date(graph)
    query = """
{
  graph(path: "g") {
      nodes {
        applyViews(views: [{typeFilter: ["Person"]}]) {
          list {
            name
          }
        }
      }
    }
  }
"""
    correct = {"graph": {"nodes": {"applyViews": {"list": [{"name": "1"}]}}}}
    run_graphql_test(query, correct, graph)


def test_apply_view_exclude_nodes():
    graph = Graph()
    create_graph_date(graph)
    query = """
{
  graph(path: "g") {
    applyViews(views: [{excludeNodes: ["6", "7"]}]) {
      latestTime {
        timestamp
      }
    }
  }
}"""
    correct = {
        "graph": {
            "applyViews": {"latestTime": {"timestamp": 1735948800000}},
        }
    }
    run_graphql_test(query, correct, graph)


def test_apply_view_too_many_arguments():
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
      earliestTime {
        timestamp
      }
      edges {
        applyViews(views: [{layers: ["finds"]}]) {
          list {
            history {
              timestamps {
                list
              }
            }
          }
        }
      }
    }
  }
    }"""
    correct = {
        "graph": {
            "applyViews": {
                "earliestTime": {"timestamp": 1735689600000},
                "edges": {
                    "applyViews": {
                        "list": [{"history": {"timestamps": {"list": [1736035200000]}}}]
                    }
                },
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
      earliestTime {
        timestamp
      }
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
                where: { eq: { str: "Berlin" } }
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
                where: { eq: { str: "fishbowl" } }
              }
            }
          }
        ]) {
          edges {
            list {
              history{
            timestamps {
              list
            }
          }}
          }
        }
      }
    }
    """
    correct = {
        "graph": {
            "applyViews": {
                "edges": {
                    "list": [{"history": {"timestamps": {"list": [1736035200000]}}}]
                }
            }
        }
    }
    run_graphql_test(query, correct, graph)


def test_apply_view_subgraph():
    graph = Graph()
    create_graph_date(graph)
    query = """
{
  graph(path: "g") {
    applyViews(views: [{subgraph: ["1", "2"]}]) {
      nodes {
        list {
          name
        }
      }
    }
  }
}"""
    correct = {
        "graph": {"applyViews": {"nodes": {"list": [{"name": "1"}, {"name": "2"}]}}}
    }
    run_graphql_test(query, correct, graph)


def test_apply_view_subgraph_node_types():
    graph = Graph()
    create_graph_date(graph)
    query = """
{
  graph(path: "g") {
    applyViews(views: [{subgraphNodeTypes: ["Person"]}]) {
      nodes {
        list {
          name
        }
      }
    }
  }
}"""
    correct = {"graph": {"applyViews": {"nodes": {"list": [{"name": "1"}]}}}}
    run_graphql_test(query, correct, graph)


def test_apply_view_nodes_multiple_views():
    graph = Graph()
    create_graph_date(graph)
    query = """
{
  graph(path: "g") {
    applyViews(views: [{window: {start: 1735689600000, end: 1735862400000}}, {layers: ["Person"]}]) {
      nodes {
        list {
          name
          history {
            timestamps {
              list
            }
          }
        }
      }
    }
  }
}"""
    correct = {
        "graph": {
            "applyViews": {
                "nodes": {
                    "list": [
                        {
                            "history": {"timestamps": {"list": [1735689600000]}},
                            "name": "1",
                        },
                    ]
                }
            },
        }
    }
    run_graphql_test(query, correct, graph)


def test_apply_view_edges_multiple_views():
    graph = Graph()
    create_graph_date(graph)
    query = """
{
  graph(path: "g") {
    applyViews(views: [{window: {start: 1735689600000, end: 1735862400000}}, {layers: ["met"]}]) {
      edges {
        list {
          src {
            name
          }
          dst {
            name
          }
          history {
            timestamps {
              list
            }
          }
        }
      }
    }
  }
}"""
    correct = {
        "graph": {
            "applyViews": {
                "edges": {
                    "list": [
                        {
                            "dst": {"name": "2"},
                            "history": {"timestamps": {"list": [1735689600000]}},
                            "src": {"name": "1"},
                        },
                    ]
                }
            },
        }
    }
    run_graphql_test(query, correct, graph)


def test_apply_view_a_lot_of_views():
    graph = Graph()
    create_graph_date(graph)
    query = """
    {
      graph(path: "g") {
        nodes {
          applyViews(views: [
            { window: { start: 1735689600000, end: 1735862400000 } },
            { layers: ["follows"] },
            { nodeFilter: { property: { name: "where", where: { eq: { str: "Berlin" } } } } }
          ]) {
            list {
              name
              history{
            timestamps {
              list
            }
          }}
          }
        }
      }
    }
    """
    correct = {
        "graph": {
            "nodes": {
                "applyViews": {
                    "list": [
                        {
                            "name": "1",
                            "history": {"timestamps": {"list": [1735689600000]}},
                        },
                        {"name": "2", "history": {"timestamps": {"list": []}}},
                        {"name": "3", "history": {"timestamps": {"list": []}}},
                        {"name": "6", "history": {"timestamps": {"list": []}}},
                        {"name": "7", "history": {"timestamps": {"list": []}}},
                    ]
                }
            }
        }
    }
    run_graphql_test(query, correct, graph)


def test_apply_view_neighbours():
    graph = Graph()
    create_graph_date(graph)
    query = """
{
  graph(path: "g") {
    nodes {
      list {
        neighbours {
          applyViews(views: [{latest: true}]) {
            list {
              name
              history {
                timestamps {
                  list
                }
              }
            }
          }
        }
      }
    }
  }
}"""

    correct = {
        "graph": {
            "nodes": {
                "list": [
                    {
                        "neighbours": {
                            "applyViews": {
                                "list": [
                                    {
                                        "history": {"timestamps": {"list": []}},
                                        "name": "2",
                                    },
                                    {
                                        "history": {"timestamps": {"list": []}},
                                        "name": "3",
                                    },
                                ]
                            }
                        }
                    },
                    {
                        "neighbours": {
                            "applyViews": {
                                "list": [
                                    {
                                        "history": {"timestamps": {"list": []}},
                                        "name": "1",
                                    }
                                ]
                            }
                        }
                    },
                    {
                        "neighbours": {
                            "applyViews": {
                                "list": [
                                    {
                                        "history": {"timestamps": {"list": []}},
                                        "name": "1",
                                    }
                                ]
                            }
                        }
                    },
                    {
                        "neighbours": {
                            "applyViews": {
                                "list": [
                                    {
                                        "history": {
                                            "timestamps": {"list": [1736035200000]}
                                        },
                                        "name": "7",
                                    }
                                ]
                            }
                        }
                    },
                    {
                        "neighbours": {
                            "applyViews": {
                                "list": [
                                    {
                                        "history": {
                                            "timestamps": {"list": [1736035200000]}
                                        },
                                        "name": "6",
                                    }
                                ]
                            }
                        }
                    },
                ]
            }
        }
    }

    run_graphql_test(query, correct, graph)


def test_apply_view_neighbours_latest():
    graph = Graph()
    create_graph_date(graph)
    query = """
{
  graph(path: "g") {
    node(name: "1") {
      neighbours {
        applyViews(views: [{latest: true}]) {
          list {
            name
            history {
              timestamps {
                list
              }
            }
          }
        }
      }
    }
  }
}"""
    correct = {
        "graph": {
            "node": {
                "neighbours": {
                    "applyViews": {
                        "list": [
                            {"history": {"timestamps": {"list": []}}, "name": "2"},
                            {"history": {"timestamps": {"list": []}}, "name": "3"},
                        ]
                    }
                }
            }
        }
    }

    run_graphql_test(query, correct, graph)


def test_apply_view_neighbours_layer():
    graph = Graph()
    create_graph_date(graph)
    query = """
{
  graph(path: "g") {
    node(name: "6") {
      neighbours {
        applyViews(views: [{layers: ["finds"]}]) {
          list {
            name
            history {
              timestamps {
                list
              }
            }
          }
        }
      }
    }
  }
}"""

    correct = {
        "graph": {
            "node": {
                "neighbours": {
                    "applyViews": {
                        "list": [
                            {
                                "history": {"timestamps": {"list": [1736035200000]}},
                                "name": "7",
                            }
                        ]
                    }
                }
            }
        }
    }

    run_graphql_test(query, correct, graph)


def test_apply_view_neighbours_exclude_layer():
    graph = Graph()
    create_graph_date(graph)
    query = """
{
  graph(path: "g") {
    node(name: "6") {
      neighbours {
        applyViews(views: [{excludeLayer: "finds"}]) {
          list {
            name
            history {
              timestamps {
                list
              }
            }
          }
        }
      }
    }
  }
}"""

    correct = {
        "graph": {
            "node": {
                "neighbours": {
                    "applyViews": {
                        "list": [{"history": {"timestamps": {"list": []}}, "name": "7"}]
                    }
                }
            }
        }
    }

    run_graphql_test(query, correct, graph)


def test_apply_view_neighbours_layers():
    graph = Graph()
    create_graph_date(graph)
    query = """
{
  graph(path: "g") {
    node(name: "1") {
      neighbours {
        applyViews(views: [{layers: ["met", "Person"]}]) {
          list {
            name
            history {
              timestamps {
                list
              }
            }
          }
        }
      }
    }
  }
}"""

    correct = {
        "graph": {
            "node": {
                "neighbours": {
                    "applyViews": {
                        "list": [
                            {
                                "history": {"timestamps": {"list": [1735689600000]}},
                                "name": "2",
                            },
                            {"history": {"timestamps": {"list": []}}, "name": "3"},
                        ]
                    }
                }
            }
        }
    }
    run_graphql_test(query, correct, graph)


def test_apply_view_neighbours_exclude_layers():
    graph = Graph()
    create_graph_date(graph)
    query = """
{
  graph(path: "g") {
    node(name: "1") {
      neighbours {
        applyViews(views: [{excludeLayers: ["met", "Person"]}]) {
          list {
            name
            history {
              timestamps {
                list
              }
            }
          }
        }
      }
    }
  }
}"""

    correct = {
        "graph": {
            "node": {
                "neighbours": {
                    "applyViews": {
                        "list": [
                            {
                                "name": "2",
                                "history": {
                                    "timestamps": {
                                        "list": [1735776000000, 1735862400000]
                                    }
                                },
                            },
                            {
                                "name": "3",
                                "history": {
                                    "timestamps": {
                                        "list": [
                                            1735776000000,
                                            1735862400000,
                                            1735948800000,
                                        ]
                                    }
                                },
                            },
                        ]
                    }
                }
            }
        }
    }
    run_graphql_test(query, correct, graph)


def test_apply_view_neighbours_after():
    graph = Graph()
    create_graph_date(graph)
    query = """
{
  graph(path: "g") {
    node(name: "1") {
      neighbours {
        applyViews(views: [{after: 1735862400000}]) {
          list {
            name
            history {
              timestamps {
                list
              }
            }
          }
        }
      }
    }
  }
}"""
    correct = {
        "graph": {
            "node": {
                "neighbours": {
                    "applyViews": {
                        "list": [
                            {"history": {"timestamps": {"list": []}}, "name": "2"},
                            {
                                "history": {"timestamps": {"list": [1735948800000]}},
                                "name": "3",
                            },
                        ]
                    }
                }
            }
        }
    }
    run_graphql_test(query, correct, graph)


def test_apply_view_neighbours_before():
    graph = Graph()
    create_graph_date(graph)
    query = """
{
  graph(path: "g") {
    node(name: "1") {
      neighbours {
        applyViews(views: [{before: 1735862400000}]) {
          list {
            name
            history {
              timestamps {
                list
              }
            }
          }
        }
      }
    }
  }
}"""
    correct = {
        "graph": {
            "node": {
                "neighbours": {
                    "applyViews": {
                        "list": [
                            {
                                "history": {
                                    "timestamps": {
                                        "list": [1735689600000, 1735776000000]
                                    }
                                },
                                "name": "2",
                            },
                            {
                                "history": {"timestamps": {"list": [1735776000000]}},
                                "name": "3",
                            },
                        ]
                    }
                }
            }
        }
    }
    run_graphql_test(query, correct, graph)


def test_apply_view_in_neighbours_window():
    graph = Graph()
    create_graph_date(graph)
    query = """
{
  graph(path: "g") {
    node(name: "1") {
      inNeighbours {
        applyViews(views: [{window: {start: 1735689600000, end: 1735862400000}}]) {
          list {
            name
            history {
              timestamps {
                list
              }
            }
          }
        }
      }
    }
  }
}"""
    correct = {"graph": {"node": {"inNeighbours": {"applyViews": {"list": []}}}}}

    run_graphql_test(query, correct, graph)


def test_apply_view_out_neighbours_window():
    graph = Graph()
    create_graph_date(graph)
    query = """
{
  graph(path: "g") {
    node(name: "1") {
      outNeighbours {
        applyViews(views: [{window: {start: 1735689600000, end: 1735862400000}}]) {
          list {
            name
            history {
              timestamps {
                list
              }
            }
          }
        }
      }
    }
  }
}"""
    correct = {
        "graph": {
            "node": {
                "outNeighbours": {
                    "applyViews": {
                        "list": [
                            {
                                "history": {
                                    "timestamps": {
                                        "list": [1735689600000, 1735776000000]
                                    }
                                },
                                "name": "2",
                            },
                            {
                                "history": {"timestamps": {"list": [1735776000000]}},
                                "name": "3",
                            },
                        ]
                    }
                }
            }
        }
    }

    run_graphql_test(query, correct, graph)


def test_apply_view_out_neighbours_shrink_window():
    graph = Graph()
    create_graph_date(graph)
    query = """
{
  graph(path: "g") {
    node(name: "6") {
      outNeighbours {
        applyViews(views: [{shrinkWindow: {start: 1735948800000, end: 1736035200000}}]) {
          list {
            name
            history {
              timestamps {
                list
              }
            }
          }
        }
      }
    }
  }
}"""
    correct = {
        "graph": {
            "node": {
                "outNeighbours": {
                    "applyViews": {
                        "list": [
                            {"history": {"timestamps": {"list": []}}, "name": "7"},
                        ]
                    }
                }
            }
        }
    }

    run_graphql_test(query, correct, graph)


def test_apply_view_in_neighbours_shrink_start():
    graph = Graph()
    create_graph_date(graph)
    query = """
{
  graph(path: "g") {
    node(name: "7") {
      inNeighbours {
        applyViews(views: [{shrinkStart: 1735948800000}]) {
          list {
            name
            history {
              timestamps {
                list
              }
            }
          }
        }
      }
    }
  }
}"""
    correct = {
        "graph": {
            "node": {
                "inNeighbours": {
                    "applyViews": {
                        "list": [
                            {
                                "history": {"timestamps": {"list": [1736035200000]}},
                                "name": "6",
                            },
                        ]
                    }
                }
            }
        }
    }
    run_graphql_test(query, correct, graph)


def test_apply_view_in_neighbours_shrink_end():
    graph = Graph()
    create_graph_date(graph)
    query = """
{
  graph(path: "g") {
    node(name: "2") {
      inNeighbours {
        applyViews(views: [{shrinkEnd: 1735862400000}]) {
          list {
            name
            history {
              timestamps {
                list
              }
            }
          }
        }
      }
    }
  }
}"""
    correct = {
        "graph": {
            "node": {
                "inNeighbours": {
                    "applyViews": {
                        "list": [
                            {
                                "name": "1",
                                "history": {
                                    "timestamps": {
                                        "list": [
                                            1735689600000,
                                            1735689600000,
                                            1735776000000,
                                            1735776000000,
                                        ]
                                    }
                                },
                            }
                        ]
                    }
                }
            }
        }
    }
    run_graphql_test(query, correct, graph)


def test_apply_view_in_neighbours_at():
    graph = Graph()
    create_graph_date(graph)
    query = """
{
  graph(path: "g") {
    node(name: "2") {
      inNeighbours {
        applyViews(views: [{at: 1735862400000}]) {
          list {
            name
            history {
              timestamps {
                list
              }
            }
          }
        }
      }
    }
  }
}"""
    correct = {
        "graph": {
            "node": {
                "inNeighbours": {
                    "applyViews": {
                        "list": [
                            {
                                "history": {
                                    "timestamps": {
                                        "list": [1735862400000, 1735862400000]
                                    }
                                },
                                "name": "1",
                            }
                        ]
                    }
                }
            }
        }
    }
    run_graphql_test(query, correct, graph)


def test_apply_view_out_neighbours_snapshot_latest():
    graph = Graph()
    create_graph_date(graph)
    query = """
{
  graph(path: "g") {
    node(name: "1") {
      outNeighbours {
        applyViews(views: [{snapshotLatest: true}]) {
          list {
            name
            history {
              timestamps {
                list
              }
            }
          }
        }
      }
    }
  }
}"""
    correct = {
        "graph": {
            "node": {
                "outNeighbours": {
                    "applyViews": {
                        "list": [
                            {
                                "name": "2",
                                "history": {
                                    "timestamps": {
                                        "list": [
                                            1735689600000,
                                            1735776000000,
                                            1735862400000,
                                        ]
                                    }
                                },
                            },
                            {
                                "name": "3",
                                "history": {
                                    "timestamps": {
                                        "list": [
                                            1735776000000,
                                            1735862400000,
                                            1735948800000,
                                        ]
                                    }
                                },
                            },
                        ]
                    }
                }
            }
        }
    }

    run_graphql_test(query, correct, graph)


def test_apply_view_out_neighbours_snapshot_at():
    graph = Graph()
    create_graph_date(graph)
    query = """
{
  graph(path: "g") {
    node(name: "1") {
      outNeighbours {
        applyViews(views: [{snapshotAt: 1735862400000}]) {
          list {
            name
            history {
              timestamps {
                list
              }
            }
          }
        }
      }
    }
  }
}"""
    correct = {
        "graph": {
            "node": {
                "outNeighbours": {
                    "applyViews": {
                        "list": [
                            {
                                "name": "2",
                                "history": {
                                    "timestamps": {
                                        "list": [
                                            1735689600000,
                                            1735776000000,
                                            1735862400000,
                                        ]
                                    }
                                },
                            },
                            {
                                "name": "3",
                                "history": {
                                    "timestamps": {
                                        "list": [1735776000000, 1735862400000]
                                    }
                                },
                            },
                        ]
                    }
                }
            }
        }
    }

    run_graphql_test(query, correct, graph)


def test_valid_graph():
    graph = PersistentGraph()
    create_persistent_graph_epoch(graph)
    query = """
            {
              graph(path:"g"){
                applyViews(views:[{valid:true}]){
                  edges{
                    list{
                      id
                      latestTime {
                        timestamp
                      }
                    }
                  }	
                }
              }
            }"""
    correct = {
        "graph": {
            "applyViews": {
                "edges": {"list": [{"id": ["6", "7"], "latestTime": {"timestamp": 5}}]}
            }
        }
    }
    run_graphql_test(query, correct, graph)
