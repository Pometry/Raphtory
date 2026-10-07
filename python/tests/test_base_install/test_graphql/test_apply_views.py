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
    filter(expr: {view: [{kind: SNAPSHOT_LATEST}]}) {
      earliestTime {
        timestamp
      }
    }
    nodes {
      filter(expr: {view: [{kind: SNAPSHOT_LATEST}]}) {
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
      filter(expr: {view: [{kind: SNAPSHOT_LATEST}]}) {
        history {
          timestamps {
            list
          }
        }
      }
    }
    edges {
      filter(expr: {view: [{kind: SNAPSHOT_LATEST}]}) {
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
      filter(expr: {view: [{kind: SNAPSHOT_LATEST}]}) {
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
            "filter": {"earliestTime": {"timestamp": 1735689600000}},
            "nodes": {
                "filter": {
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
                "filter": {
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
                "filter": {
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
                "filter": {
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
    filter(expr: {view: [{kind: DEFAULT_LAYER}]}) {
      earliestTime {
        timestamp
      }
    }
    nodes {
      filter(expr: {view: [{kind: DEFAULT_LAYER}]}) {
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
      filter(expr: {view: [{kind: DEFAULT_LAYER}]}) {
        history {
          timestamps {
            list
          }
        }
      }
    }
    edges {
      filter(expr: {view: [{kind: DEFAULT_LAYER}]}) {
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
      filter(expr: {view: [{kind: DEFAULT_LAYER}]}) {
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
            "filter": {"earliestTime": {"timestamp": 1735689600000}},
            "nodes": {
                "filter": {
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
            "node": {"filter": {"history": {"timestamps": {"list": [1735862400000]}}}},
            "edges": {
                "filter": {
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
            "edge": {"filter": {"src": {"history": {"timestamps": {"list": []}}}}},
        }
    }
    run_graphql_test(query, correct, graph)


def test_apply_view_latest():
    graph = Graph()
    create_graph_date(graph)
    query = """
{
  graph(path: "g") {
    filter(expr: {view: [{kind: LATEST}]}) {
      earliestTime {
        timestamp
      }
    }
    nodes {
      filter(expr: {view: [{kind: LATEST}]}) {
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
      filter(expr: {view: [{kind: LATEST}]}) {
        history {
          timestamps {
            list
          }
        }
      }
    }
    edges {
      filter(expr: {view: [{kind: LATEST}]}) {
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
      filter(expr: {view: [{kind: LATEST}]}) {
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
            "filter": {"earliestTime": {"timestamp": 1736035200000}},
            "nodes": {"filter": {"page": [{"history": {"timestamps": {"list": []}}}]}},
            "node": {"filter": {"history": {"timestamps": {"list": []}}}},
            "edges": {
                "filter": {
                    "page": [
                        {
                            "src": {"history": {"timestamps": {"list": []}}},
                            "dst": {"history": {"timestamps": {"list": []}}},
                        }
                    ]
                }
            },
            "edge": {
                "filter": {
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
    filter(expr: {view: [{at: 1735689600000}]}) {
      earliestTime {
        timestamp
      }
    }
    nodes {
      filter(expr: {view: [{at: 1735689600000}]}) {
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
      filter(expr: {view: [{at: 1735689600000}]}) {
        history {
          timestamps {
            list
          }
        }
      }
    }
    edges {
      filter(expr: {view: [{at: 1735689600000}]}) {
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
      filter(expr: {view: [{at: 1735689600000}]}) {
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
            "filter": {"earliestTime": {"timestamp": 1735689600000}},
            "nodes": {
                "filter": {
                    "page": [
                        {
                            "history": {
                                "timestamps": {"list": [1735689600000, 1735689600000]}
                            }
                        }
                    ]
                }
            },
            "node": {"filter": {"history": {"timestamps": {"list": [1735689600000]}}}},
            "edges": {
                "filter": {
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
            "edge": {"filter": {"src": {"history": {"timestamps": {"list": []}}}}},
        }
    }
    run_graphql_test(query, correct, graph)


def test_apply_view_snapshot_at():
    graph = Graph()
    create_graph_date(graph)
    query = """
{
  graph(path: "g") {
    filter(expr: {view: [{snapshotAt: 1740873600000}]}) {
    	latestTime {
        timestamp
      }
    }
    nodes {
      filter(expr: {view: [{snapshotAt: 1735901379000}]}) {
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
      filter(expr: {view: [{snapshotAt: 1735901379000}]}) {
        history {
          timestamps {
            list
          }
        }
      }
    }
    edges {
      filter(expr: {view: [{snapshotAt: 1735901379000}]}) {
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
      filter(expr: {view: [{snapshotAt: 1735901379000}]}) {
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
            "filter": {"latestTime": {"timestamp": 1736035200000}},
            "nodes": {
                "filter": {
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
                "filter": {
                    "history": {
                        "timestamps": {
                            "list": [1735689600000, 1735776000000, 1735862400000]
                        }
                    }
                }
            },
            "edges": {
                "filter": {
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
            "edge": {"filter": {"src": {"history": {"timestamps": {"list": []}}}}},
        }
    }
    run_graphql_test(query, correct, graph)


def test_apply_view_window():
    graph = Graph()
    create_graph_date(graph)
    query = """
{
  graph(path: "g") {
    filter(expr: {view: [{window: {
      start: 1735689600000
      end: 1735862400000
    }}]}) {
    	latestTime {
        timestamp
      }
    }
    nodes {
      filter(expr: {view: [{window: {
         start: 1735689600000
      end: 1735862400000
    }}]}) {
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
       filter(expr: {view: [{window: {
        start: 1735689600000
      end: 1735862400000
    }}]}) {
        history {
          timestamps {
            list
          }
        }
      }
    }
    edges {
       filter(expr: {view: [{window: {
        start: 1735689600000
      end: 1735862400000
    }}]}) {
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
       filter(expr: {view: [{window: {
          start: 1735689600000
          end: 1735862400000
        }}]}) {
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
            "filter": {"latestTime": {"timestamp": 1735776000000}},
            "nodes": {
                "filter": {
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
                "filter": {
                    "history": {"timestamps": {"list": [1735689600000, 1735776000000]}}
                }
            },
            "edges": {
                "filter": {
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
                "filter": {
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
    filter(expr: {view: [{before: 1735862400000}]}) {
      latestTime {
        timestamp
      }
    }
    nodes {
      filter(expr: {view: [{before: 1735862400000}]}) {
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
      filter(expr: {view: [{before: 1735862400000}]}) {
        history {
          timestamps {
            list
          }
        }
      }
    }
    edges {
      filter(expr: {view: [{before: 1735862400000}]}) {
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
      filter(expr: {view: [{before: 1735862400000}]}) {
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
            "filter": {"latestTime": {"timestamp": 1735776000000}},
            "nodes": {
                "filter": {
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
                "filter": {
                    "history": {"timestamps": {"list": [1735689600000, 1735776000000]}}
                }
            },
            "edges": {
                "filter": {
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
                "filter": {
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
    filter(expr: {view: [{after: 6}]}) {
      latestTime {
        timestamp
      }
    }
    nodes {
      filter(expr: {view: [{after: 6}]}) {
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
    node(name: "2") {
      filter(expr: {view: [{after: 3}]}) {
        history {
          timestamps {
            list
          }
        }
      }
    }
    edges {
      filter(expr: {view: [{after: 6}]}) {
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
      filter(expr: {view: [{after: 3}]}) {
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
            "filter": {"latestTime": {"timestamp": None}},
            "nodes": {
                "filter": {
                    "list": [
                        {"name": "1", "history": {"timestamps": {"list": []}}},
                        {"name": "2", "history": {"timestamps": {"list": []}}},
                        {"name": "3", "history": {"timestamps": {"list": []}}},
                        {"name": "6", "history": {"timestamps": {"list": []}}},
                        {"name": "7", "history": {"timestamps": {"list": []}}},
                    ]
                }
            },
            "node": {"filter": {"history": {"timestamps": {"list": []}}}},
            "edges": {
                "filter": {
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
                "filter": {
                    "history": {"timestamps": {"list": []}},
                    "src": {"name": "1"},
                }
            },
        }
    }
    run_graphql_test(query, correct, graph, sort_output=True)


def test_apply_view_shrink_start():
    graph = Graph()
    create_graph_date(graph)
    query = """
{
  graph(path: "g") {
    filter(expr: {view: [{shrinkStart:  1736035200000}]}) {
      latestTime {
        timestamp
      }
    }
    nodes {
     filter(expr: {view: [{shrinkStart:  1736035200000}]}) {
        list {
          name
        }
      }
    }
    node(name: "2") {
     filter(expr: {view: [{shrinkStart:  1736035200000}]}) {
        history {
          timestamps {
            list
          }
        }
      }
    }
    edges {
   filter(expr: {view: [{shrinkStart:  1736035200000}]}) {
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
    filter(expr: {view: [{shrinkStart:  1736035200000}]}) {
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
            "filter": {"latestTime": {"timestamp": 1736035200000}},
            "nodes": {
                "filter": {
                    "list": [
                        {"name": "1"},
                        {"name": "2"},
                        {"name": "3"},
                        {"name": "6"},
                        {"name": "7"},
                    ]
                }
            },
            "node": {"filter": {"history": {"timestamps": {"list": []}}}},
            "edges": {
                "filter": {
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
                "filter": {
                    "history": {"timestamps": {"list": []}},
                    "src": {"name": "1"},
                    "dst": {"name": "2"},
                }
            },
        }
    }
    run_graphql_test(query, correct, graph, sort_output=True)


def test_apply_view_shrink_end():
    graph = Graph()
    create_graph_date(graph)
    query = """
{
  graph(path: "g") {
    filter(expr: {view: [{shrinkEnd:  1735776000000}]}) {
      latestTime {
        timestamp
      }
    }
    nodes {
     filter(expr: {view: [{shrinkEnd:  1735776000000}]}) {
        list {
          name
        }
      }
    }
    node(name: "2") {
     filter(expr: {view: [{shrinkEnd:  1735776000000}]}) {
        history {
          timestamps {
            list
          }
        }
      }
    }
    edges {
   filter(expr: {view: [{shrinkEnd:  1735776000000}]}) {
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
    filter(expr: {view: [{shrinkEnd:  1735776000000}]}) {
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
            "filter": {"latestTime": {"timestamp": 1735689600000}},
            "nodes": {
                "filter": {
                    "list": [
                        {"name": "1"},
                        {"name": "2"},
                        {"name": "3"},
                        {"name": "6"},
                        {"name": "7"},
                    ]
                }
            },
            "node": {"filter": {"history": {"timestamps": {"list": [1735689600000]}}}},
            "edges": {
                "filter": {
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
                "filter": {
                    "history": {"timestamps": {"list": [1735689600000]}},
                    "src": {"name": "1"},
                    "dst": {"name": "2"},
                }
            },
        }
    }
    run_graphql_test(query, correct, graph, sort_output=True)


def test_apply_view_layers():
    graph = Graph()
    create_graph_date(graph)
    query = """
{
  graph(path: "g") {
    filter(expr: {view: [{layers: ["finds"]}]}) {
      earliestTime {
        timestamp
      }
    }
    nodes {
      filter(expr: {view: [{layers: ["finds"]}]}) {
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
      filter(expr: {view: [{layers: ["finds"]}]}) {
        history {
          timestamps {
            list
          }
        }
      }
    }
    edges {
      filter(expr: {view: [{layers: ["finds"]}]}) {
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
    filter(expr: {view: [{layers: ["finds", "met"]}]}) {
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
            "filter": {"earliestTime": {"timestamp": 1735689600000}},
            "nodes": {
                "filter": {
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
            "node": {"filter": {"history": {"timestamps": {"list": [1735689600000]}}}},
            "edges": {
                "filter": {
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
                "filter": {
                    "history": {"timestamps": {"list": [1735689600000]}},
                    "src": {"name": "1"},
                    "dst": {"name": "2"},
                }
            },
        }
    }
    run_graphql_test(query, correct, graph, sort_output=True)


def test_apply_view_layer():
    graph = Graph()
    create_graph_date(graph)
    query = """
{
  graph(path: "g") {
    filter(expr: {view: [{layers: []}]}) {
      earliestTime {
        timestamp
      }
    }
    nodes {
     select(expr: {node: {cmp: {op: EQ, lhs: {read: { field: NODE_TYPE }}, rhs: {const: {str: "Person"}}}}}) {
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
      filter(expr: {view: [{layers: ["finds"]}]}) {
        history {
          timestamps {
            list
          }
        }
      }
    }
    edges {
      filter(expr: {view: [{layers: ["finds"]}]}) {
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
  filter(expr: {view: [{layers: ["met"]}]}) {
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
            "filter": {"earliestTime": {"timestamp": 1735689600000}},
            "nodes": {
                "select": {
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
            "node": {"filter": {"history": {"timestamps": {"list": [1735689600000]}}}},
            "edges": {
                "filter": {
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
                "filter": {
                    "history": {"timestamps": {"list": [1735689600000]}},
                    "src": {"name": "1"},
                    "dst": {"name": "2"},
                }
            },
        }
    }
    run_graphql_test(query, correct, graph, sort_output=True)


def test_apply_view_exclude_layer():
    graph = Graph()
    create_graph_date(graph)
    query = """
{
  graph(path: "g") {
    filter(expr: {view: [{excludeLayers: []}]}) {
      earliestTime {
        timestamp
      }
    }
    nodes {
      filter(expr: {view: [{excludeLayers: []}]}) {
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
      filter(expr: {view: [{excludeLayers: []}]}) {
        history {
          timestamps {
            list
          }
        }
      }
    }
    edges {
      filter(expr: {view: [{excludeLayer: "finds"}]}) {
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
      filter(expr: {view: [{excludeLayer: "finds"}]}) {
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
            "filter": {"earliestTime": {"timestamp": 1735689600000}},
            "nodes": {
                "filter": {
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
                "filter": {
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
                "filter": {
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
                "filter": {
                    "history": {"timestamps": {"list": []}},
                    "src": {"name": "6"},
                    "dst": {"name": "7"},
                }
            },
        }
    }
    run_graphql_test(query, correct, graph, sort_output=True)


def test_apply_view_exclude_layers():
    graph = Graph()
    create_graph_date(graph)
    query = """
{
  graph(path: "g") {
    filter(expr: {view: [{excludeLayers: ["finds"]}]}) {
      earliestTime {
        timestamp
      }
    }
    nodes {
      filter(expr: {view: [{excludeLayers: []}]}) {
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
      filter(expr: {view: [{excludeLayers: []}]}) {
        history {
          timestamps {
            list
          }
        }
      }
    }
    edges {
      filter(expr: {view: [{excludeLayers: ["finds", "met"]}]}) {
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
     filter(expr: {view: [{excludeLayers: ["finds"]}]}) {
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
            "filter": {"earliestTime": {"timestamp": 1735689600000}},
            "nodes": {
                "filter": {
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
                "filter": {
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
                "filter": {
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
                "filter": {
                    "history": {"timestamps": {"list": []}},
                    "src": {"name": "6"},
                    "dst": {"name": "7"},
                }
            },
        }
    }
    run_graphql_test(query, correct, graph, sort_output=True)


def test_apply_view_type_filter():
    graph = Graph()
    create_graph_date(graph)
    query = """
{
  graph(path: "g") {
      nodes {
        select(expr: {node: {cmp: {op: EQ, lhs: {read: { field: NODE_TYPE }}, rhs: {const: {str: "Person"}}}}}) {
          list {
            name
          }
        }
      }
    }
  }
"""
    correct = {"graph": {"nodes": {"select": {"list": [{"name": "1"}]}}}}
    run_graphql_test(query, correct, graph)


def test_apply_view_exclude_nodes():
    graph = Graph()
    create_graph_date(graph)
    query = """
{
  graph(path: "g") {
    filter(expr: {view: [{excludeNodes: ["6", "7"]}]}) {
      latestTime {
        timestamp
      }
    }
  }
}"""
    correct = {
        "graph": {
            "filter": {"latestTime": {"timestamp": 1735948800000}},
        }
    }
    run_graphql_test(query, correct, graph)


def test_apply_view_too_many_arguments():
    graph = Graph()
    create_graph_date(graph)
    queries_and_exceptions = []
    too_many_arguments_exception = (
        'Fields "expr" conflict because they have differing arguments'
    )
    query = """
{
  graph(path: "g") {
      filter(expr: {view: [{layers: ["odd"]}]}) {
        name
      }
      filter(expr: {view: [{layers: ["Person"]}]}) {
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
    filter(expr: {view: [{layers: ["finds"]}]}) {
      earliestTime {
        timestamp
      }
      edges {
        filter(expr: {view: [{layers: ["finds"]}]}) {
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
            "filter": {
                "earliestTime": {"timestamp": 1735689600000},
                "edges": {
                    "filter": {
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
    invalid_argument = (
        'Invalid value for argument "expr.view.0.layers", expected type "String"'
    )
    query = """
{
  graph(path: "g") {
    filter(expr: {view: [{layers: 5}]}) {
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
        filter(expr: {
                      node: {
                        cmp: {
                          op: EQ
                          lhs: {
                            read: { property: "where" }
                          }
                          rhs: {
                            const: {
                              str: "Berlin"
                            }
                          }
                        }
                      }
                    }) {
          nodes {
            list {
              name
            }
          }
        }
      }
    }
    """
    correct = {"graph": {"filter": {"nodes": {"list": [{"name": "1"}]}}}}
    run_graphql_test(query, correct, graph)


def test_apply_view_edge_filter():
    graph = Graph()
    create_graph_date(graph)
    query = """
    {
      graph(path: "g") {
        filter(expr: {
                      edge: {
                        cmp: {
                          op: EQ
                          lhs: {
                            read: { property: "where" }
                          }
                          rhs: {
                            const: {
                              str: "fishbowl"
                            }
                          }
                        }
                      }
                    }) {
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
            "filter": {
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
    filter(expr: {view: [{subgraph: ["1", "2"]}]}) {
      nodes {
        list {
          name
        }
      }
    }
  }
}"""
    correct = {"graph": {"filter": {"nodes": {"list": [{"name": "1"}, {"name": "2"}]}}}}
    run_graphql_test(query, correct, graph)


def test_apply_view_subgraph_node_types():
    graph = Graph()
    create_graph_date(graph)
    query = """
{
  graph(path: "g") {
    filter(expr: {view: [{subgraphNodeTypes: ["Person"]}]}) {
      nodes {
        list {
          name
        }
      }
    }
  }
}"""
    correct = {"graph": {"filter": {"nodes": {"list": [{"name": "1"}]}}}}
    run_graphql_test(query, correct, graph)


def test_apply_view_nodes_multiple_views():
    graph = Graph()
    create_graph_date(graph)
    query = """
{
  graph(path: "g") {
    filter(expr: {view: [{window: {start: 1735689600000, end: 1735862400000}}, {layers: []}]}) {
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
            "filter": {
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
    filter(expr: {view: [{window: {start: 1735689600000, end: 1735862400000}}, {layers: ["met"]}]}) {
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
            "filter": {
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
          filter(expr: {and: [{view: [{ window: { start: 1735689600000, end: 1735862400000 } }, { layers: ["follows"] }]}, {
                        node: {
                          cmp: {
                            op: EQ
                            lhs: {
                              read: { property: "where" }
                            }
                            rhs: {
                              const: {
                                str: "Berlin"
                              }
                            }
                          }
                        }
                      }]}) {
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
                "filter": {
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
          filter(expr: {view: [{kind: LATEST}]}) {
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
                            "filter": {
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
                            "filter": {
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
                            "filter": {
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
                            "filter": {
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
                            "filter": {
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
        filter(expr: {view: [{kind: LATEST}]}) {
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
                    "filter": {
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
        filter(expr: {view: [{layers: ["finds"]}]}) {
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
                    "filter": {
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
        filter(expr: {view: [{excludeLayer: "finds"}]}) {
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
                    "filter": {
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
        filter(expr: {view: [{layers: ["met"]}]}) {
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
                    "filter": {
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
        filter(expr: {view: [{excludeLayers: ["met"]}]}) {
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
                    "filter": {
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
        filter(expr: {view: [{after: 1735862400000}]}) {
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
                    "filter": {
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
        filter(expr: {view: [{before: 1735862400000}]}) {
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
                    "filter": {
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
        filter(expr: {view: [{window: {start: 1735689600000, end: 1735862400000}}]}) {
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
    correct = {"graph": {"node": {"inNeighbours": {"filter": {"list": []}}}}}

    run_graphql_test(query, correct, graph)


def test_apply_view_out_neighbours_window():
    graph = Graph()
    create_graph_date(graph)
    query = """
{
  graph(path: "g") {
    node(name: "1") {
      outNeighbours {
        filter(expr: {view: [{window: {start: 1735689600000, end: 1735862400000}}]}) {
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
                    "filter": {
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


def test_apply_view_in_neighbours_shrink_start():
    graph = Graph()
    create_graph_date(graph)
    query = """
{
  graph(path: "g") {
    node(name: "7") {
      inNeighbours {
        filter(expr: {view: [{shrinkStart: 1735948800000}]}) {
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
                    "filter": {
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
        filter(expr: {view: [{shrinkEnd: 1735862400000}]}) {
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
                    "filter": {
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
        filter(expr: {view: [{at: 1735862400000}]}) {
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
                    "filter": {
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
        filter(expr: {view: [{kind: SNAPSHOT_LATEST}]}) {
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
                    "filter": {
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
        filter(expr: {view: [{snapshotAt: 1735862400000}]}) {
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
                    "filter": {
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
                filter(expr: {view: [{kind: VALID}]}) {
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
            "filter": {
                "edges": {"list": [{"id": [6, 7], "latestTime": {"timestamp": 5}}]}
            }
        }
    }
    run_graphql_test(query, correct, graph)
