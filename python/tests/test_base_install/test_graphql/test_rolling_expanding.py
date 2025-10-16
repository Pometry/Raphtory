from datetime import datetime, timezone

from raphtory import Graph

from utils import run_graphql_test, run_group_graphql_error_test, run_group_graphql_test


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

    g.add_edge(dates[0], 1, 2)
    g.add_edge(dates[1], 1, 2)
    g.add_edge(dates[2], 1, 2)
    g.add_edge(dates[1], 1, 3)
    g.add_edge(dates[2], 1, 3)
    g.add_edge(dates[3], 1, 3)
    g.add_edge(dates[4], 6, 7)


def test_graph_date():
    graph = Graph()
    create_graph_date(graph)
    query = """
    {
  graph(path: "g") {
    rolling(window: {duration:"1 day"}, step: {duration: "12 hours"}) {
      page(limit: 5, offset: 5) {
        nodes{
          list{
            name
          }
        }
      }
      count
      list{
        start {
          timestamp
        }
        end {
          timestamp
        }
      }
    }
  }
}
    """
    correct = {
        "graph": {
            "rolling": {
                "page": [
                    {"nodes": {"list": [{"name": "1"}, {"name": "2"}, {"name": "3"}]}},
                    {"nodes": {"list": [{"name": "1"}, {"name": "3"}]}},
                    {"nodes": {"list": [{"name": "1"}, {"name": "3"}]}},
                    {"nodes": {"list": [{"name": "6"}, {"name": "7"}]}},
                ],
                "count": 9,
                "list": [
                    {
                        "start": {"timestamp": 1735646400000},
                        "end": {"timestamp": 1735732800000},
                    },
                    {
                        "start": {"timestamp": 1735689600000},
                        "end": {"timestamp": 1735776000000},
                    },
                    {
                        "start": {"timestamp": 1735732800000},
                        "end": {"timestamp": 1735819200000},
                    },
                    {
                        "start": {"timestamp": 1735776000000},
                        "end": {"timestamp": 1735862400000},
                    },
                    {
                        "start": {"timestamp": 1735819200000},
                        "end": {"timestamp": 1735905600000},
                    },
                    {
                        "start": {"timestamp": 1735862400000},
                        "end": {"timestamp": 1735948800000},
                    },
                    {
                        "start": {"timestamp": 1735905600000},
                        "end": {"timestamp": 1735992000000},
                    },
                    {
                        "start": {"timestamp": 1735948800000},
                        "end": {"timestamp": 1736035200000},
                    },
                    {
                        "start": {"timestamp": 1735992000000},
                        "end": {"timestamp": 1736078400000},
                    },
                ],
            }
        }
    }
    run_graphql_test(query, correct, graph)

    query = """
{
  graph(path: "g") {
    expanding(step: {duration: "3 days"}) {
      list{
        start {
          timestamp
        }
        end {
          timestamp
        }
      }
    }
  }
}
    """
    correct = {
        "graph": {
            "expanding": {
                "list": [
                    {"start": None, "end": {"timestamp": 1735948800000}},
                    {"start": None, "end": {"timestamp": 1736208000000}},
                ]
            }
        }
    }
    run_graphql_test(query, correct, graph)


def test_graph_epoch():
    graph = Graph()
    create_graph_epoch(graph)
    query = """
    {
      graph(path: "g") {
        rolling(window: {epoch: 1}) {
          list {
            earliestTime {
              timestamp
            }
            latestTime {
              timestamp
            }
          }
        }
      }
    }
    """
    correct = {
        "graph": {
            "rolling": {
                "list": [
                    {"earliestTime": {"timestamp": 1}, "latestTime": {"timestamp": 1}},
                    {"earliestTime": {"timestamp": 2}, "latestTime": {"timestamp": 2}},
                    {"earliestTime": {"timestamp": 3}, "latestTime": {"timestamp": 3}},
                    {"earliestTime": {"timestamp": 4}, "latestTime": {"timestamp": 4}},
                    {"earliestTime": {"timestamp": 5}, "latestTime": {"timestamp": 5}},
                ]
            }
        }
    }
    run_graphql_test(query, correct, graph)

    query = """
    {
      graph(path: "g") {
        rolling(window: {epoch: 1}, step:{epoch: 2}) {
          list {
            earliestTime {
              timestamp
            }
            latestTime {
              timestamp
            }
          }
        }
      }
    }
    """
    correct = {
        "graph": {
            "rolling": {
                "list": [
                    {"earliestTime": {"timestamp": 2}, "latestTime": {"timestamp": 2}},
                    {"earliestTime": {"timestamp": 4}, "latestTime": {"timestamp": 4}},
                ]
            }
        }
    }
    run_graphql_test(query, correct, graph)

    query = """
    {
  graph(path: "g") {
    window(start: 2, end: 5) {
      rolling(window: {epoch: 2}, step: {epoch: 1}) {
        list {
          start {
            timestamp
          }
          end {
            timestamp
          }
        }
      }
    }
  }
}
    """
    correct = {
        "graph": {
            "window": {
                "rolling": {
                    "list": [
                        {"start": {"timestamp": 2}, "end": {"timestamp": 3}},
                        {"start": {"timestamp": 2}, "end": {"timestamp": 4}},
                        {"start": {"timestamp": 3}, "end": {"timestamp": 5}},
                    ]
                }
            }
        }
    }
    run_graphql_test(query, correct, graph)

    query = """
   {
  graph(path: "g") {
    window(start: 2, end: 7) {
      expanding(step: {epoch: 3}) {
        list {
          end {
            timestamp
          }
          nodes {
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
    correct = {
        "graph": {
            "window": {
                "expanding": {
                    "list": [
                        {
                            "end": {"timestamp": 5},
                            "nodes": {
                                "list": [{"name": "1"}, {"name": "2"}, {"name": "3"}]
                            },
                        },
                        {
                            "end": {"timestamp": 7},
                            "nodes": {
                                "list": [
                                    {"name": "1"},
                                    {"name": "2"},
                                    {"name": "3"},
                                    {"name": "6"},
                                    {"name": "7"},
                                ]
                            },
                        },
                    ]
                }
            }
        }
    }
    run_graphql_test(query, correct, graph)

    query = """
       {
      graph(path: "g") {
        window(start: 2, end: 5) {
          expanding(step: {epoch: 1}) {
            list {
              start {
                timestamp
              }
              end {
                timestamp
              }
            }
          }
        }
      }
    }
    """
    correct = {
        "graph": {
            "window": {
                "expanding": {
                    "list": [
                        {"start": {"timestamp": 2}, "end": {"timestamp": 3}},
                        {"start": {"timestamp": 2}, "end": {"timestamp": 4}},
                        {"start": {"timestamp": 2}, "end": {"timestamp": 5}},
                    ]
                }
            }
        }
    }
    run_graphql_test(query, correct, graph)

    query = """
    {
  graph(path: "g") {
    rolling(window: {epoch: 3}, step: {epoch: 4}) {
      list {
        start {
          timestamp
        }
        end {
          timestamp
        }
        earliestTime {
          timestamp
        }
        latestTime {
          timestamp
        }
      }
    }
  }
}
    """
    correct = {
        "graph": {
            "rolling": {
                "list": [
                    {
                        "start": {"timestamp": 2},
                        "end": {"timestamp": 5},
                        "earliestTime": {"timestamp": 2},
                        "latestTime": {"timestamp": 4},
                    }
                ]
            }
        }
    }
    run_graphql_test(query, correct, graph)

    # testing if a very large step returns nothing
    query = """
    {
  graph(path: "g") {
    rolling(window: {epoch: 3}, step: {epoch: 1000}) {
      list {
        start {
          timestamp
        }
        end {
          timestamp
        }
        earliestTime {
          timestamp
        }
        latestTime {
          timestamp
        }
      }
    }
  }
}
    """
    correct = {"graph": {"rolling": {"list": []}}}
    run_graphql_test(query, correct, graph)

    query = """
    {
      graph(path: "g") {
        rolling(window: {epoch: 1}, step: {epoch: 1}) {
          count 
        }
      }
    }
        """
    correct = {"graph": {"rolling": {"count": 5}}}
    run_graphql_test(query, correct, graph)

    query = """
    {
  graph(path: "g") {
    rolling(window: {epoch: 1}, step: {epoch: 1}) {
      page(limit: 2, pageIndex: 1, offset: 2) {
        earliestTime {
          timestamp
        }
        latestTime {
          timestamp
        }
      }
    }
  }
}
        """
    correct = {
        "graph": {
            "rolling": {
                "page": [
                    {"earliestTime": {"timestamp": 5}, "latestTime": {"timestamp": 5}}
                ]
            }
        }
    }
    run_graphql_test(query, correct, graph)


def test_node():
    graph = Graph()
    create_graph_epoch(graph)
    query = """
{
  graph(path: "g") {
    node(name:"1"){
      rolling(window:{epoch: 1}, step:{epoch: 1}){
        list{
          start {
            timestamp
          }
          end {
            timestamp
          }
          degree
          earliestTime {
            timestamp
          }
        }
        count
        page(limit:3,offset:3){
          start {
            timestamp
          }
          degree
        }
      }
      before(time: 4){
        expanding(step:{epoch: 1}){
        list{
          end {
            timestamp
          }
          degree
        }
        count
        page(limit:1,offset:2){
          end {
            timestamp
          }
          degree
        }
      }
      }
      
    }
  }
}
        """
    correct = {
        "graph": {
            "node": {
                "rolling": {
                    "list": [
                        {
                            "start": {"timestamp": 1},
                            "end": {"timestamp": 2},
                            "degree": 1,
                            "earliestTime": {"timestamp": 1},
                        },
                        {
                            "start": {"timestamp": 2},
                            "end": {"timestamp": 3},
                            "degree": 2,
                            "earliestTime": {"timestamp": 2},
                        },
                        {
                            "start": {"timestamp": 3},
                            "end": {"timestamp": 4},
                            "degree": 2,
                            "earliestTime": {"timestamp": 3},
                        },
                        {
                            "start": {"timestamp": 4},
                            "end": {"timestamp": 5},
                            "degree": 1,
                            "earliestTime": {"timestamp": 4},
                        },
                        {
                            "start": {"timestamp": 5},
                            "end": {"timestamp": 6},
                            "degree": 0,
                            "earliestTime": None,
                        },
                    ],
                    "count": 5,
                    "page": [
                        {"start": {"timestamp": 4}, "degree": 1},
                        {"start": {"timestamp": 5}, "degree": 0},
                    ],
                },
                "before": {
                    "expanding": {
                        "list": [
                            {"end": {"timestamp": 2}, "degree": 1},
                            {"end": {"timestamp": 3}, "degree": 2},
                            {"end": {"timestamp": 4}, "degree": 2},
                        ],
                        "count": 3,
                        "page": [{"end": {"timestamp": 4}, "degree": 2}],
                    }
                },
            }
        }
    }
    run_graphql_test(query, correct, graph)


def test_nodes():
    graph = Graph()
    create_graph_epoch(graph)

    query = """
{
  graph(path: "g") {
    nodes {
      rolling(window: {epoch: 1}, step: {epoch: 1}) {
        list {
          page(limit: 1, offset: 0) {
            id
            degree
            start {
              timestamp
            }
            end {
              timestamp
            }
            earliestTime {
              timestamp
            }
          }
        }
        count
        page(limit: 3, offset: 3) {
          page(limit: 1, offset: 0) {
            id
            degree
            start {
              timestamp
            }
            end {
              timestamp
            }
            earliestTime {
              timestamp
            }
          }
        }
      }
      after(time: 1) {
        expanding(step: {epoch: 1}) {
          list {
            page(limit: 1, offset: 0) {
              id
              degree
              start {
                timestamp
              }
              end {
                timestamp
              }
              earliestTime {
                timestamp
              }
              latestTime {
                timestamp
              }
            }
          }
          count
          page(limit: 2, pageIndex: 1) {
            page(limit: 1, offset: 0) {
              id
              degree
              start {
                timestamp
              }
              end {
                timestamp
              }
              earliestTime {
                timestamp
              }
              latestTime {
                timestamp
              }
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
            "nodes": {
                "rolling": {
                    "list": [
                        {
                            "page": [
                                {
                                    "id": "1",
                                    "degree": 1,
                                    "start": {"timestamp": 1},
                                    "end": {"timestamp": 2},
                                    "earliestTime": {"timestamp": 1},
                                }
                            ]
                        },
                        {
                            "page": [
                                {
                                    "id": "1",
                                    "degree": 2,
                                    "start": {"timestamp": 2},
                                    "end": {"timestamp": 3},
                                    "earliestTime": {"timestamp": 2},
                                }
                            ]
                        },
                        {
                            "page": [
                                {
                                    "id": "1",
                                    "degree": 2,
                                    "start": {"timestamp": 3},
                                    "end": {"timestamp": 4},
                                    "earliestTime": {"timestamp": 3},
                                }
                            ]
                        },
                        {
                            "page": [
                                {
                                    "id": "1",
                                    "degree": 1,
                                    "start": {"timestamp": 4},
                                    "end": {"timestamp": 5},
                                    "earliestTime": {"timestamp": 4},
                                }
                            ]
                        },
                        {
                            "page": [
                                {
                                    "id": "6",
                                    "degree": 1,
                                    "start": {"timestamp": 5},
                                    "end": {"timestamp": 6},
                                    "earliestTime": {"timestamp": 5},
                                }
                            ]
                        },
                    ],
                    "count": 5,
                    "page": [
                        {
                            "page": [
                                {
                                    "id": "1",
                                    "degree": 1,
                                    "start": {"timestamp": 4},
                                    "end": {"timestamp": 5},
                                    "earliestTime": {"timestamp": 4},
                                }
                            ]
                        },
                        {
                            "page": [
                                {
                                    "id": "6",
                                    "degree": 1,
                                    "start": {"timestamp": 5},
                                    "end": {"timestamp": 6},
                                    "earliestTime": {"timestamp": 5},
                                }
                            ]
                        },
                    ],
                },
                "after": {
                    "expanding": {
                        "list": [
                            {
                                "page": [
                                    {
                                        "id": "1",
                                        "degree": 2,
                                        "start": None,
                                        "end": {"timestamp": 3},
                                        "earliestTime": {"timestamp": 2},
                                        "latestTime": {"timestamp": 2},
                                    }
                                ]
                            },
                            {
                                "page": [
                                    {
                                        "id": "1",
                                        "degree": 2,
                                        "start": None,
                                        "end": {"timestamp": 4},
                                        "earliestTime": {"timestamp": 2},
                                        "latestTime": {"timestamp": 3},
                                    }
                                ]
                            },
                            {
                                "page": [
                                    {
                                        "id": "1",
                                        "degree": 2,
                                        "start": None,
                                        "end": {"timestamp": 5},
                                        "earliestTime": {"timestamp": 2},
                                        "latestTime": {"timestamp": 4},
                                    }
                                ]
                            },
                            {
                                "page": [
                                    {
                                        "id": "1",
                                        "degree": 2,
                                        "start": None,
                                        "end": {"timestamp": 6},
                                        "earliestTime": {"timestamp": 2},
                                        "latestTime": {"timestamp": 4},
                                    }
                                ]
                            },
                        ],
                        "count": 4,
                        "page": [
                            {
                                "page": [
                                    {
                                        "id": "1",
                                        "degree": 2,
                                        "start": None,
                                        "end": {"timestamp": 5},
                                        "earliestTime": {"timestamp": 2},
                                        "latestTime": {"timestamp": 4},
                                    }
                                ]
                            },
                            {
                                "page": [
                                    {
                                        "id": "1",
                                        "degree": 2,
                                        "start": None,
                                        "end": {"timestamp": 6},
                                        "earliestTime": {"timestamp": 2},
                                        "latestTime": {"timestamp": 4},
                                    }
                                ]
                            },
                        ],
                    }
                },
            }
        }
    }
    run_graphql_test(query, correct, graph)


def test_path():
    graph = Graph()
    create_graph_epoch(graph)

    query = """
{
  graph(path: "g") {
    node(name: "1") {
      neighbours {
        rolling(window: {epoch: 1}, step: {epoch: 1}) {
          list {
            page(limit: 1, offset: 0) {
              id
              degree
              start {
                timestamp
              }
              end {
                timestamp
              }
              earliestTime {
                timestamp
              }
            }
          }
          count
          page(limit: 3, offset: 3) {
            page(limit: 1, offset: 0) {
              id
              degree
              start {
                timestamp
              }
              end {
                timestamp
              }
              earliestTime {
                timestamp
              }
            }
          }
        }
        after(time: 1) {
          expanding(step: {epoch: 1}) {
            list {
              page(limit: 1, offset: 0) {
                id
                degree
                start {
                  timestamp
                }
                end {
                  timestamp
                }
                earliestTime {
                  timestamp
                }
                latestTime {
                  timestamp
                }
              }
            }
            count
            page(limit: 2, pageIndex: 1) {
              page(limit: 1, offset: 0) {
                id
                degree
                start {
                  timestamp
                }
                end {
                  timestamp
                }
                earliestTime {
                  timestamp
                }
                latestTime {
                  timestamp
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
    correct = {
        "graph": {
            "node": {
                "neighbours": {
                    "rolling": {
                        "list": [
                            {
                                "page": [
                                    {
                                        "id": "2",
                                        "degree": 1,
                                        "start": {"timestamp": 1},
                                        "end": {"timestamp": 2},
                                        "earliestTime": {"timestamp": 1},
                                    }
                                ]
                            },
                            {
                                "page": [
                                    {
                                        "id": "2",
                                        "degree": 1,
                                        "start": {"timestamp": 2},
                                        "end": {"timestamp": 3},
                                        "earliestTime": {"timestamp": 2},
                                    }
                                ]
                            },
                            {
                                "page": [
                                    {
                                        "id": "2",
                                        "degree": 1,
                                        "start": {"timestamp": 3},
                                        "end": {"timestamp": 4},
                                        "earliestTime": {"timestamp": 3},
                                    }
                                ]
                            },
                            {
                                "page": [
                                    {
                                        "id": "2",
                                        "degree": 0,
                                        "start": {"timestamp": 4},
                                        "end": {"timestamp": 5},
                                        "earliestTime": None,
                                    }
                                ]
                            },
                            {
                                "page": [
                                    {
                                        "id": "2",
                                        "degree": 0,
                                        "start": {"timestamp": 5},
                                        "end": {"timestamp": 6},
                                        "earliestTime": None,
                                    }
                                ]
                            },
                        ],
                        "count": 5,
                        "page": [
                            {
                                "page": [
                                    {
                                        "id": "2",
                                        "degree": 0,
                                        "start": {"timestamp": 4},
                                        "end": {"timestamp": 5},
                                        "earliestTime": None,
                                    }
                                ]
                            },
                            {
                                "page": [
                                    {
                                        "id": "2",
                                        "degree": 0,
                                        "start": {"timestamp": 5},
                                        "end": {"timestamp": 6},
                                        "earliestTime": None,
                                    }
                                ]
                            },
                        ],
                    },
                    "after": {
                        "expanding": {
                            "list": [
                                {
                                    "page": [
                                        {
                                            "id": "2",
                                            "degree": 1,
                                            "start": {"timestamp": 2},
                                            "end": {"timestamp": 3},
                                            "earliestTime": {"timestamp": 2},
                                            "latestTime": {"timestamp": 2},
                                        }
                                    ]
                                },
                                {
                                    "page": [
                                        {
                                            "id": "2",
                                            "degree": 1,
                                            "start": {"timestamp": 2},
                                            "end": {"timestamp": 4},
                                            "earliestTime": {"timestamp": 2},
                                            "latestTime": {"timestamp": 3},
                                        }
                                    ]
                                },
                                {
                                    "page": [
                                        {
                                            "id": "2",
                                            "degree": 1,
                                            "start": {"timestamp": 2},
                                            "end": {"timestamp": 5},
                                            "earliestTime": {"timestamp": 2},
                                            "latestTime": {"timestamp": 3},
                                        }
                                    ]
                                },
                                {
                                    "page": [
                                        {
                                            "id": "2",
                                            "degree": 1,
                                            "start": {"timestamp": 2},
                                            "end": {"timestamp": 6},
                                            "earliestTime": {"timestamp": 2},
                                            "latestTime": {"timestamp": 3},
                                        }
                                    ]
                                },
                            ],
                            "count": 4,
                            "page": [
                                {
                                    "page": [
                                        {
                                            "id": "2",
                                            "degree": 1,
                                            "start": {"timestamp": 2},
                                            "end": {"timestamp": 5},
                                            "earliestTime": {"timestamp": 2},
                                            "latestTime": {"timestamp": 3},
                                        }
                                    ]
                                },
                                {
                                    "page": [
                                        {
                                            "id": "2",
                                            "degree": 1,
                                            "start": {"timestamp": 2},
                                            "end": {"timestamp": 6},
                                            "earliestTime": {"timestamp": 2},
                                            "latestTime": {"timestamp": 3},
                                        }
                                    ]
                                },
                            ],
                        }
                    },
                }
            }
        }
    }
    run_graphql_test(query, correct, graph)


def test_edge():
    graph = Graph()
    create_graph_epoch(graph)

    query = """
    {
  graph(path: "g") {
    edge(src:"1",dst:"2"){
      rolling(window:{epoch: 1},step:{epoch: 1}){
        list{
          start {
            timestamp
          }
          end {
            timestamp
          }
          earliestTime {
            timestamp
          }
        }
        count
        page(limit:3,offset:3){
          start {
            timestamp
          }
          end {
            timestamp
          }
          earliestTime {
            timestamp
          }
        }
      }
      after(time: 1){
        expanding(step:{epoch: 1}){
        list{
          start {
            timestamp
          }
          end {
            timestamp
          }
          earliestTime {
            timestamp
          }
          latestTime {
            timestamp
          }
        }
        count
        page(limit:2,offset:2){
          start {
            timestamp
          }
          end {
            timestamp
          }
          earliestTime {
            timestamp
          }
          latestTime {
            timestamp
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
            "edge": {
                "rolling": {
                    "list": [
                        {
                            "start": {"timestamp": 1},
                            "end": {"timestamp": 2},
                            "earliestTime": {"timestamp": 1},
                        },
                        {
                            "start": {"timestamp": 2},
                            "end": {"timestamp": 3},
                            "earliestTime": {"timestamp": 2},
                        },
                        {
                            "start": {"timestamp": 3},
                            "end": {"timestamp": 4},
                            "earliestTime": {"timestamp": 3},
                        },
                        {
                            "start": {"timestamp": 4},
                            "end": {"timestamp": 5},
                            "earliestTime": None,
                        },
                        {
                            "start": {"timestamp": 5},
                            "end": {"timestamp": 6},
                            "earliestTime": None,
                        },
                    ],
                    "count": 5,
                    "page": [
                        {
                            "start": {"timestamp": 4},
                            "end": {"timestamp": 5},
                            "earliestTime": None,
                        },
                        {
                            "start": {"timestamp": 5},
                            "end": {"timestamp": 6},
                            "earliestTime": None,
                        },
                    ],
                },
                "after": {
                    "expanding": {
                        "list": [
                            {
                                "start": None,
                                "end": {"timestamp": 3},
                                "earliestTime": {"timestamp": 2},
                                "latestTime": {"timestamp": 2},
                            },
                            {
                                "start": None,
                                "end": {"timestamp": 4},
                                "earliestTime": {"timestamp": 2},
                                "latestTime": {"timestamp": 3},
                            },
                            {
                                "start": None,
                                "end": {"timestamp": 5},
                                "earliestTime": {"timestamp": 2},
                                "latestTime": {"timestamp": 3},
                            },
                            {
                                "start": None,
                                "end": {"timestamp": 6},
                                "earliestTime": {"timestamp": 2},
                                "latestTime": {"timestamp": 3},
                            },
                        ],
                        "count": 4,
                        "page": [
                            {
                                "start": None,
                                "end": {"timestamp": 5},
                                "earliestTime": {"timestamp": 2},
                                "latestTime": {"timestamp": 3},
                            },
                            {
                                "start": None,
                                "end": {"timestamp": 6},
                                "earliestTime": {"timestamp": 2},
                                "latestTime": {"timestamp": 3},
                            },
                        ],
                    }
                },
            }
        }
    }
    run_graphql_test(query, correct, graph)


def test_edges():
    graph = Graph()
    create_graph_epoch(graph)

    query = """
{
  graph(path: "g") {
    edges {
      rolling(window: {epoch: 1}, step: {epoch: 1}) {
        list {
          page(limit: 1, offset: 0) {
            id
            start {
              timestamp
            }
            end {
              timestamp
            }
            earliestTime {
              timestamp
            }
          }
        }
        count
        page(limit: 3, pageIndex: 1) {
          page(limit: 1, offset: 0) {
            id
            start {
              timestamp
            }
            end {
              timestamp
            }
            earliestTime {
              timestamp
            }
          }
        }
      }
      after(time: 1) {
        expanding(step: {epoch: 1}) {
          list {
            page(limit: 1, offset: 0) {
              id
              start {
                timestamp
              }
              end {
                timestamp
              }
              earliestTime {
                timestamp
              }
              latestTime {
                timestamp
              }
            }
          }
          count
          page(limit: 2, pageIndex: 1) {
            page(limit: 1, offset: 0) {
              id
              start {
                timestamp
              }
              end {
                timestamp
              }
              earliestTime {
                timestamp
              }
              latestTime {
                timestamp
              }
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
            "edges": {
                "rolling": {
                    "list": [
                        {
                            "page": [
                                {
                                    "id": ["1", "2"],
                                    "start": {"timestamp": 1},
                                    "end": {"timestamp": 2},
                                    "earliestTime": {"timestamp": 1},
                                }
                            ]
                        },
                        {
                            "page": [
                                {
                                    "id": ["1", "2"],
                                    "start": {"timestamp": 2},
                                    "end": {"timestamp": 3},
                                    "earliestTime": {"timestamp": 2},
                                }
                            ]
                        },
                        {
                            "page": [
                                {
                                    "id": ["1", "2"],
                                    "start": {"timestamp": 3},
                                    "end": {"timestamp": 4},
                                    "earliestTime": {"timestamp": 3},
                                }
                            ]
                        },
                        {
                            "page": [
                                {
                                    "id": ["1", "2"],
                                    "start": {"timestamp": 4},
                                    "end": {"timestamp": 5},
                                    "earliestTime": None,
                                }
                            ]
                        },
                        {
                            "page": [
                                {
                                    "id": ["1", "2"],
                                    "start": {"timestamp": 5},
                                    "end": {"timestamp": 6},
                                    "earliestTime": None,
                                }
                            ]
                        },
                    ],
                    "count": 5,
                    "page": [
                        {
                            "page": [
                                {
                                    "id": ["1", "2"],
                                    "start": {"timestamp": 4},
                                    "end": {"timestamp": 5},
                                    "earliestTime": None,
                                }
                            ]
                        },
                        {
                            "page": [
                                {
                                    "id": ["1", "2"],
                                    "start": {"timestamp": 5},
                                    "end": {"timestamp": 6},
                                    "earliestTime": None,
                                }
                            ]
                        },
                    ],
                },
                "after": {
                    "expanding": {
                        "list": [
                            {
                                "page": [
                                    {
                                        "id": ["1", "2"],
                                        "start": None,
                                        "end": {"timestamp": 3},
                                        "earliestTime": {"timestamp": 2},
                                        "latestTime": {"timestamp": 2},
                                    }
                                ]
                            },
                            {
                                "page": [
                                    {
                                        "id": ["1", "2"],
                                        "start": None,
                                        "end": {"timestamp": 4},
                                        "earliestTime": {"timestamp": 2},
                                        "latestTime": {"timestamp": 3},
                                    }
                                ]
                            },
                            {
                                "page": [
                                    {
                                        "id": ["1", "2"],
                                        "start": None,
                                        "end": {"timestamp": 5},
                                        "earliestTime": {"timestamp": 2},
                                        "latestTime": {"timestamp": 3},
                                    }
                                ]
                            },
                            {
                                "page": [
                                    {
                                        "id": ["1", "2"],
                                        "start": None,
                                        "end": {"timestamp": 6},
                                        "earliestTime": {"timestamp": 2},
                                        "latestTime": {"timestamp": 3},
                                    }
                                ]
                            },
                        ],
                        "count": 4,
                        "page": [
                            {
                                "page": [
                                    {
                                        "id": ["1", "2"],
                                        "start": None,
                                        "end": {"timestamp": 5},
                                        "earliestTime": {"timestamp": 2},
                                        "latestTime": {"timestamp": 3},
                                    }
                                ]
                            },
                            {
                                "page": [
                                    {
                                        "id": ["1", "2"],
                                        "start": None,
                                        "end": {"timestamp": 6},
                                        "earliestTime": {"timestamp": 2},
                                        "latestTime": {"timestamp": 3},
                                    }
                                ]
                            },
                        ],
                    }
                },
            }
        }
    }
    run_graphql_test(query, correct, graph)


def test_zero_step():
    graph = Graph()
    create_graph_epoch(graph)
    queries_and_exceptions = []
    zero_exception = "Failed to parse time string: 0 size step is not supported."
    # graph fail test
    query = """
    {
      graph(path: "g") {
        rolling(window:{duration:"1 day"},step:{duration:"0 day"}){
                list{
            earliestTime {
              timestamp
            }
          }
        }
      }
    }
    """
    queries_and_exceptions.append((query, zero_exception))

    query = """
    {
      graph(path: "g") {
        rolling(window:{epoch: 100},step:{epoch: 0}){
                list{
            earliestTime {
              timestamp
            }
          }
        }
      }
    }
    """
    queries_and_exceptions.append((query, zero_exception))

    query = """
    {
      graph(path: "g") {
        rolling(window:{duration:"0 day"}){
                list{
            earliestTime {
              timestamp
            }
          }
        }
      }
    }
    """
    queries_and_exceptions.append((query, zero_exception))

    query = """
    {
      graph(path: "g") {
        rolling(window:{epoch: 0}){
                list{
            earliestTime {
              timestamp
            }
          }
        }
      }
    }
    """
    queries_and_exceptions.append((query, zero_exception))

    query = """
    {
      graph(path: "g") {
        expanding(step:{duration:"0 day"}){
                list{
            earliestTime {
              timestamp
            }
          }
        }
      }
    }
    """
    queries_and_exceptions.append((query, zero_exception))

    query = """
    {
      graph(path: "g") {
        expanding(step:{epoch: 0}){
                list{
            earliestTime {
              timestamp
            }
          }
        }
      }
    }
    """
    queries_and_exceptions.append((query, zero_exception))

    # node tests
    query = """
        {
      graph(path: "g") {
        node(name: "1") {
            rolling(window:{duration:"1 day"},step:{duration:"0 year"}){
            list {
              earliestTime {
                timestamp
              }
            }
          }
        }
      }
    }
        """
    queries_and_exceptions.append((query, zero_exception))

    query = """
        {
      graph(path: "g") {
        node(name: "1") {
            rolling(window:{epoch: 100},step:{epoch: 0}){
            list {
              earliestTime {
                timestamp
              }
            }
          }
        }
      }
    }
        """
    queries_and_exceptions.append((query, zero_exception))

    query = """
        {
      graph(path: "g") {
        node(name: "1") {
            rolling(window:{duration:"0 day"}){
            list {
              earliestTime {
                timestamp
              }
            }
          }
        }
      }
    }
        """
    queries_and_exceptions.append((query, zero_exception))

    query = """
        {
      graph(path: "g") {
        node(name: "1") {
            rolling(window:{epoch: 0}){
            list {
              earliestTime {
                timestamp
              }
            }
          }
        }
      }
    }
        """
    queries_and_exceptions.append((query, zero_exception))

    query = """
        {
      graph(path: "g") {
        node(name: "1") {
            expanding(step:{duration:"0 day"}){
            list {
              earliestTime {
                timestamp
              }
            }
          }
        }
      }
    }
        """
    queries_and_exceptions.append((query, zero_exception))

    query = """
        {
      graph(path: "g") {
        node(name: "1") {
            expanding(step:{epoch: 0}){
            list {
              earliestTime {
                timestamp
              }
            }
          }
        }
      }
    }
        """
    queries_and_exceptions.append((query, zero_exception))

    # nodes tests
    query = """
    {
  graph(path: "g") {
    nodes {
      list {
        rolling(window:{duration:"1 day"},step:{duration:"0 year"}){
          list {
            earliestTime {
              timestamp
            }
          }
        }
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, zero_exception))

    query = """
{
  graph(path: "g") {
    nodes {
      list {
        rolling(window:{epoch: 100},step:{epoch: 0}){
          list {
            earliestTime {
              timestamp
            }
          }
        }
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, zero_exception))

    query = """
    {
  graph(path: "g") {
    nodes {
      list {
        rolling(window:{duration:"0 day"}){
          list {
            earliestTime {
              timestamp
            }
          }
        }
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, zero_exception))

    query = """
{
  graph(path: "g") {
    nodes {
      list {
        rolling(window:{epoch: 0}){
          list {
            earliestTime {
              timestamp
            }
          }
        }
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, zero_exception))

    query = """
    {
  graph(path: "g") {
    nodes {
      list {
        expanding(step:{duration:"0 day"}){
          list {
            earliestTime {
              timestamp
            }
          }
        }
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, zero_exception))

    query = """
    {
      graph(path: "g") {
        nodes {
          list {
            expanding(step:{epoch: 0}){
              list {
                earliestTime {
                  timestamp
                }
              }
            }
          }
        }
      }
    }
        """
    queries_and_exceptions.append((query, zero_exception))

    # path from nodes tests
    query = """
{
  graph(path: "g") {
    node(name: "1") {
      neighbours {
        rolling(window:{duration:"1 day"},step:{duration:"0 year"}){
          list {
            list {
              earliestTime {
                timestamp
              }
            }
          }
        }
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, zero_exception))

    query = """
{
  graph(path: "g") {
    node(name: "1") {
      neighbours {
        rolling(window:{epoch: 100},step:{epoch: 0}){
          list {
            list {
              earliestTime {
                timestamp
              }
            }
          }
        }
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, zero_exception))

    query = """
{
  graph(path: "g") {
    node(name: "1") {
      neighbours {
        rolling(window:{duration:"0 year"}){
          list {
            list {
              earliestTime {
                timestamp
              }
            }
          }
        }
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, zero_exception))

    query = """
{
  graph(path: "g") {
    node(name: "1") {
      neighbours {
        rolling(window:{epoch: 0}){
          list {
            list {
              earliestTime {
                timestamp
              }
            }
          }
        }
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, zero_exception))

    query = """
{
  graph(path: "g") {
    node(name: "1") {
      neighbours {
        expanding(step:{duration:"0 year"}){
          list {
            list {
              earliestTime {
                timestamp
              }
            }
          }
        }
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, zero_exception))

    query = """
    {
      graph(path: "g") {
        node(name: "1") {
          neighbours {
            expanding(step:{epoch: 0}){
              list {
                list {
                  earliestTime {
                    timestamp
                  }
                }
              }
            }
          }
        }
      }
    }
        """
    queries_and_exceptions.append((query, zero_exception))

    # edge tests
    query = """
{
  graph(path: "g") {
    edge(src: "1", dst: "2") {
    rolling(window:{duration:"1 day"},step:{duration:"0 year"}){
        list {
          earliestTime {
            timestamp
          }
        }
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, zero_exception))

    query = """
{
  graph(path: "g") {
    edge(src: "1", dst: "2") {
        rolling(window:{epoch: 100},step:{epoch: 0}){
            list {
                earliestTime {
                  timestamp
                }
            }
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, zero_exception))

    query = """
{
  graph(path: "g") {
    edge(src: "1", dst: "2") {
    rolling(window:{duration:"0 year"}){
        list {
          earliestTime {
            timestamp
          }
        }
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, zero_exception))

    query = """
{
  graph(path: "g") {
    edge(src: "1", dst: "2") {
        rolling(window:{epoch: 0}){
            list {
                earliestTime {
                  timestamp
                }
            }
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, zero_exception))

    query = """
{
  graph(path: "g") {
    edge(src: "1", dst: "2") {
    expanding(step:{duration:"0 year"}){
        list {
          earliestTime {
            timestamp
          }
        }
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, zero_exception))

    query = """
    {
      graph(path: "g") {
        edge(src: "1", dst: "2") {
            expanding(step:{epoch: 0}){
                list {
                    earliestTime {
                      timestamp
                    }
                }
          }
        }
      }
    }
        """
    queries_and_exceptions.append((query, zero_exception))

    # edges tests
    query = """
{
  graph(path: "g") {
    edges {
    rolling(window:{duration:"1 day"},step:{duration:"0 year"}){
        list {
          list{
            earliestTime {
              timestamp
            }
          }
        }
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, zero_exception))

    query = """
{
  graph(path: "g") {
    edges {
    rolling(window:{epoch: 100},step:{epoch: 0}){
        list {
          list{
            earliestTime {
              timestamp
            }
          }
        }
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, zero_exception))

    query = """
{
  graph(path: "g") {
    edges {
    rolling(window:{duration:"0 year"}){
        list {
          list{
            earliestTime {
              timestamp
            }
          }
        }
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, zero_exception))

    query = """
{
  graph(path: "g") {
    edges {
    rolling(window:{epoch: 0}){
        list {
          list{
            earliestTime {
              timestamp
            }
          }
        }
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, zero_exception))

    query = """
{
  graph(path: "g") {
    edges {
    expanding(step:{duration:"0 year"}){
        list {
          list{
            earliestTime {
              timestamp
            }
          }
        }
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, zero_exception))

    query = """
    {
      graph(path: "g") {
        edges {
        expanding(step:{epoch: 0}){
            list {
              list{
                earliestTime {
                  timestamp
                }
              }
            }
          }
        }
      }
    }
        """
    queries_and_exceptions.append((query, zero_exception))
    run_group_graphql_error_test(queries_and_exceptions, graph)


def test_mismatched_window_step_and_errors():
    graph = Graph()
    create_graph_date(graph)
    queries_and_expected_outputs = []
    queries_and_exceptions = []
    parse_exception = "Failed to parse time string: One of the tokens in the interval string supposed to be a number couldn't be parsed."
    parse_exception2 = "Failed to parse time string: 'monthdas' is not a valid unit. Valid units are year(s), month(s), week(s), day(s), hour(s), minute(s), second(s) and millisecond(s)."
    too_many_exception = "Invalid value for argument \\"
    # go forward 1 hour (end of window), then go back 1 day (start of window) from the earliest event in the graph (2025-01-01 00:00:00)
    query = """
    {
      graph(path: "g") {
        rolling(window: {duration: "1 day"}, step: {epoch: 3600000}) {
          page(limit: 3) {
            start {
              timestamp
            }
            end {
              timestamp
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "rolling": {
                "page": [
                    {
                        "start": {
                            "timestamp": int(
                                datetime(
                                    2024, 12, 31, 1, 0, tzinfo=timezone.utc
                                ).timestamp()
                            )
                            * 1000
                        },
                        "end": {
                            "timestamp": int(
                                datetime(
                                    2025, 1, 1, 1, 0, tzinfo=timezone.utc
                                ).timestamp()
                            )
                            * 1000
                        },
                    },
                    {
                        "start": {
                            "timestamp": int(
                                datetime(
                                    2024, 12, 31, 2, 0, tzinfo=timezone.utc
                                ).timestamp()
                            )
                            * 1000
                        },
                        "end": {
                            "timestamp": int(
                                datetime(
                                    2025, 1, 1, 2, 0, tzinfo=timezone.utc
                                ).timestamp()
                            )
                            * 1000
                        },
                    },
                    {
                        "start": {
                            "timestamp": int(
                                datetime(
                                    2024, 12, 31, 3, 0, tzinfo=timezone.utc
                                ).timestamp()
                            )
                            * 1000
                        },
                        "end": {
                            "timestamp": int(
                                datetime(
                                    2025, 1, 1, 3, 0, tzinfo=timezone.utc
                                ).timestamp()
                            )
                            * 1000
                        },
                    },
                ]
            }
        }
    }
    queries_and_expected_outputs.append((query, expected_output))

    # test the last windows for each query to test boundaries
    # 97 elements, so offset = 95 yields the last 2 elements. If they weren't the last windows, limit = 3 would yield 3 windows instead of 2
    query = """
    {
      graph(path: "g") {
        rolling(window: {duration: "1 day"}, step: {epoch: 3600000}) {
          page(limit: 3, offset: 95) { 
            start {
              timestamp
            }
            end {
              timestamp
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "rolling": {
                "page": [
                    {
                        "start": {
                            "timestamp": int(
                                datetime(
                                    2025, 1, 4, 0, 0, tzinfo=timezone.utc
                                ).timestamp()
                            )
                            * 1000
                        },
                        "end": {
                            "timestamp": int(
                                datetime(
                                    2025, 1, 5, 0, 0, tzinfo=timezone.utc
                                ).timestamp()
                            )
                            * 1000
                        },
                    },
                    {
                        "start": {
                            "timestamp": int(
                                datetime(
                                    2025, 1, 4, 1, 0, tzinfo=timezone.utc
                                ).timestamp()
                            )
                            * 1000
                        },
                        "end": {
                            "timestamp": int(
                                datetime(
                                    2025, 1, 5, 1, 0, tzinfo=timezone.utc
                                ).timestamp()
                            )
                            * 1000
                        },
                    },
                ]
            }
        }
    }
    queries_and_expected_outputs.append((query, expected_output))

    # go forward 1 day (end of window), then go back 1 hour (start of window)
    query = """
    {
      graph(path: "g") {
        rolling(window: {epoch: 3600000}, step: {duration: "1 day"}) {
          list {
            start {
              timestamp
            }
            end {
              timestamp
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "rolling": {
                "list": [
                    {
                        "start": {
                            "timestamp": int(
                                datetime(
                                    2025, 1, 1, 23, 0, tzinfo=timezone.utc
                                ).timestamp()
                            )
                            * 1000
                        },
                        "end": {
                            "timestamp": int(
                                datetime(
                                    2025, 1, 2, 0, 0, tzinfo=timezone.utc
                                ).timestamp()
                            )
                            * 1000
                        },
                    },
                    {
                        "start": {
                            "timestamp": int(
                                datetime(
                                    2025, 1, 2, 23, 0, tzinfo=timezone.utc
                                ).timestamp()
                            )
                            * 1000
                        },
                        "end": {
                            "timestamp": int(
                                datetime(
                                    2025, 1, 3, 0, 0, tzinfo=timezone.utc
                                ).timestamp()
                            )
                            * 1000
                        },
                    },
                    {
                        "start": {
                            "timestamp": int(
                                datetime(
                                    2025, 1, 3, 23, 0, tzinfo=timezone.utc
                                ).timestamp()
                            )
                            * 1000
                        },
                        "end": {
                            "timestamp": int(
                                datetime(
                                    2025, 1, 4, 0, 0, tzinfo=timezone.utc
                                ).timestamp()
                            )
                            * 1000
                        },
                    },
                    {
                        "start": {
                            "timestamp": int(
                                datetime(
                                    2025, 1, 4, 23, 0, tzinfo=timezone.utc
                                ).timestamp()
                            )
                            * 1000
                        },
                        "end": {
                            "timestamp": int(
                                datetime(
                                    2025, 1, 5, 0, 0, tzinfo=timezone.utc
                                ).timestamp()
                            )
                            * 1000
                        },
                    },
                ]
            }
        }
    }
    queries_and_expected_outputs.append((query, expected_output))

    query = """
    {
      graph(path: "g") {
        rolling(window: {duration: "1dasdas day"}) {
          list {
            earliestTime {
              timestamp
            }
          }
        }
      }
    }
    """
    queries_and_exceptions.append((query, parse_exception))

    query = """
    {
      graph(path: "g") {
        rolling(window: {duration: "1 day"}, step: {duration: "1 monthdas"}) {
          list {
            earliestTime {
              timestamp
            }
          }
        }
      }
    }
    """
    queries_and_exceptions.append((query, parse_exception2))

    query = """
    {
      graph(path: "g") {
        rolling(window: {duration: "1 day", epoch: 11}) {
          list {
            earliestTime {
              timestamp
            }
          }
        }
      }
    }
    """
    queries_and_exceptions.append((query, too_many_exception))

    # node tests
    query = """
    {
      graph(path: "g") {
        node(name: "1") {
          rolling(window: {duration: "1 day"}, step: {epoch: 11}) {
            page(limit: 3) {
              start {
                timestamp
              }
              end {
                timestamp
              }
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "node": {
                "rolling": {
                    "page": [
                        {
                            "start": {
                                "timestamp": int(
                                    datetime(
                                        2024,
                                        12,
                                        31,
                                        0,
                                        0,
                                        0,
                                        11_000,
                                        tzinfo=timezone.utc,
                                    ).timestamp()
                                    * 1000
                                )
                            },
                            "end": {
                                "timestamp": int(
                                    datetime(
                                        2025, 1, 1, 0, 0, 0, 11_000, tzinfo=timezone.utc
                                    ).timestamp()
                                    * 1000
                                )
                            },
                        },
                        {
                            "start": {
                                "timestamp": int(
                                    datetime(
                                        2024,
                                        12,
                                        31,
                                        0,
                                        0,
                                        0,
                                        22_000,
                                        tzinfo=timezone.utc,
                                    ).timestamp()
                                    * 1000
                                )
                            },
                            "end": {
                                "timestamp": int(
                                    datetime(
                                        2025, 1, 1, 0, 0, 0, 22_000, tzinfo=timezone.utc
                                    ).timestamp()
                                    * 1000
                                )
                            },
                        },
                        {
                            "start": {
                                "timestamp": int(
                                    datetime(
                                        2024,
                                        12,
                                        31,
                                        0,
                                        0,
                                        0,
                                        33_000,
                                        tzinfo=timezone.utc,
                                    ).timestamp()
                                    * 1000
                                )
                            },
                            "end": {
                                "timestamp": int(
                                    datetime(
                                        2025, 1, 1, 0, 0, 0, 33_000, tzinfo=timezone.utc
                                    ).timestamp()
                                    * 1000
                                )
                            },
                        },
                    ]
                }
            }
        }
    }
    queries_and_expected_outputs.append((query, expected_output))

    # last window, 31418182 items
    query = """
    {
      graph(path: "g") {
        node(name: "1") {
          rolling(window: {duration: "1 day"}, step: {epoch: 11}) {
            page(limit: 3, offset: 31418180) {
              start {
                timestamp
              }
              end {
                timestamp
              }
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "node": {
                "rolling": {
                    "page": [
                        {
                            "start": {
                                "timestamp": int(
                                    datetime(
                                        2025,
                                        1,
                                        3,
                                        23,
                                        59,
                                        59,
                                        991_000,
                                        tzinfo=timezone.utc,
                                    ).timestamp()
                                    * 1000
                                )
                            },
                            "end": {
                                "timestamp": int(
                                    datetime(
                                        2025,
                                        1,
                                        4,
                                        23,
                                        59,
                                        59,
                                        991_000,
                                        tzinfo=timezone.utc,
                                    ).timestamp()
                                    * 1000
                                )
                            },
                        },
                        {
                            "start": {
                                "timestamp": int(
                                    datetime(
                                        2025, 1, 4, 0, 0, 0, 2_000, tzinfo=timezone.utc
                                    ).timestamp()
                                    * 1000
                                )
                            },
                            "end": {
                                "timestamp": int(
                                    datetime(
                                        2025, 1, 5, 0, 0, 0, 2_000, tzinfo=timezone.utc
                                    ).timestamp()
                                    * 1000
                                )
                            },
                        },
                    ]
                }
            }
        }
    }
    queries_and_expected_outputs.append((query, expected_output))

    query = """
    {
      graph(path: "g") {
        node(name: "1") {
          rolling(window: {epoch: 3600000}, step: {duration: "1 day"}) {
            list {
              start {
                timestamp
              }
              end {
                timestamp
              }
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "node": {
                "rolling": {
                    "list": [
                        {
                            "start": {
                                "timestamp": int(
                                    datetime(
                                        2025, 1, 1, 23, 0, tzinfo=timezone.utc
                                    ).timestamp()
                                    * 1000
                                )
                            },
                            "end": {
                                "timestamp": int(
                                    datetime(
                                        2025, 1, 2, 0, 0, tzinfo=timezone.utc
                                    ).timestamp()
                                    * 1000
                                )
                            },
                        },
                        {
                            "start": {
                                "timestamp": int(
                                    datetime(
                                        2025, 1, 2, 23, 0, tzinfo=timezone.utc
                                    ).timestamp()
                                    * 1000
                                )
                            },
                            "end": {
                                "timestamp": int(
                                    datetime(
                                        2025, 1, 3, 0, 0, tzinfo=timezone.utc
                                    ).timestamp()
                                    * 1000
                                )
                            },
                        },
                        {
                            "start": {
                                "timestamp": int(
                                    datetime(
                                        2025, 1, 3, 23, 0, tzinfo=timezone.utc
                                    ).timestamp()
                                    * 1000
                                )
                            },
                            "end": {
                                "timestamp": int(
                                    datetime(
                                        2025, 1, 4, 0, 0, tzinfo=timezone.utc
                                    ).timestamp()
                                    * 1000
                                )
                            },
                        },
                        {
                            "start": {
                                "timestamp": int(
                                    datetime(
                                        2025, 1, 4, 23, 0, tzinfo=timezone.utc
                                    ).timestamp()
                                    * 1000
                                )
                            },
                            "end": {
                                "timestamp": int(
                                    datetime(
                                        2025, 1, 5, 0, 0, tzinfo=timezone.utc
                                    ).timestamp()
                                    * 1000
                                )
                            },
                        },
                    ]
                }
            }
        }
    }
    queries_and_expected_outputs.append((query, expected_output))

    query = """
    {
      graph(path: "g") {
        nodes {
          page(limit: 2) {
            rolling(window: {duration: "1 day"}, step: {epoch: 60000}) {
              page(limit: 2) {
                start {
                  timestamp
                }
                end {
                  timestamp
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
                "page": [
                    {
                        "rolling": {
                            "page": [
                                {
                                    "start": {
                                        "timestamp": int(
                                            datetime(
                                                2024,
                                                12,
                                                31,
                                                0,
                                                1,
                                                0,
                                                tzinfo=timezone.utc,
                                            ).timestamp()
                                            * 1000
                                        )
                                    },
                                    "end": {
                                        "timestamp": int(
                                            datetime(
                                                2025,
                                                1,
                                                1,
                                                0,
                                                1,
                                                0,
                                                tzinfo=timezone.utc,
                                            ).timestamp()
                                            * 1000
                                        )
                                    },
                                },
                                {
                                    "start": {
                                        "timestamp": int(
                                            datetime(
                                                2024,
                                                12,
                                                31,
                                                0,
                                                2,
                                                0,
                                                tzinfo=timezone.utc,
                                            ).timestamp()
                                            * 1000
                                        )
                                    },
                                    "end": {
                                        "timestamp": int(
                                            datetime(
                                                2025,
                                                1,
                                                1,
                                                0,
                                                2,
                                                0,
                                                tzinfo=timezone.utc,
                                            ).timestamp()
                                            * 1000
                                        )
                                    },
                                },
                            ]
                        }
                    },
                    {
                        "rolling": {
                            "page": [
                                {
                                    "start": {
                                        "timestamp": int(
                                            datetime(
                                                2024,
                                                12,
                                                31,
                                                0,
                                                1,
                                                0,
                                                tzinfo=timezone.utc,
                                            ).timestamp()
                                            * 1000
                                        )
                                    },
                                    "end": {
                                        "timestamp": int(
                                            datetime(
                                                2025,
                                                1,
                                                1,
                                                0,
                                                1,
                                                0,
                                                tzinfo=timezone.utc,
                                            ).timestamp()
                                            * 1000
                                        )
                                    },
                                },
                                {
                                    "start": {
                                        "timestamp": int(
                                            datetime(
                                                2024,
                                                12,
                                                31,
                                                0,
                                                2,
                                                0,
                                                tzinfo=timezone.utc,
                                            ).timestamp()
                                            * 1000
                                        )
                                    },
                                    "end": {
                                        "timestamp": int(
                                            datetime(
                                                2025,
                                                1,
                                                1,
                                                0,
                                                2,
                                                0,
                                                tzinfo=timezone.utc,
                                            ).timestamp()
                                            * 1000
                                        )
                                    },
                                },
                            ]
                        }
                    },
                ]
            }
        }
    }
    queries_and_expected_outputs.append((query, expected_output))

    # last window
    query = """
    {
      graph(path: "g") {
        nodes {
          page(limit: 2) {
            rolling(window: {duration: "1 day"}, step: {epoch: 60000}) {
              page(limit: 2, offset: 5760) {
                start {
                  timestamp
                }
                end {
                  timestamp
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
                "page": [
                    {
                        "rolling": {
                            "page": [
                                {
                                    "start": {
                                        "timestamp": int(
                                            datetime(
                                                2025, 1, 4, 0, 1, 0, tzinfo=timezone.utc
                                            ).timestamp()
                                            * 1000
                                        )
                                    },
                                    "end": {
                                        "timestamp": int(
                                            datetime(
                                                2025, 1, 5, 0, 1, 0, tzinfo=timezone.utc
                                            ).timestamp()
                                            * 1000
                                        )
                                    },
                                }
                            ]
                        }
                    },
                    {
                        "rolling": {
                            "page": [
                                {
                                    "start": {
                                        "timestamp": int(
                                            datetime(
                                                2025, 1, 4, 0, 1, 0, tzinfo=timezone.utc
                                            ).timestamp()
                                            * 1000
                                        )
                                    },
                                    "end": {
                                        "timestamp": int(
                                            datetime(
                                                2025, 1, 5, 0, 1, 0, tzinfo=timezone.utc
                                            ).timestamp()
                                            * 1000
                                        )
                                    },
                                }
                            ]
                        }
                    },
                ]
            }
        }
    }
    queries_and_expected_outputs.append((query, expected_output))

    query = """
    {
      graph(path: "g") {
        nodes {
          page(limit: 2) {
            rolling(window: {epoch: 3600000}, step: {duration: "1 day"}) {
              list {
                start {
                  timestamp
                }
                end {
                  timestamp
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
                "page": [
                    {
                        "rolling": {
                            "list": [
                                {
                                    "start": {
                                        "timestamp": int(
                                            datetime(
                                                2025, 1, 1, 23, 0, tzinfo=timezone.utc
                                            ).timestamp()
                                            * 1000
                                        )
                                    },
                                    "end": {
                                        "timestamp": int(
                                            datetime(
                                                2025, 1, 2, 0, 0, tzinfo=timezone.utc
                                            ).timestamp()
                                            * 1000
                                        )
                                    },
                                },
                                {
                                    "start": {
                                        "timestamp": int(
                                            datetime(
                                                2025, 1, 2, 23, 0, tzinfo=timezone.utc
                                            ).timestamp()
                                            * 1000
                                        )
                                    },
                                    "end": {
                                        "timestamp": int(
                                            datetime(
                                                2025, 1, 3, 0, 0, tzinfo=timezone.utc
                                            ).timestamp()
                                            * 1000
                                        )
                                    },
                                },
                                {
                                    "start": {
                                        "timestamp": int(
                                            datetime(
                                                2025, 1, 3, 23, 0, tzinfo=timezone.utc
                                            ).timestamp()
                                            * 1000
                                        )
                                    },
                                    "end": {
                                        "timestamp": int(
                                            datetime(
                                                2025, 1, 4, 0, 0, tzinfo=timezone.utc
                                            ).timestamp()
                                            * 1000
                                        )
                                    },
                                },
                                {
                                    "start": {
                                        "timestamp": int(
                                            datetime(
                                                2025, 1, 4, 23, 0, tzinfo=timezone.utc
                                            ).timestamp()
                                            * 1000
                                        )
                                    },
                                    "end": {
                                        "timestamp": int(
                                            datetime(
                                                2025, 1, 5, 0, 0, tzinfo=timezone.utc
                                            ).timestamp()
                                            * 1000
                                        )
                                    },
                                },
                            ]
                        }
                    },
                    {
                        "rolling": {
                            "list": [
                                {
                                    "start": {
                                        "timestamp": int(
                                            datetime(
                                                2025, 1, 1, 23, 0, tzinfo=timezone.utc
                                            ).timestamp()
                                            * 1000
                                        )
                                    },
                                    "end": {
                                        "timestamp": int(
                                            datetime(
                                                2025, 1, 2, 0, 0, tzinfo=timezone.utc
                                            ).timestamp()
                                            * 1000
                                        )
                                    },
                                },
                                {
                                    "start": {
                                        "timestamp": int(
                                            datetime(
                                                2025, 1, 2, 23, 0, tzinfo=timezone.utc
                                            ).timestamp()
                                            * 1000
                                        )
                                    },
                                    "end": {
                                        "timestamp": int(
                                            datetime(
                                                2025, 1, 3, 0, 0, tzinfo=timezone.utc
                                            ).timestamp()
                                            * 1000
                                        )
                                    },
                                },
                                {
                                    "start": {
                                        "timestamp": int(
                                            datetime(
                                                2025, 1, 3, 23, 0, tzinfo=timezone.utc
                                            ).timestamp()
                                            * 1000
                                        )
                                    },
                                    "end": {
                                        "timestamp": int(
                                            datetime(
                                                2025, 1, 4, 0, 0, tzinfo=timezone.utc
                                            ).timestamp()
                                            * 1000
                                        )
                                    },
                                },
                                {
                                    "start": {
                                        "timestamp": int(
                                            datetime(
                                                2025, 1, 4, 23, 0, tzinfo=timezone.utc
                                            ).timestamp()
                                            * 1000
                                        )
                                    },
                                    "end": {
                                        "timestamp": int(
                                            datetime(
                                                2025, 1, 5, 0, 0, tzinfo=timezone.utc
                                            ).timestamp()
                                            * 1000
                                        )
                                    },
                                },
                            ]
                        }
                    },
                ]
            }
        }
    }
    queries_and_expected_outputs.append((query, expected_output))

    query = """
    {
      graph(path: "g") {
        node(name: "1") {
          neighbours {
            rolling(window: {duration: "1 day"}, step: {epoch: 60000}) {
              page(limit: 2) {
                list {
                  start {
                    timestamp
                  }
                  end {
                    timestamp
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
            "node": {
                "neighbours": {
                    "rolling": {
                        "page": [
                            {
                                "list": [
                                    {
                                        "start": {
                                            "timestamp": int(
                                                datetime(
                                                    2024,
                                                    12,
                                                    31,
                                                    0,
                                                    1,
                                                    tzinfo=timezone.utc,
                                                ).timestamp()
                                                * 1000
                                            )
                                        },
                                        "end": {
                                            "timestamp": int(
                                                datetime(
                                                    2025,
                                                    1,
                                                    1,
                                                    0,
                                                    1,
                                                    tzinfo=timezone.utc,
                                                ).timestamp()
                                                * 1000
                                            )
                                        },
                                    },
                                    {
                                        "start": {
                                            "timestamp": int(
                                                datetime(
                                                    2024,
                                                    12,
                                                    31,
                                                    0,
                                                    1,
                                                    tzinfo=timezone.utc,
                                                ).timestamp()
                                                * 1000
                                            )
                                        },
                                        "end": {
                                            "timestamp": int(
                                                datetime(
                                                    2025,
                                                    1,
                                                    1,
                                                    0,
                                                    1,
                                                    tzinfo=timezone.utc,
                                                ).timestamp()
                                                * 1000
                                            )
                                        },
                                    },
                                ]
                            },
                            {
                                "list": [
                                    {
                                        "start": {
                                            "timestamp": int(
                                                datetime(
                                                    2024,
                                                    12,
                                                    31,
                                                    0,
                                                    2,
                                                    tzinfo=timezone.utc,
                                                ).timestamp()
                                                * 1000
                                            )
                                        },
                                        "end": {
                                            "timestamp": int(
                                                datetime(
                                                    2025,
                                                    1,
                                                    1,
                                                    0,
                                                    2,
                                                    tzinfo=timezone.utc,
                                                ).timestamp()
                                                * 1000
                                            )
                                        },
                                    },
                                    {
                                        "start": {
                                            "timestamp": int(
                                                datetime(
                                                    2024,
                                                    12,
                                                    31,
                                                    0,
                                                    2,
                                                    tzinfo=timezone.utc,
                                                ).timestamp()
                                                * 1000
                                            )
                                        },
                                        "end": {
                                            "timestamp": int(
                                                datetime(
                                                    2025,
                                                    1,
                                                    1,
                                                    0,
                                                    2,
                                                    tzinfo=timezone.utc,
                                                ).timestamp()
                                                * 1000
                                            )
                                        },
                                    },
                                ]
                            },
                        ]
                    }
                }
            }
        }
    }
    queries_and_expected_outputs.append((query, expected_output))

    query = """
    {
      graph(path: "g") {
        node(name: "1") {
          neighbours {
            rolling(window: {epoch: 3600000}, step: {duration: "1 day"}) {
              page(limit: 2) {
                list {
                  start {
                    timestamp
                  }
                  end {
                    timestamp
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
            "node": {
                "neighbours": {
                    "rolling": {
                        "page": [
                            {
                                "list": [
                                    {
                                        "start": {
                                            "timestamp": int(
                                                datetime(
                                                    2025,
                                                    1,
                                                    1,
                                                    23,
                                                    0,
                                                    tzinfo=timezone.utc,
                                                ).timestamp()
                                                * 1000
                                            )
                                        },
                                        "end": {
                                            "timestamp": int(
                                                datetime(
                                                    2025,
                                                    1,
                                                    2,
                                                    0,
                                                    0,
                                                    tzinfo=timezone.utc,
                                                ).timestamp()
                                                * 1000
                                            )
                                        },
                                    },
                                    {
                                        "start": {
                                            "timestamp": int(
                                                datetime(
                                                    2025,
                                                    1,
                                                    1,
                                                    23,
                                                    0,
                                                    tzinfo=timezone.utc,
                                                ).timestamp()
                                                * 1000
                                            )
                                        },
                                        "end": {
                                            "timestamp": int(
                                                datetime(
                                                    2025,
                                                    1,
                                                    2,
                                                    0,
                                                    0,
                                                    tzinfo=timezone.utc,
                                                ).timestamp()
                                                * 1000
                                            )
                                        },
                                    },
                                ]
                            },
                            {
                                "list": [
                                    {
                                        "start": {
                                            "timestamp": int(
                                                datetime(
                                                    2025,
                                                    1,
                                                    2,
                                                    23,
                                                    0,
                                                    tzinfo=timezone.utc,
                                                ).timestamp()
                                                * 1000
                                            )
                                        },
                                        "end": {
                                            "timestamp": int(
                                                datetime(
                                                    2025,
                                                    1,
                                                    3,
                                                    0,
                                                    0,
                                                    tzinfo=timezone.utc,
                                                ).timestamp()
                                                * 1000
                                            )
                                        },
                                    },
                                    {
                                        "start": {
                                            "timestamp": int(
                                                datetime(
                                                    2025,
                                                    1,
                                                    2,
                                                    23,
                                                    0,
                                                    tzinfo=timezone.utc,
                                                ).timestamp()
                                                * 1000
                                            )
                                        },
                                        "end": {
                                            "timestamp": int(
                                                datetime(
                                                    2025,
                                                    1,
                                                    3,
                                                    0,
                                                    0,
                                                    tzinfo=timezone.utc,
                                                ).timestamp()
                                                * 1000
                                            )
                                        },
                                    },
                                ]
                            },
                        ]
                    }
                }
            }
        }
    }
    queries_and_expected_outputs.append((query, expected_output))

    query = """
    {
      graph(path: "g") {
        edge(src: "1", dst: "2") {
          rolling(window: {duration: "1 day"}, step: {epoch: 60000}) {
            page(limit: 3) {
              start {
                timestamp
              }
              end {
                timestamp
              }
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "edge": {
                "rolling": {
                    "page": [
                        {
                            "start": {
                                "timestamp": int(
                                    datetime(
                                        2024, 12, 31, 0, 1, tzinfo=timezone.utc
                                    ).timestamp()
                                    * 1000
                                )
                            },
                            "end": {
                                "timestamp": int(
                                    datetime(
                                        2025, 1, 1, 0, 1, tzinfo=timezone.utc
                                    ).timestamp()
                                    * 1000
                                )
                            },
                        },
                        {
                            "start": {
                                "timestamp": int(
                                    datetime(
                                        2024, 12, 31, 0, 2, tzinfo=timezone.utc
                                    ).timestamp()
                                    * 1000
                                )
                            },
                            "end": {
                                "timestamp": int(
                                    datetime(
                                        2025, 1, 1, 0, 2, tzinfo=timezone.utc
                                    ).timestamp()
                                    * 1000
                                )
                            },
                        },
                        {
                            "start": {
                                "timestamp": int(
                                    datetime(
                                        2024, 12, 31, 0, 3, tzinfo=timezone.utc
                                    ).timestamp()
                                    * 1000
                                )
                            },
                            "end": {
                                "timestamp": int(
                                    datetime(
                                        2025, 1, 1, 0, 3, tzinfo=timezone.utc
                                    ).timestamp()
                                    * 1000
                                )
                            },
                        },
                    ]
                }
            }
        }
    }
    queries_and_expected_outputs.append((query, expected_output))

    # last window
    query = """
    {
      graph(path: "g") {
        edge(src: "1", dst: "2") {
          rolling(window: {duration: "1 day"}, step: {epoch: 60000}) {
            page(limit: 2, offset: 5760) {
              start {
                timestamp
              }
              end {
                timestamp
              }
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "edge": {
                "rolling": {
                    "page": [
                        {
                            "start": {
                                "timestamp": int(
                                    datetime(
                                        2025, 1, 4, 0, 1, tzinfo=timezone.utc
                                    ).timestamp()
                                    * 1000
                                )
                            },
                            "end": {
                                "timestamp": int(
                                    datetime(
                                        2025, 1, 5, 0, 1, tzinfo=timezone.utc
                                    ).timestamp()
                                    * 1000
                                )
                            },
                        }
                    ]
                }
            }
        }
    }
    queries_and_expected_outputs.append((query, expected_output))

    query = """
    {
      graph(path: "g") {
        edge(src: "1", dst: "2") {
          rolling(window: {epoch: 3600000}, step: {duration: "1 day"}) {
            list {
              start {
                timestamp
              }
              end {
                timestamp
              }
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "edge": {
                "rolling": {
                    "list": [
                        {
                            "start": {
                                "timestamp": int(
                                    datetime(
                                        2025, 1, 1, 23, 0, tzinfo=timezone.utc
                                    ).timestamp()
                                    * 1000
                                )
                            },
                            "end": {
                                "timestamp": int(
                                    datetime(
                                        2025, 1, 2, 0, 0, tzinfo=timezone.utc
                                    ).timestamp()
                                    * 1000
                                )
                            },
                        },
                        {
                            "start": {
                                "timestamp": int(
                                    datetime(
                                        2025, 1, 2, 23, 0, tzinfo=timezone.utc
                                    ).timestamp()
                                    * 1000
                                )
                            },
                            "end": {
                                "timestamp": int(
                                    datetime(
                                        2025, 1, 3, 0, 0, tzinfo=timezone.utc
                                    ).timestamp()
                                    * 1000
                                )
                            },
                        },
                        {
                            "start": {
                                "timestamp": int(
                                    datetime(
                                        2025, 1, 3, 23, 0, tzinfo=timezone.utc
                                    ).timestamp()
                                    * 1000
                                )
                            },
                            "end": {
                                "timestamp": int(
                                    datetime(
                                        2025, 1, 4, 0, 0, tzinfo=timezone.utc
                                    ).timestamp()
                                    * 1000
                                )
                            },
                        },
                        {
                            "start": {
                                "timestamp": int(
                                    datetime(
                                        2025, 1, 4, 23, 0, tzinfo=timezone.utc
                                    ).timestamp()
                                    * 1000
                                )
                            },
                            "end": {
                                "timestamp": int(
                                    datetime(
                                        2025, 1, 5, 0, 0, tzinfo=timezone.utc
                                    ).timestamp()
                                    * 1000
                                )
                            },
                        },
                    ]
                }
            }
        }
    }
    queries_and_expected_outputs.append((query, expected_output))

    query = """
    {
      graph(path: "g") {
        edges {
          rolling(window: {duration: "1 day"}, step: {epoch: 60000}) {
            page(limit: 2) {
              list {
                start {
                  timestamp
                }
                end {
                  timestamp
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
            "edges": {
                "rolling": {
                    "page": [
                        {
                            "list": [
                                {
                                    "start": {
                                        "timestamp": int(
                                            datetime(
                                                2024, 12, 31, 0, 1, tzinfo=timezone.utc
                                            ).timestamp()
                                            * 1000
                                        )
                                    },
                                    "end": {
                                        "timestamp": int(
                                            datetime(
                                                2025, 1, 1, 0, 1, tzinfo=timezone.utc
                                            ).timestamp()
                                            * 1000
                                        )
                                    },
                                },
                                {
                                    "start": {
                                        "timestamp": int(
                                            datetime(
                                                2024, 12, 31, 0, 1, tzinfo=timezone.utc
                                            ).timestamp()
                                            * 1000
                                        )
                                    },
                                    "end": {
                                        "timestamp": int(
                                            datetime(
                                                2025, 1, 1, 0, 1, tzinfo=timezone.utc
                                            ).timestamp()
                                            * 1000
                                        )
                                    },
                                },
                                {
                                    "start": {
                                        "timestamp": int(
                                            datetime(
                                                2024, 12, 31, 0, 1, tzinfo=timezone.utc
                                            ).timestamp()
                                            * 1000
                                        )
                                    },
                                    "end": {
                                        "timestamp": int(
                                            datetime(
                                                2025, 1, 1, 0, 1, tzinfo=timezone.utc
                                            ).timestamp()
                                            * 1000
                                        )
                                    },
                                },
                            ]
                        },
                        {
                            "list": [
                                {
                                    "start": {
                                        "timestamp": int(
                                            datetime(
                                                2024, 12, 31, 0, 2, tzinfo=timezone.utc
                                            ).timestamp()
                                            * 1000
                                        )
                                    },
                                    "end": {
                                        "timestamp": int(
                                            datetime(
                                                2025, 1, 1, 0, 2, tzinfo=timezone.utc
                                            ).timestamp()
                                            * 1000
                                        )
                                    },
                                },
                                {
                                    "start": {
                                        "timestamp": int(
                                            datetime(
                                                2024, 12, 31, 0, 2, tzinfo=timezone.utc
                                            ).timestamp()
                                            * 1000
                                        )
                                    },
                                    "end": {
                                        "timestamp": int(
                                            datetime(
                                                2025, 1, 1, 0, 2, tzinfo=timezone.utc
                                            ).timestamp()
                                            * 1000
                                        )
                                    },
                                },
                                {
                                    "start": {
                                        "timestamp": int(
                                            datetime(
                                                2024, 12, 31, 0, 2, tzinfo=timezone.utc
                                            ).timestamp()
                                            * 1000
                                        )
                                    },
                                    "end": {
                                        "timestamp": int(
                                            datetime(
                                                2025, 1, 1, 0, 2, tzinfo=timezone.utc
                                            ).timestamp()
                                            * 1000
                                        )
                                    },
                                },
                            ]
                        },
                    ]
                }
            }
        }
    }
    queries_and_expected_outputs.append((query, expected_output))

    query = """
    {
      graph(path: "g") {
        edges {
          rolling(window: {epoch: 3600000}, step: {duration: "1 day"}) {
            page(limit: 2) {
              list {
                start {
                  timestamp
                }
                end {
                  timestamp
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
            "edges": {
                "rolling": {
                    "page": [
                        {
                            "list": [
                                {
                                    "start": {
                                        "timestamp": int(
                                            datetime(
                                                2025, 1, 1, 23, 0, tzinfo=timezone.utc
                                            ).timestamp()
                                            * 1000
                                        )
                                    },
                                    "end": {
                                        "timestamp": int(
                                            datetime(
                                                2025, 1, 2, 0, 0, tzinfo=timezone.utc
                                            ).timestamp()
                                            * 1000
                                        )
                                    },
                                },
                                {
                                    "start": {
                                        "timestamp": int(
                                            datetime(
                                                2025, 1, 1, 23, 0, tzinfo=timezone.utc
                                            ).timestamp()
                                            * 1000
                                        )
                                    },
                                    "end": {
                                        "timestamp": int(
                                            datetime(
                                                2025, 1, 2, 0, 0, tzinfo=timezone.utc
                                            ).timestamp()
                                            * 1000
                                        )
                                    },
                                },
                                {
                                    "start": {
                                        "timestamp": int(
                                            datetime(
                                                2025, 1, 1, 23, 0, tzinfo=timezone.utc
                                            ).timestamp()
                                            * 1000
                                        )
                                    },
                                    "end": {
                                        "timestamp": int(
                                            datetime(
                                                2025, 1, 2, 0, 0, tzinfo=timezone.utc
                                            ).timestamp()
                                            * 1000
                                        )
                                    },
                                },
                            ]
                        },
                        {
                            "list": [
                                {
                                    "start": {
                                        "timestamp": int(
                                            datetime(
                                                2025, 1, 2, 23, 0, tzinfo=timezone.utc
                                            ).timestamp()
                                            * 1000
                                        )
                                    },
                                    "end": {
                                        "timestamp": int(
                                            datetime(
                                                2025, 1, 3, 0, 0, tzinfo=timezone.utc
                                            ).timestamp()
                                            * 1000
                                        )
                                    },
                                },
                                {
                                    "start": {
                                        "timestamp": int(
                                            datetime(
                                                2025, 1, 2, 23, 0, tzinfo=timezone.utc
                                            ).timestamp()
                                            * 1000
                                        )
                                    },
                                    "end": {
                                        "timestamp": int(
                                            datetime(
                                                2025, 1, 3, 0, 0, tzinfo=timezone.utc
                                            ).timestamp()
                                            * 1000
                                        )
                                    },
                                },
                                {
                                    "start": {
                                        "timestamp": int(
                                            datetime(
                                                2025, 1, 2, 23, 0, tzinfo=timezone.utc
                                            ).timestamp()
                                            * 1000
                                        )
                                    },
                                    "end": {
                                        "timestamp": int(
                                            datetime(
                                                2025, 1, 3, 0, 0, tzinfo=timezone.utc
                                            ).timestamp()
                                            * 1000
                                        )
                                    },
                                },
                            ]
                        },
                    ]
                }
            }
        }
    }
    queries_and_expected_outputs.append((query, expected_output))

    run_group_graphql_test(queries_and_expected_outputs, graph)
    run_group_graphql_error_test(queries_and_exceptions, graph)


def test_alignment():
    g = Graph()
    dt1 = datetime(2025, 3, 15, 14, 37, 52)  # March 15
    dt2 = datetime(2025, 7, 8, 9, 12, 5)  # July 8
    dt3 = datetime(2025, 11, 22, 21, 45, 30)  # November 22
    dt_edge = datetime(2025, 8, 30, 5, 30, 15)  # August 30

    g.add_node(dt1, 1)
    g.add_node(dt2, 1)
    g.add_node(dt3, 1)
    g.add_edge(dt_edge, 1, 2)

    query = """
    {
      graph(path: "g") {
        rolling(window: {duration: "1 month"}) {
          page(limit: 3) {
            start {
              timestamp
            }
            end {
              timestamp
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "rolling": {
                "page": [
                    {
                        "start": {
                            "timestamp": int(
                                datetime(
                                    2025, 3, 1, 0, 0, 0, tzinfo=timezone.utc
                                ).timestamp()
                                * 1000
                            )
                        },
                        "end": {
                            "timestamp": int(
                                datetime(
                                    2025, 4, 1, 0, 0, 0, tzinfo=timezone.utc
                                ).timestamp()
                                * 1000
                            )
                        },
                    },
                    {
                        "start": {
                            "timestamp": int(
                                datetime(
                                    2025, 4, 1, 0, 0, 0, tzinfo=timezone.utc
                                ).timestamp()
                                * 1000
                            )
                        },
                        "end": {
                            "timestamp": int(
                                datetime(
                                    2025, 5, 1, 0, 0, 0, tzinfo=timezone.utc
                                ).timestamp()
                                * 1000
                            )
                        },
                    },
                    {
                        "start": {
                            "timestamp": int(
                                datetime(
                                    2025, 5, 1, 0, 0, 0, tzinfo=timezone.utc
                                ).timestamp()
                                * 1000
                            )
                        },
                        "end": {
                            "timestamp": int(
                                datetime(
                                    2025, 6, 1, 0, 0, 0, tzinfo=timezone.utc
                                ).timestamp()
                                * 1000
                            )
                        },
                    },
                ]
            }
        }
    }
    queries_and_expected_outputs = [(query, expected_output)]

    query = """
    {
      graph(path: "g") {
        rolling(window: {duration: "1 month"}) {
          page(limit: 3) {
            start {
              timestamp
            }
            end {
              timestamp
            }
          }
        }
      }
    }
    """
    # same expected output
    queries_and_expected_outputs.append((query, expected_output))

    query = """
    {
      graph(path: "g") {
        rolling(window: {duration: "1 month"}, alignmentUnit: UNALIGNED) {
          page(limit: 3) {
            start {
              timestamp
            }
            end {
              timestamp
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "rolling": {
                "page": [
                    {
                        "start": {
                            "timestamp": int(
                                datetime(
                                    2025, 3, 15, 14, 37, 52, tzinfo=timezone.utc
                                ).timestamp()
                                * 1000
                            )
                        },
                        "end": {
                            "timestamp": int(
                                datetime(
                                    2025, 4, 15, 14, 37, 52, tzinfo=timezone.utc
                                ).timestamp()
                                * 1000
                            )
                        },
                    },
                    {
                        "start": {
                            "timestamp": int(
                                datetime(
                                    2025, 4, 15, 14, 37, 52, tzinfo=timezone.utc
                                ).timestamp()
                                * 1000
                            )
                        },
                        "end": {
                            "timestamp": int(
                                datetime(
                                    2025, 5, 15, 14, 37, 52, tzinfo=timezone.utc
                                ).timestamp()
                                * 1000
                            )
                        },
                    },
                    {
                        "start": {
                            "timestamp": int(
                                datetime(
                                    2025, 5, 15, 14, 37, 52, tzinfo=timezone.utc
                                ).timestamp()
                                * 1000
                            )
                        },
                        "end": {
                            "timestamp": int(
                                datetime(
                                    2025, 6, 15, 14, 37, 52, tzinfo=timezone.utc
                                ).timestamp()
                                * 1000
                            )
                        },
                    },
                ]
            }
        }
    }
    queries_and_expected_outputs.append((query, expected_output))

    query = """
    {
      graph(path: "g") {
        rolling(window: {duration: "1 month"}, alignmentUnit: DAY) {
          page(limit: 3) {
            start {
              timestamp
            }
            end {
              timestamp
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "rolling": {
                "page": [
                    {
                        "start": {
                            "timestamp": int(
                                datetime(
                                    2025, 3, 15, 0, 0, 0, tzinfo=timezone.utc
                                ).timestamp()
                                * 1000
                            )
                        },
                        "end": {
                            "timestamp": int(
                                datetime(
                                    2025, 4, 15, 0, 0, 0, tzinfo=timezone.utc
                                ).timestamp()
                                * 1000
                            )
                        },
                    },
                    {
                        "start": {
                            "timestamp": int(
                                datetime(
                                    2025, 4, 15, 0, 0, 0, tzinfo=timezone.utc
                                ).timestamp()
                                * 1000
                            )
                        },
                        "end": {
                            "timestamp": int(
                                datetime(
                                    2025, 5, 15, 0, 0, 0, tzinfo=timezone.utc
                                ).timestamp()
                                * 1000
                            )
                        },
                    },
                    {
                        "start": {
                            "timestamp": int(
                                datetime(
                                    2025, 5, 15, 0, 0, 0, tzinfo=timezone.utc
                                ).timestamp()
                                * 1000
                            )
                        },
                        "end": {
                            "timestamp": int(
                                datetime(
                                    2025, 6, 15, 0, 0, 0, tzinfo=timezone.utc
                                ).timestamp()
                                * 1000
                            )
                        },
                    },
                ]
            }
        }
    }
    queries_and_expected_outputs.append((query, expected_output))

    query = """
    {
      graph(path: "g") {
        rolling(window: {duration: "1 month"}, alignmentUnit: HOUR) {
          page(limit: 3) {
            start {
              timestamp
            }
            end {
              timestamp
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "rolling": {
                "page": [
                    {
                        "start": {
                            "timestamp": int(
                                datetime(
                                    2025, 3, 15, 14, 0, 0, tzinfo=timezone.utc
                                ).timestamp()
                                * 1000
                            )
                        },
                        "end": {
                            "timestamp": int(
                                datetime(
                                    2025, 4, 15, 14, 0, 0, tzinfo=timezone.utc
                                ).timestamp()
                                * 1000
                            )
                        },
                    },
                    {
                        "start": {
                            "timestamp": int(
                                datetime(
                                    2025, 4, 15, 14, 0, 0, tzinfo=timezone.utc
                                ).timestamp()
                                * 1000
                            )
                        },
                        "end": {
                            "timestamp": int(
                                datetime(
                                    2025, 5, 15, 14, 0, 0, tzinfo=timezone.utc
                                ).timestamp()
                                * 1000
                            )
                        },
                    },
                    {
                        "start": {
                            "timestamp": int(
                                datetime(
                                    2025, 5, 15, 14, 0, 0, tzinfo=timezone.utc
                                ).timestamp()
                                * 1000
                            )
                        },
                        "end": {
                            "timestamp": int(
                                datetime(
                                    2025, 6, 15, 14, 0, 0, tzinfo=timezone.utc
                                ).timestamp()
                                * 1000
                            )
                        },
                    },
                ]
            }
        }
    }
    queries_and_expected_outputs.append((query, expected_output))

    query = """
    {
      graph(path: "g") {
        rolling(window: {duration: "1 month"}, step: {duration: "2 weeks"}, alignmentUnit: DAY) {
          page(limit: 3) {
            start {
              timestamp
            }
            end {
              timestamp
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "rolling": {
                "page": [
                    {
                        "start": {
                            "timestamp": int(
                                datetime(
                                    2025, 2, 28, 0, 0, 0, tzinfo=timezone.utc
                                ).timestamp()
                                * 1000
                            )
                        },
                        "end": {
                            "timestamp": int(
                                datetime(
                                    2025, 3, 29, 0, 0, 0, tzinfo=timezone.utc
                                ).timestamp()
                                * 1000
                            )
                        },
                    },
                    {
                        "start": {
                            "timestamp": int(
                                datetime(
                                    2025, 3, 12, 0, 0, 0, tzinfo=timezone.utc
                                ).timestamp()
                                * 1000
                            )
                        },
                        "end": {
                            "timestamp": int(
                                datetime(
                                    2025, 4, 12, 0, 0, 0, tzinfo=timezone.utc
                                ).timestamp()
                                * 1000
                            )
                        },
                    },
                    {
                        "start": {
                            "timestamp": int(
                                datetime(
                                    2025, 3, 26, 0, 0, 0, tzinfo=timezone.utc
                                ).timestamp()
                                * 1000
                            )
                        },
                        "end": {
                            "timestamp": int(
                                datetime(
                                    2025, 4, 26, 0, 0, 0, tzinfo=timezone.utc
                                ).timestamp()
                                * 1000
                            )
                        },
                    },
                ]
            }
        }
    }
    queries_and_expected_outputs.append((query, expected_output))

    query = """
    {
      graph(path: "g") {
        rolling(window: {duration: "1 day"}) {
          page(limit: 3) {
            start {
              timestamp
            }
            end {
              timestamp
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "rolling": {
                "page": [
                    {
                        "start": {
                            "timestamp": int(
                                datetime(
                                    2025, 3, 15, 0, 0, 0, tzinfo=timezone.utc
                                ).timestamp()
                                * 1000
                            )
                        },
                        "end": {
                            "timestamp": int(
                                datetime(
                                    2025, 3, 16, 0, 0, 0, tzinfo=timezone.utc
                                ).timestamp()
                                * 1000
                            )
                        },
                    },
                    {
                        "start": {
                            "timestamp": int(
                                datetime(
                                    2025, 3, 16, 0, 0, 0, tzinfo=timezone.utc
                                ).timestamp()
                                * 1000
                            )
                        },
                        "end": {
                            "timestamp": int(
                                datetime(
                                    2025, 3, 17, 0, 0, 0, tzinfo=timezone.utc
                                ).timestamp()
                                * 1000
                            )
                        },
                    },
                    {
                        "start": {
                            "timestamp": int(
                                datetime(
                                    2025, 3, 17, 0, 0, 0, tzinfo=timezone.utc
                                ).timestamp()
                                * 1000
                            )
                        },
                        "end": {
                            "timestamp": int(
                                datetime(
                                    2025, 3, 18, 0, 0, 0, tzinfo=timezone.utc
                                ).timestamp()
                                * 1000
                            )
                        },
                    },
                ]
            }
        }
    }
    queries_and_expected_outputs.append((query, expected_output))

    query = """
    {
      graph(path: "g") {
        rolling(window: {duration: "1 month and 1 day"}) {
          page(limit: 3) {
            start {
              timestamp
            }
            end {
              timestamp
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "rolling": {
                "page": [
                    {
                        "start": {
                            "timestamp": int(
                                datetime(
                                    2025, 3, 15, 0, 0, 0, tzinfo=timezone.utc
                                ).timestamp()
                                * 1000
                            )
                        },
                        "end": {
                            "timestamp": int(
                                datetime(
                                    2025, 4, 16, 0, 0, 0, tzinfo=timezone.utc
                                ).timestamp()
                                * 1000
                            )
                        },
                    },
                    {
                        "start": {
                            "timestamp": int(
                                datetime(
                                    2025, 4, 16, 0, 0, 0, tzinfo=timezone.utc
                                ).timestamp()
                                * 1000
                            )
                        },
                        "end": {
                            "timestamp": int(
                                datetime(
                                    2025, 5, 17, 0, 0, 0, tzinfo=timezone.utc
                                ).timestamp()
                                * 1000
                            )
                        },
                    },
                    {
                        "start": {
                            "timestamp": int(
                                datetime(
                                    2025, 5, 17, 0, 0, 0, tzinfo=timezone.utc
                                ).timestamp()
                                * 1000
                            )
                        },
                        "end": {
                            "timestamp": int(
                                datetime(
                                    2025, 6, 18, 0, 0, 0, tzinfo=timezone.utc
                                ).timestamp()
                                * 1000
                            )
                        },
                    },
                ]
            }
        }
    }
    queries_and_expected_outputs.append((query, expected_output))

    query = """
    {
      graph(path: "g") {
        rolling(window: {duration: "1 month"}, step: {duration: "1 day"}) {
          page(limit: 3) {
            start {
              timestamp
            }
            end {
              timestamp
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "rolling": {
                "page": [
                    {
                        "start": {
                            "timestamp": int(
                                datetime(
                                    2025, 2, 16, 0, 0, 0, tzinfo=timezone.utc
                                ).timestamp()
                                * 1000
                            )
                        },
                        "end": {
                            "timestamp": int(
                                datetime(
                                    2025, 3, 16, 0, 0, 0, tzinfo=timezone.utc
                                ).timestamp()
                                * 1000
                            )
                        },
                    },
                    {
                        "start": {
                            "timestamp": int(
                                datetime(
                                    2025, 2, 17, 0, 0, 0, tzinfo=timezone.utc
                                ).timestamp()
                                * 1000
                            )
                        },
                        "end": {
                            "timestamp": int(
                                datetime(
                                    2025, 3, 17, 0, 0, 0, tzinfo=timezone.utc
                                ).timestamp()
                                * 1000
                            )
                        },
                    },
                    {
                        "start": {
                            "timestamp": int(
                                datetime(
                                    2025, 2, 18, 0, 0, 0, tzinfo=timezone.utc
                                ).timestamp()
                                * 1000
                            )
                        },
                        "end": {
                            "timestamp": int(
                                datetime(
                                    2025, 3, 18, 0, 0, 0, tzinfo=timezone.utc
                                ).timestamp()
                                * 1000
                            )
                        },
                    },
                ]
            }
        }
    }
    queries_and_expected_outputs.append((query, expected_output))

    # discrete interval — no alignment (1000 ms = 1 second)
    query = """
    {
      graph(path: "g") {
        rolling(window: {epoch: 1000}) {
          page(limit: 1) {
            start {
              timestamp
            }
            end {
              timestamp
            }
          }
        }
      }
    }
    """
    # earliest = 2025-03-15 14:37:52
    expected_output = {
        "graph": {
            "rolling": {
                "page": [
                    {
                        "start": {
                            "timestamp": int(
                                datetime(
                                    2025, 3, 15, 14, 37, 52, tzinfo=timezone.utc
                                ).timestamp()
                                * 1000
                            )
                        },
                        "end": {
                            "timestamp": int(
                                datetime(
                                    2025, 3, 15, 14, 37, 53, tzinfo=timezone.utc
                                ).timestamp()
                                * 1000
                            )
                        },
                    }
                ]
            }
        }
    }
    queries_and_expected_outputs.append((query, expected_output))

    # test expanding
    query = """
    {
      graph(path: "g") {
        expanding(step: {duration: "1 day"}) {
          page(limit: 2) {
            end {
              timestamp
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "expanding": {
                "page": [
                    {
                        "end": {
                            "timestamp": int(
                                datetime(
                                    2025, 3, 16, 0, 0, 0, tzinfo=timezone.utc
                                ).timestamp()
                                * 1000
                            )
                        }
                    },
                    {
                        "end": {
                            "timestamp": int(
                                datetime(
                                    2025, 3, 17, 0, 0, 0, tzinfo=timezone.utc
                                ).timestamp()
                                * 1000
                            )
                        }
                    },
                ]
            }
        }
    }
    queries_and_expected_outputs.append((query, expected_output))

    query = """
    {
      graph(path: "g") {
        expanding(step: {duration: "1 month"}, alignmentUnit: UNALIGNED) {
          page(limit: 2) {
            end {
              timestamp
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "expanding": {
                "page": [
                    {
                        "end": {
                            "timestamp": int(
                                datetime(
                                    2025, 4, 15, 14, 37, 52, tzinfo=timezone.utc
                                ).timestamp()
                                * 1000
                            )
                        }
                    },
                    {
                        "end": {
                            "timestamp": int(
                                datetime(
                                    2025, 5, 15, 14, 37, 52, tzinfo=timezone.utc
                                ).timestamp()
                                * 1000
                            )
                        }
                    },
                ]
            }
        }
    }
    queries_and_expected_outputs.append((query, expected_output))

    query = """
    {
      graph(path: "g") {
        expanding(step: {duration: "1 month"}, alignmentUnit: DAY) {
          page(limit: 2) {
            end {
              timestamp
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "expanding": {
                "page": [
                    {
                        "end": {
                            "timestamp": int(
                                datetime(
                                    2025, 4, 15, 0, 0, 0, tzinfo=timezone.utc
                                ).timestamp()
                                * 1000
                            )
                        }
                    },
                    {
                        "end": {
                            "timestamp": int(
                                datetime(
                                    2025, 5, 15, 0, 0, 0, tzinfo=timezone.utc
                                ).timestamp()
                                * 1000
                            )
                        }
                    },
                ]
            }
        }
    }
    queries_and_expected_outputs.append((query, expected_output))

    query = """
    {
      graph(path: "g") {
        expanding(step: {duration: "1 month"}, alignmentUnit: WEEK) {
          page(limit: 2) {
            end {
              timestamp
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "expanding": {
                "page": [
                    {
                        "end": {
                            "timestamp": int(
                                datetime(
                                    2025, 4, 13, 0, 0, 0, tzinfo=timezone.utc
                                ).timestamp()
                                * 1000
                            )
                        }
                    },
                    {
                        "end": {
                            "timestamp": int(
                                datetime(
                                    2025, 5, 13, 0, 0, 0, tzinfo=timezone.utc
                                ).timestamp()
                                * 1000
                            )
                        }
                    },
                ]
            }
        }
    }
    queries_and_expected_outputs.append((query, expected_output))

    query = """
    {
      graph(path: "g") {
        node(name: "1") {
          expanding(step: {duration: "2 days"}) {
            page(limit: 2) {
              end {
                timestamp
              }
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "node": {
                "expanding": {
                    "page": [
                        {
                            "end": {
                                "timestamp": int(
                                    datetime(
                                        2025, 3, 17, 0, 0, 0, tzinfo=timezone.utc
                                    ).timestamp()
                                    * 1000
                                )
                            }
                        },
                        {
                            "end": {
                                "timestamp": int(
                                    datetime(
                                        2025, 3, 19, 0, 0, 0, tzinfo=timezone.utc
                                    ).timestamp()
                                    * 1000
                                )
                            }
                        },
                    ]
                }
            }
        }
    }
    queries_and_expected_outputs.append((query, expected_output))

    # last window
    query = """
    {
      graph(path: "g") {
        node(name: "1") {
          expanding(step: {duration: "2 days"}) {
            page(limit: 2, offset: 126) {
              end {
                timestamp
              }
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "node": {
                "expanding": {
                    "page": [
                        {
                            "end": {
                                "timestamp": int(
                                    datetime(
                                        2025, 11, 24, 0, 0, 0, tzinfo=timezone.utc
                                    ).timestamp()
                                    * 1000
                                )
                            }
                        }
                    ]
                }
            }
        }
    }
    queries_and_expected_outputs.append((query, expected_output))

    # no alignment bc epoch
    query = """
    {
      graph(path: "g") {
        nodes {
          page(limit: 2) {
            expanding(step: {epoch: 60000}) {
              page(limit: 2) {
                end {
                  timestamp
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
                "page": [
                    {
                        "expanding": {
                            "page": [
                                {
                                    "end": {
                                        "timestamp": int(
                                            datetime(
                                                2025,
                                                3,
                                                15,
                                                14,
                                                38,
                                                52,
                                                tzinfo=timezone.utc,
                                            ).timestamp()
                                            * 1000
                                        )
                                    }
                                },
                                {
                                    "end": {
                                        "timestamp": int(
                                            datetime(
                                                2025,
                                                3,
                                                15,
                                                14,
                                                39,
                                                52,
                                                tzinfo=timezone.utc,
                                            ).timestamp()
                                            * 1000
                                        )
                                    }
                                },
                            ]
                        }
                    },
                    {
                        "expanding": {
                            "page": [
                                {
                                    "end": {
                                        "timestamp": int(
                                            datetime(
                                                2025,
                                                3,
                                                15,
                                                14,
                                                38,
                                                52,
                                                tzinfo=timezone.utc,
                                            ).timestamp()
                                            * 1000
                                        )
                                    }
                                },
                                {
                                    "end": {
                                        "timestamp": int(
                                            datetime(
                                                2025,
                                                3,
                                                15,
                                                14,
                                                39,
                                                52,
                                                tzinfo=timezone.utc,
                                            ).timestamp()
                                            * 1000
                                        )
                                    }
                                },
                            ]
                        }
                    },
                ]
            }
        }
    }
    queries_and_expected_outputs.append((query, expected_output))

    # last window
    query = """
    {
      graph(path: "g") {
        nodes {
          page(limit: 2) {
            expanding(step: {epoch: 60000}) {
              page(limit: 2, offset: 363307) {
                end {
                  timestamp
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
                "page": [
                    {
                        "expanding": {
                            "page": [
                                {
                                    "end": {
                                        "timestamp": int(
                                            datetime(
                                                2025,
                                                11,
                                                22,
                                                21,
                                                45,
                                                52,
                                                tzinfo=timezone.utc,
                                            ).timestamp()
                                            * 1000
                                        )
                                    }
                                }
                            ]
                        }
                    },
                    {
                        "expanding": {
                            "page": [
                                {
                                    "end": {
                                        "timestamp": int(
                                            datetime(
                                                2025,
                                                11,
                                                22,
                                                21,
                                                45,
                                                52,
                                                tzinfo=timezone.utc,
                                            ).timestamp()
                                            * 1000
                                        )
                                    }
                                }
                            ]
                        }
                    },
                ]
            }
        }
    }
    queries_and_expected_outputs.append((query, expected_output))

    query = """
    {
      graph(path: "g") {
        edge(src: "1", dst: "2") {
          expanding(step: {duration: "3 months"}) {
            list {
              end {
                timestamp
              }
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "edge": {
                "expanding": {
                    "list": [
                        {
                            "end": {
                                "timestamp": int(
                                    datetime(
                                        2025, 6, 1, 0, 0, 0, tzinfo=timezone.utc
                                    ).timestamp()
                                    * 1000
                                )
                            }
                        },
                        {
                            "end": {
                                "timestamp": int(
                                    datetime(
                                        2025, 9, 1, 0, 0, 0, tzinfo=timezone.utc
                                    ).timestamp()
                                    * 1000
                                )
                            }
                        },
                        {
                            "end": {
                                "timestamp": int(
                                    datetime(
                                        2025, 12, 1, 0, 0, 0, tzinfo=timezone.utc
                                    ).timestamp()
                                    * 1000
                                )
                            }
                        },
                    ]
                }
            }
        }
    }
    queries_and_expected_outputs.append((query, expected_output))

    query = """
    {
      graph(path: "g") {
        edges {
          expanding(step: {duration: "10 weeks"}) {
            list {
              list {
                end {
                  timestamp
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
            "edges": {
                "expanding": {
                    "list": [
                        {
                            "list": [
                                {
                                    "end": {
                                        "timestamp": int(
                                            datetime(
                                                2025,
                                                5,
                                                22,
                                                0,
                                                0,
                                                0,
                                                tzinfo=timezone.utc,
                                            ).timestamp()
                                            * 1000
                                        )
                                    }
                                }
                            ]
                        },
                        {
                            "list": [
                                {
                                    "end": {
                                        "timestamp": int(
                                            datetime(
                                                2025,
                                                7,
                                                31,
                                                0,
                                                0,
                                                0,
                                                tzinfo=timezone.utc,
                                            ).timestamp()
                                            * 1000
                                        )
                                    }
                                }
                            ]
                        },
                        {
                            "list": [
                                {
                                    "end": {
                                        "timestamp": int(
                                            datetime(
                                                2025,
                                                10,
                                                9,
                                                0,
                                                0,
                                                0,
                                                tzinfo=timezone.utc,
                                            ).timestamp()
                                            * 1000
                                        )
                                    }
                                }
                            ]
                        },
                        {
                            "list": [
                                {
                                    "end": {
                                        "timestamp": int(
                                            datetime(
                                                2025,
                                                12,
                                                18,
                                                0,
                                                0,
                                                0,
                                                tzinfo=timezone.utc,
                                            ).timestamp()
                                            * 1000
                                        )
                                    }
                                }
                            ]
                        },
                    ]
                }
            }
        }
    }
    queries_and_expected_outputs.append((query, expected_output))

    query = """
    {
      graph(path: "g") {
        node(name: "1") {
          neighbours {
            expanding(step: {duration: "70 days"}) {
              page(limit: 2) {
                list {
                  end {
                    timestamp
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
            "node": {
                "neighbours": {
                    "expanding": {
                        "page": [
                            {
                                "list": [
                                    {
                                        "end": {
                                            "timestamp": int(
                                                datetime(
                                                    2025,
                                                    5,
                                                    24,
                                                    0,
                                                    0,
                                                    0,
                                                    tzinfo=timezone.utc,
                                                ).timestamp()
                                                * 1000
                                            )
                                        }
                                    }
                                ]
                            },
                            {
                                "list": [
                                    {
                                        "end": {
                                            "timestamp": int(
                                                datetime(
                                                    2025,
                                                    8,
                                                    2,
                                                    0,
                                                    0,
                                                    0,
                                                    tzinfo=timezone.utc,
                                                ).timestamp()
                                                * 1000
                                            )
                                        }
                                    }
                                ]
                            },
                        ]
                    }
                }
            }
        }
    }
    queries_and_expected_outputs.append((query, expected_output))

    run_group_graphql_test(queries_and_expected_outputs, g)
