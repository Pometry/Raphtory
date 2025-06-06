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
      page(limit: 5, offset: 1) {
        nodes{
          list{
            name
          }
        }
      }
      count
      list{
        start
        end
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
                    {"start": 1735646400000, "end": 1735732800000},
                    {"start": 1735689600000, "end": 1735776000000},
                    {"start": 1735732800000, "end": 1735819200000},
                    {"start": 1735776000000, "end": 1735862400000},
                    {"start": 1735819200000, "end": 1735905600000},
                    {"start": 1735862400000, "end": 1735948800000},
                    {"start": 1735905600000, "end": 1735992000000},
                    {"start": 1735948800000, "end": 1736035200000},
                    {"start": 1735992000000, "end": 1736078400000},
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
        start
        end
      }
    }
  }
}
    """
    correct = {
        "graph": {
            "expanding": {
                "list": [
                    {"start": None, "end": 1735948800000},
                    {"start": None, "end": 1736208000000},
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
            earliestTime
            latestTime
          }
        }
      }
    }
    """
    correct = {
        "graph": {
            "rolling": {
                "list": [
                    {"earliestTime": 1, "latestTime": 1},
                    {"earliestTime": 2, "latestTime": 2},
                    {"earliestTime": 3, "latestTime": 3},
                    {"earliestTime": 4, "latestTime": 4},
                    {"earliestTime": 5, "latestTime": 5},
                ]
            }
        }
    }
    run_graphql_test(query, correct, graph)

    query = """
    {
      graph(path: "g") {
        rolling(window: {epoch: 1},step:{epoch:2}) {
          list {
            earliestTime
            latestTime
          }
        }
      }
    }
    """
    correct = {
        "graph": {
            "rolling": {
                "list": [
                    {"earliestTime": 2, "latestTime": 2},
                    {"earliestTime": 4, "latestTime": 4},
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
          start
          end
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
                        {"start": 2, "end": 3},
                        {"start": 2, "end": 4},
                        {"start": 3, "end": 5},
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
          end
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
                            "end": 5,
                            "nodes": {
                                "list": [{"name": "1"}, {"name": "2"}, {"name": "3"}]
                            },
                        },
                        {
                            "end": 7,
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
              start
              end
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
                        {"start": 2, "end": 3},
                        {"start": 2, "end": 4},
                        {"start": 2, "end": 5},
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
        start
        end
        earliestTime
        latestTime
      }
    }
  }
}
    """
    correct = {
        "graph": {
            "rolling": {
                "list": [{"start": 2, "end": 5, "earliestTime": 2, "latestTime": 4}]
            }
        }
    }
    run_graphql_test(query, correct, graph)

    # testing if a very large step returns nothing
    query = """
    {
  graph(path: "g") {
    rolling(window: {epoch: 3}, step: {epoch:1000}) {
      list {
        start
        end
        earliestTime
        latestTime
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
        rolling(window: {epoch: 1}, step: {epoch:1}) {
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
      page(limit: 2, offset: 2) {
        earliestTime
        latestTime
      }
    }
  }
}
        """
    correct = {"graph": {"rolling": {"page": [{"earliestTime": 5, "latestTime": 5}]}}}
    run_graphql_test(query, correct, graph)


def test_node():
    graph = Graph()
    create_graph_epoch(graph)
    query = """
{
  graph(path: "g") {
    node(name:"1"){
      rolling(window:{epoch:1},step:{epoch:1}){
        list{
          start
          end
          degree
          earliestTime
        }
        count
        page(limit:3,offset:1){
          start
          degree
        }
      }
      before(time:4){
        expanding(step:{epoch:1}){
        list{
          end
          degree
        }
        count
        page(limit:1,offset:2){
          end
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
                        {"start": 1, "end": 2, "degree": 1, "earliestTime": 1},
                        {"start": 2, "end": 3, "degree": 2, "earliestTime": 2},
                        {"start": 3, "end": 4, "degree": 2, "earliestTime": 3},
                        {"start": 4, "end": 5, "degree": 1, "earliestTime": 4},
                        {"start": 5, "end": 6, "degree": 0, "earliestTime": None},
                    ],
                    "count": 5,
                    "page": [{"start": 4, "degree": 1}, {"start": 5, "degree": 0}],
                },
                "before": {
                    "expanding": {
                        "list": [
                            {"end": 2, "degree": 1},
                            {"end": 3, "degree": 2},
                            {"end": 4, "degree": 2},
                        ],
                        "count": 3,
                        "page": [{"end": 4, "degree": 2}],
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
            start
            end
            earliestTime
          }
        }
        count
        page(limit: 3, offset: 1) {
          page(limit: 1, offset: 0) {
            id
            degree
            start
            end
            earliestTime
          }
        }
      }
      after(time: 1) {
        expanding(step: {epoch: 1}) {
          list {
            page(limit: 1, offset: 0) {
              id
              degree
              start
              end
              earliestTime
              latestTime
            }
          }
          count
          page(limit: 2, offset: 1) {
            page(limit: 1, offset: 0) {
              id
              degree
              start
              end
              earliestTime
              latestTime
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
                                    "start": 1,
                                    "end": 2,
                                    "earliestTime": 1,
                                }
                            ]
                        },
                        {
                            "page": [
                                {
                                    "id": "1",
                                    "degree": 2,
                                    "start": 2,
                                    "end": 3,
                                    "earliestTime": 2,
                                }
                            ]
                        },
                        {
                            "page": [
                                {
                                    "id": "1",
                                    "degree": 2,
                                    "start": 3,
                                    "end": 4,
                                    "earliestTime": 3,
                                }
                            ]
                        },
                        {
                            "page": [
                                {
                                    "id": "1",
                                    "degree": 1,
                                    "start": 4,
                                    "end": 5,
                                    "earliestTime": 4,
                                }
                            ]
                        },
                        {
                            "page": [
                                {
                                    "id": "6",
                                    "degree": 1,
                                    "start": 5,
                                    "end": 6,
                                    "earliestTime": 5,
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
                                    "start": 4,
                                    "end": 5,
                                    "earliestTime": 4,
                                }
                            ]
                        },
                        {
                            "page": [
                                {
                                    "id": "6",
                                    "degree": 1,
                                    "start": 5,
                                    "end": 6,
                                    "earliestTime": 5,
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
                                        "end": 3,
                                        "earliestTime": 2,
                                        "latestTime": 2,
                                    }
                                ]
                            },
                            {
                                "page": [
                                    {
                                        "id": "1",
                                        "degree": 2,
                                        "start": None,
                                        "end": 4,
                                        "earliestTime": 2,
                                        "latestTime": 3,
                                    }
                                ]
                            },
                            {
                                "page": [
                                    {
                                        "id": "1",
                                        "degree": 2,
                                        "start": None,
                                        "end": 5,
                                        "earliestTime": 2,
                                        "latestTime": 4,
                                    }
                                ]
                            },
                            {
                                "page": [
                                    {
                                        "id": "1",
                                        "degree": 2,
                                        "start": None,
                                        "end": 6,
                                        "earliestTime": 2,
                                        "latestTime": 4,
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
                                        "end": 5,
                                        "earliestTime": 2,
                                        "latestTime": 4,
                                    }
                                ]
                            },
                            {
                                "page": [
                                    {
                                        "id": "1",
                                        "degree": 2,
                                        "start": None,
                                        "end": 6,
                                        "earliestTime": 2,
                                        "latestTime": 4,
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
              start
              end
              earliestTime
            }
          }
          count
          page(limit: 3, offset: 1) {
            page(limit: 1, offset: 0) {
              id
              degree
              start
              end
              earliestTime
            }
          }
        }
        after(time: 1) {
          expanding(step: {epoch: 1}) {
            list {
              page(limit: 1, offset: 0) {
                id
                degree
                start
                end
                earliestTime
                latestTime
              }
            }
            count
            page(limit: 2, offset: 1) {
              page(limit: 1, offset: 0) {
                id
                degree
                start
                end
                earliestTime
                latestTime
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
                                        "start": 1,
                                        "end": 2,
                                        "earliestTime": 1,
                                    }
                                ]
                            },
                            {
                                "page": [
                                    {
                                        "id": "2",
                                        "degree": 1,
                                        "start": 2,
                                        "end": 3,
                                        "earliestTime": 2,
                                    }
                                ]
                            },
                            {
                                "page": [
                                    {
                                        "id": "2",
                                        "degree": 1,
                                        "start": 3,
                                        "end": 4,
                                        "earliestTime": 3,
                                    }
                                ]
                            },
                            {
                                "page": [
                                    {
                                        "id": "2",
                                        "degree": 0,
                                        "start": 4,
                                        "end": 5,
                                        "earliestTime": None,
                                    }
                                ]
                            },
                            {
                                "page": [
                                    {
                                        "id": "2",
                                        "degree": 0,
                                        "start": 5,
                                        "end": 6,
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
                                        "start": 4,
                                        "end": 5,
                                        "earliestTime": None,
                                    }
                                ]
                            },
                            {
                                "page": [
                                    {
                                        "id": "2",
                                        "degree": 0,
                                        "start": 5,
                                        "end": 6,
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
                                            "start": 2,
                                            "end": 3,
                                            "earliestTime": 2,
                                            "latestTime": 2,
                                        }
                                    ]
                                },
                                {
                                    "page": [
                                        {
                                            "id": "2",
                                            "degree": 1,
                                            "start": 2,
                                            "end": 4,
                                            "earliestTime": 2,
                                            "latestTime": 3,
                                        }
                                    ]
                                },
                                {
                                    "page": [
                                        {
                                            "id": "2",
                                            "degree": 1,
                                            "start": 2,
                                            "end": 5,
                                            "earliestTime": 2,
                                            "latestTime": 3,
                                        }
                                    ]
                                },
                                {
                                    "page": [
                                        {
                                            "id": "2",
                                            "degree": 1,
                                            "start": 2,
                                            "end": 6,
                                            "earliestTime": 2,
                                            "latestTime": 3,
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
                                            "start": 2,
                                            "end": 5,
                                            "earliestTime": 2,
                                            "latestTime": 3,
                                        }
                                    ]
                                },
                                {
                                    "page": [
                                        {
                                            "id": "2",
                                            "degree": 1,
                                            "start": 2,
                                            "end": 6,
                                            "earliestTime": 2,
                                            "latestTime": 3,
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
      rolling(window:{epoch:1},step:{epoch:1}){
        list{
          start
          end
          earliestTime
        }
        count
        page(limit:3,offset:1){
          start
          end
          earliestTime
        }
      }
      after(time:1){
        expanding(step:{epoch:1}){
        list{
          start
          end
          earliestTime
          latestTime
        }
        count
        page(limit:2,offset:1){
          start
          end
          earliestTime
          latestTime
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
                        {"start": 1, "end": 2, "earliestTime": 1},
                        {"start": 2, "end": 3, "earliestTime": 2},
                        {"start": 3, "end": 4, "earliestTime": 3},
                        {"start": 4, "end": 5, "earliestTime": None},
                        {"start": 5, "end": 6, "earliestTime": None},
                    ],
                    "count": 5,
                    "page": [
                        {"start": 4, "end": 5, "earliestTime": None},
                        {"start": 5, "end": 6, "earliestTime": None},
                    ],
                },
                "after": {
                    "expanding": {
                        "list": [
                            {
                                "start": None,
                                "end": 3,
                                "earliestTime": 2,
                                "latestTime": 2,
                            },
                            {
                                "start": None,
                                "end": 4,
                                "earliestTime": 2,
                                "latestTime": 3,
                            },
                            {
                                "start": None,
                                "end": 5,
                                "earliestTime": 2,
                                "latestTime": 3,
                            },
                            {
                                "start": None,
                                "end": 6,
                                "earliestTime": 2,
                                "latestTime": 3,
                            },
                        ],
                        "count": 4,
                        "page": [
                            {
                                "start": None,
                                "end": 5,
                                "earliestTime": 2,
                                "latestTime": 3,
                            },
                            {
                                "start": None,
                                "end": 6,
                                "earliestTime": 2,
                                "latestTime": 3,
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
            start
            end
            earliestTime
          }
        }
        count
        page(limit: 3, offset: 1) {
          page(limit: 1, offset: 0) {
            id
            start
            end
            earliestTime
          }
        }
      }
      after(time: 1) {
        expanding(step: {epoch: 1}) {
          list {
            page(limit: 1, offset: 0) {
              id
              start
              end
              earliestTime
              latestTime
            }
          }
          count
          page(limit: 2, offset: 1) {
            page(limit: 1, offset: 0) {
              id
              start
              end
              earliestTime
              latestTime
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
                                    "start": 1,
                                    "end": 2,
                                    "earliestTime": 1,
                                }
                            ]
                        },
                        {
                            "page": [
                                {
                                    "id": ["1", "2"],
                                    "start": 2,
                                    "end": 3,
                                    "earliestTime": 2,
                                }
                            ]
                        },
                        {
                            "page": [
                                {
                                    "id": ["1", "2"],
                                    "start": 3,
                                    "end": 4,
                                    "earliestTime": 3,
                                }
                            ]
                        },
                        {
                            "page": [
                                {
                                    "id": ["1", "2"],
                                    "start": 4,
                                    "end": 5,
                                    "earliestTime": None,
                                }
                            ]
                        },
                        {
                            "page": [
                                {
                                    "id": ["1", "2"],
                                    "start": 5,
                                    "end": 6,
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
                                    "start": 4,
                                    "end": 5,
                                    "earliestTime": None,
                                }
                            ]
                        },
                        {
                            "page": [
                                {
                                    "id": ["1", "2"],
                                    "start": 5,
                                    "end": 6,
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
                                        "end": 3,
                                        "earliestTime": 2,
                                        "latestTime": 2,
                                    }
                                ]
                            },
                            {
                                "page": [
                                    {
                                        "id": ["1", "2"],
                                        "start": None,
                                        "end": 4,
                                        "earliestTime": 2,
                                        "latestTime": 3,
                                    }
                                ]
                            },
                            {
                                "page": [
                                    {
                                        "id": ["1", "2"],
                                        "start": None,
                                        "end": 5,
                                        "earliestTime": 2,
                                        "latestTime": 3,
                                    }
                                ]
                            },
                            {
                                "page": [
                                    {
                                        "id": ["1", "2"],
                                        "start": None,
                                        "end": 6,
                                        "earliestTime": 2,
                                        "latestTime": 3,
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
                                        "end": 5,
                                        "earliestTime": 2,
                                        "latestTime": 3,
                                    }
                                ]
                            },
                            {
                                "page": [
                                    {
                                        "id": ["1", "2"],
                                        "start": None,
                                        "end": 6,
                                        "earliestTime": 2,
                                        "latestTime": 3,
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
    zero_exception = "Failed to parse time string: 0 size step is not supported"
    # graph fail test
    query = """
    {
      graph(path: "g") {
        rolling(window:{duration:"1 day"},step:{duration:"0 day"}){
                list{
            earliestTime
          }
        }
      }
    }
    """
    queries_and_exceptions.append((query, zero_exception))

    query = """
    {
      graph(path: "g") {
        rolling(window:{epoch:100},step:{epoch:0}){
                list{
            earliestTime
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
            earliestTime
          }
        }
      }
    }
    """
    queries_and_exceptions.append((query, zero_exception))

    query = """
    {
      graph(path: "g") {
        rolling(window:{epoch:0}){
                list{
            earliestTime
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
            earliestTime
          }
        }
      }
    }
    """
    queries_and_exceptions.append((query, zero_exception))

    query = """
    {
      graph(path: "g") {
        expanding(step:{epoch:0}){
                list{
            earliestTime
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
              earliestTime
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
            rolling(window:{epoch:100},step:{epoch:0}){
            list {
              earliestTime
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
              earliestTime
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
            rolling(window:{epoch:0}){
            list {
              earliestTime
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
              earliestTime
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
            expanding(step:{epoch:0}){
            list {
              earliestTime
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
            earliestTime
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
        rolling(window:{epoch:100},step:{epoch:0}){
          list {
            earliestTime
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
            earliestTime
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
        rolling(window:{epoch:0}){
          list {
            earliestTime
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
            earliestTime
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
            expanding(step:{epoch:0}){
              list {
                earliestTime
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
              earliestTime
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
        rolling(window:{epoch:100},step:{epoch:0}){
          list {
            list {
              earliestTime
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
              earliestTime
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
        rolling(window:{epoch:0}){
          list {
            list {
              earliestTime
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
              earliestTime
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
            expanding(step:{epoch:0}){
              list {
                list {
                  earliestTime
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
          earliestTime
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
        rolling(window:{epoch:100},step:{epoch:0}){
            list {
                earliestTime
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
          earliestTime
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
        rolling(window:{epoch:0}){
            list {
                earliestTime
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
          earliestTime
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
            expanding(step:{epoch:0}){
                list {
                    earliestTime
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
            earliestTime
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
    rolling(window:{epoch:100},step:{epoch:0}){
        list {
          list{
            earliestTime
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
            earliestTime
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
    rolling(window:{epoch:0}){
        list {
          list{
            earliestTime
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
            earliestTime
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
        expanding(step:{epoch:0}){
            list {
              list{
                earliestTime
              }
    
            }
          }
        }
      }
    }
        """
    queries_and_exceptions.append((query, zero_exception))
    run_group_graphql_error_test(queries_and_exceptions, graph)


def test_wrong_window():
    graph = Graph()
    create_graph_epoch(graph)
    queries_and_exceptions = []
    mismatch_exception = "Your window and step must be of the same type: duration (string) or epoch (int)"
    parse_exception = "Failed to parse time string: one of the tokens in the interval string supposed to be a number couldn't be parsed"
    parse_exception2 = "Failed to parse time string: 'monthdas' is not a valid unit"
    too_many_exception = "Invalid value for argument \\"
    # graph fail test
    query = """
    {
      graph(path: "g") {
        rolling(window:{duration:"1 day"},step:{epoch:100}){
                list{
            earliestTime
          }
        }
      }
    }
    """
    queries_and_exceptions.append((query, mismatch_exception))

    query = """
    {
      graph(path: "g") {
        rolling(window:{epoch:100},step:{duration:"1 day"}){
                list{
            earliestTime
          }
        }
      }
    }
    """
    queries_and_exceptions.append((query, mismatch_exception))

    query = """
    {
      graph(path: "g") {
        rolling(window:{duration:"1dasdas day"}){
                list{
            earliestTime
          }
        }
      }
    }
    """
    queries_and_exceptions.append((query, parse_exception))

    query = """
    {
  graph(path: "g") {
    rolling(window:{duration:"1 day"},step:{duration:"1 monthdas"}){
			list{
        earliestTime
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, parse_exception2))

    query = """
    {
  graph(path: "g") {
    rolling(window:{duration:"1 day",epoch:11}){
			list{
        earliestTime
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
        list {
          earliestTime
        }
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, mismatch_exception))

    query = """
    {
  graph(path: "g") {
    node(name: "1") {
      rolling(window: {epoch: 100}, step: {duration: "1 day"}) {
        list {
          earliestTime
        }
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, mismatch_exception))

    # nodes tests
    query = """
    {
  graph(path: "g") {
    nodes {
      list {
      rolling(window: {duration: "1 day"}, step: {epoch: 11}) {
          list {
            earliestTime
          }
        }
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, mismatch_exception))

    query = """
{
  graph(path: "g") {
    nodes {
      list {
        rolling(window: {epoch: 100}, step: {duration: "1 day"}) {
          list {
            earliestTime
          }
        }
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, mismatch_exception))

    # path from nodes tests
    query = """
{
  graph(path: "g") {
    node(name: "1") {
      neighbours {
      rolling(window: {duration: "1 day"}, step: {epoch: 11}) {
          list {
            list {
              earliestTime
            }
          }
        }
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, mismatch_exception))

    query = """
{
  graph(path: "g") {
    node(name: "1") {
      neighbours {
        rolling(window: {epoch: 100}, step: {duration: "1 day"}) {
          list {
            list {
              earliestTime
            }
          }
        }
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, mismatch_exception))

    # edge tests
    query = """
{
  graph(path: "g") {
    edge(src: "1", dst: "2") {
      rolling(window: {duration: "1 day"}, step: {epoch: 11}) {
        list {
          earliestTime
        }
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, mismatch_exception))

    query = """
{
  graph(path: "g") {
    edge(src: "1", dst: "2") {
      rolling(window: {epoch: 100}, step: {duration: "1 day"}) {
        list {
          earliestTime
        }
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, mismatch_exception))

    # edges tests
    query = """
{
  graph(path: "g") {
    edges {
      rolling(window: {duration: "1 day"}, step: {epoch: 11}) {
        list {
          list{
            earliestTime
          }
          
        }
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, mismatch_exception))

    query = """
{
  graph(path: "g") {
    edges {
      rolling(window: {epoch: 100}, step: {duration: "1 day"}) {
        list {
          list{
            earliestTime
          }
          
        }
      }
    }
  }
}
    """
    queries_and_exceptions.append((query, mismatch_exception))
    run_group_graphql_error_test(queries_and_exceptions, graph)
