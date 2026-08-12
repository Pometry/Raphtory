from raphtory import Graph

from utils import run_graphql_test


def init_graph(graph: Graph):
    graph.add_edge(1, "a", "b")
    graph.add_edge(2, "a", "c")
    graph.add_edge(3, "b", "c")
    return graph


def test_algorithm_pagerank():
    graph = init_graph(Graph())
    query = """{
      graph(path: "g") {
        algorithm {
          pagerank(iterCount: 20) {
            count
            nodes { list { name } }
            columns {
              name
              values {
                __typename
                ... on NodeStateProp { prop }
              }
            }
          }
        }
      }
    }"""
    expected_output = {
        "graph": {
            "algorithm": {
                "pagerank": {
                    "count": 3,
                    "nodes": {"list": [{"name": "a"}, {"name": "b"}, {"name": "c"}]},
                    "columns": [
                        {
                            "name": "pagerank_score",
                            "values": [
                                {
                                    "__typename": "NodeStateProp",
                                    "prop": 0.197580035313204,
                                },
                                {
                                    "__typename": "NodeStateProp",
                                    "prop": 0.28155081033755053,
                                },
                                {
                                    "__typename": "NodeStateProp",
                                    "prop": 0.5208691543492454,
                                },
                            ],
                        }
                    ],
                }
            }
        }
    }
    run_graphql_test(query, expected_output, graph)
