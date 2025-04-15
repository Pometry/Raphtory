import pytest
from raphtory import Graph, PersistentGraph

from utils import run_graphql_test

graph = Graph()
graph.add_edge(0, 0, 1)

persistent_graph = graph.persistent_graph()


@pytest.mark.parametrize("graph", [graph, persistent_graph])
def test_graph_node_sort_by_nothing(graph):
    query = """
        {
          graph(path: "g") {
            nodes(ids: ["0"]) {
              list {
                id
                degree
              }
            }
          }
        }
    """
    expected_output = {"graph": {"nodes": {"list": [{"id": "0", "degree": 1}]}}}
    run_graphql_test(query, expected_output, graph)
