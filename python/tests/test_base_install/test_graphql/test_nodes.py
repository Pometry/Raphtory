import pytest
from raphtory import Graph, PersistentGraph

from utils import run_graphql_test
from filters_setup import init_graph, init_graph2

EVENT_GRAPH = init_graph2(Graph())
PERSISTENT_GRAPH = init_graph2(PersistentGraph())


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_node_sort_by_nothing(graph):
    query = """{
      graph(path: "g") {
        nodes(select: {
          node: { 
            field: NODE_ID
            where: { eq: { u64: 1 } }
          }
        }) {
          list {
            name
            degree
          }
        }
      }
    }"""
    expected_output = {"graph": {"nodes": {"list": [{"degree": 2, "name": "1"}]}}}
    run_graphql_test(query, expected_output, graph)
