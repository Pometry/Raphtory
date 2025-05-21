import json
from utils import run_graphql_test
from raphtory import Graph
from raphtory.graphql import *

work_dir = "/tmp/harry_potter_graph/"
PORT = 1737


def write_graph(tmp_work_dir: str = work_dir):
    graph = create_graph()
    server = graphql.GraphServer(tmp_work_dir)
    client = server.start().get_client()
    client.send_graph(path="harry_potter", graph=graph)


def print_query_output(query: str, graph: Graph, tmp_work_dir: str = work_dir):
    with graphql.GraphServer(tmp_work_dir).start(PORT) as server:
        client = server.get_client()
        client.send_graph(path="harry_potter", graph=graph, overwrite=True)
        response = client.query(query)

        # Convert response to a dictionary if needed and compare
        response_dict = json.loads(response) if isinstance(response, str) else response
        print(response_dict)


def run_graphql_test(query: str, expected_output: dict, graph: Graph, tmp_work_dir: str = work_dir):
    with GraphServer(tmp_work_dir).start(PORT) as server:
        client = server.get_client()
        client.send_graph(path="harry_potter", graph=graph, overwrite=True)
        response = client.query(query)

        # Convert response to a dictionary if needed and compare
        response_dict = json.loads(response) if isinstance(response, str) else response
        assert response_dict == expected_output


def create_graph() -> Graph:
    graph = Graph()

    # Add nodes with timestamps and properties
    graph.add_node(100, "Dumbledore")
    graph.add_node(200, "Dumbledore", properties={"Age": 50})
    graph.add_node(300, "Dumbledore", properties={"Age": 51})

    graph.add_node(150, "Harry")
    graph.add_node(250, "Harry", properties={"Age": 20})
    graph.add_node(350, "Harry", properties={"Age": 21})

    # Add edges with timestamps and layers
    graph.add_edge(150, "Dumbledore", "Harry", layer="communication")
    graph.add_edge(200, "Dumbledore", "Harry", properties={"weight": 0.5}, layer="friendship")
    graph.add_edge(300, "Dumbledore", "Harry", properties={"weight": 0.7}, layer="communication")
    graph.add_edge(350, "Dumbledore", "Harry", properties={"weight": 0.9}, layer="friendship")
    return graph


def test_history_node():
    # FIXME: When viewing graph, "Field "history" of type "History" must have a selection of subfields"
    # FIXME: GraphQL shows edge count: 1, but we added the Dumbledore -> Harry edge 4 times
    graph = create_graph()
    query = """
        {
            graph(path:"harry_potter"){
  	            node(name: "Dumbledore"){
                    history{
                        timestamps
                    }
                }
            }
	    }
    """
    expected_output = {'graph': {'node': {'history': {'timestamps': [100, 150, 200, 200, 300, 300, 350]}}}}
    run_graphql_test(query, expected_output, graph)


def test_history_edge():
    graph = create_graph()
    query = """
        {
            graph(path: "harry_potter") {
                edge(src: "Dumbledore", dst: "Harry") {
                    history {
                        timestamps
                    }
                }
            }
        }
    """
    expected_output = {'graph': {'edge': {'history': {'timestamps': [150, 200, 300, 350]}}}}
    run_graphql_test(query, expected_output, graph)


def test_history_window_node():
    graph = create_graph()
    # window(0, 150)
    query = """
        {
            graph(path: "harry_potter") {
                window(start: 0, end: 150) {
                    node(name: "Dumbledore") {
                        history {
                            timestamps
                        }
                    }
                }
            }
        }
    """
    expected_output_1 = {'graph': {'window': {'node': {'history': {'timestamps': [100]}}}}}
    run_graphql_test(query, expected_output_1, graph)

    # window(150, 300)
    query = """
        {
            graph(path: "harry_potter") {
                window(start: 150, end: 300) {
                    node(name: "Dumbledore") {
                        history {
                            timestamps
                        }
                    }
                }
            }
        }
    """
    expected_output_2 = {'graph': {'window': {'node': {'history': {'timestamps': [150, 200, 200]}}}}}
    run_graphql_test(query, expected_output_2, graph)

    # window(300, 450)
    query = """
        {
            graph(path: "harry_potter") {
                window(start: 300, end: 450) {
                    node(name: "Dumbledore") {
                        history {
                            timestamps
                        }
                    }
                }
            }
        }
    """
    expected_output_3 = {'graph': {'window': {'node': {'history': {'timestamps': [300, 300, 350]}}}}}
    run_graphql_test(query, expected_output_3, graph)


def test_history_window_edge():
    graph = create_graph()
    # window(0, 150)
    query = """
    {
      graph(path: "harry_potter") {
        window(start: 0, end: 150) {
          edge(src: "Dumbledore", dst: "Harry") {
            history {
              timestamps
            }
          }
        }
      }
    }
    """
    expected_output_1 = {'graph': {'window': {'edge': None}}}
    run_graphql_test(query, expected_output_1, graph)

    # window(150, 300)
    query = """
    {
      graph(path: "harry_potter") {
        window(start: 150, end: 300) {
          edge(src: "Dumbledore", dst: "Harry") {
            history {
              timestamps
            }
          }
        }
      }
    }
    """
    expected_output_2 = {'graph': {'window': {'edge': {'history': {'timestamps': [150, 200]}}}}}
    run_graphql_test(query, expected_output_2, graph)

    # window(300, 450)
    query = """
    {
      graph(path: "harry_potter") {
        window(start: 300, end: 450) {
          edge(src: "Dumbledore", dst: "Harry") {
            history {
              timestamps
            }
          }
        }
      }
    }
    """
    expected_output_3 = {'graph': {'window': {'edge': {'history': {'timestamps': [300, 350]}}}}}
    run_graphql_test(query, expected_output_3, graph)

def test_history_layer_node():
    graph = create_graph()
    query = """
    {
        graph(path: "harry_potter") {
            layer(name: "friendship") {
                node(name: "Dumbledore") {
                    history {
                        timestamps
                    }
                }
            }
        }
    }
    """
    # FIXME: Currently fails because the layer doesn't affect node history
    expected_output = {'graph': {'layer': {'node': {'history': {'timestamps': [100, 200, 200, 300, 350]}}}}}
    # run_graphql_test(query, expected_output, graph)

def test_history_layer_edge():
    graph = create_graph()
    query = """
    {
      graph(path: "harry_potter") {
        layer(name: "communication") {
          edge(src: "Dumbledore", dst: "Harry") {
            history {
              timestamps
            }
          }
        }
      }
    }
    """
    expected_output = {'graph': {'layer': {'edge': {'history': {'timestamps': [150, 300]}}}}}
    run_graphql_test(query, expected_output, graph)


def test_history_prop_filter_edge():
    graph = create_graph()
    # the edge only has one value associated to "weight"
    # even tho we updated the edge with multiple different weights at different timestamps, there is only 0.9 (the latest)
    query_1 = """
    {
      graph(path: "harry_potter") {
        edgeFilter(
          property: "weight"
          condition: {operator: EQUAL, value: {f64: 0.9}}) {
          edge(src: "Dumbledore", dst: "Harry") {
            history {
              timestamps
            }
          }
        }
      }
    }
    """
    expected_output_1 = {'graph': {'edgeFilter': {'edge': {'history': {'timestamps': [150, 200, 300, 350]}}}}}
    run_graphql_test(query_1, expected_output_1, graph)

    # other weights should have no hits because the weight is updated, not appended
    query_2 = """
    {
      graph(path: "harry_potter") {
        edgeFilter(
          property: "weight"
          condition: {operator: EQUAL, value: {f64: 0.7}}) {
          edge(src: "Dumbledore", dst: "Harry") {
            history {
              timestamps
            }
          }
        }
      }
    }
    """
    expected_output_2 = {'graph': {'edgeFilter': {'edge': None}}}
    run_graphql_test(query_2, expected_output_2, graph)

def test_history_prop_filter_node():
    graph = create_graph()
    # the edge only has one value associated to "weight"
    # even tho we updated the edge with multiple different weights at different timestamps, there is only 0.9 (the latest)
    query_1 = """
    {
      graph(path: "harry_potter") {
        nodeFilter(
          property: "Age" 
          condition: {operator: LESS_THAN, value: {i64: 30}}) {
          node(name: "Dumbledore") {
            history {
              timestamps
            }
          }
        }
      }
    }
    """
    expected_output_1 = {'graph': {'nodeFilter': {'node': None}}}
    run_graphql_test(query_1, expected_output_1, graph)

    # other weights should have no hits because the weight is updated, not appended
    query_2 = """
    {
      graph(path: "harry_potter") {
        nodeFilter(
          property: "Age" 
          condition: {operator: GREATER_THAN, value: {i64: 30}}) {
          node(name: "Harry") {
            history {
              timestamps
            }
          }
        }
      }
    }
    """
    expected_output_2 = {'graph': {'nodeFilter': {'node': None}}}
    run_graphql_test(query_2, expected_output_2, graph)