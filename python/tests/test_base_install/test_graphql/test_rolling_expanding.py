import tempfile

import pytest

from raphtory.graphql import GraphServer
from raphtory import Graph, PersistentGraph
import json
import re


def create_graph_epoch(g):
    g.add_edge(1, 1, 2)
    g.add_edge(2, 1, 2)
    g.add_edge(3, 1, 2)
    g.add_edge(2, 1, 3)
    g.add_edge(3, 1, 3)
    g.add_edge(4, 1, 3)


def run_graphql_test(query, expected_output, graph):
    tmp_work_dir = tempfile.mkdtemp()
    with GraphServer(tmp_work_dir).start() as server:
        client = server.get_client()
        client.send_graph(path="g", graph=graph)

        response = client.query(query)

        # Convert response to a dictionary if needed and compare
        response_dict = json.loads(response) if isinstance(response, str) else response
        assert response_dict == expected_output


def run_graphql_error_test(query, expected_error_message, graph):
    tmp_work_dir = tempfile.mkdtemp()
    with GraphServer(tmp_work_dir).start() as server:
        client = server.get_client()
        client.send_graph(path="g", graph=graph)

        with pytest.raises(Exception) as excinfo:
            client.query(query)

        full_error_message = str(excinfo.value)
        match = re.search(r'"message":"(.*?)"', full_error_message)
        error_message = match.group(1) if match else ""

        assert (
            error_message == expected_error_message
        ), f"Expected '{expected_error_message}', but got '{error_message}'"


# def test_graph_rolling(graph):
#     query = """
#         {
#           graph(path: "g") {
#             rolling(
#             nodes(ids: ["0"]) {
#               list {
#                 id
#                 degree
#               }
#             }
#           }
#         }
#     """
#     expected_output = {"graph": {"nodes": {"list": [{"id": "0", "degree": 1}]}}}
#     run_graphql_test(query, expected_output, graph)
