import base64
import os
import tempfile

import pytest

from raphtory import Graph
from raphtory.graphql import GraphServer


def test_receive_graph_fails_if_no_graph_found():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start() as server:
        client = server.get_client()

        query = """{ receiveGraph(path: "g2") }"""
        with pytest.raises(Exception) as excinfo:
            client.query(query)
        assert "Graph not found" in str(excinfo.value)


def test_receive_graph_succeeds_if_graph_found():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start() as server:
        client = server.get_client()

        g = Graph()
        g.add_edge(1, "ben", "hamza")
        g.add_edge(2, "haaroon", "hamza")
        g.add_edge(3, "ben", "haaroon")

        g.save_to_file(os.path.join(work_dir, "g1"))

        query = """{ receiveGraph(path: "g1") }"""
        received_graph = client.query(query)["receiveGraph"]

        decoded_bytes = base64.b64decode(received_graph)
        g = Graph.deserialise(decoded_bytes)
        assert g.nodes.name == ["ben", "hamza", "haaroon"]


def test_receive_graph_using_client_api_succeeds_if_graph_found():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start() as server:
        client = server.get_client()

        g = Graph()
        g.add_edge(1, "ben", "hamza")
        g.add_edge(2, "haaroon", "hamza")
        g.add_edge(3, "ben", "haaroon")

        g.save_to_file(os.path.join(work_dir, "g1"))
        received_graph = client.receive_graph("g1")
        assert received_graph.nodes.name == ["ben", "hamza", "haaroon"]


def test_receive_graph_fails_if_no_graph_found_at_namespace():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start() as server:
        client = server.get_client()

        query = """{ receiveGraph(path: "shivam/g2") }"""
        with pytest.raises(Exception) as excinfo:
            client.query(query)
        assert "Graph not found" in str(excinfo.value)


def test_receive_graph_succeeds_if_graph_found_at_namespace():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start() as server:
        client = server.get_client()

        g = Graph()
        g.add_edge(1, "ben", "hamza")
        g.add_edge(2, "haaroon", "hamza")
        g.add_edge(3, "ben", "haaroon")

        os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)
        g.save_to_file(os.path.join(work_dir, "shivam", "g2"))

        query = """{ receiveGraph(path: "shivam/g2") }"""
        received_graph = client.query(query)["receiveGraph"]

        decoded_bytes = base64.b64decode(received_graph)

        g = Graph.deserialise(decoded_bytes)
        assert g.nodes.name == ["ben", "hamza", "haaroon"]