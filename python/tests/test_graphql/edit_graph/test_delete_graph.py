import os
import tempfile

import pytest
from raphtory import Graph
from raphtory.graphql import GraphServer, RaphtoryClient


def test_delete_graph_fails_if_graph_not_found():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")

        query = """mutation {
          deleteGraph(
            path: "ben/g5",
          )
        }"""
        with pytest.raises(Exception) as excinfo:
            client.query(query)
        assert "Graph not found" in str(excinfo.value)


def test_delete_graph_succeeds_if_graph_found():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")

        g = Graph()
        g.add_edge(1, "ben", "hamza")
        g.add_edge(2, "haaroon", "hamza")
        g.add_edge(3, "ben", "haaroon")

        g.save_to_file(os.path.join(work_dir, "g1"))

        query = """mutation {
          deleteGraph(
            path: "g1",
          )
        }"""
        client.query(query)

        query = """{graph(path: "g1") {nodes {list {name}}}}"""
        with pytest.raises(Exception) as excinfo:
            client.query(query)
        assert "Graph not found" in str(excinfo.value)


def test_delete_graph_using_client_api_succeeds_if_graph_found():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")

        g = Graph()
        g.add_edge(1, "ben", "hamza")
        g.add_edge(2, "haaroon", "hamza")
        g.add_edge(3, "ben", "haaroon")

        g.save_to_file(os.path.join(work_dir, "g1"))
        client.delete_graph("g1")

        query = """{graph(path: "g1") {nodes {list {name}}}}"""
        with pytest.raises(Exception) as excinfo:
            client.query(query)
        assert "Graph not found" in str(excinfo.value)


def test_delete_graph_succeeds_if_graph_found_at_namespace():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")

        g = Graph()
        g.add_edge(1, "ben", "hamza")
        g.add_edge(2, "haaroon", "hamza")
        g.add_edge(3, "ben", "haaroon")

        os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)
        g.save_to_file(os.path.join(work_dir, "shivam", "g1"))

        query = """mutation {
          deleteGraph(
            path: "shivam/g1",
          )
        }"""
        client.query(query)
        query = """{graph(path: "g1") {nodes {list {name}}}}"""
        with pytest.raises(Exception) as excinfo:
            client.query(query)
        assert "Graph not found" in str(excinfo.value)
