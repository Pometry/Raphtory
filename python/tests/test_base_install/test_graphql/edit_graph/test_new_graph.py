import os
import tempfile

import pytest
from raphtory import Graph
from raphtory.graphql import GraphServer, RaphtoryClient


def test_new_graph_succeeds():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")

        query = """mutation {
          newGraph(
            path: "test/path/g1",
            graphType: EVENT
          )
        }"""
        client.query(query)

        query = """{graph(path: "test/path/g1") {nodes {list {name}}}}"""
        client.query(query)


def test_new_graph_fails_if_graph_found():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")

        g = Graph()
        g.add_edge(1, "ben", "hamza")
        g.add_edge(2, "haaroon", "hamza")
        g.add_edge(3, "ben", "haaroon")

        file_path = os.path.join(work_dir, "test/path/g1")
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        g.save_to_file(file_path)

        query = """mutation {
          newGraph(
            path: "test/path/g1",
            graphType: EVENT
          )
        }"""
        with pytest.raises(Exception) as excinfo:
            client.query(query)
        assert "Graph already exists by name" in str(excinfo.value)


def test_client_new_graph_works():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")
        client.new_graph("path/to/event_graph", "EVENT")
        client.new_graph("path/to/persistent_graph", "PERSISTENT")

        query = """{graph(path: "path/to/event_graph") {nodes {list {name}}}}"""
        client.query(query)
        query = """{graph(path: "path/to/persistent_graph") {nodes {list {name}}}}"""
        client.query(query)


def test_client_new_graph_broken_type():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")
        with pytest.raises(Exception) as excinfo:
            client.new_graph("path/to/event_graph", "EVENdddT")
        assert "Invalid value for argument" in str(excinfo.value)
