import os
import tempfile

from raphtory import Graph
from raphtory.graphql import GraphServer, RaphtoryClient
import pytest


def test_send_graph_succeeds_if_no_graph_found_with_same_name():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    tmp_work_dir = tempfile.mkdtemp()
    with GraphServer(tmp_work_dir).start():
        client = RaphtoryClient("http://localhost:1736")
        client.send_graph(path="g", graph=g)


def test_send_graph_fails_if_graph_already_exists():
    tmp_work_dir = tempfile.mkdtemp()

    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")
    g.save_to_file(os.path.join(tmp_work_dir, "g"))

    with GraphServer(tmp_work_dir).start():
        client = RaphtoryClient("http://localhost:1736")
        with pytest.raises(Exception) as excinfo:
            client.send_graph(path="g", graph=g)
        assert "Graph already exists by name = g" in str(excinfo.value)


def test_send_graph_succeeds_if_graph_already_exists_with_overwrite_enabled():
    tmp_work_dir = tempfile.mkdtemp()

    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")
    g.save_to_file(os.path.join(tmp_work_dir, "g"))

    with GraphServer(tmp_work_dir).start():
        client = RaphtoryClient("http://localhost:1736")

        g = Graph()
        g.add_edge(1, "ben", "hamza")
        g.add_edge(2, "haaroon", "hamza")
        g.add_edge(3, "ben", "haaroon")
        g.add_edge(4, "ben", "shivam")
        client.send_graph(path="g", graph=g, overwrite=True)

        query = """{graph(path: "g") {nodes {list {name}}}}"""
        assert client.query(query) == {
            "graph": {
                "nodes": {
                    "list": [
                        {"name": "ben"},
                        {"name": "hamza"},
                        {"name": "haaroon"},
                        {"name": "shivam"},
                    ]
                }
            }
        }


def test_send_graph_succeeds_if_no_graph_found_with_same_name_at_namespace():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    tmp_work_dir = tempfile.mkdtemp()
    with GraphServer(tmp_work_dir).start():
        client = RaphtoryClient("http://localhost:1736")
        client.send_graph(path="shivam/g", graph=g)


def test_send_graph_fails_if_graph_already_exists_at_namespace():
    tmp_work_dir = tempfile.mkdtemp()

    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")
    os.makedirs(os.path.join(tmp_work_dir, "shivam"), exist_ok=True)
    g.save_to_file(os.path.join(tmp_work_dir, "shivam", "g"))

    with GraphServer(tmp_work_dir).start():
        client = RaphtoryClient("http://localhost:1736")
        with pytest.raises(Exception) as excinfo:
            client.send_graph(path="shivam/g", graph=g)
        assert "Graph already exists by name" in str(excinfo.value)


def test_send_graph_succeeds_if_graph_already_exists_at_namespace_with_overwrite_enabled():
    tmp_work_dir = tempfile.mkdtemp()

    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")
    os.makedirs(os.path.join(tmp_work_dir, "shivam"), exist_ok=True)
    g.save_to_file(os.path.join(tmp_work_dir, "shivam", "g"))

    with GraphServer(tmp_work_dir).start():
        client = RaphtoryClient("http://localhost:1736")

        g = Graph()
        g.add_edge(1, "ben", "hamza")
        g.add_edge(2, "haaroon", "hamza")
        g.add_edge(3, "ben", "haaroon")
        g.add_edge(4, "ben", "shivam")
        client.send_graph(path="shivam/g", graph=g, overwrite=True)

        query = """{graph(path: "shivam/g") {nodes {list {name}}}}"""
        assert client.query(query) == {
            "graph": {
                "nodes": {
                    "list": [
                        {"name": "ben"},
                        {"name": "hamza"},
                        {"name": "haaroon"},
                        {"name": "shivam"},
                    ]
                }
            }
        }
