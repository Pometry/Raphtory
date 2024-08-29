import os
import tempfile

import pytest

from raphtory import Graph
from raphtory.graphql import GraphServer, RaphtoryClient


def test_copy_graph_fails_if_graph_not_found():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")

        query = """mutation {
          copyGraph(
            path: "ben/g5",
            newPath: "g6",
          )
        }"""
        with pytest.raises(Exception) as excinfo:
            client.query(query)
        assert "Graph not found" in str(excinfo.value)


def test_copy_graph_fails_if_graph_with_same_name_already_exists():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    work_dir = tempfile.mkdtemp()
    os.makedirs(os.path.join(work_dir, "ben"), exist_ok=True)
    g.save_to_file(os.path.join(work_dir, "ben", "g5"))
    g.save_to_file(os.path.join(work_dir, "g6"))

    with GraphServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")

        query = """mutation {
          copyGraph(
            path: "ben/g5",
            newPath: "g6",
          )
        }"""
        with pytest.raises(Exception) as excinfo:
            client.query(query)
        assert "Graph already exists by name" in str(excinfo.value)


def test_copy_graph_fails_if_graph_with_same_name_already_exists_at_same_namespace_as_graph():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    work_dir = tempfile.mkdtemp()
    os.makedirs(os.path.join(work_dir, "ben"), exist_ok=True)
    g.save_to_file(os.path.join(work_dir, "ben", "g5"))
    g.save_to_file(os.path.join(work_dir, "ben", "g6"))

    with GraphServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")

        query = """mutation {
          copyGraph(
            path: "ben/g5",
            newPath: "ben/g6",
          )
        }"""
        with pytest.raises(Exception) as excinfo:
            client.query(query)
        assert "Graph already exists by name" in str(excinfo.value)


def test_copy_graph_fails_if_graph_with_same_name_already_exists_at_diff_namespace_as_graph():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    work_dir = tempfile.mkdtemp()
    os.makedirs(os.path.join(work_dir, "ben"), exist_ok=True)
    os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)
    g.save_to_file(os.path.join(work_dir, "ben", "g5"))
    g.save_to_file(os.path.join(work_dir, "shivam", "g6"))

    with GraphServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")

        query = """mutation {
          copyGraph(
            path: "ben/g5",
            newPath: "shivam/g6",
          )
        }"""
        with pytest.raises(Exception) as excinfo:
            client.query(query)
        assert "Graph already exists by name" in str(excinfo.value)


def test_copy_graph_succeeds():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    work_dir = tempfile.mkdtemp()
    os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)
    g.save_to_file(os.path.join(work_dir, "shivam", "g3"))

    with GraphServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")

        # Assert if copy graph succeeds and old graph is retained
        query = """mutation {
          copyGraph(
            path: "shivam/g3",
            newPath: "g4",
          )
        }"""
        client.query(query)

        query = """{graph(path: "shivam/g3") { nodes {list {name}} }}"""
        result = client.query(query)
        assert result["graph"]["nodes"]["list"] == [
            {"name": "ben"},
            {"name": "hamza"},
            {"name": "haaroon"},
        ]

        query = """{graph(path: "g4") {
                nodes {list {name}}
                properties {
                    constant {
                        lastOpened: get(key: "lastOpened") { value }
                    }
                }
            }}"""

        result = client.query(query)
        assert result["graph"]["nodes"]["list"] == [
            {"name": "ben"},
            {"name": "hamza"},
            {"name": "haaroon"},
        ]
        assert (
            result["graph"]["properties"]["constant"]["lastOpened"]["value"] is not None
        )


def test_copy_graph_using_client_api_succeeds():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    work_dir = tempfile.mkdtemp()
    os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)
    g.save_to_file(os.path.join(work_dir, "shivam", "g3"))

    with GraphServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")

        # Assert if copy graph succeeds and old graph is retained
        client.copy_graph("shivam/g3", "ben/g4")

        query = """{graph(path: "shivam/g3") { nodes {list {name}} }}"""
        result = client.query(query)
        assert result["graph"]["nodes"]["list"] == [
            {"name": "ben"},
            {"name": "hamza"},
            {"name": "haaroon"},
        ]

        query = """{graph(path: "ben/g4") {
                nodes {list {name}}
                properties {
                    constant {
                        lastOpened: get(key: "lastOpened") { value }
                    }
                }
            }}"""

        result = client.query(query)
        assert result["graph"]["nodes"]["list"] == [
            {"name": "ben"},
            {"name": "hamza"},
            {"name": "haaroon"},
        ]
        assert (
            result["graph"]["properties"]["constant"]["lastOpened"]["value"] is not None
        )


def test_copy_graph_succeeds_at_same_namespace_as_graph():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    work_dir = tempfile.mkdtemp()
    os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)

    g.save_to_file(os.path.join(work_dir, "shivam", "g3"))

    with GraphServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")

        # Assert if rename graph succeeds and old graph is deleted
        query = """mutation {
          copyGraph(
            path: "shivam/g3",
            newPath: "shivam/g4",
          )
        }"""
        client.query(query)

        query = """{graph(path: "shivam/g3") { nodes {list {name}} }}"""
        result = client.query(query)
        assert result["graph"]["nodes"]["list"] == [
            {"name": "ben"},
            {"name": "hamza"},
            {"name": "haaroon"},
        ]

        query = """{graph(path: "shivam/g4") {
                nodes {list {name}}
                properties {
                    constant {
                        lastOpened: get(key: "lastOpened") { value }
                    }
                }
            }}"""

        result = client.query(query)
        assert result["graph"]["nodes"]["list"] == [
            {"name": "ben"},
            {"name": "hamza"},
            {"name": "haaroon"},
        ]
        assert (
            result["graph"]["properties"]["constant"]["lastOpened"]["value"] is not None
        )


def test_copy_graph_succeeds_at_diff_namespace_as_graph():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    work_dir = tempfile.mkdtemp()
    os.makedirs(os.path.join(work_dir, "ben"), exist_ok=True)
    os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)

    g.save_to_file(os.path.join(work_dir, "ben", "g3"))

    with GraphServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")

        # Assert if rename graph succeeds and old graph is deleted
        query = """mutation {
          copyGraph(
            path: "ben/g3",
            newPath: "shivam/g4",
          )
        }"""
        client.query(query)

        query = """{graph(path: "ben/g3") { nodes {list {name}} }}"""
        result = client.query(query)
        assert result["graph"]["nodes"]["list"] == [
            {"name": "ben"},
            {"name": "hamza"},
            {"name": "haaroon"},
        ]

        query = """{graph(path: "shivam/g4") {
                nodes {list {name}}
                properties {
                    constant {
                        lastOpened: get(key: "lastOpened") { value }
                    }
                }
            }}"""

        result = client.query(query)
        assert result["graph"]["nodes"]["list"] == [
            {"name": "ben"},
            {"name": "hamza"},
            {"name": "haaroon"},
        ]
        assert (
            result["graph"]["properties"]["constant"]["lastOpened"]["value"] is not None
        )
