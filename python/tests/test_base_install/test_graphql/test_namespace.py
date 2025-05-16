import tempfile
from threading import Thread

import pytest
from raphtory import Graph
from raphtory.graphql import GraphServer, RaphtoryClient

import json


def make_folder_structure(client):
    g = Graph()
    g.add_node(1, 1)
    client.send_graph("graph", g, overwrite=True)
    client.send_graph("test/graph", g, overwrite=True)
    client.send_graph("test/first/internal/graph", g, overwrite=True)
    client.send_graph("test/second/internal/graph1", g, overwrite=True)
    client.send_graph("test/second/internal/graph2", g, overwrite=True)


def sort_dict(d):
    """Recursively sort lists inside a dictionary to enable order-independent comparison."""
    if isinstance(d, dict):
        return {k: sort_dict(v) for k, v in d.items()}
    elif isinstance(d, list):
        return sorted(
            (sort_dict(i) for i in d), key=json.dumps
        )  # Sorting by JSON string representation
    else:
        return d


def test_children():
    work_dir = tempfile.mkdtemp()

    with GraphServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")
        make_folder_structure(client)

        query = """{
                      root {
                        path
                        graphs {
                          path
                        }
                        children {
                          path
                          graphs {
                            path
                          }
                          children {
                            path
                            graphs {
                              path
                            }
                            children {
                              path
                              graphs {
                                path
                              }
                              children {
                                path
                                graphs {
                                  path
                                }
                                children {
                                  path
                                }
                              }
                            }
                          }
                        }
                      }
                    }
            """
        result = client.query(query)

        correct = {
            "root": {
                "path": "",
                "graphs": [{"path": "graph"}],
                "children": [
                    {
                        "path": "test",
                        "graphs": [{"path": "test/graph"}],
                        "children": [
                            {
                                "path": "test/first",
                                "graphs": [],
                                "children": [
                                    {
                                        "path": "test/first/internal",
                                        "graphs": [
                                            {"path": "test/first/internal/graph"}
                                        ],
                                        "children": [],
                                    }
                                ],
                            },
                            {
                                "path": "test/second",
                                "graphs": [],
                                "children": [
                                    {
                                        "path": "test/second/internal",
                                        "graphs": [
                                            {"path": "test/second/internal/graph2"},
                                            {"path": "test/second/internal/graph1"},
                                        ],
                                        "children": [],
                                    }
                                ],
                            },
                        ],
                    }
                ],
            }
        }

        assert sort_dict(result) == sort_dict(correct)


def test_escaping_parent():
    work_dir = tempfile.mkdtemp()

    with GraphServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")
        make_folder_structure(client)

        query = """{
                      root {
                        parent {
                          path
                        }
                      }
                      namespace(path: "") {
                        parent {
                          path
                        }
                      }
                      internal: namespace(path: "test") {
                        parent {
                          path
                          parent {
                            path
                          }
                        }
                      }
                    }
            """
        result = client.query(query)
        correct = {
            "root": {"parent": None},
            "namespace": {"parent": None},
            "internal": {"parent": {"path": "", "parent": None}},
        }

        assert sort_dict(result) == sort_dict(correct)


def test_wrong_paths():
    work_dir = tempfile.mkdtemp()

    with GraphServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")
        make_folder_structure(client)

        query = """{
                      namespace(path: "not real") {
                        children {
                          path
                        }
                      }
                    }
            """
        with pytest.raises(Exception) as excinfo:
            client.query(query)
            assert "The path provided does not exists as a namespace: not real" in str(
                excinfo.value
            )

        query = """{
                  namespace(path: "/test") {
                    children {
                      path
                    }
                  }
                }
            """
        with pytest.raises(Exception) as excinfo:
            client.query(query)
            assert (
                "Only relative paths are allowed to be used within the working_dir: /test"
                in str(excinfo.value)
            )

        query = """{
                  namespace(path: "/test/../..") {
                    children {
                      path
                    }
                  }
                }
            """
        with pytest.raises(Exception) as excinfo:
            client.query(query)
            assert (
                "References to the parent dir are not allowed within the path: test/../../"
                in str(excinfo.value)
            )

        query = """{
                  namespace(path: "./test") {
                    children {
                      path
                    }
                  }
                }
            """
        with pytest.raises(Exception) as excinfo:
            client.query(query)
            assert (
                "References to the current dir are not allowed within the path: ./test"
                in str(excinfo.value)
            )

        query = """{
                  namespace(path: ""test/second/internal/graph1"") {
                    children {
                      path
                    }
                  }
                }
            """
        with pytest.raises(Exception) as excinfo:
            client.query(query)
            assert (
                "The path to the graph contains a subpath to an existing graph: test/second/internal/graph1"
                in str(excinfo.value)
            )


def test_namespaces():
    work_dir = tempfile.mkdtemp()

    with GraphServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")
        make_folder_structure(client)
        import time

        time.sleep(500)
        query = """
            {
                namespaces{
                graphs{
                    name
                  }
                  path
                }
                
            }"""
        result = client.query(query)
        correct = {}

        assert sort_dict(result) == sort_dict(correct)
