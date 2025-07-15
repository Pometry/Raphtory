import json
import tempfile

import pytest
from raphtory import Graph
from raphtory.graphql import GraphServer, RaphtoryClient


def make_folder_structure(client):
    g = Graph()
    g.add_node(1, 1)
    client.send_graph("graph", g, overwrite=True)
    client.send_graph("test/graph", g, overwrite=True)
    client.send_graph("test/first/internal/graph", g, overwrite=True)
    client.send_graph("test/second/internal/graph1", g, overwrite=True)
    client.send_graph("test/second/internal/graph2", g, overwrite=True)


def sort_dict(d):
    """Recursively sort lists *and* dictionary keys for order-independent comparison."""
    if isinstance(d, dict):
        return dict(sorted((k, sort_dict(v)) for k, v in d.items()))
    elif isinstance(d, list):
        return sorted((sort_dict(i) for i in d), key=json.dumps)
    else:
        return d


def test_namespaces_and_metagraph():
    work_dir = tempfile.mkdtemp()

    with GraphServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")
        make_folder_structure(client)

        # tests list and page on namespaces and metagraphs
        query = """{
          root {
            path
            graphs {
              list {
                path
              }
            }
            children {
              list {
                path
                graphs {
                  list {
                    path
                  }
                }
                children {
                  page(limit: 1, offset: 1) {
                    path
                    children {
                      list {
                        graphs {
                          page(limit: 1, offset: 1) {
                            path
                          }
                        }
                      }
                    }
                  }
                  list {
                    path
                    graphs {
                      list {
                        path
                      }
                    }
                    children {
                      list {
                        path
                        graphs {
                          list {
                            path
                          }
                        }
                        children {
                          list {
                            path
                            graphs {
                              list {
                                path
                              }
                            }
                            children {
                              list {
                                path
                              }
                            }
                          }
                        }
                      }
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
            "graphs": {"list": [{"path": "graph"}]},
            "children": {
                "list": [
                    {
                        "path": "test",
                        "graphs": {"list": [{"path": "test/graph"}]},
                        "children": {
                            "page": [
                                {
                                    "path": "test/second",
                                    "children": {
                                        "list": [
                                            {
                                                "graphs": {
                                                    "page": [
                                                        {
                                                            "path": "test/second/internal/graph2"
                                                        }
                                                    ]
                                                }
                                            }
                                        ]
                                    },
                                }
                            ],
                            "list": [
                                {
                                    "path": "test/first",
                                    "graphs": {"list": []},
                                    "children": {
                                        "list": [
                                            {
                                                "path": "test/first/internal",
                                                "graphs": {
                                                    "list": [
                                                        {
                                                            "path": "test/first/internal/graph"
                                                        }
                                                    ]
                                                },
                                                "children": {"list": []},
                                            }
                                        ]
                                    },
                                },
                                {
                                    "path": "test/second",
                                    "graphs": {"list": []},
                                    "children": {
                                        "list": [
                                            {
                                                "path": "test/second/internal",
                                                "graphs": {
                                                    "list": [
                                                        {
                                                            "path": "test/second/internal/graph1"
                                                        },
                                                        {
                                                            "path": "test/second/internal/graph2"
                                                        },
                                                    ]
                                                },
                                                "children": {"list": []},
                                            }
                                        ]
                                    },
                                },
                            ],
                        },
                    }
                ]
            },
        }
    }

    assert sort_dict(result) == sort_dict(correct)


def test_counting():
    work_dir = tempfile.mkdtemp()

    with GraphServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")
        make_folder_structure(client)

        query = """
            {
              root {
                children{
                  count
                }
              }
              namespaces{
                page(limit:2 offset:4){
                  path
                  graphs{
                    count
                  }
                }
              }
            }
        """

        result = client.query(query)
        correct = {
            "root": {"children": {"count": 1}},
            "namespaces": {
                "page": [
                    {"path": "test/second", "graphs": {"count": 0}},
                    {"path": "test/second/internal", "graphs": {"count": 2}},
                ]
            },
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

        query = """
         {
          namespaces {
            list {
              path
              children {
                page(limit: 5, offset: 0) {
                  path
                }
              }
              graphs {
                list {
                  name
                  path
                }
              }
            }
          }
        }"""
        result = client.query(query)
        correct = {
            "namespaces": {
                "list": [
                    {
                        "path": "",
                        "children": {"page": [{"path": "test"}]},
                        "graphs": {"list": [{"name": "graph", "path": "graph"}]},
                    },
                    {
                        "path": "test",
                        "children": {
                            "page": [{"path": "test/first"}, {"path": "test/second"}]
                        },
                        "graphs": {"list": [{"name": "graph", "path": "test/graph"}]},
                    },
                    {
                        "path": "test/first",
                        "children": {"page": [{"path": "test/first/internal"}]},
                        "graphs": {"list": []},
                    },
                    {
                        "path": "test/first/internal",
                        "children": {"page": []},
                        "graphs": {
                            "list": [
                                {"name": "graph", "path": "test/first/internal/graph"}
                            ]
                        },
                    },
                    {
                        "path": "test/second",
                        "children": {"page": [{"path": "test/second/internal"}]},
                        "graphs": {"list": []},
                    },
                    {
                        "path": "test/second/internal",
                        "children": {"page": []},
                        "graphs": {
                            "list": [
                                {
                                    "name": "graph1",
                                    "path": "test/second/internal/graph1",
                                },
                                {
                                    "name": "graph2",
                                    "path": "test/second/internal/graph2",
                                },
                            ]
                        },
                    },
                ]
            }
        }

        assert result == correct
