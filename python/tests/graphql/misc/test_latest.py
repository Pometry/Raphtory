from raphtory.graphql import RaphtoryClient


def test_latest_and_active():
    from raphtory.graphql import GraphServer
    from raphtory import Graph
    import tempfile

    query = """
        {
          graph(path: "graph") {
            node(name: "1") {
              name
              isActive
              latest {
                history
              }
            }
            e12: edge(src: "1", dst: "2") {
              isActive
              latest {
                history
              }
            }
            e13: edge(src: "1", dst: "3") {
              latest {
                isActive
                history
              }
            }
            nodes {
              list {
                name
                edges {
                  latest {
                    list {
                      history
                    }
                  }
                }
              }
              latest {
                list {
                  name
                  history
                }
              }
            }
            edges {
              latest {
                list {
                  history
                }
              }
            }
          }
        }
    """

    result = {
        "graph": {
            "node": {"name": "1", "isActive": True, "latest": {"history": [3]}},
            "e12": {"isActive": True, "latest": {"history": [3]}},
            "e13": {"latest": {"isActive": False, "history": []}},
            "nodes": {
                "list": [
                    {
                        "name": "1",
                        "edges": {
                            "latest": {
                                "list": [
                                    {"history": [3]},
                                    {"history": []},
                                    {"history": [3]},
                                ]
                            }
                        },
                    },
                    {"name": "2", "edges": {"latest": {"list": [{"history": [3]}]}}},
                    {"name": "3", "edges": {"latest": {"list": [{"history": []}]}}},
                    {"name": "4", "edges": {"latest": {"list": [{"history": [3]}]}}},
                ],
                "latest": {
                    "list": [
                        {"name": "1", "history": [3]},
                        {"name": "2", "history": [3]},
                        {"name": "4", "history": [3]},
                    ]
                },
            },
            "edges": {
                "latest": {
                    "list": [{"history": [3]}, {"history": []}, {"history": [3]}]
                }
            },
        }
    }

    work_dir = tempfile.mkdtemp()
    g = Graph()
    g.add_edge(1, 1, 2, {"int_prop": 123})
    g.add_edge(2, 1, 2, {"int_prop": 124})
    g.add_edge(3, 1, 2, {"int_prop": 125})

    g.add_edge(1, 1, 3, {"int_prop": 123})
    g.add_edge(2, 1, 3, {"int_prop": 124})
    g.add_edge(3, 1, 4, {"int_prop": 125})

    g.add_node(1, 1, {"int_prop": 123})
    g.add_node(2, 1, {"int_prop": 124})
    g.add_node(1, 2, {"int_prop": 125})
    g.add_node(2, 2, {"int_prop": 125})

    g.save_to_file(work_dir + "/graph")
    with GraphServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")
        assert client.query(query) == result
