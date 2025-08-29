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
                history {
                  list {
                    timestamp
                    secondaryIndex
                  }
                }
              }
            }
            e12: edge(src: "1", dst: "2") {
              isActive
              latest {
                history {
                  list {
                    timestamp
                    secondaryIndex
                  }
                }
              }
            }
            e13: edge(src: "1", dst: "3") {
              latest {
                isActive
                history {
                  list {
                    timestamp
                    secondaryIndex
                  }
                }
              }
            }
            nodes {
              list {
                name
                edges {
                  latest {
                    list {
                      history {
                        list {
                          timestamp
                          secondaryIndex
                        }
                      }
                    }
                  }
                }
              }
              latest {
                list {
                  name
                  history {
                    list {
                      timestamp
                      secondaryIndex
                    }
                  }
                }
              }
            }
            edges {
              latest {
                list {
                  history {
                    list {
                      timestamp
                      secondaryIndex
                    }
                  }
                }
              }
            }
          }
        }
    """

    result = {
        "graph": {
            "node": {
                "name": "1",
                "isActive": True,
                "latest": {
                    "history": {
                        "list": [{"timestamp": 3, "secondaryIndex": 2}, {"timestamp": 3, "secondaryIndex": 5}
                        ]
                    }
                }
            },
            "e12": {
                "isActive": True,
                "latest": {
                    "history": {
                        "list": [{"timestamp": 3, "secondaryIndex": 2}]
                    }
                }
            },
            "e13": {
                "latest": {
                    "isActive": False,
                    "history": {
                        "list": []
                    }
                }
            },
            "nodes": {
                "list": [
                    {
                        "name": "1",
                        "edges": {
                            "latest": {
                                "list": [
                                    {
                                        "history": {
                                            "list": [{"timestamp": 3, "secondaryIndex": 2}]
                                        }
                                    },
                                    {
                                        "history": {
                                            "list": []
                                        }
                                    },
                                    {
                                        "history": {
                                            "list": [{"timestamp": 3, "secondaryIndex": 5}]
                                        }
                                    }
                                ]
                            }
                        }
                    },
                    {
                        "name": "2",
                        "edges": {
                            "latest": {
                                "list": [
                                    {
                                        "history": {
                                            "list": [{"timestamp": 3, "secondaryIndex": 2}
                                            ]
                                        }
                                    }
                                ]
                            }
                        }
                    },
                    {
                        "name": "3",
                        "edges": {
                            "latest": {
                                "list": [
                                    {
                                        "history": {
                                            "list": []
                                        }
                                    }
                                ]
                            }
                        }
                    },
                    {
                        "name": "4",
                        "edges": {
                            "latest": {
                                "list": [
                                    {
                                        "history": {
                                            "list": [{"timestamp": 3, "secondaryIndex": 5}]
                                        }
                                    }
                                ]
                            }
                        }
                    }
                ],
                "latest": {
                    "list": [
                        {
                            "name": "1",
                            "history": {
                                "list": [{"timestamp": 3, "secondaryIndex": 2}, {"timestamp": 3, "secondaryIndex": 5}]
                            }
                        },
                        {
                            "name": "2",
                            "history": {
                                "list": [{"timestamp": 3, "secondaryIndex": 2}]
                            }
                        },
                        {
                            "name": "4",
                            "history": {
                                "list": [{"timestamp": 3, "secondaryIndex": 5}]
                            }
                        }
                    ]
                }
            },
            "edges": {
                "latest": {
                    "list": [
                        {
                            "history": {
                                "list": [{"timestamp": 3, "secondaryIndex": 2}]
                            }
                        },
                        {
                            "history": {
                                "list": []
                            }
                        },
                        {
                            "history": {
                                "list": [{"timestamp": 3, "secondaryIndex": 5}]
                            }
                        }
                    ]
                }
            }
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
