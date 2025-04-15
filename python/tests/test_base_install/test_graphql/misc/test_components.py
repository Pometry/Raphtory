from raphtory.graphql import RaphtoryClient
from raphtory.graphql import GraphServer
from raphtory import Graph
import tempfile


def sort_components(data):
    if "inComponent" in data:
        data["inComponent"]["list"] = sorted(
            data["inComponent"]["list"], key=lambda x: x["name"]
        )
    if "outComponent" in data:
        data["outComponent"]["list"] = sorted(
            data["outComponent"]["list"], key=lambda x: x["name"]
        )


def prepare_for_comparison(structure):
    if "node" in structure:
        sort_components(structure["node"])
    if "window" in structure:
        sort_components(structure["window"]["node"])
    if "at" in structure:
        sort_components(structure["at"]["node"])


def test_in_out_components():
    query = """
        {
          graph(path: "graph") {
            node(name: "3") {
              inComponent {
                list {
                  name
                }
              }
              outComponent {
                list {
                  name
                }
              }
            }
            window(start:1,end:6){
              node(name:"3"){
                inComponent{
                  list {
                    name
                  }
                }
              }
            }
            at(time:4){
              node(name:"4"){
                outComponent{
                  list {
                    name
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
                "inComponent": {
                    "list": [{"name": "7"}, {"name": "1"}],
                },
                "outComponent": {
                    "list": [{"name": "6"}, {"name": "4"}, {"name": "5"}],
                },
            },
            "window": {
                "node": {"inComponent": {"list": [{"name": "1"}]}},
            },
            "at": {
                "node": {"outComponent": {"list": [{"name": "5"}]}},
            },
        }
    }
    work_dir = tempfile.mkdtemp()
    g = Graph()
    g.add_edge(1, 1, 2)
    g.add_edge(2, 1, 3)
    g.add_edge(3, 3, 4)
    g.add_edge(4, 4, 5)
    g.add_edge(5, 3, 6)
    g.add_edge(6, 7, 3)

    g.save_to_file(work_dir + "/graph")
    with GraphServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")
        query_res = client.query(query)
        prepare_for_comparison(query_res["graph"])
        prepare_for_comparison(result["graph"])
        assert query_res == result


def test_in_out_component_hop_reset():
    work_dir = tempfile.mkdtemp()
    g = Graph()
    g.add_edge(1, 1, 2)
    g.add_edge(2, 1, 3)
    g.add_edge(3, 3, 4)
    g.add_edge(4, 4, 5)
    g.add_edge(5, 3, 6)
    g.add_edge(6, 7, 3)
    g.save_to_file(work_dir + "/graph")

    query_out = """
        {
          graph(path: "graph") {
            node(name: "1") {
              window(start: 2, end: 4) {
                outComponent {
                  list {
                    name
                    degree
                  }
                }
              }
            }
          }
        }
    """

    result_out = {
        "graph": {
            "node": {
                "window": {
                    "outComponent": {
                        "list": [
                            {
                                "name": "3",
                                "degree": 4,
                            },
                            {
                                "name": "4",
                                "degree": 2,
                            },
                        ]
                    }
                }
            }
        }
    }

    query_in = """
        {
          graph(path: "graph") {
            node(name: "3") {
              window(start: 2, end: 4) {
                inComponent {
                  list {
                    name
                    degree
                  }
                }
              }
            }
          }
        }
    """

    result_in = {
        "graph": {
            "node": {
                "window": {
                    "inComponent": {
                        "list": [
                            {
                                "name": "1",
                                "degree": 2,
                            }
                        ]
                    }
                }
            }
        }
    }
    with GraphServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")
        query_res = client.query(query_out)
        prepare_for_comparison(query_res["graph"])
        prepare_for_comparison(result_out["graph"])
        assert query_res == result_out

        query_res = client.query(query_in)
        prepare_for_comparison(query_res["graph"])
        prepare_for_comparison(result_in["graph"])
        assert query_res == result_in
