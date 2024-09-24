from raphtory.graphql import RaphtoryClient
from raphtory.graphql import GraphServer
from raphtory import Graph
import tempfile


def test_in_out_components():

    def sort_components(data):
        if "inComponent" in data:
            data["inComponent"] = sorted(data["inComponent"], key=lambda x: x["name"])
        if "outComponent" in data:
            data["outComponent"] = sorted(data["outComponent"], key=lambda x: x["name"])

    def prepare_for_comparison(structure):
        if "node" in structure:
            sort_components(structure["node"])
        if "window" in structure:
            sort_components(structure["window"]["node"])
        if "at" in structure:
            sort_components(structure["at"]["node"])

    query = """
        {
          graph(path: "graph") {
            node(name: "3") {
              inComponent {
                name
              }
              outComponent {
                name
              }
            }
            window(start:1,end:6){
              node(name:"3"){
                inComponent{
                  name
                }
              }
            }
            at(time:4){
              node(name:"4"){
                outComponent{
                  name
                }
              }
            }
          }
        }
    """
    result = {
        "graph": {
            "node": {
                "inComponent": [{"name": "7"}, {"name": "1"}],
                "outComponent": [{"name": "6"}, {"name": "4"}, {"name": "5"}],
            },
            "window": {"node": {"inComponent": [{"name": "1"}]}},
            "at": {"node": {"outComponent": [{"name": "5"}]}},
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
