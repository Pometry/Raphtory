import os
import tempfile
import json
from raphtory import Graph
from raphtory.graphql import GraphServer, RaphtoryClient

edges = [(1, 1, 2), (2, 1, 3), (-1, 2, 1), (0, 1, 1), (7, 3, 2), (1, 1, 1)]


def sort_properties(properties):
    return sorted(properties, key=lambda x: x["propertyType"])


def test_node_edge_properties_schema():
    g = Graph()
    g.add_node(0, 1, {"type": "wallet", "cost": 99.5}, "a")
    g.add_node(1, 2, {"type": "wallet", "cost": 10.0}, "a")
    g.add_node(6, 3, {"type": "wallet", "cost": 76.0}, "a")

    g.node(1).add_constant_properties({"lol": "smile"})

    for e in edges:
        g.add_edge(e[0], e[1], e[2], {"prop1": 1, "prop2": 9.8, "prop3": "test"})

    g.edge(edges[0][1], edges[0][2]).add_constant_properties({"static": "test"})

    work_dir = tempfile.mkdtemp()
    graph_file_path = os.path.join(work_dir, "g3")
    g.save_to_file(graph_file_path)

    with GraphServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")

        query = """{
          graph(path: "g3") {
            schema {
              layers {
                edges {
                  properties {
                    key
                    propertyType
                    variants
                  }
                }
              }
              nodes {
                properties {
                  key
                  propertyType
                  variants
                }
              }
            } 
          }
        }"""
        result = client.query(query)

        node_properties = result["graph"]["schema"]["nodes"][0]["properties"]
        sorted_node_properties = sort_properties(node_properties)
        assert sorted_node_properties[0]["propertyType"] == "F64"
        assert sorted_node_properties[1]["propertyType"] == "Str"
        assert sorted_node_properties[2]["propertyType"] == "Str"

        edge_properties = result["graph"]["schema"]["layers"][0]["edges"][0]["properties"]
        sorted_edge_properties = sort_properties(edge_properties)
        assert sorted_edge_properties[0]["propertyType"] == "F64"
        assert sorted_edge_properties[1]["propertyType"] == "I64"
        assert sorted_edge_properties[2]["propertyType"] == "Str"
        assert sorted_edge_properties[3]["propertyType"] == "Str"

        # pretty_json = json.dumps(result, indent=4)
        # print(pretty_json)
