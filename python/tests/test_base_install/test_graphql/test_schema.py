import os
import tempfile
import json
from raphtory import Graph
from raphtory.graphql import GraphServer, RaphtoryClient


def sort_lists_in_structure(key_main, obj):
    if isinstance(obj, dict):
        return {key: sort_lists_in_structure(key, value) for key, value in obj.items()}
    elif isinstance(obj, list):
        if len(obj) > 0 and isinstance(obj[0], dict):
            # print(key_main,obj)
            if key_main == "layers":
                return sorted(
                    (sort_lists_in_structure(None, item) for item in obj),
                    key=lambda x: x["name"],
                )
            if key_main == "nodes":
                return sorted(
                    (sort_lists_in_structure(None, item) for item in obj),
                    key=lambda x: x["typeName"],
                )
            if key_main == "edges":
                return sorted(
                    (sort_lists_in_structure(None, item) for item in obj),
                    key=lambda x: (x["srcType"], x["dstType"]),
                )
            if key_main == "properties":
                return sorted(
                    (sort_lists_in_structure(None, item) for item in obj),
                    key=lambda x: (x["key"]),
                )
        return sorted(sort_lists_in_structure(None, item) for item in obj)
    else:
        return obj


def test_node_edge_properties_schema():
    g = Graph()
    g.add_node(0, 1, {"t": "wallet", "cost": 99.5}, "a")
    g.add_node(1, 2, {"t": "person"})
    g.add_node(6, 3, {"list_prop": [1.1, 2.2, 3.3], "cost_b": 76.0}, "b")
    g.add_node(7, 4, {"str_prop": "hello", "bool_prop": True}, "b")

    g.node(1).add_constant_properties({"lol": "smile"})

    g.add_edge(1, 1, 2, {"prop1": 1, "prop3": "test"}, layer="0")
    g.add_edge(1, 2, 3, {"prop1": 1, "prop2": 9.8}, layer="0")
    g.add_edge(1, 3, 4, {"prop4": 1, "prop5": 9.8, "prop6": {"data": "map"}}, layer="1")
    g.add_edge(1, 4, 5, {"prop1": 1, "prop2": 9.8, "propArray": [1, 2, 3]}, layer="1")
    g.add_edge(1, 6, 7, {"prop1": 1, "prop2": 9.8, "prop3": "test"}, layer="2")
    g.add_edge(1, 6, 7, layer="3")

    g.edge(1, 2).add_constant_properties({"static": "test"}, layer="0")

    work_dir = tempfile.mkdtemp()
    graph_file_path = os.path.join(work_dir, "graph")
    g.save_to_file(graph_file_path)

    with GraphServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")

        query = """{
          graph(path: "graph") {
            schema {
              layers {
                name
                edges {
                  srcType
                  dstType
                  properties {
                    key
                    propertyType
                    variants
                  }
                }
              }
              nodes {
                typeName
                properties {
                  key
                  propertyType
                  variants
                }
              }
            }
          }
        }
        """
        result = client.query(query)
        correct = {
            "graph": {
                "schema": {
                    "layers": [
                        {
                            "name": "0",
                            "edges": [
                                {
                                    "srcType": "a",
                                    "dstType": "None",
                                    "properties": [
                                        {
                                            "key": "prop1",
                                            "propertyType": "I64",
                                            "variants": ["1"],
                                        },
                                        {
                                            "key": "static",
                                            "propertyType": "Str",
                                            "variants": ["test"],
                                        },
                                        {
                                            "key": "prop3",
                                            "propertyType": "Str",
                                            "variants": ["test"],
                                        },
                                    ],
                                },
                                {
                                    "srcType": "None",
                                    "dstType": "b",
                                    "properties": [
                                        {
                                            "key": "prop1",
                                            "propertyType": "I64",
                                            "variants": ["1"],
                                        },
                                        {
                                            "key": "prop2",
                                            "propertyType": "F64",
                                            "variants": ["9.8"],
                                        },
                                    ],
                                },
                            ],
                        },
                        {
                            "name": "1",
                            "edges": [
                                {
                                    "srcType": "b",
                                    "dstType": "b",
                                    "properties": [
                                        {
                                            "key": "prop4",
                                            "propertyType": "I64",
                                            "variants": ["1"],
                                        },
                                        {
                                            "key": "prop5",
                                            "propertyType": "F64",
                                            "variants": ["9.8"],
                                        },
                                        {
                                            "key": "prop6",
                                            "propertyType": "Map{ data: Str }",
                                            "variants": ['{"data": "map"}'],
                                        },
                                    ],
                                },
                                {
                                    "srcType": "b",
                                    "dstType": "None",
                                    "properties": [
                                        {
                                            "key": "prop1",
                                            "propertyType": "I64",
                                            "variants": ["1"],
                                        },
                                        {
                                            "key": "propArray",
                                            "propertyType": "List<I64>",
                                            "variants": ["[1, 2, 3]"],
                                        },
                                        {
                                            "key": "prop2",
                                            "propertyType": "F64",
                                            "variants": ["9.8"],
                                        },
                                    ],
                                },
                            ],
                        },
                        {
                            "name": "2",
                            "edges": [
                                {
                                    "srcType": "None",
                                    "dstType": "None",
                                    "properties": [
                                        {
                                            "key": "prop1",
                                            "propertyType": "I64",
                                            "variants": ["1"],
                                        },
                                        {
                                            "key": "prop2",
                                            "propertyType": "F64",
                                            "variants": ["9.8"],
                                        },
                                        {
                                            "key": "prop3",
                                            "propertyType": "Str",
                                            "variants": ["test"],
                                        },
                                    ],
                                }
                            ],
                        },
                        {
                            "name": "3",
                            "edges": [
                                {"srcType": "None", "dstType": "None", "properties": []}
                            ],
                        },
                    ],
                    "nodes": [
                        {
                            "typeName": "a",
                            "properties": [
                                {
                                    "key": "t",
                                    "propertyType": "Str",
                                    "variants": ["wallet"],
                                },
                                {
                                    "key": "cost",
                                    "propertyType": "F64",
                                    "variants": ["99.5"],
                                },
                                {
                                    "key": "lol",
                                    "propertyType": "Str",
                                    "variants": ["smile"],
                                },
                            ],
                        },
                        {
                            "typeName": "None",
                            "properties": [
                                {
                                    "key": "t",
                                    "propertyType": "Str",
                                    "variants": ["person"],
                                }
                            ],
                        },
                        {
                            "typeName": "b",
                            "properties": [
                                {
                                    "key": "cost_b",
                                    "propertyType": "F64",
                                    "variants": ["76"],
                                },
                                {
                                    "key": "bool_prop",
                                    "propertyType": "Bool",
                                    "variants": ["true"],
                                },
                                {
                                    "key": "list_prop",
                                    "propertyType": "List<F64>",
                                    "variants": ["[1.1, 2.2, 3.3]"],
                                },
                                {
                                    "key": "str_prop",
                                    "propertyType": "Str",
                                    "variants": ["hello"],
                                },
                            ],
                        },
                    ],
                }
            }
        }

        assert sort_lists_in_structure(None, result) == sort_lists_in_structure(
            None, correct
        )
