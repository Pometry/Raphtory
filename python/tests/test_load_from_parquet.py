import os

import pyarrow as pa
import pyarrow.parquet as pq
from raphtory import Graph, PersistentGraph


def test_load_from_parquet():
    data = {
        "src": [1, 2, 3, 4, 5],
        "dst": [2, 3, 4, 5, 6],
        "time": [1, 2, 3, 4, 5],
        "weight": [1.0, 2.0, 3.0, 4.0, 5.0],
        "marbles": ["red", "blue", "green", "yellow", "purple"],
    }

    table = pa.table(data)
    pq.write_table(table, '/tmp/parquet/test_data.parquet')


#
#     expected_nodes = [1, 2, 3, 4, 5, 6]
#     expected_edges = [
#         (1, 2, 1.0, "red"),
#         (2, 3, 2.0, "blue"),
#         (3, 4, 3.0, "green"),
#         (4, 5, 4.0, "yellow"),
#         (5, 6, 5.0, "purple"),
#     ]
#
#     def assertions(g):
#         edges = []
#         for e in g.edges:
#             weight = e["weight"]
#             marbles = e["marbles"]
#             edges.append((e.src.id, e.dst.id, weight, marbles))
#
#         assert g.nodes.id.collect() == expected_nodes
#         assert edges == expected_edges
#
#     g = Graph.load_from_parquet('test_data.parquet', "src", "dst", "time", ["weight", "marbles"])
#     assertions(g)
#
#     # g = PersistentGraph.load_from_parquet(
#     #     'test_data.parquet', "src", "dst", "time", ["weight", "marbles"]
#     # )
#     # assertions(g)

def test_load_edges_from_parquet():
    file_path = os.path.join(os.path.dirname(__file__), 'data', 'parquet', 'edges.parquet')
    expected_edges = [
        (1, 2, 1.0, "red"),
        (2, 3, 2.0, "blue"),
        (3, 4, 3.0, "green"),
        (4, 5, 4.0, "yellow"),
        (5, 6, 5.0, "purple"),
    ]

    g = Graph()
    g.load_edges_from_parquet(file_path, "src", "dst", "time", ["weight", "marbles"])

    edges = []
    for e in g.edges:
        weight = e["weight"]
        marbles = e["marbles"]
        edges.append((e.src.id, e.dst.id, weight, marbles))

    assert edges == expected_edges
