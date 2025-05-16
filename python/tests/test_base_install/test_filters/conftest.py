from raphtory import Graph, PersistentGraph
import tempfile
import pytest
import shutil
import atexit

def init_graph(graph):
    nodes = [
         (1, 1, {"p1": "shivam_kapoor", "p9": 5, "p10": "Paper_airplane"}, "fire_nation"),
         (2, 2, {"p1": "prop12", "p2": 2, "p10": "Paper_ship"}, "air_nomads"),
         (3, 1, {"p1": "shivam_kapoor", "p9": 5}, "fire_nation"),
         (3, 3, {"p2": 6, "p3": 1, "p10": "Paper_airplane"}, "fire_nation"),
         (4, 1, {"p1": "shivam_kapoor", "p9": 5}, "fire_nation"),
         (3, 4, {"p4": "pometry"}, None),
         (4, 4, {"p5": 12}, None),
    ]

    for time, id, props, node_type in nodes:
            graph.add_node(time, str(id), props, node_type)

    edge_data = [
        (1, "1", "2", {"p1": "shivam_kapoor", "p10": "Paper_airplane"}, "fire_nation"),
        (2, "1", "2", {"p1": "shivam_kapoor", "p2": 4}, "fire_nation"),
        (2, "2", "3", {"p1": "prop12", "p2": 2, "p10": "Paper_ship"}, "air_nomads"),
        (3, "3", "1", {"p2": 6, "p3": 1}, "fire_nation"),
        (3, "2", "1", {"p2": 6, "p3": 1, "p10": "Paper_airplane"}, None),
        (4, "David Gilmour", "John Mayer", {"p2": 6, "p3": 1}, None),
        (4, "John Mayer", "Jimmy Page", {"p2": 6, "p3": 1}, None),
    ]

    for time, src, dst, props, edge_type in edge_data:
        graph.add_edge(time, src, dst, props, edge_type)

    return graph

