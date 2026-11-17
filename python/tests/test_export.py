from raphtory import Graph, PersistentGraph
import pandas as pd
import pyarrow as pa
import json
from pathlib import Path
import numpy as np
from datetime import datetime, timezone

base_dir = Path(__file__).parent

utc = timezone.utc


def build_graph():
    edges_df = pd.read_csv(base_dir / "data/network_traffic_edges.csv")
    edges_df["timestamp"] = pd.to_datetime(edges_df["timestamp"]).astype(
        "datetime64[ms, UTC]"
    )

    nodes_df = pd.read_csv(base_dir / "data/network_traffic_nodes.csv")
    nodes_df["timestamp"] = pd.to_datetime(nodes_df["timestamp"]).astype(
        "datetime64[ms, UTC]"
    )
    g = Graph()
    g.load_edges(
        edges_df,
        time="timestamp",
        src="source",
        dst="destination",
        properties=["data_size_MB"],
        layer_col="transaction_type",
        metadata=["is_encrypted"],
        shared_metadata={"datasource": "data/network_traffic_edges.csv"},
    )
    g.load_nodes(
        data=nodes_df,
        id="server_id",
        time="timestamp",
        properties=["OS_version", "primary_function", "uptime_days"],
        metadata=["server_name", "hardware_type"],
        shared_metadata={"datasource": "data/network_traffic_edges.csv"},
    )
    return g


def build_graph_without_datetime_type():
    edges_df = pd.read_csv(base_dir / "data/network_traffic_edges.csv")
    edges_df["timestamp"] = pd.to_datetime(edges_df["timestamp"])

    nodes_df = pd.read_csv(base_dir / "data/network_traffic_nodes.csv")
    nodes_df["timestamp"] = pd.to_datetime(nodes_df["timestamp"])

    g = Graph()
    g.load_edges(
        edges_df,
        time="timestamp",
        src="source",
        dst="destination",
        properties=["data_size_MB"],
        layer_col="transaction_type",
        metadata=["is_encrypted"],
        shared_metadata={"datasource": "data/network_traffic_edges.csv"},
    )
    g.load_nodes(
        data=nodes_df,
        id="server_id",
        time="timestamp",
        properties=["OS_version", "primary_function", "uptime_days"],
        metadata=["server_name", "hardware_type"],
        shared_metadata={"datasource": "data/network_traffic_edges.csv"},
    )
    return g


def test_graph_timestamp_list_properties():
    array_column = [np.array([1, 2, 3]), np.array([4, 5, 6]), np.array([7, 8, 9])]

    string_column = ["a", "b", "c"]
    bool_column = [True, False, True]
    int_column = [10, 20, 30]
    date_column = [datetime.now(), datetime.now(), datetime.now()]

    df = pd.DataFrame(
        {
            "array_column": array_column,
            "string_column": string_column,
            "bool_column": bool_column,
            "int_column": int_column,
            "date_column": date_column,
        }
    )

    df["date_column_ms"] = df["date_column"].astype("datetime64[ms]")
    df["date_column_us"] = df["date_column"].astype("datetime64[us]")
    df["date_column_ns"] = df["date_column"].astype("datetime64[ns]")

    g = Graph()
    g.load_nodes(
        df,
        time="date_column",
        id="string_column",
        properties=[
            "array_column",
            "date_column",
            "date_column_ms",
            "date_column_us",
            "date_column_ns",
        ],
    )

    assert g.node("a").properties.get("array_column").tolist() == [1, 2, 3]

    assert g.node("a").properties.get("date_column_ms") == df["date_column_ms"][0]
    assert g.node("a").properties.get("date_column_us") == df["date_column_us"][0]

    assert g.node("a").properties.get("date_column") == date_column[0]
    assert g.node("a").properties.get("date_column_ns") == df["date_column_ns"][0]


def test_graph_build_from_pandas_without_datetime_type():
    g = build_graph_without_datetime_type()
    assert g.node("ServerA").name == "ServerA"
    assert g.node("ServerA").earliest_time == 1693555200000


def test_py_vis():
    g = build_graph()
    pyvis_g = g.to_pyvis(directed=True)

    compare_list_of_dicts(
        pyvis_g.nodes,
        [
            {
                "color": "#97c2fc",
                "id": "ServerA",
                "label": "ServerA",
                "shape": "dot",
            },
            {
                "color": "#97c2fc",
                "id": "ServerB",
                "label": "ServerB",
                "shape": "dot",
            },
            {
                "color": "#97c2fc",
                "id": "ServerC",
                "label": "ServerC",
                "shape": "dot",
            },
            {
                "color": "#97c2fc",
                "id": "ServerD",
                "label": "ServerD",
                "shape": "dot",
            },
            {
                "color": "#97c2fc",
                "id": "ServerE",
                "label": "ServerE",
                "shape": "dot",
            },
        ],
    )

    compare_list_of_dicts(
        pyvis_g.edges,
        [
            {
                "arrowStrikethrough": False,
                "arrows": "to",
                "color": "#000000",
                "from": "ServerA",
                "title": "Critical System Request",
                "to": "ServerB",
            },
            {
                "arrowStrikethrough": False,
                "arrows": "to",
                "color": "#000000",
                "from": "ServerA",
                "title": "File Transfer",
                "to": "ServerC",
            },
            {
                "arrowStrikethrough": False,
                "arrows": "to",
                "color": "#000000",
                "from": "ServerB",
                "title": "Standard Service Request",
                "to": "ServerD",
            },
            {
                "arrowStrikethrough": False,
                "arrows": "to",
                "color": "#000000",
                "from": "ServerC",
                "title": "Critical System Request",
                "to": "ServerA",
            },
            {
                "arrowStrikethrough": False,
                "arrows": "to",
                "color": "#000000",
                "from": "ServerD",
                "title": "Administrative Command",
                "to": "ServerE",
            },
            {
                "arrowStrikethrough": False,
                "arrows": "to",
                "color": "#000000",
                "from": "ServerD",
                "title": "Standard Service Request",
                "to": "ServerC",
            },
            {
                "arrowStrikethrough": False,
                "arrows": "to",
                "color": "#000000",
                "from": "ServerE",
                "title": "File Transfer",
                "to": "ServerB",
            },
        ],
    )


def test_networkx_full_history():
    g = build_graph()

    networkxGraph = g.to_networkx()
    assert networkxGraph.number_of_nodes() == 5
    assert networkxGraph.number_of_edges() == 7

    nodeList = list(networkxGraph.nodes(data=True))
    server_list = [
        (
            "ServerA",
            {
                "metadata": {
                    "datasource": "data/network_traffic_edges.csv",
                    "hardware_type": "Blade Server",
                    "server_name": "Alpha",
                },
                "temporal": [
                    ("OS_version", ((1693555200000, 9), "Ubuntu 20.04")),
                    ("OS_version", ((1693555260000, 10), "Ubuntu 20.04")),
                    ("OS_version", ((1693555320000, 11), "Ubuntu 20.04")),
                    ("primary_function", ((1693555200000, 9), "Database")),
                    ("primary_function", ((1693555260000, 10), "Database")),
                    ("primary_function", ((1693555320000, 11), "Database")),
                    ("uptime_days", ((1693555200000, 9), 120)),
                    ("uptime_days", ((1693555260000, 10), 121)),
                    ("uptime_days", ((1693555320000, 11), 122)),
                ],
                "update_history": [
                    (1693555200000, 0),
                    (1693555200000, 9),
                    (1693555260000, 10),
                    (1693555320000, 11),
                    (1693555500000, 1),
                    (1693556400000, 4),
                ],
            },
        ),
        (
            "ServerB",
            {
                "metadata": {
                    "hardware_type": "Rack Server",
                    "datasource": "data/network_traffic_edges.csv",
                    "server_name": "Beta",
                },
                "temporal": [
                    ("OS_version", ((1693555500000, 12), "Red Hat 8.1")),
                    ("primary_function", ((1693555500000, 12), "Web Server")),
                    ("uptime_days", ((1693555500000, 12), 45)),
                ],
                "update_history": [
                    (1693555200000, 0),
                    (1693555500000, 12),
                    (1693555800000, 2),
                    (1693556700000, 5),
                ],
            },
        ),
        (
            "ServerC",
            {
                "metadata": {
                    "hardware_type": "Blade Server",
                    "datasource": "data/network_traffic_edges.csv",
                    "server_name": "Charlie",
                },
                "temporal": [
                    ("OS_version", ((1693555800000, 13), "Windows Server 2022")),
                    ("primary_function", ((1693555800000, 13), "File Storage")),
                    ("uptime_days", ((1693555800000, 13), 90)),
                ],
                "update_history": [
                    (1693555500000, 1),
                    (1693555800000, 13),
                    (1693556400000, 4),
                    (1693557000000, 6),
                    (1693557060000, 7),
                    (1693557120000, 8),
                ],
            },
        ),
        (
            "ServerD",
            {
                "metadata": {
                    "hardware_type": "Tower Server",
                    "datasource": "data/network_traffic_edges.csv",
                    "server_name": "Delta",
                },
                "temporal": [
                    ("OS_version", ((1693556100000, 14), "Ubuntu 20.04")),
                    ("primary_function", ((1693556100000, 14), "Application Server")),
                    ("uptime_days", ((1693556100000, 14), 60)),
                ],
                "update_history": [
                    (1693555800000, 2),
                    (1693556100000, 3),
                    (1693556100000, 14),
                    (1693557000000, 6),
                    (1693557060000, 7),
                    (1693557120000, 8),
                ],
            },
        ),
        (
            "ServerE",
            {
                "metadata": {
                    "server_name": "Echo",
                    "hardware_type": "Rack Server",
                    "datasource": "data/network_traffic_edges.csv",
                },
                "temporal": [
                    ("OS_version", ((1693556400000, 15), "Red Hat 8.1")),
                    ("primary_function", ((1693556400000, 15), "Backup")),
                    ("uptime_days", ((1693556400000, 15), 30)),
                ],
                "update_history": [
                    (1693556100000, 3),
                    (1693556400000, 15),
                    (1693556700000, 5),
                ],
            },
        ),
    ]
    compare_list_of_dicts(nodeList, server_list)

    edgeList = list(networkxGraph.edges(data=True))
    resultList = [
        (
            "ServerA",
            "ServerB",
            {
                "metadata": {
                    "is_encrypted": True,
                    "datasource": "data/network_traffic_edges.csv",
                },
                "temporal": [("data_size_MB", [((1693555200000, 0), 5.6)])],
                "layer": "Critical System Request",
                "update_history": [(1693555200000, 0)],
            },
        ),
        (
            "ServerA",
            "ServerC",
            {
                "metadata": {
                    "datasource": "data/network_traffic_edges.csv",
                    "is_encrypted": False,
                },
                "temporal": [("data_size_MB", [((1693555500000, 1), 7.1)])],
                "layer": "File Transfer",
                "update_history": [(1693555500000, 1)],
            },
        ),
        (
            "ServerB",
            "ServerD",
            {
                "metadata": {
                    "is_encrypted": True,
                    "datasource": "data/network_traffic_edges.csv",
                },
                "temporal": [("data_size_MB", [((1693555800000, 2), 3.2)])],
                "layer": "Standard Service Request",
                "update_history": [(1693555800000, 2)],
            },
        ),
        (
            "ServerC",
            "ServerA",
            {
                "metadata": {
                    "is_encrypted": True,
                    "datasource": "data/network_traffic_edges.csv",
                },
                "temporal": [("data_size_MB", [((1693556400000, 4), 4.5)])],
                "layer": "Critical System Request",
                "update_history": [(1693556400000, 4)],
            },
        ),
        (
            "ServerD",
            "ServerE",
            {
                "metadata": {
                    "is_encrypted": False,
                    "datasource": "data/network_traffic_edges.csv",
                },
                "temporal": [("data_size_MB", [((1693556100000, 3), 8.9)])],
                "layer": "Administrative Command",
                "update_history": [(1693556100000, 3)],
            },
        ),
        (
            "ServerD",
            "ServerC",
            {
                "metadata": {
                    "is_encrypted": True,
                    "datasource": "data/network_traffic_edges.csv",
                },
                "temporal": [
                    (
                        "data_size_MB",
                        [
                            ((1693557000000, 6), 5.0),
                            ((1693557060000, 7), 10.0),
                            ((1693557120000, 8), 15.0),
                        ],
                    )
                ],
                "layer": "Standard Service Request",
                "update_history": [
                    (1693557000000, 6),
                    (1693557060000, 7),
                    (1693557120000, 8),
                ],
            },
        ),
        (
            "ServerE",
            "ServerB",
            {
                "metadata": {
                    "is_encrypted": False,
                    "datasource": "data/network_traffic_edges.csv",
                },
                "temporal": [("data_size_MB", [((1693556700000, 5), 6.2)])],
                "layer": "File Transfer",
                "update_history": [(1693556700000, 5)],
            },
        ),
    ]
    compare_list_of_dicts(edgeList, resultList)


def test_networkx_exploded():
    g = build_graph()

    networkxGraph = g.to_networkx(explode_edges=True)
    assert networkxGraph.number_of_nodes() == 5
    assert networkxGraph.number_of_edges() == 9

    edgeList = list(networkxGraph.edges(data=True))
    resultList = [
        (
            "ServerA",
            "ServerB",
            {
                "metadata": {
                    "datasource": "data/network_traffic_edges.csv",
                    "is_encrypted": True,
                },
                "temporal": [("data_size_MB", [((1693555200000, 0), 5.6)])],
                "layer": "Critical System Request",
                "update_history": (1693555200000, 0),
            },
        ),
        (
            "ServerA",
            "ServerC",
            {
                "metadata": {
                    "is_encrypted": False,
                    "datasource": "data/network_traffic_edges.csv",
                },
                "temporal": [("data_size_MB", [((1693555500000, 1), 7.1)])],
                "layer": "File Transfer",
                "update_history": (1693555500000, 1),
            },
        ),
        (
            "ServerB",
            "ServerD",
            {
                "metadata": {
                    "is_encrypted": True,
                    "datasource": "data/network_traffic_edges.csv",
                },
                "temporal": [("data_size_MB", [((1693555800000, 2), 3.2)])],
                "layer": "Standard Service Request",
                "update_history": (1693555800000, 2),
            },
        ),
        (
            "ServerC",
            "ServerA",
            {
                "metadata": {
                    "is_encrypted": True,
                    "datasource": "data/network_traffic_edges.csv",
                },
                "temporal": [("data_size_MB", [((1693556400000, 4), 4.5)])],
                "layer": "Critical System Request",
                "update_history": (1693556400000, 4),
            },
        ),
        (
            "ServerD",
            "ServerE",
            {
                "metadata": {
                    "datasource": "data/network_traffic_edges.csv",
                    "is_encrypted": False,
                },
                "temporal": [("data_size_MB", [((1693556100000, 3), 8.9)])],
                "layer": "Administrative Command",
                "update_history": (1693556100000, 3),
            },
        ),
        (
            "ServerD",
            "ServerC",
            {
                "metadata": {
                    "is_encrypted": True,
                    "datasource": "data/network_traffic_edges.csv",
                },
                "temporal": [("data_size_MB", [((1693557000000, 6), 5.0)])],
                "layer": "Standard Service Request",
                "update_history": (1693557000000, 6),
            },
        ),
        (
            "ServerD",
            "ServerC",
            {
                "metadata": {
                    "datasource": "data/network_traffic_edges.csv",
                    "is_encrypted": True,
                },
                "temporal": [("data_size_MB", [((1693557060000, 7), 10.0)])],
                "layer": "Standard Service Request",
                "update_history": (1693557060000, 7),
            },
        ),
        (
            "ServerD",
            "ServerC",
            {
                "metadata": {
                    "is_encrypted": True,
                    "datasource": "data/network_traffic_edges.csv",
                },
                "temporal": [("data_size_MB", [((1693557120000, 8), 15.0)])],
                "layer": "Standard Service Request",
                "update_history": (1693557120000, 8),
            },
        ),
        (
            "ServerE",
            "ServerB",
            {
                "metadata": {
                    "is_encrypted": False,
                    "datasource": "data/network_traffic_edges.csv",
                },
                "temporal": [("data_size_MB", [((1693556700000, 5), 6.2)])],
                "layer": "File Transfer",
                "update_history": (1693556700000, 5),
            },
        ),
    ]
    compare_list_of_dicts(edgeList, resultList)


def test_networkx_no_props():
    g = build_graph()

    networkxGraph = g.to_networkx(
        include_node_properties=False, include_edge_properties=False
    )

    nodeList = list(networkxGraph.nodes(data=True))
    resultList = [
        (
            "ServerA",
            {
                "update_history": [
                    (1693555200000, 0),
                    (1693555200000, 9),
                    (1693555260000, 10),
                    (1693555320000, 11),
                    (1693555500000, 1),
                    (1693556400000, 4),
                ]
            },
        ),
        (
            "ServerB",
            {
                "update_history": [
                    (1693555200000, 0),
                    (1693555500000, 12),
                    (1693555800000, 2),
                    (1693556700000, 5),
                ]
            },
        ),
        (
            "ServerC",
            {
                "update_history": [
                    (1693555500000, 1),
                    (1693555800000, 13),
                    (1693556400000, 4),
                    (1693557000000, 6),
                    (1693557060000, 7),
                    (1693557120000, 8),
                ]
            },
        ),
        (
            "ServerD",
            {
                "update_history": [
                    (1693555800000, 2),
                    (1693556100000, 3),
                    (1693556100000, 14),
                    (1693557000000, 6),
                    (1693557060000, 7),
                    (1693557120000, 8),
                ]
            },
        ),
        (
            "ServerE",
            {
                "update_history": [
                    (1693556100000, 3),
                    (1693556400000, 15),
                    (1693556700000, 5),
                ]
            },
        ),
    ]
    compare_list_of_dicts(nodeList, resultList)

    edgeList = list(networkxGraph.edges(data=True))
    resultList = [
        (
            "ServerA",
            "ServerB",
            {
                "layer": "Critical System Request",
                "update_history": [(1693555200000, 0)],
            },
        ),
        (
            "ServerA",
            "ServerC",
            {"layer": "File Transfer", "update_history": [(1693555500000, 1)]},
        ),
        (
            "ServerB",
            "ServerD",
            {
                "layer": "Standard Service Request",
                "update_history": [(1693555800000, 2)],
            },
        ),
        (
            "ServerC",
            "ServerA",
            {
                "layer": "Critical System Request",
                "update_history": [(1693556400000, 4)],
            },
        ),
        (
            "ServerD",
            "ServerC",
            {
                "layer": "Standard Service Request",
                "update_history": [
                    (1693557000000, 6),
                    (1693557060000, 7),
                    (1693557120000, 8),
                ],
            },
        ),
        (
            "ServerD",
            "ServerE",
            {"layer": "Administrative Command", "update_history": [(1693556100000, 3)]},
        ),
        (
            "ServerE",
            "ServerB",
            {"layer": "File Transfer", "update_history": [(1693556700000, 5)]},
        ),
    ]
    compare_list_of_dicts(edgeList, resultList)

    networkxGraph = g.to_networkx(
        include_node_properties=False,
        include_edge_properties=False,
        include_update_history=False,
    )

    nodeList = list(networkxGraph.nodes(data=True))
    resultList = [
        ("ServerA", {}),
        ("ServerB", {}),
        ("ServerC", {}),
        ("ServerD", {}),
        ("ServerE", {}),
    ]
    compare_list_of_dicts(nodeList, resultList)

    edgeList = list(networkxGraph.edges(data=True))
    resultList = [
        ("ServerA", "ServerB", {"layer": "Critical System Request"}),
        ("ServerA", "ServerC", {"layer": "File Transfer"}),
        ("ServerB", "ServerD", {"layer": "Standard Service Request"}),
        ("ServerC", "ServerA", {"layer": "Critical System Request"}),
        ("ServerD", "ServerC", {"layer": "Standard Service Request"}),
        ("ServerD", "ServerE", {"layer": "Administrative Command"}),
        ("ServerE", "ServerB", {"layer": "File Transfer"}),
    ]
    compare_list_of_dicts(edgeList, resultList)

    networkxGraph = g.to_networkx(include_edge_properties=False, explode_edges=True)
    edgeList = list(networkxGraph.edges(data=True))
    resultList = [
        (
            "ServerA",
            "ServerB",
            {"layer": "Critical System Request", "update_history": (1693555200000, 0)},
        ),
        (
            "ServerA",
            "ServerC",
            {"layer": "File Transfer", "update_history": (1693555500000, 1)},
        ),
        (
            "ServerB",
            "ServerD",
            {"layer": "Standard Service Request", "update_history": (1693555800000, 2)},
        ),
        (
            "ServerC",
            "ServerA",
            {"layer": "Critical System Request", "update_history": (1693556400000, 4)},
        ),
        (
            "ServerD",
            "ServerC",
            {"layer": "Standard Service Request", "update_history": (1693557000000, 6)},
        ),
        (
            "ServerD",
            "ServerC",
            {"layer": "Standard Service Request", "update_history": (1693557060000, 7)},
        ),
        (
            "ServerD",
            "ServerC",
            {"layer": "Standard Service Request", "update_history": (1693557120000, 8)},
        ),
        (
            "ServerD",
            "ServerE",
            {"layer": "Administrative Command", "update_history": (1693556100000, 3)},
        ),
        (
            "ServerE",
            "ServerB",
            {"layer": "File Transfer", "update_history": (1693556700000, 5)},
        ),
    ]
    compare_list_of_dicts(edgeList, resultList)


def test_networkx_no_history():
    g = build_graph()

    networkxGraph = g.to_networkx(
        include_property_history=False, include_update_history=False
    )

    nodeList = list(networkxGraph.nodes(data=True))
    resultList = [
        (
            "ServerA",
            {
                "OS_version": "Ubuntu 20.04",
                "datasource": "data/network_traffic_edges.csv",
                "hardware_type": "Blade Server",
                "primary_function": "Database",
                "server_name": "Alpha",
                "uptime_days": 122,
            },
        ),
        (
            "ServerB",
            {
                "OS_version": "Red Hat 8.1",
                "datasource": "data/network_traffic_edges.csv",
                "hardware_type": "Rack Server",
                "primary_function": "Web Server",
                "server_name": "Beta",
                "uptime_days": 45,
            },
        ),
        (
            "ServerC",
            {
                "OS_version": "Windows Server 2022",
                "datasource": "data/network_traffic_edges.csv",
                "hardware_type": "Blade Server",
                "primary_function": "File Storage",
                "server_name": "Charlie",
                "uptime_days": 90,
            },
        ),
        (
            "ServerD",
            {
                "OS_version": "Ubuntu 20.04",
                "datasource": "data/network_traffic_edges.csv",
                "hardware_type": "Tower Server",
                "primary_function": "Application Server",
                "server_name": "Delta",
                "uptime_days": 60,
            },
        ),
        (
            "ServerE",
            {
                "OS_version": "Red Hat 8.1",
                "datasource": "data/network_traffic_edges.csv",
                "hardware_type": "Rack Server",
                "primary_function": "Backup",
                "server_name": "Echo",
                "uptime_days": 30,
            },
        ),
    ]
    compare_list_of_dicts(nodeList, resultList)

    edgeList = list(networkxGraph.edges(data=True))
    resultList = [
        (
            "ServerA",
            "ServerB",
            {
                "data_size_MB": 5.6,
                "datasource": "data/network_traffic_edges.csv",
                "is_encrypted": True,
                "layer": "Critical System Request",
            },
        ),
        (
            "ServerA",
            "ServerC",
            {
                "data_size_MB": 7.1,
                "datasource": "data/network_traffic_edges.csv",
                "is_encrypted": False,
                "layer": "File Transfer",
            },
        ),
        (
            "ServerB",
            "ServerD",
            {
                "data_size_MB": 3.2,
                "datasource": "data/network_traffic_edges.csv",
                "is_encrypted": True,
                "layer": "Standard Service Request",
            },
        ),
        (
            "ServerC",
            "ServerA",
            {
                "data_size_MB": 4.5,
                "datasource": "data/network_traffic_edges.csv",
                "is_encrypted": True,
                "layer": "Critical System Request",
            },
        ),
        (
            "ServerD",
            "ServerC",
            {
                "data_size_MB": 15.0,
                "datasource": "data/network_traffic_edges.csv",
                "is_encrypted": True,
                "layer": "Standard Service Request",
            },
        ),
        (
            "ServerD",
            "ServerE",
            {
                "data_size_MB": 8.9,
                "datasource": "data/network_traffic_edges.csv",
                "is_encrypted": False,
                "layer": "Administrative Command",
            },
        ),
        (
            "ServerE",
            "ServerB",
            {
                "data_size_MB": 6.2,
                "datasource": "data/network_traffic_edges.csv",
                "is_encrypted": False,
                "layer": "File Transfer",
            },
        ),
    ]
    compare_list_of_dicts(edgeList, resultList)

    networkxGraph = g.to_networkx(include_property_history=False, explode_edges=True)
    edgeList = list(networkxGraph.edges(data=True))
    resultList = [
        (
            "ServerA",
            "ServerB",
            {
                "data_size_MB": 5.6,
                "datasource": "data/network_traffic_edges.csv",
                "is_encrypted": True,
                "layer": "Critical System Request",
                "update_history": (1693555200000, 0),
            },
        ),
        (
            "ServerA",
            "ServerC",
            {
                "data_size_MB": 7.1,
                "datasource": "data/network_traffic_edges.csv",
                "is_encrypted": False,
                "layer": "File Transfer",
                "update_history": (1693555500000, 1),
            },
        ),
        (
            "ServerB",
            "ServerD",
            {
                "data_size_MB": 3.2,
                "datasource": "data/network_traffic_edges.csv",
                "is_encrypted": True,
                "layer": "Standard Service Request",
                "update_history": (1693555800000, 2),
            },
        ),
        (
            "ServerC",
            "ServerA",
            {
                "data_size_MB": 4.5,
                "datasource": "data/network_traffic_edges.csv",
                "is_encrypted": True,
                "layer": "Critical System Request",
                "update_history": (1693556400000, 4),
            },
        ),
        (
            "ServerD",
            "ServerC",
            {
                "data_size_MB": 5.0,
                "datasource": "data/network_traffic_edges.csv",
                "is_encrypted": True,
                "layer": "Standard Service Request",
                "update_history": (1693557000000, 6),
            },
        ),
        (
            "ServerD",
            "ServerC",
            {
                "data_size_MB": 10.0,
                "datasource": "data/network_traffic_edges.csv",
                "is_encrypted": True,
                "layer": "Standard Service Request",
                "update_history": (1693557060000, 7),
            },
        ),
        (
            "ServerD",
            "ServerC",
            {
                "data_size_MB": 15.0,
                "datasource": "data/network_traffic_edges.csv",
                "is_encrypted": True,
                "layer": "Standard Service Request",
                "update_history": (1693557120000, 8),
            },
        ),
        (
            "ServerD",
            "ServerE",
            {
                "data_size_MB": 8.9,
                "datasource": "data/network_traffic_edges.csv",
                "is_encrypted": False,
                "layer": "Administrative Command",
                "update_history": (1693556100000, 3),
            },
        ),
        (
            "ServerE",
            "ServerB",
            {
                "data_size_MB": 6.2,
                "datasource": "data/network_traffic_edges.csv",
                "is_encrypted": False,
                "layer": "File Transfer",
                "update_history": (1693556700000, 5),
            },
        ),
    ]
    compare_list_of_dicts(edgeList, resultList)


def save_df_to_json(df, filename):
    df.to_json(filename)
    # Below is if you want to pretty print the json
    # json_str = df.to_json()
    # parsed = json.loads(json_str)
    # with open(filename, "w") as f:
    #    json.dump(parsed, f, indent=4)


# DO NOT RUN UNLESS RECREATING THE OUTPUT
def build_to_df():
    g = build_graph()

    edge_df = g.edges.to_df()
    save_df_to_json(edge_df, "expected/dataframe_output/edge_df_all.json")
    edge_df = g.edges.to_df(include_property_history=False)
    save_df_to_json(edge_df, "expected/dataframe_output/edge_df_no_props.json")
    edge_df = g.edges.to_df(explode=False)
    save_df_to_json(edge_df, "expected/dataframe_output/edge_df_no_explode.json")
    edge_df = g.edges.to_df(convert_datetime=False)
    save_df_to_json(edge_df, "expected/dataframe_output/edge_df_no_datetime.json")

    edge_df = g.edges.to_df(explode=True)
    save_df_to_json(edge_df, "expected/dataframe_output/edge_df_explode.json")
    edge_df = g.edges.to_df(explode=True, convert_datetime=True)
    save_df_to_json(edge_df, "expected/dataframe_output/edge_df_explode_datetime.json")
    edge_df = g.edges.to_df(explode=True, include_property_history=True)
    save_df_to_json(edge_df, "expected/dataframe_output/edge_df_explode_no_hist.json")


def jsonify_df(df):
    """
    normalises data frame using json with sorted keys to enable order-invariant comparison of rows between data frames
    """
    lines = []
    for _, row in df.iterrows():
        values = sorted(zip(df.columns, (normalise_dict(v) for v in row)))
        lines.append(values)
    lines.sort()
    return lines


def compare_df(df1, df2):
    # Have to do this way due to the number of maps inside the dataframes
    # Comparison is invariant to the order of rows and columns
    s1 = jsonify_df(df1)
    s2 = jsonify_df(df2)
    assert s1 == s2


def normalise_dict(d):
    s = json.dumps(
        d,
        ensure_ascii=True,
        sort_keys=True,
        default=lambda o: o.tolist() if isinstance(o, np.ndarray) else o,
    )
    return s


def compare_list_of_dicts(list1, list2):
    assert sorted(normalise_dict(v) for v in list1) == sorted(
        normalise_dict(v) for v in list2
    )


def test_to_df():
    g = build_graph()

    compare_df(
        g.edges.to_df(),
        pd.read_json(base_dir / "expected/dataframe_output/edge_df_all.json"),
    )

    compare_df(
        g.edges.to_df(include_property_history=False),
        pd.read_json(base_dir / "expected/dataframe_output/edge_df_no_props.json"),
    )

    compare_df(
        g.edges.to_df(explode=False),
        pd.read_json(base_dir / "expected/dataframe_output/edge_df_no_explode.json"),
    )

    compare_df(
        g.edges.to_df(convert_datetime=False),
        pd.read_json(base_dir / "expected/dataframe_output/edge_df_no_datetime.json"),
    )

    compare_df(
        g.edges.to_df(explode=True),
        pd.read_json(base_dir / "expected/dataframe_output/edge_df_explode.json"),
    )

    compare_df(
        g.edges.explode().to_df(),
        pd.read_json(base_dir / "expected/dataframe_output/edge_df_explode.json"),
    )

    # compare_df(
    #     g.edges.to_df(explode=True, convert_datetime=True),
    #     pd.read_json(base_dir / "expected/dataframe_output/edge_df_explode_datetime.json")
    # )

    compare_df(
        g.edges.to_df(explode=True, include_property_history=True),
        pd.read_json(
            base_dir / "expected/dataframe_output/edge_df_explode_no_hist.json"
        ),
    )


def build_node_update_graph():
    g = Graph()
    g.add_node(1, "a", properties={"x": 1}, node_type="server")
    g.add_node(2, "a", properties={"y": "up"})
    g.add_node(3, "b")
    g.add_node(4, "b", properties={"x": 7}, layer="maintenance")
    g.node("a").add_metadata({"rack": 3})
    g.node("b").add_metadata({"owner": "ops"})
    return g


def cells(df):
    """The rows of a DataFrame as tuples, with empty cells as None."""
    return [
        tuple(None if pd.isna(v) else v for v in row)
        for row in df.itertuples(index=False)
    ]


def node_rows(df):
    """The rows of a node update DataFrame without event ids, sorted by node and time."""
    return sorted(
        cells(df.drop(columns="rap_secondary_index")), key=lambda row: (row[0], row[2])
    )


def test_nodes_to_df():
    g = build_node_update_graph()
    g.add_node(5, "a", properties={"x": 2})  # replaces a's x from time 1
    g.add_edge(6, "a", "c")  # c only exists through an edge
    df, _ = g.nodes.to_df()
    history, _ = g.nodes.to_df(include_property_history=True)

    assert list(df.columns) == list(history.columns)
    # only the updates that hold a property's latest value, each with just those properties
    assert node_rows(df) == [
        ("a", "server", 2, None, None, "up"),
        ("a", "server", 5, None, 2, None),
        ("b", None, 4, "maintenance", 7, None),
    ]
    # each row is an update from the history, with the same time, event id and layer
    update = ["rap_node_gid", "rap_time", "rap_secondary_index", "rap_layer"]
    assert set(cells(df[update])) <= set(cells(history[update]))
    # rows are grouped by node, in node order
    with_rows = set(df["rap_node_gid"])
    assert list(dict.fromkeys(df["rap_node_gid"])) == [
        node for node in g.nodes.id if node in with_rows
    ]
    # the values are the latest values the properties API reports
    for name in ["x", "y"]:
        values = {
            node: value
            for node, value in zip(df["rap_node_gid"], df[name])
            if not pd.isna(value)
        }
        latest = g.nodes.properties.temporal.get(name).value()
        assert values == {
            node: value for node, value in zip(g.nodes.id, latest) if value is not None
        }
    # an integer property keeps its type even though some rows leave it empty
    assert str(df["x"].dtype) == "int64[pyarrow]"
    df, _ = g.nodes.to_df(convert_datetime=True)
    assert str(df["rap_time"].dtype) == "timestamp[ms, tz=UTC][pyarrow]"


def test_nodes_to_df_latest_follows_the_view():
    df, _ = build_node_update_graph().window(0, 4).nodes.to_df()

    # b's update at time 4 is outside the window, and its update at time 3 sets no property
    assert node_rows(df) == [
        ("a", "server", 1, None, 1, None),
        ("a", "server", 2, None, None, "up"),
    ]


def test_nodes_to_df_latest_follows_the_layers():
    g = build_node_update_graph()
    g.add_node(5, "b", properties={"x": 9}, layer="other")
    view = g.layer("maintenance")
    df, _ = view.nodes.to_df()

    # b's update on the "other" layer is outside the view
    assert node_rows(df) == [
        ("a", "server", 1, None, 1, None),
        ("a", "server", 2, None, None, "up"),
        ("b", None, 4, "maintenance", 7, None),
    ]
    values = {
        node: value
        for node, value in zip(df["rap_node_gid"], df["x"])
        if not pd.isna(value)
    }
    assert values == dict(zip(view.nodes.id, view.nodes.properties.temporal.get("x").value()))


def test_nodes_to_df_latest_persistent_window():
    g = PersistentGraph()
    g.add_node(1, "a", properties={"x": 1})
    g.add_node(2, "a", properties={"y": "up"})
    g.add_node(6, "a", properties={"x": 3})
    view = g.window(4, 10)
    df, _ = view.nodes.to_df()

    # y was set before the window, so it is seen at the start of the window
    assert node_rows(df) == [
        ("a", None, 4, None, None, "up"),
        ("a", None, 6, None, 3, None),
    ]
    update = ["rap_node_gid", "rap_time", "rap_secondary_index", "rap_layer"]
    history, _ = view.nodes.to_df(include_property_history=True)
    assert set(cells(df[update])) <= set(cells(history[update]))


def test_nodes_to_df_history():
    df, _ = build_node_update_graph().nodes.to_df(include_property_history=True)

    assert list(df.columns) == [
        "rap_node_gid",
        "rap_node_type",
        "rap_time",
        "rap_secondary_index",
        "rap_layer",
        "x",
        "y",
    ]
    assert node_rows(df) == [
        ("a", "server", 1, None, 1, None),
        ("a", "server", 2, None, None, "up"),
        ("b", None, 3, None, None, None),
        ("b", None, 4, "maintenance", 7, None),
    ]
    event_ids = df["rap_secondary_index"]
    assert event_ids.notna().all() and event_ids.is_unique
    # an integer property keeps its type even though some updates leave it empty
    assert str(df["x"].dtype) == "int64[pyarrow]"


def test_nodes_to_df_history_convert_datetime():
    df, _ = build_node_update_graph().nodes.to_df(
        include_property_history=True, convert_datetime=True
    )

    assert str(df["rap_time"].dtype) == "timestamp[ms, tz=UTC][pyarrow]"
    assert sorted(df["rap_time"]) == [
        datetime(1970, 1, 1, microsecond=t * 1000, tzinfo=utc) for t in [1, 2, 3, 4]
    ]


def test_nodes_to_arrow_history_convert_datetime():
    nodes = build_node_update_graph().nodes
    table, _ = nodes.to_arrow(include_property_history=True, convert_datetime=True)
    df, _ = nodes.to_df(include_property_history=True, convert_datetime=True)

    assert table.schema.field("rap_time").type == pa.timestamp("ms", tz="UTC")
    assert table.to_pandas(types_mapper=pd.ArrowDtype).equals(df)


def test_nodes_to_df_history_follows_the_view():
    g = build_node_update_graph()

    df, _ = g.window(2, 4).nodes.to_df(include_property_history=True)
    assert sorted(zip(df["rap_node_gid"], df["rap_time"])) == [("a", 2), ("b", 3)]

    df, _ = g.nodes.type_filter(["server"]).to_df(include_property_history=True)
    assert set(df["rap_node_gid"]) == {"a"}


def test_nodes_to_df_metadata():
    g = build_node_update_graph()
    g.add_edge(6, "a", "c")  # c only exists through an edge
    _, metadata = g.nodes.to_df()

    assert list(metadata.columns) == ["rap_node_gid", "rap_node_type", "rack", "owner"]
    # one row per node, in node order, whether or not the node has metadata
    assert list(metadata["rap_node_gid"]) == list(g.nodes.id)
    assert sorted(cells(metadata)) == [
        ("a", "server", 3, None),
        ("b", None, None, "ops"),
        ("c", None, None, None),
    ]
    # an integer key keeps its type even though some nodes have no value for it
    assert str(metadata["rack"].dtype) == "int64[pyarrow]"
    # the metadata does not depend on how the updates are returned
    for kwargs in [{"include_property_history": True}, {"convert_datetime": True}]:
        assert g.nodes.to_df(**kwargs)[1].equals(metadata)


def test_nodes_to_df_metadata_follows_the_view():
    g = build_node_update_graph()

    _, metadata = g.window(0, 3).nodes.to_df()
    assert cells(metadata) == [("a", "server", 3, None)]

    _, metadata = g.nodes.type_filter(["server"]).to_df()
    assert cells(metadata) == [("a", "server", 3, None)]


def load_node_tables(updates, metadata):
    """A graph built from the node tables returned by `to_df` or `to_arrow`."""
    g = Graph()
    g.load_nodes(
        updates,
        time="rap_time",
        id="rap_node_gid",
        event_id="rap_secondary_index",
        layer_col="rap_layer",
        node_type_col="rap_node_type",
        properties=["x", "y"],
    )
    g.load_node_metadata(
        metadata,
        id="rap_node_gid",
        node_type_col="rap_node_type",
        metadata=["rack", "owner"],
    )
    return g


def test_nodes_to_df_round_trip():
    updates, metadata = build_node_update_graph().nodes.to_df(
        include_property_history=True
    )

    loaded = load_node_tables(updates, metadata).nodes
    loaded_updates, loaded_metadata = loaded.to_df(include_property_history=True)
    assert node_rows(loaded_updates) == node_rows(updates)
    assert sorted(cells(loaded_metadata)) == sorted(cells(metadata))


def test_nodes_to_arrow():
    nodes = build_node_update_graph().nodes

    for include_property_history in [False, True]:
        tables = nodes.to_arrow(include_property_history=include_property_history)
        dfs = nodes.to_df(include_property_history=include_property_history)
        assert len(tables) == len(dfs) == 2
        for table, df in zip(tables, dfs):
            assert isinstance(table, pa.Table)
            assert table.to_pandas(types_mapper=pd.ArrowDtype).equals(df)

    updates, metadata = nodes.to_arrow(include_property_history=True)
    assert updates.schema.field("rap_time").type == pa.int64()
    assert updates.schema.field("x").type == pa.int64()
    assert metadata.schema.field("rack").type == pa.int64()
    # the tables load back into a graph without going through pandas
    loaded = load_node_tables(updates, metadata).nodes
    loaded_updates, loaded_metadata = loaded.to_df(include_property_history=True)
    expected_updates, expected_metadata = nodes.to_df(include_property_history=True)
    assert node_rows(loaded_updates) == node_rows(expected_updates)
    assert sorted(cells(loaded_metadata)) == sorted(cells(expected_metadata))
