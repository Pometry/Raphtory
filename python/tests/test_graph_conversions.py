from raphtory import Graph
import pandas as pd
import json
from pathlib import Path
import numpy as np
from datetime import datetime, timezone
import pytz

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

    return Graph.load_from_pandas(
        edge_df=edges_df,
        edge_time="timestamp",
        edge_src="source",
        edge_dst="destination",
        edge_properties=["data_size_MB"],
        edge_layer="transaction_type",
        edge_constant_properties=["is_encrypted"],
        edge_shared_constant_properties={"datasource": "data/network_traffic_edges.csv"},
        node_df=nodes_df,
        node_id="server_id",
        node_time="timestamp",
        node_properties=["OS_version", "primary_function", "uptime_days"],
        node_constant_properties=["server_name", "hardware_type"],
        node_shared_constant_properties={"datasource": "data/network_traffic_edges.csv"},
    )


def build_graph_without_datetime_type():
    edges_df = pd.read_csv(base_dir / "data/network_traffic_edges.csv")
    edges_df["timestamp"] = pd.to_datetime(edges_df["timestamp"])

    nodes_df = pd.read_csv(base_dir / "data/network_traffic_nodes.csv")
    nodes_df["timestamp"] = pd.to_datetime(nodes_df["timestamp"])

    return Graph.load_from_pandas(
        edge_df=edges_df,
        edge_time="timestamp",
        edge_src="source",
        edge_dst="destination",
        edge_properties=["data_size_MB"],
        edge_layer="transaction_type",
        edge_constant_properties=["is_encrypted"],
        edge_shared_constant_properties={"datasource": "data/network_traffic_edges.csv"},
        node_df=nodes_df,
        node_id="server_id",
        node_time="timestamp",
        node_properties=["OS_version", "primary_function", "uptime_days"],
        node_constant_properties=["server_name", "hardware_type"],
        node_shared_constant_properties={"datasource": "data/network_traffic_edges.csv"},
    )


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
    g.load_nodes_from_pandas(
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

    assert g.node("a")["array_column"] == [1, 2, 3]

    assert g.node("a")["date_column_ms"] == df["date_column_ms"][0]
    assert g.node("a")["date_column_us"] == df["date_column_us"][0]

    assert g.node("a")["date_column"] == date_column[0]
    assert g.node("a")["date_column_ns"] == df["date_column_ns"][0]


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
                "id": 'ServerA',
                "image": "https://cdn-icons-png.flaticon.com/512/7584/7584620.png",
                "label": "ServerA",
                "shape": "dot",
            },
            {
                "color": "#97c2fc",
                "id": 'ServerB',
                "image": "https://cdn-icons-png.flaticon.com/512/7584/7584620.png",
                "label": "ServerB",
                "shape": "dot",
            },
            {
                "color": "#97c2fc",
                "id": 'ServerC',
                "image": "https://cdn-icons-png.flaticon.com/512/7584/7584620.png",
                "label": "ServerC",
                "shape": "dot",
            },
            {
                "color": "#97c2fc",
                "id": 'ServerD',
                "image": "https://cdn-icons-png.flaticon.com/512/7584/7584620.png",
                "label": "ServerD",
                "shape": "dot",
            },
            {
                "color": "#97c2fc",
                "id": 'ServerE',
                "image": "https://cdn-icons-png.flaticon.com/512/7584/7584620.png",
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
                "from": 'ServerA',
                "title": "",
                "to": 'ServerB',
                "value": 0.0,
            },
            {
                "arrowStrikethrough": False,
                "arrows": "to",
                "color": "#000000",
                "from": 'ServerA',
                "title": "",
                "to": 'ServerC',
                "value": 0.0,
            },
            {
                "arrowStrikethrough": False,
                "arrows": "to",
                "color": "#000000",
                "from": 'ServerB',
                "title": "",
                "to": 'ServerD',
                "value": 0.0,
            },
            {
                "arrowStrikethrough": False,
                "arrows": "to",
                "color": "#000000",
                "from": 'ServerC',
                "title": "",
                "to": 'ServerA',
                "value": 0.0,
            },
            {
                "arrowStrikethrough": False,
                "arrows": "to",
                "color": "#000000",
                "from": 'ServerD',
                "title": "",
                "to": 'ServerC',
                "value": 0.0,
            },
            {
                "arrowStrikethrough": False,
                "arrows": "to",
                "color": "#000000",
                "from": 'ServerD',
                "title": "",
                "to": 'ServerE',
                "value": 0.0,
            },
            {
                "arrowStrikethrough": False,
                "arrows": "to",
                "color": "#000000",
                "from": 'ServerE',
                "title": "",
                "to": 'ServerB',
                "value": 0.0,
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
                "constant": {
                    "datasource": "data/network_traffic_edges.csv",
                    "hardware_type": "Blade Server",
                    "server_name": "Alpha",
                },
                "temporal": [
                    ("OS_version", (1693555200000, "Ubuntu 20.04")),
                    ("OS_version", (1693555260000, "Ubuntu 20.04")),
                    ("OS_version", (1693555320000, "Ubuntu 20.04")),
                    ("primary_function", (1693555200000, "Database")),
                    ("primary_function", (1693555260000, "Database")),
                    ("primary_function", (1693555320000, "Database")),
                    ("uptime_days", (1693555200000, 120)),
                    ("uptime_days", (1693555260000, 121)),
                    ("uptime_days", (1693555320000, 122)),
                ],
                "update_history": [
                    1693555200000,
                    1693555260000,
                    1693555320000,
                    1693555500000,
                    1693556400000,
                ],
            },
        ),
        (
            "ServerB",
            {
                "constant": {
                    "hardware_type": "Rack Server",
                    "datasource": "data/network_traffic_edges.csv",
                    "server_name": "Beta",
                },
                "temporal": [
                    ("OS_version", (1693555500000, "Red Hat 8.1")),
                    ("primary_function", (1693555500000, "Web Server")),
                    ("uptime_days", (1693555500000, 45)),
                ],
                "update_history": [
                    1693555200000,
                    1693555500000,
                    1693555800000,
                    1693556700000,
                ],
            },
        ),
        (
            "ServerC",
            {
                "constant": {
                    "hardware_type": "Blade Server",
                    "datasource": "data/network_traffic_edges.csv",
                    "server_name": "Charlie",
                },
                "temporal": [
                    ("OS_version", (1693555800000, "Windows Server 2022")),
                    ("primary_function", (1693555800000, "File Storage")),
                    ("uptime_days", (1693555800000, 90)),
                ],
                "update_history": [
                    1693555500000,
                    1693555800000,
                    1693556400000,
                    1693557000000,
                    1693557060000,
                    1693557120000,
                ],
            },
        ),
        (
            "ServerD",
            {
                "constant": {
                    "hardware_type": "Tower Server",
                    "datasource": "data/network_traffic_edges.csv",
                    "server_name": "Delta",
                },
                "temporal": [
                    ("OS_version", (1693556100000, "Ubuntu 20.04")),
                    ("primary_function", (1693556100000, "Application Server")),
                    ("uptime_days", (1693556100000, 60)),
                ],
                "update_history": [
                    1693555800000,
                    1693556100000,
                    1693557000000,
                    1693557060000,
                    1693557120000,
                ],
            },
        ),
        (
            "ServerE",
            {
                "constant": {
                    "server_name": "Echo",
                    "hardware_type": "Rack Server",
                    "datasource": "data/network_traffic_edges.csv",
                },
                "temporal": [
                    ("OS_version", (1693556400000, "Red Hat 8.1")),
                    ("primary_function", (1693556400000, "Backup")),
                    ("uptime_days", (1693556400000, 30)),
                ],
                "update_history": [1693556100000, 1693556400000, 1693556700000],
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
                "constant": {
                    "is_encrypted": True,
                    "datasource": "data/network_traffic_edges.csv",
                },
                "temporal": [("data_size_MB", [(1693555200000, 5.6)])],
                "layer": "Critical System Request",
                "update_history": [1693555200000],
            },
        ),
        (
            "ServerA",
            "ServerC",
            {
                "constant": {
                    "datasource": "data/network_traffic_edges.csv",
                    "is_encrypted": False,
                },
                "temporal": [("data_size_MB", [(1693555500000, 7.1)])],
                "layer": "File Transfer",
                "update_history": [1693555500000],
            },
        ),
        (
            "ServerB",
            "ServerD",
            {
                "constant": {
                    "is_encrypted": True,
                    "datasource": "data/network_traffic_edges.csv",
                },
                "temporal": [("data_size_MB", [(1693555800000, 3.2)])],
                "layer": "Standard Service Request",
                "update_history": [1693555800000],
            },
        ),
        (
            "ServerC",
            "ServerA",
            {
                "constant": {
                    "is_encrypted": True,
                    "datasource": "data/network_traffic_edges.csv",
                },
                "temporal": [("data_size_MB", [(1693556400000, 4.5)])],
                "layer": "Critical System Request",
                "update_history": [1693556400000],
            },
        ),
        (
            "ServerD",
            "ServerE",
            {
                "constant": {
                    "is_encrypted": False,
                    "datasource": "data/network_traffic_edges.csv",
                },
                "temporal": [("data_size_MB", [(1693556100000, 8.9)])],
                "layer": "Administrative Command",
                "update_history": [1693556100000],
            },
        ),
        (
            "ServerD",
            "ServerC",
            {
                "constant": {
                    "is_encrypted": True,
                    "datasource": "data/network_traffic_edges.csv",
                },
                "temporal": [
                    (
                        "data_size_MB",
                        [
                            (1693557000000, 5.0),
                            (1693557060000, 10.0),
                            (1693557120000, 15.0),
                        ],
                    )
                ],
                "layer": "Standard Service Request",
                "update_history": [1693557000000, 1693557060000, 1693557120000],
            },
        ),
        (
            "ServerE",
            "ServerB",
            {
                "constant": {
                    "is_encrypted": False,
                    "datasource": "data/network_traffic_edges.csv",
                },
                "temporal": [("data_size_MB", [(1693556700000, 6.2)])],
                "layer": "File Transfer",
                "update_history": [1693556700000],
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
                "constant": {
                    "datasource": "data/network_traffic_edges.csv",
                    "is_encrypted": True,
                },
                "temporal": [("data_size_MB", [(1693555200000, 5.6)])],
                "layer": "Critical System Request",
                "update_history": 1693555200000,
            },
        ),
        (
            "ServerA",
            "ServerC",
            {
                "constant": {
                    "is_encrypted": False,
                    "datasource": "data/network_traffic_edges.csv",
                },
                "temporal": [("data_size_MB", [(1693555500000, 7.1)])],
                "layer": "File Transfer",
                "update_history": 1693555500000,
            },
        ),
        (
            "ServerB",
            "ServerD",
            {
                "constant": {
                    "is_encrypted": True,
                    "datasource": "data/network_traffic_edges.csv",
                },
                "temporal": [("data_size_MB", [(1693555800000, 3.2)])],
                "layer": "Standard Service Request",
                "update_history": 1693555800000,
            },
        ),
        (
            "ServerC",
            "ServerA",
            {
                "constant": {
                    "is_encrypted": True,
                    "datasource": "data/network_traffic_edges.csv",
                },
                "temporal": [("data_size_MB", [(1693556400000, 4.5)])],
                "layer": "Critical System Request",
                "update_history": 1693556400000,
            },
        ),
        (
            "ServerD",
            "ServerE",
            {
                "constant": {
                    "datasource": "data/network_traffic_edges.csv",
                    "is_encrypted": False,
                },
                "temporal": [("data_size_MB", [(1693556100000, 8.9)])],
                "layer": "Administrative Command",
                "update_history": 1693556100000,
            },
        ),
        (
            "ServerD",
            "ServerC",
            {
                "constant": {
                    "is_encrypted": True,
                    "datasource": "data/network_traffic_edges.csv",
                },
                "temporal": [("data_size_MB", [(1693557000000, 5.0)])],
                "layer": "Standard Service Request",
                "update_history": 1693557000000,
            },
        ),
        (
            "ServerD",
            "ServerC",
            {
                "constant": {
                    "datasource": "data/network_traffic_edges.csv",
                    "is_encrypted": True,
                },
                "temporal": [("data_size_MB", [(1693557060000, 10.0)])],
                "layer": "Standard Service Request",
                "update_history": 1693557060000,
            },
        ),
        (
            "ServerD",
            "ServerC",
            {
                "constant": {
                    "is_encrypted": True,
                    "datasource": "data/network_traffic_edges.csv",
                },
                "temporal": [("data_size_MB", [(1693557120000, 15.0)])],
                "layer": "Standard Service Request",
                "update_history": 1693557120000,
            },
        ),
        (
            "ServerE",
            "ServerB",
            {
                "constant": {
                    "is_encrypted": False,
                    "datasource": "data/network_traffic_edges.csv",
                },
                "temporal": [("data_size_MB", [(1693556700000, 6.2)])],
                "layer": "File Transfer",
                "update_history": 1693556700000,
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
                    1693555200000,
                    1693555260000,
                    1693555320000,
                    1693555500000,
                    1693556400000,
                ]
            },
        ),
        (
            "ServerB",
            {
                "update_history": [
                    1693555200000,
                    1693555500000,
                    1693555800000,
                    1693556700000,
                ]
            },
        ),
        (
            "ServerC",
            {
                "update_history": [
                    1693555500000,
                    1693555800000,
                    1693556400000,
                    1693557000000,
                    1693557060000,
                    1693557120000,
                ]
            },
        ),
        (
            "ServerD",
            {
                "update_history": [
                    1693555800000,
                    1693556100000,
                    1693557000000,
                    1693557060000,
                    1693557120000,
                ]
            },
        ),
        ("ServerE", {"update_history": [1693556100000, 1693556400000, 1693556700000]}),
    ]
    compare_list_of_dicts(nodeList, resultList)

    edgeList = list(networkxGraph.edges(data=True))
    resultList = [
        (
            "ServerA",
            "ServerB",
            {"layer": "Critical System Request", "update_history": [1693555200000]},
        ),
        (
            "ServerA",
            "ServerC",
            {"layer": "File Transfer", "update_history": [1693555500000]},
        ),
        (
            "ServerB",
            "ServerD",
            {"layer": "Standard Service Request", "update_history": [1693555800000]},
        ),
        (
            "ServerC",
            "ServerA",
            {"layer": "Critical System Request", "update_history": [1693556400000]},
        ),
        (
            "ServerD",
            "ServerC",
            {
                "layer": "Standard Service Request",
                "update_history": [1693557000000, 1693557060000, 1693557120000],
            },
        ),
        (
            "ServerD",
            "ServerE",
            {"layer": "Administrative Command", "update_history": [1693556100000]},
        ),
        (
            "ServerE",
            "ServerB",
            {"layer": "File Transfer", "update_history": [1693556700000]},
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
            {"layer": "Critical System Request", "update_history": 1693555200000},
        ),
        (
            "ServerA",
            "ServerC",
            {"layer": "File Transfer", "update_history": 1693555500000},
        ),
        (
            "ServerB",
            "ServerD",
            {"layer": "Standard Service Request", "update_history": 1693555800000},
        ),
        (
            "ServerC",
            "ServerA",
            {"layer": "Critical System Request", "update_history": 1693556400000},
        ),
        (
            "ServerD",
            "ServerC",
            {"layer": "Standard Service Request", "update_history": 1693557000000},
        ),
        (
            "ServerD",
            "ServerC",
            {"layer": "Standard Service Request", "update_history": 1693557060000},
        ),
        (
            "ServerD",
            "ServerC",
            {"layer": "Standard Service Request", "update_history": 1693557120000},
        ),
        (
            "ServerD",
            "ServerE",
            {"layer": "Administrative Command", "update_history": 1693556100000},
        ),
        (
            "ServerE",
            "ServerB",
            {"layer": "File Transfer", "update_history": 1693556700000},
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
                "update_history": 1693555200000,
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
                "update_history": 1693555500000,
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
                "update_history": 1693555800000,
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
                "update_history": 1693556400000,
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
                "update_history": 1693557000000,
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
                "update_history": 1693557060000,
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
                "update_history": 1693557120000,
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
                "update_history": 1693556100000,
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
                "update_history": 1693556700000,
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

    node_df = g.nodes.to_df()
    save_df_to_json(node_df, "expected/dataframe_output/node_df_all.json")
    node_df = g.nodes.to_df(include_property_history=False)
    save_df_to_json(node_df, "expected/dataframe_output/node_df_no_hist.json")
    node_df = g.nodes.to_df(convert_datetime=False)
    save_df_to_json(node_df, "expected/dataframe_output/node_df_no_datetime.json")


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
    s = json.dumps(d, ensure_ascii=True, sort_keys=True)
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

    compare_df(
        g.nodes.to_df(include_property_history=False),
        pd.read_json(base_dir / "expected/dataframe_output/node_df_no_hist.json"),
    )

    compare_df(
        g.nodes.to_df(include_property_history=True),
        pd.read_json(base_dir / "expected/dataframe_output/node_df_no_explode.json"),
    )

    compare_df(
        g.nodes.to_df(convert_datetime=False, include_property_history=True),
        pd.read_json(base_dir / "expected/dataframe_output/node_df_no_datetime.json"),
    )
