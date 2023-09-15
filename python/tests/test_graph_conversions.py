from raphtory import Graph
from raphtory import export
import pandas as pd
import json
from pathlib import Path

base_dir = Path(__file__).parent


def build_graph():
    edges_df = pd.read_csv(base_dir / "data/network_traffic_edges.csv")
    edges_df["timestamp"] = pd.to_datetime(edges_df["timestamp"]).astype(
        "datetime64[ms, UTC]"
    )

    vertices_df = pd.read_csv(base_dir / "data/network_traffic_vertices.csv")
    vertices_df["timestamp"] = pd.to_datetime(vertices_df["timestamp"]).astype(
        "datetime64[ms, UTC]"
    )

    return Graph.load_from_pandas(
        edges_df=edges_df,
        src="source",
        dst="destination",
        time="timestamp",
        props=["data_size_MB"],
        layer_in_df="transaction_type",
        const_props=["is_encrypted"],
        shared_const_props={"datasource": "data/network_traffic_edges.csv"},
        vertex_df=vertices_df,
        vertex_col="server_id",
        vertex_time_col="timestamp",
        vertex_props=["OS_version", "primary_function", "uptime_days"],
        vertex_const_props=["server_name", "hardware_type"],
        vertex_shared_const_props={"datasource": "data/network_traffic_edges.csv"},
    )


def test_py_vis():
    g = build_graph()
    pyvis_g = export.to_pyvis(g, directed=True)

    assert pyvis_g.nodes == [
        {
            "color": "#97c2fc",
            "id": 7678824742430955432,
            "image": "https://cdn-icons-png.flaticon.com/512/7584/7584620.png",
            "label": "ServerA",
            "shape": "dot",
        },
        {
            "color": "#97c2fc",
            "id": 7718004695861170879,
            "image": "https://cdn-icons-png.flaticon.com/512/7584/7584620.png",
            "label": "ServerB",
            "shape": "dot",
        },
        {
            "color": "#97c2fc",
            "id": 17918514325589227856,
            "image": "https://cdn-icons-png.flaticon.com/512/7584/7584620.png",
            "label": "ServerC",
            "shape": "dot",
        },
        {
            "color": "#97c2fc",
            "id": 14902018402467198225,
            "image": "https://cdn-icons-png.flaticon.com/512/7584/7584620.png",
            "label": "ServerD",
            "shape": "dot",
        },
        {
            "color": "#97c2fc",
            "id": 11577954539736240602,
            "image": "https://cdn-icons-png.flaticon.com/512/7584/7584620.png",
            "label": "ServerE",
            "shape": "dot",
        },
    ]
    assert pyvis_g.edges == [
        {
            "arrowStrikethrough": False,
            "arrows": "to",
            "color": "#000000",
            "from": 7678824742430955432,
            "title": "",
            "to": 7718004695861170879,
            "value": 1,
        },
        {
            "arrowStrikethrough": False,
            "arrows": "to",
            "color": "#000000",
            "from": 7678824742430955432,
            "title": "",
            "to": 17918514325589227856,
            "value": 1,
        },
        {
            "arrowStrikethrough": False,
            "arrows": "to",
            "color": "#000000",
            "from": 7718004695861170879,
            "title": "",
            "to": 14902018402467198225,
            "value": 1,
        },
        {
            "arrowStrikethrough": False,
            "arrows": "to",
            "color": "#000000",
            "from": 17918514325589227856,
            "title": "",
            "to": 7678824742430955432,
            "value": 1,
        },
        {
            "arrowStrikethrough": False,
            "arrows": "to",
            "color": "#000000",
            "from": 14902018402467198225,
            "title": "",
            "to": 17918514325589227856,
            "value": 1,
        },
        {
            "arrowStrikethrough": False,
            "arrows": "to",
            "color": "#000000",
            "from": 14902018402467198225,
            "title": "",
            "to": 11577954539736240602,
            "value": 1,
        },
        {
            "arrowStrikethrough": False,
            "arrows": "to",
            "color": "#000000",
            "from": 11577954539736240602,
            "title": "",
            "to": 7718004695861170879,
            "value": 1,
        },
    ]


def test_networkx_full_history():
    g = build_graph()

    networkxGraph = export.to_networkx(g)
    assert networkxGraph.number_of_nodes() == 5
    assert networkxGraph.number_of_edges() == 7

    nodeList = list(networkxGraph.nodes(data=True))
    server_list = [
        (
            "ServerA",
            {
                "OS_version": [
                    (1693555200000, "Ubuntu 20.04"),
                    (1693555260000, "Ubuntu 20.04"),
                    (1693555320000, "Ubuntu 20.04"),
                ],
                "_id": "ServerA",
                "datasource": "data/network_traffic_edges.csv",
                "hardware_type": "Blade Server",
                "primary_function": [
                    (1693555200000, "Database"),
                    (1693555260000, "Database"),
                    (1693555320000, "Database"),
                ],
                "server_name": "Alpha",
                "update_history": [
                    1693555200000,
                    1693555260000,
                    1693555320000,
                    1693555500000,
                    1693556400000,
                ],
                "uptime_days": [
                    (1693555200000, 120),
                    (1693555260000, 121),
                    (1693555320000, 122),
                ],
            },
        ),
        (
            "ServerB",
            {
                "OS_version": [(1693555500000, "Red Hat 8.1")],
                "_id": "ServerB",
                "datasource": "data/network_traffic_edges.csv",
                "hardware_type": "Rack Server",
                "primary_function": [(1693555500000, "Web Server")],
                "server_name": "Beta",
                "update_history": [
                    1693555200000,
                    1693555500000,
                    1693555800000,
                    1693556700000,
                ],
                "uptime_days": [(1693555500000, 45)],
            },
        ),
        (
            "ServerC",
            {
                "OS_version": [(1693555800000, "Windows Server 2022")],
                "_id": "ServerC",
                "datasource": "data/network_traffic_edges.csv",
                "hardware_type": "Blade Server",
                "primary_function": [(1693555800000, "File Storage")],
                "server_name": "Charlie",
                "update_history": [
                    1693555500000,
                    1693555800000,
                    1693556400000,
                    1693557000000,
                    1693557060000,
                    1693557120000,
                ],
                "uptime_days": [(1693555800000, 90)],
            },
        ),
        (
            "ServerD",
            {
                "OS_version": [(1693556100000, "Ubuntu 20.04")],
                "_id": "ServerD",
                "datasource": "data/network_traffic_edges.csv",
                "hardware_type": "Tower Server",
                "primary_function": [(1693556100000, "Application Server")],
                "server_name": "Delta",
                "update_history": [
                    1693555800000,
                    1693556100000,
                    1693557000000,
                    1693557060000,
                    1693557120000,
                ],
                "uptime_days": [(1693556100000, 60)],
            },
        ),
        (
            "ServerE",
            {
                "OS_version": [(1693556400000, "Red Hat 8.1")],
                "_id": "ServerE",
                "datasource": "data/network_traffic_edges.csv",
                "hardware_type": "Rack Server",
                "primary_function": [(1693556400000, "Backup")],
                "server_name": "Echo",
                "update_history": [1693556100000, 1693556400000, 1693556700000],
                "uptime_days": [(1693556400000, 30)],
            },
        ),
    ]
    assert nodeList == server_list

    edgeList = list(networkxGraph.edges(data=True))
    resultList = [
        (
            "ServerA",
            "ServerB",
            {
                "data_size_MB": [(1693555200000, 5.6)],
                "datasource": "data/network_traffic_edges.csv",
                "is_encrypted": True,
                "layer": "Critical System Request",
                "update_history": [1693555200000],
            },
        ),
        (
            "ServerA",
            "ServerC",
            {
                "data_size_MB": [(1693555500000, 7.1)],
                "datasource": "data/network_traffic_edges.csv",
                "is_encrypted": False,
                "layer": "File Transfer",
                "update_history": [1693555500000],
            },
        ),
        (
            "ServerB",
            "ServerD",
            {
                "data_size_MB": [(1693555800000, 3.2)],
                "datasource": "data/network_traffic_edges.csv",
                "is_encrypted": True,
                "layer": "Standard Service Request",
                "update_history": [1693555800000],
            },
        ),
        (
            "ServerC",
            "ServerA",
            {
                "data_size_MB": [(1693556400000, 4.5)],
                "datasource": "data/network_traffic_edges.csv",
                "is_encrypted": True,
                "layer": "Critical System Request",
                "update_history": [1693556400000],
            },
        ),
        (
            "ServerD",
            "ServerC",
            {
                "data_size_MB": [
                    (1693557000000, 5.0),
                    (1693557060000, 10.0),
                    (1693557120000, 15.0),
                ],
                "datasource": "data/network_traffic_edges.csv",
                "is_encrypted": True,
                "layer": "Standard Service Request",
                "update_history": [1693557000000, 1693557060000, 1693557120000],
            },
        ),
        (
            "ServerD",
            "ServerE",
            {
                "data_size_MB": [(1693556100000, 8.9)],
                "datasource": "data/network_traffic_edges.csv",
                "is_encrypted": False,
                "layer": "Administrative Command",
                "update_history": [1693556100000],
            },
        ),
        (
            "ServerE",
            "ServerB",
            {
                "data_size_MB": [(1693556700000, 6.2)],
                "datasource": "data/network_traffic_edges.csv",
                "is_encrypted": False,
                "layer": "File Transfer",
                "update_history": [1693556700000],
            },
        ),
    ]
    assert edgeList == resultList


def test_networkx_exploded():
    g = build_graph()

    networkxGraph = export.to_networkx(g, explode_edges=True)
    assert networkxGraph.number_of_nodes() == 5
    assert networkxGraph.number_of_edges() == 9

    edgeList = list(networkxGraph.edges(data=True))
    resultList = [
        (
            "ServerA",
            "ServerB",
            {
                "data_size_MB": [(1693555200000, 5.6)],
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
                "data_size_MB": [(1693555500000, 7.1)],
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
                "data_size_MB": [(1693555800000, 3.2)],
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
                "data_size_MB": [(1693556400000, 4.5)],
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
                "data_size_MB": [(1693557000000, 5.0)],
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
                "data_size_MB": [(1693557060000, 10.0)],
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
                "data_size_MB": [(1693557120000, 15.0)],
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
                "data_size_MB": [(1693556100000, 8.9)],
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
                "data_size_MB": [(1693556700000, 6.2)],
                "datasource": "data/network_traffic_edges.csv",
                "is_encrypted": False,
                "layer": "File Transfer",
                "update_history": 1693556700000,
            },
        ),
    ]
    assert edgeList == resultList


def test_networkx_no_props():
    g = build_graph()

    networkxGraph = export.to_networkx(
        g, include_vertex_properties=False, include_edge_properties=False
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
    assert nodeList == resultList

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
    assert edgeList == resultList

    networkxGraph = export.to_networkx(
        g,
        include_vertex_properties=False,
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
    assert nodeList == resultList

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
    assert edgeList == resultList

    networkxGraph = export.to_networkx(
        g, include_edge_properties=False, explode_edges=True
    )
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
    assert edgeList == resultList


def test_networkx_no_history():
    g = build_graph()

    networkxGraph = export.to_networkx(
        g, include_property_histories=False, include_update_history=False
    )

    nodeList = list(networkxGraph.nodes(data=True))
    resultList = [
        (
            "ServerA",
            {
                "OS_version": "Ubuntu 20.04",
                "_id": "ServerA",
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
                "_id": "ServerB",
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
                "_id": "ServerC",
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
                "_id": "ServerD",
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
                "_id": "ServerE",
                "datasource": "data/network_traffic_edges.csv",
                "hardware_type": "Rack Server",
                "primary_function": "Backup",
                "server_name": "Echo",
                "uptime_days": 30,
            },
        ),
    ]
    assert nodeList == resultList

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
    assert edgeList == resultList

    networkxGraph = export.to_networkx(
        g, include_property_histories=False, explode_edges=True
    )
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
    assert edgeList == resultList


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

    edge_df = export.to_edge_df(g)
    save_df_to_json(edge_df, "expected/dataframe_output/edge_df_all.json")
    edge_df = export.to_edge_df(g, include_edge_properties=False)
    save_df_to_json(edge_df, "expected/dataframe_output/edge_df_no_props.json")
    edge_df = export.to_edge_df(g, include_update_history=False)
    save_df_to_json(edge_df, "expected/dataframe_output/edge_df_no_hist.json")
    edge_df = export.to_edge_df(g, include_property_histories=False)
    save_df_to_json(edge_df, "expected/dataframe_output/edge_df_no_prop_hist.json")

    edge_df = export.to_edge_df(g, explode_edges=True)
    save_df_to_json(edge_df, "expected/dataframe_output/edge_df_exploded.json")
    edge_df = export.to_edge_df(g, explode_edges=True, include_edge_properties=False)
    save_df_to_json(edge_df, "expected/dataframe_output/edge_df_exploded_no_props.json")
    edge_df = export.to_edge_df(g, explode_edges=True, include_update_history=False)
    save_df_to_json(edge_df, "expected/dataframe_output/edge_df_exploded_no_hist.json")
    edge_df = export.to_edge_df(g, explode_edges=True, include_property_histories=False)
    save_df_to_json(
        edge_df, "expected/dataframe_output/edge_df_exploded_no_prop_hist.json"
    )

    vertex_df = export.to_vertex_df(g)
    save_df_to_json(vertex_df, "expected/dataframe_output/vertex_df_all.json")
    vertex_df = export.to_vertex_df(g, include_vertex_properties=False)
    save_df_to_json(vertex_df, "expected/dataframe_output/vertex_df_no_props.json")
    vertex_df = export.to_vertex_df(g, include_update_history=False)
    save_df_to_json(vertex_df, "expected/dataframe_output/vertex_df_no_hist.json")
    vertex_df = export.to_vertex_df(g, include_property_histories=False)
    save_df_to_json(vertex_df, "expected/dataframe_output/vertex_df_no_prop_hist.json")


def compare_df(df1, df2):
    # Have to do this way due to the number of maps inside the dataframes
    s1 = df1.to_json()
    s2 = df2.to_json()
    data1 = json.loads(s1)
    data2 = json.loads(s2)
    assert data1 == data2


def test_to_df():
    g = build_graph()

    compare_df(
        export.to_edge_df(g),
        pd.read_json(base_dir / "expected/dataframe_output/edge_df_all.json"),
    )

    compare_df(
        export.to_edge_df(g, include_edge_properties=False),
        pd.read_json(base_dir / "expected/dataframe_output/edge_df_no_props.json"),
    )

    compare_df(
        export.to_edge_df(g, include_update_history=False),
        pd.read_json(base_dir / "expected/dataframe_output/edge_df_no_hist.json"),
    )

    compare_df(
        export.to_edge_df(g, include_property_histories=False),
        pd.read_json(base_dir / "expected/dataframe_output/edge_df_no_prop_hist.json"),
    )

    compare_df(
        export.to_edge_df(g, explode_edges=True),
        pd.read_json(base_dir / "expected/dataframe_output/edge_df_exploded.json"),
    )
    compare_df(
        export.to_edge_df(g, explode_edges=True, include_edge_properties=False),
        pd.read_json(
            base_dir / "expected/dataframe_output/edge_df_exploded_no_props.json"
        ),
    )

    compare_df(
        export.to_edge_df(g, explode_edges=True, include_update_history=False),
        pd.read_json(
            base_dir / "expected/dataframe_output/edge_df_exploded_no_hist.json"
        ),
    )

    compare_df(
        export.to_edge_df(g, explode_edges=True, include_property_histories=False),
        pd.read_json(
            base_dir / "expected/dataframe_output/edge_df_exploded_no_prop_hist.json"
        ),
    )

    compare_df(
        export.to_vertex_df(g),
        pd.read_json(base_dir / "expected/dataframe_output/vertex_df_all.json"),
    )
    compare_df(
        export.to_vertex_df(g, include_vertex_properties=False),
        pd.read_json(base_dir / "expected/dataframe_output/vertex_df_no_props.json"),
    )
    compare_df(
        export.to_vertex_df(g, include_update_history=False),
        pd.read_json(base_dir / "expected/dataframe_output/vertex_df_no_hist.json"),
    )
    compare_df(
        export.to_vertex_df(g, include_property_histories=False),
        pd.read_json(
            base_dir / "expected/dataframe_output/vertex_df_no_prop_hist.json"
        ),
    )
