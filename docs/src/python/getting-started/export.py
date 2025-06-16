# --8<-- [start:ingest_data]
from raphtory import Graph
import pandas as pd

server_edges_df = pd.read_csv("data/network_traffic_edges.csv")
server_edges_df["timestamp"] = pd.to_datetime(server_edges_df["timestamp"])

server_nodes_df = pd.read_csv("data/network_traffic_nodes.csv")
server_nodes_df["timestamp"] = pd.to_datetime(server_nodes_df["timestamp"])

print("Network Traffic Edges:")
print(f"{server_edges_df}\n")
print("Network Traffic Servers:")
print(f"{server_nodes_df}\n")

traffic_graph = Graph()
traffic_graph.load_edges_from_pandas(
    df=server_edges_df,
    src="source",
    dst="destination",
    time="timestamp",
    properties=["data_size_MB"],
    layer_col="transaction_type",
    constant_properties=["is_encrypted"],
    shared_constant_properties={"datasource": "data/network_traffic_edges.csv"},
)
traffic_graph.load_nodes_from_pandas(
    df=server_nodes_df,
    id="server_id",
    time="timestamp",
    properties=["OS_version", "primary_function", "uptime_days"],
    constant_properties=["server_name", "hardware_type"],
    shared_constant_properties={"datasource": "data/network_traffic_edges.csv"},
)

monkey_edges_df = pd.read_csv(
    "data/OBS_data.txt", sep="\t", header=0, usecols=[0, 1, 2, 3, 4], parse_dates=[0]
)
monkey_edges_df["DateTime"] = pd.to_datetime(monkey_edges_df["DateTime"])
monkey_edges_df.dropna(axis=0, inplace=True)
monkey_edges_df["Weight"] = monkey_edges_df["Category"].apply(
    lambda c: 1 if (c == "Affiliative") else (-1 if (c == "Agonistic") else 0)
)

print("Monkey Interactions:")
print(f"{monkey_edges_df}\n")

monkey_graph = Graph()
monkey_graph.load_edges_from_pandas(
    df=monkey_edges_df,
    src="Actor",
    dst="Recipient",
    time="DateTime",
    layer_col="Behavior",
    properties=["Weight"],
)


# --8<-- [end:ingest_data]


# --8<-- [start:node_df]

df = traffic_graph.nodes.to_df()
print("--- to_df with default parameters --- ")
print(f"{df}\n")
print()
df = traffic_graph.nodes.to_df(include_property_history=True, convert_datetime=True)
print("--- to_df with property history and datetime conversion ---")
print(f"{df}\n")

# --8<-- [end:node_df]


# --8<-- [start:edge_df]

subgraph = monkey_graph.subgraph(["ANGELE", "FELIPE"])
df = subgraph.edges.to_df()
print("Interactions between Angele and Felipe:")
print(f"{df}\n")

grunting_graph = subgraph.layer("Grunting-Lipsmacking")
print(grunting_graph)
print(grunting_graph.edges)
df = grunting_graph.edges.to_df()
print("Exploding the grunting-Lipsmacking layer")
print(df)
# --8<-- [end:edge_df]


# --8<-- [start:networkX]
nx_g = traffic_graph.to_networkx()

print("Networkx graph:")
print(nx_g)
print()
print("Full property history of ServerA:")
print(nx_g.nodes["ServerA"])
print()

nx_g = traffic_graph.to_networkx(include_property_history=False)

print("Only the latest properties of ServerA:")
print(nx_g.nodes["ServerA"])

# --8<-- [end:networkX]

# --8<-- [start:networkX_vis]
# mkdocs: render
import matplotlib.pyplot as plt
import networkx as nx

from raphtory import Graph
import pandas as pd

server_edges_df = pd.read_csv("data/network_traffic_edges.csv")
server_edges_df["timestamp"] = pd.to_datetime(server_edges_df["timestamp"])

traffic_graph = Graph()
traffic_graph.load_edges_from_pandas(
    df=server_edges_df,
    time="timestamp",
    src="source",
    dst="destination",
)

nx_g = traffic_graph.to_networkx()
nx.draw(nx_g, with_labels=True, node_color="lightblue", edge_color="gray")

# --8<-- [end:networkX_vis]
