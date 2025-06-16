# --8<-- [start:read_data]
import pandas as pd

edges_df = pd.read_csv(
    "data/OBS_data.txt", sep="\t", header=0, usecols=[0, 1, 2, 3, 4], parse_dates=[0]
)
edges_df["DateTime"] = pd.to_datetime(edges_df["DateTime"])
edges_df.dropna(axis=0, inplace=True)
edges_df["Weight"] = edges_df["Category"].apply(
    lambda c: 1 if (c == "Affiliative") else (-1 if (c == "Agonistic") else 0)
)
print(edges_df.head())
# --8<-- [end:read_data]

# --8<-- [start:new_graph]
import raphtory as rp

g = rp.Graph()
g.load_edges_from_pandas(
    df=edges_df,
    src="Actor",
    dst="Recipient",
    time="DateTime",
    layer_col="Behavior",
    properties=["Weight"],
)
print(g)
# --8<-- [end:new_graph]

# --8<-- [start:graph_metrics]

print("Stats on the graph structure:")

number_of_nodes = g.count_nodes()
number_of_edges = g.count_edges()
total_interactions = g.count_temporal_edges()
unique_layers = g.unique_layers

print("Number of nodes (Baboons):", number_of_nodes)
print("Number of unique edges (src,dst,layer):", number_of_edges)
print("Total interactions (edge updates):", total_interactions)
print("Unique layers:", unique_layers, "\n")


print("Stats on the graphs time range:")

earliest_datetime = g.earliest_date_time
latest_datetime = g.latest_date_time
earliest_epoch = g.earliest_time
latest_epoch = g.latest_time

print("Earliest datetime:", earliest_datetime)
print("Latest datetime:", latest_datetime)
print("Earliest time (Unix Epoch):", earliest_epoch)
print("Latest time (Unix Epoch):", latest_epoch)
# --8<-- [end:graph_metrics]

# --8<-- [start:graph_functions]
print("Checking if specific nodes and edges are in the graph:")
if g.has_node(id="LOME"):
    print("Lomme is in the graph")
if g.layer("Playing with").has_edge(src="LOME", dst="NEKKE"):
    print("Lomme has played with Nekke \n")

print("Getting individual nodes and edges:")
print(g.node("LOME"))
print(g.edge("LOME", "NEKKE"), "\n")

print("Getting iterators over all nodes and edges:")
print(g.nodes)
print(g.edges)
# --8<-- [end:graph_functions]


# --8<-- [start:node_metrics]
v = g.node("FELIPE")
print(
    f"{v.name}'s first interaction was at {v.earliest_date_time} and their last interaction was at {v.latest_date_time}\n"
)
history = v.history_date_time()
# We format the returned datetime objects here to make the list more readable
history_formatted = [date.strftime("%Y-%m-%d %H:%M:%S") for date in history]

print(f"{v.name} had interactions at the following times: {history_formatted}\n")

# --8<-- [end:node_metrics]

# --8<-- [start:node_neighbours]
v = g.node("FELIPE")
v_name = v.name
in_degree = v.in_degree()
out_degree = v.out_degree()
in_edges = v.in_edges
neighbours = v.neighbours
neighbour_names = v.neighbours.name.collect()

print(
    f"{v_name} has {in_degree} incoming interactions and {out_degree} outgoing interactions.\n"
)
print(in_edges)
print(neighbours, "\n")
print(f"{v_name} interacted with the following baboons {neighbour_names}")
# --8<-- [end:node_neighbours]

# --8<-- [start:edge_history]
e = g.edge("FELIPE", "MAKO")
e_reversed = g.edge("MAKO", "FELIPE")
e_history = [date.strftime("%Y-%m-%d %H:%M:%S") for date in e.history_date_time()]
e_reversed_history = [
    date.strftime("%Y-%m-%d %H:%M:%S") for date in e_reversed.history_date_time()
]

print(
    f"The edge from {e.src.name} to {e.dst.name} covers the following layers: {e.layer_names}"
)
print(
    f"and has updates between {e.earliest_date_time} and {e.latest_date_time} at the following times: {e_history}\n"
)
print(
    f"The edge from {e_reversed.src.name} to {e_reversed.dst.name} covers the following layers: {e_reversed.layer_names}"
)
print(
    f"and has updates between {e_reversed.earliest_date_time} and {e_reversed.latest_date_time} at the following times: {e_reversed_history}"
)
# --8<-- [end:edge_history]

# --8<-- [start:edge_explode_layer]
print("Update history per layer:")
for e in g.edge("FELIPE", "MAKO").explode_layers():
    e_history = [date.strftime("%Y-%m-%d %H:%M:%S") for date in e.history_date_time()]
    print(
        f"{e.src.name} interacted with {e.dst.name} with the following behaviour '{e.layer_name}' at this times: {e_history}"
    )

print()
print("Individual updates as edges:")
for e in g.edge("FELIPE", "MAKO").explode():
    print(
        f"At {e.date_time} {e.src.name} interacted with {e.dst.name} in the following manner: '{e.layer_name}'"
    )

print()
print("Individual updates for 'Touching' and 'Carrying:")
for e in g.edge("FELIPE", "MAKO").layers(["Touching", "Carrying"]).explode():
    print(
        f"At {e.date_time} {e.src.name} interacted with {e.dst.name} in the following manner: '{e.layer_name}'"
    )
# --8<-- [end:edge_explode_layer]

# --8<-- [start:properties]
from raphtory import Graph

property_g = Graph()
# Create the node and add a variety of temporal properties
v = property_g.add_node(
    timestamp=1,
    id="User",
    properties={"count": 1, "greeting": "hi", "encrypted": True},
)
property_g.add_node(
    timestamp=2,
    id="User",
    properties={"count": 2, "balance": 0.6, "encrypted": False},
)
property_g.add_node(
    timestamp=3,
    id="User",
    properties={"balance": 0.9, "greeting": "hello", "encrypted": True},
)
# Add some constant properties
v.add_constant_properties(
    properties={
        "inner data": {"name": "bob", "value list": [1, 2, 3]},
        "favourite greetings": ["hi", "hello", "howdy"],
    },
)
# Call all of the functions on the properties object
properties = v.properties
print("Property keys:", properties.keys())
print("Property values:", properties.values())
print("Property tuples:", properties.items())
print("Latest value of balance:", properties.get("balance"))
print("Property keys:", properties.as_dict(), "\n")

# Access the keys of the constant and temporal properties individually
constant_properties = properties.constant
temporal_properties = properties.temporal
print("Constant property keys:", constant_properties.keys())
print("Constant property keys:", temporal_properties.keys())
# --8<-- [end:properties]


# --8<-- [start:temporal_properties]
properties = g.edge("FELIPE", "MAKO").properties.temporal
print("Property keys:", properties.keys())
weight_prop = properties.get("Weight")
print("Weight property history:", weight_prop.items())
print("Average interaction weight:", weight_prop.mean())
print("Total interactions:", weight_prop.count())
print("Total interaction weight:", weight_prop.sum())

# --8<-- [end:temporal_properties]

# --8<-- [start:function_chains]
node_names = g.nodes.name
two_hop_neighbours = g.nodes.neighbours.neighbours.name.collect()
combined = zip(node_names, two_hop_neighbours)
for name, two_hop_neighbour in combined:
    print(f"{name} has the following two hop neighbours {two_hop_neighbour}")

# --8<-- [end:function_chains]


# --8<-- [start:friendship]
v = g.node("FELIPE")
neighbours_weighted = list(
    zip(
        v.out_edges.dst.name,
        v.out_edges.properties.temporal.get("Weight").values().sum(),
    )
)
sorted_weights = sorted(neighbours_weighted, key=lambda v: v[1], reverse=True)
print(f"Felipe's favourite baboons in descending order are {sorted_weights}")

annoying_monkeys = list(
    zip(
        g.nodes.name,
        g.nodes.in_edges.properties.temporal.get("Weight")
        .values()
        .sum()  # sum the weights within each edge
        .mean()  # average the summed weights for each monkey
        .collect(),
    )
)
most_annoying = sorted(annoying_monkeys, key=lambda v: v[1])[0]
print(
    f"{most_annoying[0]} is the most annoying monkey with an average score of {most_annoying[1]}"
)

# --8<-- [end:friendship]

# --8<-- [start:at]
v = g.node("LOME")

print(f"Across the full dataset {v.name} interacted with {v.degree()} other monkeys.")

v_before = g.before(1560428239000).node("LOME")  # 13/06/2019 12:17:19 as epoch
print(
    f"Between {v_before.start_date_time} and {v_before.end_date_time}, {v_before.name} interacted with {v_before.degree()} other monkeys."
)

v_after = g.node("LOME").after("2019-06-30 9:07:31")
print(
    f"Between {v_after.start_date_time} and {v_after.end_date_time}, {v_after.name} interacted with {v_after.degree()} other monkeys."
)

# --8<-- [end:at]

# --8<-- [start:window]
from datetime import datetime

start_day = datetime.strptime("2019-06-13", "%Y-%m-%d")
end_day = datetime.strptime("2019-06-14", "%Y-%m-%d")
e = g.edge("LOME", "NEKKE")
print(
    f"Across the full dataset {e.src.name} interacted with {e.dst.name} {len(e.history())} times"
)
e = e.window(start_day, end_day)
print(
    f"Between {e.start_date_time} and {e.end_date_time}, {e.src.name} interacted with {e.dst.name} {len(e.history())} times"
)
print(
    f"Window start: {e.start_date_time}, First update: {e.earliest_date_time}, Last update: {e.latest_date_time}, Window End: {e.end_date_time}"
)

# --8<-- [end:window]

# --8<-- [start:hopping]
first_day = datetime.strptime("2019-06-20", "%Y-%m-%d")
second_day = datetime.strptime("2019-06-25", "%Y-%m-%d")

one_hop_neighbours = g.before(first_day).node("LOME").neighbours.name.collect()
two_hop_neighbours = (
    g.before(first_day).node("LOME").neighbours.after(second_day).neighbours.collect()
)
print(
    f"When the before is applied to the graph, LOME's one hop neighbours are: {one_hop_neighbours}"
)
print(
    f"When the before is applied to the graph, LOME's two hop neighbours are: {two_hop_neighbours}"
)
one_hop_neighbours = g.node("LOME").before(first_day).neighbours.name.collect()
two_hop_neighbours = (
    g.node("LOME")
    .before(first_day)
    .neighbours.after(second_day)
    .neighbours.name.collect()
)
print(
    f"When the before is applied to the node, LOME's one hop neighbours are: {one_hop_neighbours}"
)
print(
    f"When the before is applied to the node, LOME's two hop neighbours are: {two_hop_neighbours}"
)

# --8<-- [end:hopping]

# --8<-- [start:expanding]
print(
    f"The full range of time in the graph is {g.earliest_date_time} to {g.latest_date_time}\n"
)

for expanding_g in g.expanding("1 week"):
    print(
        f"From {expanding_g.start_date_time} to {expanding_g.end_date_time} there were {expanding_g.count_temporal_edges()} monkey interactions"
    )

print()
start_day = datetime.strptime("2019-06-13", "%Y-%m-%d")
end_day = datetime.strptime("2019-06-23", "%Y-%m-%d")
for expanding_g in g.window(start_day, end_day).expanding(
    "2 days, 3 hours, 12 minutes and 6 seconds"
):
    print(
        f"From {expanding_g.start_date_time} to {expanding_g.end_date_time} there were {expanding_g.count_temporal_edges()} monkey interactions"
    )

# --8<-- [end:expanding]

# --8<-- [start:rolling_intro]
print("Rolling 1 week")
for rolling_g in g.rolling(window="1 week"):
    print(
        f"From {rolling_g.start_date_time} to {rolling_g.end_date_time} there were {rolling_g.count_temporal_edges()} monkey interactions"
    )
# --8<-- [end:rolling_intro]

# --8<-- [start:rolling_intro_2]
print("\nRolling 1 week, stepping 2 days (overlapping window)")
for rolling_g in g.rolling(window="1 week", step="2 days"):
    print(
        f"From {rolling_g.start_date_time} to {rolling_g.end_date_time} there were {rolling_g.count_temporal_edges()} monkey interactions"
    )
# --8<-- [end:rolling_intro_2]

# --8<-- [start:rolling]
# mkdocs: render
###RECREATION OF THE GRAPH SO IT CAN BE RENDERED
import matplotlib.pyplot as plt
import pandas as pd
from raphtory import Graph

edges_df = pd.read_csv(
    "data/OBS_data.txt", sep="\t", header=0, usecols=[0, 1, 2, 3, 4], parse_dates=[0]
)
edges_df["DateTime"] = pd.to_datetime(edges_df["DateTime"])
edges_df.dropna(axis=0, inplace=True)
edges_df["Weight"] = edges_df["Category"].apply(
    lambda c: 1 if (c == "Affiliative") else (-1 if (c == "Agonistic") else 0)
)

g = Graph()
g.load_edges_from_pandas(
    df=edges_df,
    time="DateTime",
    src="Actor",
    dst="Recipient",
    layer_col="Behavior",
    properties=["Weight"],
)

###ACTUAL IMPORT CODE
importance = []
time = []
for rolling_lome in g.node("LOME").rolling("1 day"):
    importance.append(rolling_lome.degree())
    time.append(rolling_lome.end_date_time)

plt.plot(time, importance, marker="o")
plt.xlabel("Date")
plt.xticks(rotation=45)
plt.ylabel("Daily Unique Interactions")
plt.title("Lome's daily interaction count")
plt.grid(True)
# --8<-- [end:rolling]

# --8<-- [start:layered]
total_weight = g.edges.properties.temporal.get("Weight").values().sum().sum()
print(f"Total weight across all edges is {total_weight}.")

total_weight = (
    g.layers(["Grooming", "Resting"])
    .edges.properties.temporal.get("Weight")
    .values()
    .sum()
    .sum()
)
print(f"Total weight across Grooming and Resting is {total_weight}.")

start_day = datetime.strptime("2019-06-13", "%Y-%m-%d")
end_day = datetime.strptime("2019-06-20", "%Y-%m-%d")
total_weight = (
    g.layers(["Grooming", "Resting"])
    .window(start_day, end_day)
    .edges.properties.temporal.get("Weight")
    .values()
    .sum()
    .sum()
)
print(
    f"Total weight across Grooming and Resting between {start_day} and {end_day} is {total_weight}."
)
# --8<-- [end:layered]

# --8<-- [start:layered_hopping]
two_hop_neighbours = set(
    g.node("LOME")
    .layer("Grooming")
    .neighbours.layer("Resting")
    .neighbours.name.collect()
)
print(
    f"When the Grooming layer is applied to the node, LOME's two hop neighbours are: {two_hop_neighbours}"
)

# --8<-- [end:layered_hopping]

# --8<-- [start:subgraph]
temp = g.count_nodes()
print(f"There are {temp} monkeys in the whole graph")

subgraph = g.subgraph(["FELIPE", "LIPS", "NEKKE", "LOME", "BOBO"])
print(f"There are {subgraph.count_nodes()} monkeys in the subgraph")
neighbours = g.node("FELIPE").neighbours.name.collect()
print(f"FELIPE has the following neighbours in the full graph: {neighbours}")
neighbours = subgraph.node("FELIPE").neighbours.name.collect()
print(f"FELIPE has the following neighbours in the subgraph: {neighbours}")
start_day = datetime.strptime("2019-06-17", "%Y-%m-%d")
end_day = datetime.strptime("2019-06-18", "%Y-%m-%d")
neighbours = (
    subgraph.node("FELIPE").window(start_day, end_day).neighbours.name.collect()
)
print(
    f"FELIPE has the following neighbours in the subgraph between {start_day} and {end_day}: {neighbours}"
)

# --8<-- [end:subgraph]

# --8<-- [start:materialize]
start_time = datetime.strptime("2019-06-17", "%Y-%m-%d")
end_time = datetime.strptime("2019-06-18", "%Y-%m-%d")
windowed_view = g.window(start_time, end_time)

materialized_graph = windowed_view.materialize()
print(
    f"Before the update the view had {windowed_view.count_temporal_edges()} edge updates"
)
print(
    f"Before the update the materialized graph had {materialized_graph.count_temporal_edges()} edge updates"
)
print("Adding new update to materialized_graph")
materialized_graph.add_edge(1, "FELIPE", "LOME", properties={"Weight": 1}, layer="Grooming")
print(
    f"After the update the view had {windowed_view.count_temporal_edges()} edge updates"
)
print(
    f"After the update the materialized graph had {materialized_graph.count_temporal_edges()} edge updates"
)
# --8<-- [end:materialize]
