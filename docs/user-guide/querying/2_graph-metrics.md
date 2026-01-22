
# Graph metrics and functions

## Basic metrics

Beginning with the [previous graph](1_intro.md) we can start probing it for some basic metrics, such as how many nodes and edges it contains and the time range over which it exists.

In the below code example  the functions `count_edges()` and `count_temporal_edges()` are called and return different results. This is because `count_edges()` returns the number of unique edges and `count_temporal_edges()` returns the total edge updates which have occurred.

Using `count_temporal_edges()` is useful if you want to imagine each edge update as a separate connection between the two nodes. The edges can be accessed in this manner via `edge.explode()`, as is discussed in [edge metrics and functions](../querying/4_edge-metrics.md).

!!! info

    The property APIs are the same for the graph, nodes and edges, these are discussed together in [Property queries](../querying/5_properties.md).

/// tab | :fontawesome-brands-python: Python
```python
import pandas as pd
from raphtory import Graph
from datetime import datetime

edges_df = pd.read_csv(
    "../data/OBS_data.txt", sep="\t", header=0, usecols=[0, 1, 2, 3, 4], parse_dates=[0]
)
edges_df["DateTime"] = pd.to_datetime(edges_df["DateTime"])
edges_df.dropna(axis=0, inplace=True)
edges_df["Weight"] = edges_df["Category"].apply(
    lambda c: 1 if (c == "Affiliative") else (-1 if (c == "Agonistic") else 0)
)

g = Graph()
g.load_edges(
    data=edges_df,
    src="Actor",
    dst="Recipient",
    time="DateTime",
    layer_col="Behavior",
    properties=["Weight"],
)

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

earliest_datetime = g.earliest_time.dt
latest_datetime = g.latest_time.dt
earliest_epoch = g.earliest_time.t
latest_epoch = g.latest_time.t

print("Earliest datetime:", earliest_datetime)
print("Latest datetime:", latest_datetime)
print("Earliest time (Unix Epoch):", earliest_epoch)
print("Latest time (Unix Epoch):", latest_epoch)
```
///

```{.python continuation hide}
assert number_of_nodes == 22
assert number_of_edges == 290
assert total_interactions == 3196
```

!!! Output

    ```output
    Stats on the graph structure:
    Number of nodes (Baboons): 22
    Number of unique edges (src,dst,layer): 290
    Total interactions (edge updates): 3196
    Unique layers: ['Touching', 'Grooming', 'Resting', 'Playing with', 'Presenting', 'Attacking', 'Grunting-Lipsmacking', 'Threatening', 'Embracing', 'Chasing', 'Supplanting', 'Mounting', 'Submission', 'Copulating', 'Carrying', 'Avoiding'] 

    Stats on the graphs time range:
    Earliest datetime: 2019-06-13 09:50:00+00:00
    Latest datetime: 2019-07-10 11:05:00+00:00
    Earliest time (Unix Epoch): 1560419400000
    Latest time (Unix Epoch): 1562756700000
    ```

## Accessing nodes and edges

Three types of functions are provided for accessing the nodes and edges within a graph:

- **Existence check:** using `has_node()` and `has_edge()` you can check if an entity is present within the graph.
- **Direct access:** `node()` and `edge()` will return a node or edge object if the entity is present and `None` if it is not.
- **Iterable access:** `nodes` and `edges` will return iterables for all nodes/edges which can be used within a for loop or as part of a [function chain](../querying/6_chaining.md).

All of these functions are shown in the code below and will appear in several other examples throughout this tutorial.

/// tab | :fontawesome-brands-python: Python
```python
import pandas as pd
from raphtory import Graph
from datetime import datetime

edges_df = pd.read_csv(
    "../data/OBS_data.txt", sep="\t", header=0, usecols=[0, 1, 2, 3, 4], parse_dates=[0]
)
edges_df["DateTime"] = pd.to_datetime(edges_df["DateTime"])
edges_df.dropna(axis=0, inplace=True)
edges_df["Weight"] = edges_df["Category"].apply(
    lambda c: 1 if (c == "Affiliative") else (-1 if (c == "Agonistic") else 0)
)

g = Graph()
g.load_edges(
    data=edges_df,
    src="Actor",
    dst="Recipient",
    time="DateTime",
    layer_col="Behavior",
    properties=["Weight"],
)

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
```
///

```{.python continuation hide}
assert str(g.node("LOME")) == "Node(name=LOME, earliest_time=EventTime(timestamp=1560419520000, event_id=7), latest_time=EventTime(timestamp=1562756100000, event_id=3189))"
```

!!! Output

    ```output
    Checking if specific nodes and edges are in the graph:
    Lomme is in the graph
    Lomme has played with Nekke
    
    Getting individual nodes and edges:
    Node(name=LOME, earliest_time=EventTime(timestamp=1560419520000, event_id=7), latest_time=EventTime(timestamp=1562756100000, event_id=3189))
    Edge(source=LOME, target=NEKKE, earliest_time=EventTime(timestamp=1560421080000, event_id=22), latest_time=EventTime(timestamp=1562755980000, event_id=3185), properties={Weight: 1}, layer(s)=[Playing with, Resting, Grooming, Touching, Carrying])
    
    Getting iterators over all nodes and edges:
    Nodes(Node(name=BOBO, earliest_time=EventTime(timestamp=1560419520000, event_id=8), latest_time=EventTime(timestamp=1562755500000, event_id=3182)), Node(name=PIPO, earliest_time=EventTime(timestamp=1560420660000, event_id=16), latest_time=EventTime(timestamp=1562752560000, event_id=3143)), Node(name=MUSE, earliest_time=EventTime(timestamp=1560421080000, event_id=23), latest_time=EventTime(timestamp=1562755500000, event_id=3182)), Node(name=VIOLETTE, earliest_time=EventTime(timestamp=1560423600000, event_id=112), latest_time=EventTime(timestamp=1562754900000, event_id=3172)), Node(name=FELIPE, earliest_time=EventTime(timestamp=1560419400000, event_id=0), latest_time=EventTime(timestamp=1562756700000, event_id=3195)), Node(name=MAKO, earliest_time=EventTime(timestamp=1560421620000, event_id=38), latest_time=EventTime(timestamp=1562756100000, event_id=3189)), Node(name=EWINE, earliest_time=EventTime(timestamp=1560442020000, event_id=192), latest_time=EventTime(timestamp=1562754600000, event_id=3169)), Node(name=LIPS, earliest_time=EventTime(timestamp=1560419460000, event_id=3), latest_time=EventTime(timestamp=1562756700000, event_id=3195)), Node(name=KALI, earliest_time=EventTime(timestamp=1560420660000, event_id=16), latest_time=EventTime(timestamp=1562752560000, event_id=3143)), Node(name=ANGELE, earliest_time=EventTime(timestamp=1560419400000, event_id=0), latest_time=EventTime(timestamp=1562754600000, event_id=3170)), ...)
    Edges(Edge(source=ANGELE, target=FELIPE, earliest_time=EventTime(timestamp=1560419400000, event_id=0), latest_time=EventTime(timestamp=1562753640000, event_id=3151), properties={Weight: 1}, layer(s)=[Resting, Grunting-Lipsmacking, Presenting, Grooming, Copulating, Submission]), Edge(source=ATMOSPHERE, target=LIPS, earliest_time=EventTime(timestamp=1560420000000, event_id=14), latest_time=EventTime(timestamp=1560420000000, event_id=14), properties={Weight: 1}, layer(s)=[Playing with]), Edge(source=PIPO, target=KALI, earliest_time=EventTime(timestamp=1560420660000, event_id=16), latest_time=EventTime(timestamp=1562752560000, event_id=3143), properties={Weight: 1}, layer(s)=[Resting, Grunting-Lipsmacking, Grooming, Copulating, Touching, Chasing, Mounting]), Edge(source=PIPO, target=FELIPE, earliest_time=EventTime(timestamp=1560420720000, event_id=19), latest_time=EventTime(timestamp=1562151240000, event_id=2244), properties={Weight: 1}, layer(s)=[Resting, Grunting-Lipsmacking, Presenting, Avoiding]), Edge(source=PIPO, target=LIPS, earliest_time=EventTime(timestamp=1560420720000, event_id=21), latest_time=EventTime(timestamp=1560420720000, event_id=21), properties={Weight: 1}, layer(s)=[Resting]), Edge(source=NEKKE, target=LIPS, earliest_time=EventTime(timestamp=1560421980000, event_id=70), latest_time=EventTime(timestamp=1562668860000, event_id=2965), properties={Weight: 1}, layer(s)=[Playing with, Resting, Embracing]), Edge(source=MUSE, target=NEKKE, earliest_time=EventTime(timestamp=1560421620000, event_id=37), latest_time=EventTime(timestamp=1562755380000, event_id=3179), properties={Weight: 1}, layer(s)=[Playing with, Resting, Embracing, Grooming, Touching, Mounting]), Edge(source=NEKKE, target=MAKO, earliest_time=EventTime(timestamp=1560421980000, event_id=71), latest_time=EventTime(timestamp=1562751120000, event_id=3118), properties={Weight: 1}, layer(s)=[Playing with, Resting, Embracing, Presenting, Grooming]), Edge(source=FELIPE, target=ANGELE, earliest_time=EventTime(timestamp=1560419460000, event_id=2), latest_time=EventTime(timestamp=1562754600000, event_id=3170), properties={Weight: 1}, layer(s)=[Resting, Embracing, Grunting-Lipsmacking, Presenting, Touching, Chasing, Mounting, Supplanting, Submission]), Edge(source=NEKKE, target=MUSE, earliest_time=EventTime(timestamp=1560421980000, event_id=72), latest_time=EventTime(timestamp=1562682060000, event_id=3088), properties={Weight: 1}, layer(s)=[Playing with, Resting, Embracing]), ...)
    ```
