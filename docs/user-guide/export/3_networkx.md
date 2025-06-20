
# Exporting to NetworkX

When converting to a networkx graph there is only one function (`to_networkx()`), which has flags for node and edge history and for exploding edges. By default all history is included and the edges are separated by layer. 

In the below example we call `to_networkx()` on the network traffic graph, keeping all the default arguments so that it exports the full history. We extract `ServerA` from this graph and print to show how the history is modelled.

!!! info 
    The resulting graph is a networkx `MultiDiGraph` since Raphtory graphs are both directed and have multiple edges between nodes.

We call `to_networkx()` again, disabling the property and update history and reprint `ServerA` to show the difference.

/// tab | :fontawesome-brands-python: Python

```python
from raphtory import Graph
import pandas as pd

server_edges_df = pd.read_csv("../data/network_traffic_edges.csv")
server_edges_df["timestamp"] = pd.to_datetime(server_edges_df["timestamp"])

server_nodes_df = pd.read_csv("../data/network_traffic_nodes.csv")
server_nodes_df["timestamp"] = pd.to_datetime(server_nodes_df["timestamp"])

traffic_graph = Graph()
traffic_graph.load_edges_from_pandas(
    df=server_edges_df,
    src="source",
    dst="destination",
    time="timestamp",
    properties=["data_size_MB"],
    layer_col="transaction_type",
    constant_properties=["is_encrypted"],
    shared_constant_properties={"datasource": "docs/data/network_traffic_edges.csv"},
)
traffic_graph.load_nodes_from_pandas(
    df=server_nodes_df,
    id="server_id",
    time="timestamp",
    properties=["OS_version", "primary_function", "uptime_days"],
    constant_properties=["server_name", "hardware_type"],
    shared_constant_properties={"datasource": "docs/data/network_traffic_edges.csv"},
)

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
```
///

!!! Output

    ```output
    Networkx graph:
    MultiDiGraph with 5 nodes and 7 edges

    Full property history of ServerA:
    {'constant': {'datasource': 'docs/data/network_traffic_edges.csv', 'hardware_type': 'Blade Server', 'server_name': 'Alpha'}, 'temporal': [('OS_version', (1693555200000, 'Ubuntu 20.04')), ('primary_function', (1693555200000, 'Database')), ('uptime_days', (1693555200000, 120))], 'update_history': [1693555200000, 1693555500000, 1693556400000]}

    Only the latest properties of ServerA:
    {'OS_version': 'Ubuntu 20.04', 'uptime_days': 120, 'primary_function': 'Database', 'server_name': 'Alpha', 'datasource': 'docs/data/network_traffic_edges.csv', 'hardware_type': 'Blade Server', 'update_history': [1693555200000, 1693555500000, 1693556400000]}
    ```

## Visualisation

Once converted into a networkX graph you have access to their full suite of functionality. For example, using their [drawing](https://networkx.org/documentation/stable/reference/drawing.html) library for visualising graphs.

In the code snippet below we use this functionality to draw a network traffic graph, labelling the nodes with their Server ID. For more information, see the [networkx](https://networkx.org/documentation/stable/reference/drawing.html) documentation.

/// tab | :fontawesome-brands-python: Python

```python
# mkdocs: render
import matplotlib.pyplot as plt
import networkx as nx

from raphtory import Graph
import pandas as pd

server_edges_df = pd.read_csv("../data/network_traffic_edges.csv")
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
```
///