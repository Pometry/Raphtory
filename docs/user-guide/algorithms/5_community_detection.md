# Community detection

One important feature of graphs is the degree of clustering and presence of community structures. Groups of nodes that have a high degree of connection between members of the group but comparatively few connections with the rest of the graph can be considered distinct communities.

Identifying clusters can be informative in social, biological and technological networks. For example, identifying clusters in web clients accessing a site can help optimise performance using a CDN or spotting changes in the communities amongst a baboon pack over time might inform theories about group dynamics. Raphtory provides a variety of algorithms to analyse community structures in your graphs.

## Exploring Zachary's karate club network

As an example, we use a data set from the paper "An Information Flow Model for Conflict and Fission in Small Groups" by Wayne W. Zachary which captures social links between the 34 members of the club. 

### Ingest data

First we set up imports and ingest the data using NetworkX and Pandas to handle the `karate.gml` file.

/// tab | :fontawesome-brands-python: Python
```python
from raphtory import Graph
import pandas as pd
import networkx as nx
from raphtory import graphql
from raphtory import algorithms as rp
import matplotlib.pyplot as plt

# Load the GML file using NetworkX
karate = nx.read_gml("./karate.gml", label=None)

# Convert edges to a DataFrame
edges_df = pd.DataFrame(list(karate.edges()), columns=["source", "target"])
# Add a dummy timestamp column (midnight for all entries)
edges_df["time"] = pd.Timestamp("2023-01-01 00:00:00")

# Convert nodes and their attributes to a DataFrame
nodes_df = pd.DataFrame({"id": list(karate.nodes())})
# Add a dummy timestamp column (midnight for all entries)
nodes_df["time"] = pd.Timestamp("2023-01-01 00:00:00")

print(edges_df.head())
```
///

!!! Output

    ```output
       source  target       time
    0       1       2 2023-01-01
    1       1       3 2023-01-01
    2       1       4 2023-01-01
    3       1       5 2023-01-01
    4       1       6 2023-01-01
    ```

### Create the graph

The dataframe can then be used to create a Raphtory graph:

/// tab | :fontawesome-brands-python: Python
```{.python continuation}
raphG = Graph()

raphG.load_edges_from_pandas(
    df=edges_df,
    src="source",
    dst="target",
    time="time",
)

raphG.load_nodes_from_pandas(
    df=nodes_df,
    id="id",
    time="time",
)
```
///

### Analyse the clustering of club members

Raphtory provides multiple algorithms to perform community detection, including the following:

- [Louvain][raphtory.algorithms.louvain] - a commonly used and well understood algorithm.
- [Label propagation][raphtory.algorithms.label_propagation] - a more efficient cluster detection algorithm when used at scale.

Here we use the [Louvain][raphtory.algorithms.louvain] algorithm to identify distinct clusters of nodes.

/// tab | :fontawesome-brands-python: Python
```{.python continuation}
clustering = rp.louvain(raphG)

# Extract unique cluster values
unique_clusters = {cluster for node, cluster in clustering.items()}
print("Number of unique clusters:", len(unique_clusters))
```
///

!!! Output

    ```output
    Number of unique clusters: 4
    ```

The algorithm identifies four clusters of nodes which could be interpreted as four social groups amongst the students.

### Visualise the data

We can display our graph using the results of our cluster detection algorithm to colourise the results.

/// tab | :fontawesome-brands-python: Python
```{.python continuation}
edgelist = []
for edge in raphG.edges.latest():
    #print(edge.src.name, edge.src.name)
    link = (int(edge.src.name), int(edge.src.name))
    edgelist.append(link)

cluster_0 = []
cluster_1 = []
cluster_2 = []
cluster_3 = []

# check value of cluster for each node and add to corresponding cluster list
for node, cluster in clustering.items():
    if cluster == 0:
        cluster_0.append(node.name)
    elif cluster == 1:
        cluster_1.append(node.name)
    elif cluster == 2:
        cluster_2.append(node.name)
    elif cluster == 3:
        cluster_3.append(node.name)

nx_g = raphG.to_networkx()

pos = nx.spring_layout(nx_g, seed=3113794652)  # positions for all nodes


# nodes
options = {"edgecolors": "tab:gray", "node_size": 800, "alpha": 0.9}
nx.draw_networkx_nodes(nx_g, pos, nodelist=cluster_0 , node_color="tab:red", **options)
nx.draw_networkx_nodes(nx_g, pos,  nodelist=cluster_1 , node_color="tab:blue", **options)
nx.draw_networkx_nodes(nx_g, pos,  nodelist=cluster_2 , node_color="tab:green", **options)
nx.draw_networkx_nodes(nx_g, pos,  nodelist=cluster_3 , node_color="tab:orange", **options)

nx.draw_networkx_edges(nx_g, pos, width=1.0, alpha=0.5)
nx.draw_networkx_edges(
    nx_g,
    pos,
    edgelist=edgelist,
    width=8,
    alpha=0.5,
    edge_color="tab:red",
)
```
///