
# Graph metrics and functions

## Basic metrics
Begining with the [previous graph](1_intro.md) we can start probing it for some basic metrics, such as how many nodes and edges it contains and the time range over which it exists. 

In the below code example  the functions `count_edges()` and `count_temporal_edges()` are called and return different results. This is because `count_edges()` returns the number of unique edges and `count_temporal_edges()` returns the total edge updates which have occurred. 
    
Using `count_temporal_edges()` is useful if you want to imagine each edge update as a separate connection between the two nodes. The edges can be accessed in this manner via `edge.explode()`, as is discussed in [edge metrics and functions](../querying/4_edge-metrics.md).

!!! info
    The property APIs are the same for the graph, nodes and edges, these are discussed together in [Property queries](../querying/5_properties.md).

/// tab | :fontawesome-brands-python: Python
```python
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
```
///

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

* **Existence check:** using `has_node()` and `has_edge()` you can check if an entity is present within the graph.
* **Direct access:** `node()` and `edge()` will return a node or edge object if the entity is present and `None` if it is not.
* **Iterable access:** `nodes` and `edges` will return iterables for all nodes/edges which can be used within a for loop or as part of a [function chain](../querying/6_chaining.md).

All of these functions are shown in the code below and will appear in several other examples throughout this tutorial.

/// tab | :fontawesome-brands-python: Python
```python
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

!!! Output

    ```output
    Checking if specific nodes and edges are in the graph:
    Lomme is in the graph
    Lomme has played with Nekke 

    Getting individual nodes and edges:
    Node(name=LOME, earliest_time=1560419520000, latest_time=1562756100000)
    Edge(source=LOME, target=NEKKE, earliest_time=1560421080000, latest_time=1562755980000, properties={Weight: 1}, layer(s)=[Touching, Grooming, Resting, Playing with, Carrying]) 

    Getting iterators over all nodes and edges:
    Nodes(Node(name=MALI, earliest_time=1560422040000, latest_time=1562755320000), Node(name=LOME, earliest_time=1560419520000, latest_time=1562756100000), Node(name=NEKKE, earliest_time=1560419520000, latest_time=1562756700000), Node(name=PETOULETTE, earliest_time=1560422520000, latest_time=1562754420000), Node(name=EWINE, earliest_time=1560442020000, latest_time=1562754600000), Node(name=ANGELE, earliest_time=1560419400000, latest_time=1562754600000), Node(name=VIOLETTE, earliest_time=1560423600000, latest_time=1562754900000), Node(name=BOBO, earliest_time=1560419520000, latest_time=1562755500000), Node(name=MAKO, earliest_time=1560421620000, latest_time=1562756100000), Node(name=FEYA, earliest_time=1560420000000, latest_time=1562756040000), ...)
    Edges(Edge(source=ANGELE, target=FELIPE, earliest_time=1560419400000, latest_time=1562753640000, properties={Weight: 1}, layer(s)=[Grooming, Resting, Presenting, Grunting-Lipsmacking, Submission, Copulating]), Edge(source=PIPO, target=KALI, earliest_time=1560420660000, latest_time=1562752560000, properties={Weight: 1}, layer(s)=[Touching, Grooming, Resting, Grunting-Lipsmacking, Chasing, Mounting, Copulating]), Edge(source=LOME, target=NEKKE, earliest_time=1560421080000, latest_time=1562755980000, properties={Weight: 1}, layer(s)=[Touching, Grooming, Resting, Playing with, Carrying]), Edge(source=LOME, target=MUSE, earliest_time=1560421080000, latest_time=1562584200000, properties={Weight: 1}, layer(s)=[Resting, Playing with]), Edge(source=ATMOSPHERE, target=LIPS, earliest_time=1560420000000, latest_time=1560420000000, properties={Weight: 1}, layer(s)=[Playing with]), Edge(source=PIPO, target=FELIPE, earliest_time=1560420720000, latest_time=1562151240000, properties={Weight: 1}, layer(s)=[Resting, Presenting, Grunting-Lipsmacking, Avoiding]), Edge(source=MUSE, target=NEKKE, earliest_time=1560421620000, latest_time=1562755380000, properties={Weight: 1}, layer(s)=[Touching, Grooming, Resting, Playing with, Embracing, Mounting]), Edge(source=MUSE, target=MAKO, earliest_time=1560421620000, latest_time=1562251620000, properties={Weight: 1}, layer(s)=[Touching, Grooming, Resting, Playing with]), Edge(source=PIPO, target=LIPS, earliest_time=1560420720000, latest_time=1560420720000, properties={Weight: 1}, layer(s)=[Resting]), Edge(source=MUSE, target=LOME, earliest_time=1560421620000, latest_time=1562251620000, properties={Weight: 1}, layer(s)=[Playing with, Submission]), ...)
    ```
