# Node centric algorithms

The second category of algorithms are `node centric` which return a value for each node in the graph. These results are stored within a `NodeState` object which has functions for sorting, grouping, top_k, and conversion to dataframes.

## Continuous Results: PageRank

[PageRank](https://en.wikipedia.org/wiki/PageRank) is an centrality metric developed by Google's founders to rank web pages in search engine results based on their importance and relevance. This has since become a standard ranking algorithm for a whole host of other usecases.

Raphtory's implementation returns the score for each node. These are **continuous values**, meaning we can discover the most important characters in our Lord of the Rings dataset via `top_k()`.

In the example below we first get the result of an individual character (Gandalf), followed by the values of the top 5 most important characters.

/// tab | :fontawesome-brands-python: Python
```python
from raphtory import algorithms as rp
from raphtory import Graph
import pandas as pd

df = pd.read_csv("../data/lotr.csv")

lotr_graph = Graph()
lotr_graph.load_edges_from_pandas(
    df=df,time="time", src="src", dst="dst"
)

results = rp.pagerank(lotr_graph)

# Getting the results for an individual character (Gandalf)
gandalf_rank = results.get("Gandalf")
print(f"Gandalf's ranking is {round(gandalf_rank, 5)}\n")

# Getting the top 5 most important characters and printing out their scores
top_5 = results.top_k(5)
for rank, (node, score) in enumerate(top_5.items(),1):
    print(f"Rank {rank}: {node.name} with a score of {score:.5f}")
```
///

```{.python continuation hide}
assert str(f"Gandalf's ranking is {round(gandalf_rank, 5)}") == "Gandalf's ranking is 0.01581"
```

!!! Output

    ```output
    Gandalf's ranking is 0.015810830531114206

    Rank 1: Aragorn with a score of 0.09526
    Rank 2: Faramir with a score of 0.06148
    Rank 3: Elrond with a score of 0.04042
    Rank 4: Boromir with a score of 0.03468
    Rank 5: Legolas with a score of 0.03323
    ```

## Discrete Results: Connected Components

[Weakly connected components](https://en.wikipedia.org/wiki/Component_(graph_theory)) in a directed graph are `subgraphs` where every node is reachable from every other node if edge direction is ignored. 

For each node this algorithm finds which component it is a member of and returns the id of the component. These are **discrete values**, meaning we can use `groups` to find additional insights like the size of the [largest connected component](https://en.wikipedia.org/wiki/Giant_component). 

!!! info

    The `component ID (value)` is generated from the lowest `node ID` in the component.

In the example below we first run the algorithm and print the result so we can see what it looks like. 

Next we take the results and group the nodes by these IDs and calculate the size of the largest component. Almost all nodes are within this component (134 of the 139), as is typical for social networks.

/// tab | :fontawesome-brands-python: Python
```python
from raphtory import algorithms as rp
from raphtory import Graph
import pandas as pd

df = pd.read_csv("../data/lotr.csv")

lotr_graph = Graph()
lotr_graph.load_edges_from_pandas(
    df=df,time="time", src="src", dst="dst"
)

results = rp.weakly_connected_components(lotr_graph)

print(f"{results}\n")

# Group the components together
components = results.groups()

# Get the size of each component
component_sizes = {key: len(value) for key, value in components}
# Get the key for the largest component
largest_component = max(component_sizes, key=component_sizes.get)
# Print the size of the largest component
print(
    f"The largest component contains {component_sizes[largest_component]} of the {lotr_graph.count_nodes()} nodes in the graph."
)
```
///

```{.python continuation hide}
assert str(f"The largest component contains {component_sizes[largest_component]} of the {lotr_graph.count_nodes()} nodes in the graph.") == "The largest component contains 134 of the 139 nodes in the graph."
```

!!! Output

    ```output
    NodeState(Frodo: 0, Elrond: 0, Gandalf: 0, Isildur: 0, Shadowfax: 0, Imrahil: 0, Nazgûl: 0, Gollum: 0, Gimli: 0, Aragorn: 0, ...)

    The largest component contains 134 of the 139 nodes in the graph.
    ```