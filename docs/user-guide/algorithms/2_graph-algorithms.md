# Graph wide algorithms

The following examples cover three `graphwide` algorithms:

* [Graph Density](https://en.wikipedia.org/wiki/Dense_graph) - which represents the ratio between the edges present in a graph and the maximum number of edges that the graph could contain.
* [Clustering coefficient](https://en.wikipedia.org/wiki/Clustering_coefficient) - which is a measure of the degree to which nodes in a graph tend to cluster together e.g. how many of your friends are also friends.
* [Reciprocity](https://en.wikipedia.org/wiki/Reciprocity_(network_science)) - which is a measure of the likelihood of nodes in a directed network to be mutually connected e.g. if you follow someone on twitter, whats the change of them following you back.

To run an algorithm you simply need to import the algorithm package, choose an algorithm to run, and give it your graph.

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

density = rp.directed_graph_density(lotr_graph)
clustering_coefficient = rp.global_clustering_coefficient(lotr_graph)
reciprocity = rp.global_reciprocity(lotr_graph)

print(f"The graph's density is {density}")
print(f"The graph's clustering coefficient is {clustering_coefficient}")
print(f"The graph's reciprocity is {reciprocity}")
```
///

```{.python continuation hide}
assert str(f"The graph's density is {density}") == "The graph's density is 0.03654467730163695"
assert str(f"The graph's clustering coefficient is {clustering_coefficient}") == "The graph's clustering coefficient is 0.4163023913602468"
assert str(f"The graph's reciprocity is {reciprocity}") == "The graph's reciprocity is 0.19115549215406563"
```

!!! Output

    ```output
    The graph's density is 0.03654467730163695
    The graph's clustering coefficient is 0.4163023913602468
    The graph's reciprocity is 0.19115549215406563
    ```
