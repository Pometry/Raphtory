# Subgraph

For some use cases you may only be interested in a subset of `nodes` within the graph. One solution to this could be to call `g.nodes` and filtering this before continuing your workflow. However, this does not remove anything from future function calls. You would have to constantly recheck these lists. 

To handle these corner cases Raphtory provides the `subgraph()` function which takes a list of `nodes` of interest. This applies a view such that all `nodes` not in the list are hidden from all future function calls. This also hides any edges linked to hidden `nodes` to keep the subgraph consistent. 

In the below example we demonstrate this by looking at the neighbours of `FELIPE` in the full graph, compared to a subgraph of `FELIPE`, `LIPS`, `NEKKE`, `LOME` and `BOBO`. We also show how that `subgraph()` can be combined with other view functions, in this case a window between the 17th and 18th of June.

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
g.load_edges_from_pandas(
    df=edges_df,
    src="Actor",
    dst="Recipient",
    time="DateTime",
    layer_col="Behavior",
    properties=["Weight"],
)

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
```
///

!!! Output

    ```output
    There are 22 monkeys in the whole graph
    There are 5 monkeys in the subgraph
    FELIPE has the following neighbours in the full graph: ['MALI', 'LOME', 'NEKKE', 'PETOULETTE', 'EWINE', 'ANGELE', 'VIOLETTE', 'BOBO', 'MAKO', 'FEYA', 'LIPS', 'ATMOSPHERE', 'FANA', 'MUSE', 'HARLEM', 'PIPO', 'ARIELLE', 'SELF']
    FELIPE has the following neighbours in the subgraph: ['LOME', 'NEKKE', 'BOBO', 'LIPS']
    FELIPE has the following neighbours in the subgraph between 2019-06-17 00:00:00 and 2019-06-18 00:00:00: ['NEKKE']
    ```