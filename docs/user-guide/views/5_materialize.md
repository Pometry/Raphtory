# Materialize

All of the view functions hold zero updates of their own, simply providing a lens through which to look at a graph. This is by design so that you can have many views without expensive data duplication. 

However, if the original graph is updated, all of the views over it will also update. If you do not want this to happen you can `materialize()` a view, creating a new graph and copying all the updates the view contains into it. 

In the below example, we create a windowed view between the 17th and 18th of June and then materialize this view. After adding a new monkey interaction in the materialized graph, we can see the original graph does not contain this update, but the materialized graph does.

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
```
///

```{.python continuation hide}
assert str(f"After the update the view had {windowed_view.count_temporal_edges()} edge updates") == "After the update the view had 132 edge updates"
assert str(f"After the update the materialized graph had {materialized_graph.count_temporal_edges()} edge updates") == "After the update the materialized graph had 133 edge updates"
```

!!! Output

    ```output
    Before the update the view had 132 edge updates
    Before the update the materialized graph had 132 edge updates
    Adding new update to materialized_graph
    After the update the view had 132 edge updates
    After the update the materialized graph had 133 edge updates
    ```