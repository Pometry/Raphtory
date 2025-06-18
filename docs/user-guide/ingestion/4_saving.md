# Saving and loading graphs
The fastest way to ingest a graph is to load one from Raphtory's on-disk format using the `load_from_file()` function on the graph. 

Once a graph has been created by direct updates or by ingesting a dataframe you can save it via `save_to_file()` or `save_to_zip()` functions. This means you do not need to parse the data every time you run a Raphtory script which is useful for large datasets.

!!! info
    You can also [pickle](https://docs.python.org/3/library/pickle.html) Raphtory graphs, which uses these functions under the hood.

In the example below we ingest the edge dataframe from the [last section](3_dataframes.md), save this graph and reload it into a second graph. These are both printed to show they contain the same data.

```python
from raphtory import Graph
import pandas as pd

edges_df = pd.read_csv("data/network_traffic_edges.csv")
edges_df["timestamp"] = pd.to_datetime(edges_df["timestamp"])

g = Graph()
g.load_edges_from_pandas(
    df=edges_df,
    time="timestamp",
    src="source",
    dst="destination",
    properties=["data_size_MB"],
    layer_col="transaction_type",
)
g.save_to_file("/tmp/saved_graph") 
loaded_graph = Graph.load_from_file("/tmp/saved_graph")
print(g)
print(loaded_graph)
```

!!! Output

    ```python
    Graph(number_of_nodes=5, number_of_edges=7, number_of_temporal_edges=7, earliest_time=1693555200000, latest_time=1693557000000)
    Graph(number_of_nodes=5, number_of_edges=7, number_of_temporal_edges=7, earliest_time=1693555200000, latest_time=1693557000000)
    ```
