# Filtering

The `filter` module provides a variety of functions prefixed with 'filter' that take a [filter expression][raphtory.filter.FilterExpr] and return a corresponding view.

Filter expressions allow you to create complex logical queries to select a narrower set of your data based on multiple criteria. This is useful when you already have some knowledge of the subset you want to isolate.

The following functions can be called on a `graph` or `node`:

- [filter_edges][raphtory.GraphView.filter_edges]
- [filter_exploded_edges][raphtory.GraphView.filter_exploded_edges]
- [filter_nodes][raphtory.GraphView.filter_nodes]

For example, a cybersecurity team investigating the impact of a CVE on your companies servers might filter for nodes which function as public facing servers and that have a specific operating system. This would give the security team a view that contains only nodes that might be vulnerable.

Using the traffic dataset you can explore this scenario by using `filter_nodes()` to create a new `GraphView` that contains only the nodes that match the CVE description:

/// tab | :fontawesome-brands-python: Python

```python
from raphtory import Graph
from raphtory import filter
import pandas as pd

server_edges_df = pd.read_csv("./network_traffic_edges.csv")
server_edges_df["timestamp"] = pd.to_datetime(server_edges_df["timestamp"])

server_nodes_df = pd.read_csv("./network_traffic_nodes.csv")
server_nodes_df["timestamp"] = pd.to_datetime(server_nodes_df["timestamp"])

traffic_graph = Graph()
traffic_graph.load_edges_from_pandas(
    df=server_edges_df,
    src="source",
    dst="destination",
    time="timestamp",
    properties=["data_size_MB"],
    layer_col="transaction_type",
    metadata=["is_encrypted"],
    shared_metadata={"datasource": "./network_traffic_edges.csv"},
)
traffic_graph.load_nodes_from_pandas(
    df=server_nodes_df,
    id="server_id",
    time="timestamp",
    properties=["OS_version", "primary_function", "uptime_days"],
    metadata=["server_name", "hardware_type"],
    shared_metadata={"datasource": "./network_traffic_edges.csv"},
)

my_filter = filter.Property("OS_version").is_in(["Ubuntu 20.04", "Red Hat 8.1"]) & filter.Property("primary_function").is_in(["Web Server", "Application Server"])

cve_view = traffic_graph.filter_nodes(my_filter)

print(cve_view.nodes)

```

You can print the nodes in the filtered view to see which machines you should investigate.

!!! output

    ```
    Nodes(Node(name=ServerB, earliest_time=1693555500000, latest_time=1693555800000, properties=Properties({OS_version: Red Hat 8.1, primary_function: Web Server, uptime_days: 45})), Node(name=ServerD, earliest_time=1693555800000, latest_time=1693556100000, properties=Properties({OS_version: Ubuntu 20.04, primary_function: Application Server, uptime_days: 60})))
    ```

```{.python continuation hide}
assert len(cve_view.nodes) == 2
```
