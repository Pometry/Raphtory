# Exporting and visualising your graph

There are many different formats and libraries that you can use to export your graphs. In this section we explore two of these, Pandas dataframes and NetworkX graphs.

All functions mentioned in this section work on both graphs, nodes, and edges and their matching views. This allows you to specify different windows, layers and subgraphs before conversion. 

By default, exporting will include all properties and all update history. However, this can be modified via flags on each export function, depending on use case requirements. You can find a description of these flags in the API docs for the function.

!!! Info
    If we are missing a format that you believe to be important, please raise an [issue](https://github.com/Pometry/Raphtory/issues) and it will be available before you know it!

The following example reuses the network traffic dataset from the [ingestion tutorial](../ingestion/3_dataframes.md) and the monkey interaction network from the [querying tutorial](../querying/1_intro.md). In the below code example you can get a refresher of what these datasets looks like. 

```python
from raphtory import Graph
import pandas as pd

server_edges_df = pd.read_csv("docs/data/network_traffic_edges.csv")
server_edges_df["timestamp"] = pd.to_datetime(server_edges_df["timestamp"])

server_nodes_df = pd.read_csv("docs/data/network_traffic_nodes.csv")
server_nodes_df["timestamp"] = pd.to_datetime(server_nodes_df["timestamp"])

print("Network Traffic Edges:")
print(f"{server_edges_df}\n")
print("Network Traffic Servers:")
print(f"{server_nodes_df}\n")

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

monkey_edges_df = pd.read_csv(
    "docs/data/OBS_data.txt", sep="\t", header=0, usecols=[0, 1, 2, 3, 4], parse_dates=[0]
)
monkey_edges_df["DateTime"] = pd.to_datetime(monkey_edges_df["DateTime"])
monkey_edges_df.dropna(axis=0, inplace=True)
monkey_edges_df["Weight"] = monkey_edges_df["Category"].apply(
    lambda c: 1 if (c == "Affiliative") else (-1 if (c == "Agonistic") else 0)
)

print("Monkey Interactions:")
print(f"{monkey_edges_df}\n")

monkey_graph = Graph()
monkey_graph.load_edges_from_pandas(
    df=monkey_edges_df,
    src="Actor",
    dst="Recipient",
    time="DateTime",
    layer_col="Behavior",
    properties=["Weight"],
)
```

!!! Output

    ```output
    Network Traffic Edges:
                    timestamp   source destination  data_size_MB  \
    0 2023-09-01 08:00:00+00:00  ServerA     ServerB           5.6   
    1 2023-09-01 08:05:00+00:00  ServerA     ServerC           7.1   
    2 2023-09-01 08:10:00+00:00  ServerB     ServerD           3.2   
    3 2023-09-01 08:15:00+00:00  ServerD     ServerE           8.9   
    4 2023-09-01 08:20:00+00:00  ServerC     ServerA           4.5   
    5 2023-09-01 08:25:00+00:00  ServerE     ServerB           6.2   
    6 2023-09-01 08:30:00+00:00  ServerD     ServerC           5.0   

            transaction_type  is_encrypted  
    0   Critical System Request          True  
    1             File Transfer         False  
    2  Standard Service Request          True  
    3    Administrative Command         False  
    4   Critical System Request          True  
    5             File Transfer         False  
    6  Standard Service Request          True  

    Network Traffic Servers:
                    timestamp server_id server_name hardware_type  \
    0 2023-09-01 08:00:00+00:00   ServerA       Alpha  Blade Server   
    1 2023-09-01 08:05:00+00:00   ServerB        Beta   Rack Server   
    2 2023-09-01 08:10:00+00:00   ServerC     Charlie  Blade Server   
    3 2023-09-01 08:15:00+00:00   ServerD       Delta  Tower Server   
    4 2023-09-01 08:20:00+00:00   ServerE        Echo   Rack Server   

                OS_version    primary_function  uptime_days  
    0         Ubuntu 20.04            Database          120  
    1          Red Hat 8.1          Web Server           45  
    2  Windows Server 2022        File Storage           90  
    3         Ubuntu 20.04  Application Server           60  
    4          Red Hat 8.1              Backup           30  

    Monkey Interactions:
                    DateTime    Actor Recipient      Behavior     Category  Weight
    15   2019-06-13 09:50:00   ANGELE    FELIPE      Grooming  Affiliative       1
    17   2019-06-13 09:50:00   ANGELE    FELIPE      Grooming  Affiliative       1
    19   2019-06-13 09:51:00   FELIPE    ANGELE       Resting  Affiliative       1
    20   2019-06-13 09:51:00   FELIPE      LIPS       Resting  Affiliative       1
    21   2019-06-13 09:51:00   ANGELE    FELIPE      Grooming  Affiliative       1
    ...                  ...      ...       ...           ...          ...     ...
    5370 2019-07-10 11:02:00  ARIELLE      LIPS      Touching  Affiliative       1
    5371 2019-07-10 11:05:00     LIPS     NEKKE  Playing with  Affiliative       1
    5372 2019-07-10 11:05:00     LIPS    FELIPE       Resting  Affiliative       1
    5373 2019-07-10 11:05:00     LIPS     NEKKE       Resting  Affiliative       1
    5374 2019-07-10 11:05:00     LIPS    FELIPE       Resting  Affiliative       1

    [3196 rows x 6 columns]
    ```
