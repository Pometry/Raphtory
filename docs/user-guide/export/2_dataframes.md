
# Exporting to Pandas dataframes

You can ingest from a set of dataframes, work on them in Raphtory formats then convert back into dataframes. Raphtory provides the `to_df()` function on both the `Nodes` and `Edges` for this purpose. 

## Node Dataframe

To explore the use of `to_df()` on the nodes we can first we call the function with default parameters. This exports only the latest property updates and utilises epoch timestamps - the output from this can be seen below. 

To demonstrate flags, we call `to_df()` again, this time enabling the property history and utilising datetime timestamps. The output for this can also be seen below.

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

df = traffic_graph.nodes.to_df()
print("--- to_df with default parameters --- ")
print(f"{df}\n")
print()
df = traffic_graph.nodes.to_df(include_property_history=True, convert_datetime=True)
print("--- to_df with property history and datetime conversion ---")
print(f"{df}\n")
```
///

!!! Output

    ```output
    --- to_df with default parameters --- 
        name type                           datasource hardware_type  \
    0  ServerA       docs/data/network_traffic_edges.csv  Blade Server   
    1  ServerE       docs/data/network_traffic_edges.csv   Rack Server   
    2  ServerB       docs/data/network_traffic_edges.csv   Rack Server   
    3  ServerD       docs/data/network_traffic_edges.csv  Tower Server   
    4  ServerC       docs/data/network_traffic_edges.csv  Blade Server   

    server_name    primary_function  uptime_days           OS_version  \
    0       Alpha            Database          120         Ubuntu 20.04   
    1        Echo              Backup           30          Red Hat 8.1   
    2        Beta          Web Server           45          Red Hat 8.1   
    3       Delta  Application Server           60         Ubuntu 20.04   
    4     Charlie        File Storage           90  Windows Server 2022   

                                        update_history  
    0      [1693555200000, 1693555500000, 1693556400000]  
    1      [1693556100000, 1693556400000, 1693556700000]  
    2  [1693555200000, 1693555500000, 1693555800000, ...  
    3      [1693555800000, 1693556100000, 1693557000000]  
    4  [1693555500000, 1693555800000, 1693556400000, ...  


    --- to_df with property history and datetime conversion ---
        name type hardware_type                           datasource  \
    0  ServerA       Blade Server  docs/data/network_traffic_edges.csv   
    1  ServerE        Rack Server  docs/data/network_traffic_edges.csv   
    2  ServerB        Rack Server  docs/data/network_traffic_edges.csv   
    3  ServerD       Tower Server  docs/data/network_traffic_edges.csv   
    4  ServerC       Blade Server  docs/data/network_traffic_edges.csv   

    server_name                                   primary_function  \
    0       Alpha            [[2023-09-01 08:00:00+00:00, Database]]   
    1        Echo              [[2023-09-01 08:20:00+00:00, Backup]]   
    2        Beta          [[2023-09-01 08:05:00+00:00, Web Server]]   
    3       Delta  [[2023-09-01 08:15:00+00:00, Application Server]]   
    4     Charlie        [[2023-09-01 08:10:00+00:00, File Storage]]   

                                            OS_version  \
    0        [[2023-09-01 08:00:00+00:00, Ubuntu 20.04]]   
    1         [[2023-09-01 08:20:00+00:00, Red Hat 8.1]]   
    2         [[2023-09-01 08:05:00+00:00, Red Hat 8.1]]   
    3        [[2023-09-01 08:15:00+00:00, Ubuntu 20.04]]   
    4  [[2023-09-01 08:10:00+00:00, Windows Server 20...   

                            uptime_days  \
    0  [[2023-09-01 08:00:00+00:00, 120]]   
    1   [[2023-09-01 08:20:00+00:00, 30]]   
    2   [[2023-09-01 08:05:00+00:00, 45]]   
    3   [[2023-09-01 08:15:00+00:00, 60]]   
    4   [[2023-09-01 08:10:00+00:00, 90]]   

                                        update_history  
    0  [2023-09-01 08:00:00+00:00, 2023-09-01 08:05:0...  
    1  [2023-09-01 08:15:00+00:00, 2023-09-01 08:20:0...  
    2  [2023-09-01 08:00:00+00:00, 2023-09-01 08:05:0...  
    3  [2023-09-01 08:10:00+00:00, 2023-09-01 08:15:0...  
    4  [2023-09-01 08:05:00+00:00, 2023-09-01 08:10:0...
    ```

## Edge Dataframe

Exporting to an edge dataframe via `to_df()` generally works the same as for the nodes. However, by default this will export the property history for each edge, split by edge layer. This is because `to_df()` has an alternative flag to explode the edges and view each update individually (which will then ignore the `include_property_history` flag). 

In the below example we first create a subgraph of the monkey interactions, selecting `ANGELE` and `FELIPE` as the monkeys we are interested in. This isn't a required step, but helps to demonstrate the export of graph views. 

Then we call `to_df()` on the subgraph edges, setting no flags. In the output you can see the property history for each interaction type (layer) between `ANGELE` and `FELIPE`.

Finally, we call `to_df()` again, turning off the property history and exploding the edges. In the output you can see each interaction that occurred between `ANGELE` and `FELIPE`.
 
!!! info 

    We have further reduced the graph to only one layer (`Grunting-Lipsmacking`) to reduce the output size.

/// tab | :fontawesome-brands-python: Python
```python
from raphtory import Graph
import pandas as pd

monkey_edges_df = pd.read_csv(
    "../data/OBS_data.txt", sep="\t", header=0, usecols=[0, 1, 2, 3, 4], parse_dates=[0]
)
monkey_edges_df["DateTime"] = pd.to_datetime(monkey_edges_df["DateTime"])
monkey_edges_df.dropna(axis=0, inplace=True)
monkey_edges_df["Weight"] = monkey_edges_df["Category"].apply(
    lambda c: 1 if (c == "Affiliative") else (-1 if (c == "Agonistic") else 0)
)

monkey_graph = Graph()
monkey_graph.load_edges_from_pandas(
    df=monkey_edges_df,
    src="Actor",
    dst="Recipient",
    time="DateTime",
    layer_col="Behavior",
    properties=["Weight"],
)

subgraph = monkey_graph.subgraph(["ANGELE", "FELIPE"])
df = subgraph.edges.to_df()
print("Interactions between Angele and Felipe:")
print(f"{df}\n")

grunting_graph = subgraph.layer("Grunting-Lipsmacking")
print(grunting_graph)
print(grunting_graph.edges)
df = grunting_graph.edges.to_df()
print("Exploding the grunting-Lipsmacking layer")
print(df)
```
///

```{.python continuation hide}
assert str(grunting_graph) == "Graph(number_of_nodes=2, number_of_edges=2, number_of_temporal_edges=6, earliest_time=1560526320000, latest_time=1562253540000)"
assert str(grunting_graph.edges) == "Edges(Edge(source=ANGELE, target=FELIPE, earliest_time=1560526320000, latest_time=1561042620000, properties={Weight: 1}, layer(s)=[Grunting-Lipsmacking]), Edge(source=FELIPE, target=ANGELE, earliest_time=1560526320000, latest_time=1562253540000, properties={Weight: 1}, layer(s)=[Grunting-Lipsmacking]))"
```

!!! Output

    ```output
    Interactions between Angele and Felipe:
        src     dst                 layer  \
    0   ANGELE  FELIPE               Resting   
    1   ANGELE  FELIPE            Presenting   
    2   ANGELE  FELIPE  Grunting-Lipsmacking   
    3   ANGELE  FELIPE              Grooming   
    4   ANGELE  FELIPE            Copulating   
    5   ANGELE  FELIPE            Submission   
    6   FELIPE  ANGELE               Resting   
    7   FELIPE  ANGELE            Presenting   
    8   FELIPE  ANGELE              Touching   
    9   FELIPE  ANGELE  Grunting-Lipsmacking   
    10  FELIPE  ANGELE               Chasing   
    11  FELIPE  ANGELE              Mounting   
    12  FELIPE  ANGELE            Submission   
    13  FELIPE  ANGELE             Embracing   
    14  FELIPE  ANGELE           Supplanting   

                                                Weight  \
    0   [[1560422580000, 1], [1560441780000, 1], [1560...   
    1                                [[1560855660000, 1]]   
    2   [[1560526320000, 1], [1560855660000, 1], [1561...   
    3   [[1560419400000, 1], [1560419400000, 1], [1560...   
    4                                [[1561720320000, 0]]   
    5                               [[1562253540000, -1]]   
    6   [[1560419460000, 1], [1560419520000, 1], [1560...   
    7                                [[1562321580000, 1]]   
    8   [[1560526260000, 1], [1562253540000, 1], [1562...   
    9   [[1560526320000, 1], [1561972860000, 1], [1562...   
    10         [[1562057520000, -1], [1562671200000, -1]]   
    11                               [[1562253540000, 1]]   
    12                              [[1562057520000, -1]]   
    13                               [[1560526320000, 1]]   
    14                              [[1561110180000, -1]]   

                                        update_history  
    0   [1560422580000, 1560441780000, 1560441780000, ...  
    1                                     [1560855660000]  
    2       [1560526320000, 1560855660000, 1561042620000]  
    3   [1560419400000, 1560419400000, 1560419460000, ...  
    4                                     [1561720320000]  
    5                                     [1562253540000]  
    6   [1560419460000, 1560419520000, 1560419580000, ...  
    7                                     [1562321580000]  
    8       [1560526260000, 1562253540000, 1562321580000]  
    9       [1560526320000, 1561972860000, 1562253540000]  
    10                     [1562057520000, 1562671200000]  
    11                                    [1562253540000]  
    12                                    [1562057520000]  
    13                                    [1560526320000]  
    14                                    [1561110180000]  

    Graph(number_of_nodes=2, number_of_edges=2, number_of_temporal_edges=6, earliest_time=1560526320000, latest_time=1562253540000)
    Edges(Edge(source=ANGELE, target=FELIPE, earliest_time=1560526320000, latest_time=1561042620000, properties={Weight: 1}, layer(s)=[Grunting-Lipsmacking]), Edge(source=FELIPE, target=ANGELE, earliest_time=1560526320000, latest_time=1562253540000, properties={Weight: 1}, layer(s)=[Grunting-Lipsmacking]))
    Exploding the grunting-Lipsmacking layer
        src     dst                 layer  \
    0  ANGELE  FELIPE  Grunting-Lipsmacking   
    1  FELIPE  ANGELE  Grunting-Lipsmacking   

                                                Weight  \
    0  [[1560526320000, 1], [1560855660000, 1], [1561...   
    1  [[1560526320000, 1], [1561972860000, 1], [1562...   

                                    update_history  
    0  [1560526320000, 1560855660000, 1561042620000]  
    1  [1560526320000, 1561972860000, 1562253540000]
    ```
