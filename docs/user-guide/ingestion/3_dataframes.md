# Ingesting from dataframes
If you prefer to initially manipulate your data in a `dataframe` before converting into a graph, Raphtory can directly ingest dataframes and convert these into node and edge updates. 

## Creating a graph from dataframes
In the example below we are ingesting some network traffic data which includes different types of interactions between servers. First we read the data from disk into two dataframes, one for the server information (nodes) and one for the server interactions (edges). Then we convert the timestamp column to datetime objects. Finally, the two dataframes are printed out so you can see the headers and values.

=== ":fontawesome-brands-python: Python"

    ```python
    from raphtory import Graph
    import pandas as pd

    edges_df = pd.read_csv("docs/data/network_traffic_edges.csv")
    edges_df["timestamp"] = pd.to_datetime(edges_df["timestamp"])

    nodes_df = pd.read_csv("docs/data/network_traffic_nodes.csv")
    nodes_df["timestamp"] = pd.to_datetime(nodes_df["timestamp"])

    pd.set_option('display.max_columns', None)  # so all columns are printed
    print("--- Edge Dataframe ---")
    print(f"{edges_df.head(2)}\n")
    print()
    print("--- Node Dataframe ---")
    print(f"{nodes_df.head(2)}\n")
    ```

!!! Output

    ```
    --- Edge Dataframe ---
                    timestamp   source destination  data_size_MB  \
    0 2023-09-01 08:00:00+00:00  ServerA     ServerB           5.6   
    1 2023-09-01 08:05:00+00:00  ServerA     ServerC           7.1   

            transaction_type  is_encrypted  
    0  Critical System Request          True  
    1            File Transfer         False  


    --- Node Dataframe ---
                    timestamp server_id server_name hardware_type    OS_version  \
    0 2023-09-01 08:00:00+00:00   ServerA       Alpha  Blade Server  Ubuntu 20.04   
    1 2023-09-01 08:05:00+00:00   ServerB        Beta   Rack Server   Red Hat 8.1   

    primary_function  uptime_days  
    0         Database          120  
    1       Web Server           45  
    ```

Next, to ingest these dataframes into Raphtory, we use the `load_edges_from_pandas()` and `load_nodes_from_pandas()` functions.  These functions have optional arguments to cover everything we have seen in the prior [direct updates example](2_direct-updates.md). 

For the parameters of the edges we specify:

- `edges_df` - dataframe that is ingested
- `source` - source column within the dataframe 
- `destination` - destination column within the dataframe
- `timestamp` - timestamp column within the dataframe
- `data_size_MB` - temporal property
- `is_encrypted` - constant property
- `datasource` - an additional shared constant property which labels the origin of the information
- `transaction_type` - layer column

For the parameters for the nodes, we specify:

- `nodes_df` - dataframe that is ingested
- `server_id` - node ID
- `timestamp` - time column
- Temporal properties
    - `OS_version`
    - `primary_function`
    - `uptime_days`
- Constant properties
    - `server_name`
    - `hardware_type`
- `datasource` - an additional shared constant property which labels the origin of the information

The resulting graph and an example node/edge are then printed to show the data fully converted.

=== ":fontawesome-brands-python: Python"

    ```python
    g = Graph()
    g.load_edges_from_pandas(
        df=edges_df,
        time="timestamp",
        src="source",
        dst="destination",
        properties=["data_size_MB"],
        layer_col="transaction_type",
        constant_properties=["is_encrypted"],
        shared_constant_properties={"datasource": "docs/data/network_traffic_edges.csv"},
    )
    g.load_nodes_from_pandas(
        df=nodes_df,
        time="timestamp",
        id="server_id",
        properties=["OS_version", "primary_function", "uptime_days"],
        constant_properties=["server_name", "hardware_type"],
        shared_constant_properties={"datasource": "docs/data/network_traffic_edges.csv"},

    )

    print("The resulting graphs and example node/edge:")
    print(g)
    print(g.node("ServerA"))
    print(g.edge("ServerA", "ServerB"))
    ```

!!! Output

    ```python
    The resulting graphs and example node/edge:
    Graph(number_of_nodes=5, number_of_edges=7, number_of_temporal_edges=7, earliest_time=1693555200000, latest_time=1693557000000)
    Node(name=ServerA, earliest_time=1693555200000, latest_time=1693556400000, properties=Properties({OS_version: Ubuntu 20.04, primary_function: Database, uptime_days: 120, datasource: docs/data/network_traffic_edges.csv, server_name: Alpha, hardware_type: Blade Server}))
    Edge(source=ServerA, target=ServerB, earliest_time=1693555200000, latest_time=1693555200000, properties={data_size_MB: 5.6, datasource: {Critical System Request: docs/data/network_traffic_edges.csv}, is_encrypted: {Critical System Request: true}}, layer(s)=[Critical System Request])
    ```

## Adding constant properties via dataframes
There may be instances where you are adding a dataset which has no timestamps. To handle this when ingesting via dataframes the graph has the `load_edge_props_from_pandas()` and `load_node_props_from_pandas()` functions.

Below we break the ingestion into a four stage process, adding the constant properties at the end. This example uses the same two dataframes for brevity but in real instances these would probably be four different dataframes, one for each function call.

!!! warning 
    Constant properties can only be added to nodes and edges which are part of the graph. If you attempt to add a constant property without first adding the node/edge then Raphtory will throw an error.

=== ":fontawesome-brands-python: Python"

    ```python
    g = Graph()
    g.load_edges_from_pandas(
        df=edges_df,
        src="source",
        dst="destination",
        time="timestamp",
        properties=["data_size_MB"],
        layer_col="transaction_type",
    )

    g.load_nodes_from_pandas(
        df=nodes_df,
        id="server_id",
        time="timestamp",
        properties=["OS_version", "primary_function", "uptime_days"],
    )

    g.load_edge_props_from_pandas(
        df=edges_df,
        src="source",
        dst="destination",
        layer_col="transaction_type",
        constant_properties=["is_encrypted"],
        shared_constant_properties={"datasource": "docs/data/network_traffic_edges.csv"},
    )

    g.load_node_props_from_pandas(
        df=nodes_df,
        id="server_id",
        constant_properties=["server_name", "hardware_type"],
        shared_constant_properties={"datasource": "docs/data/network_traffic_edges.csv"},
    )

    print(g)
    print(g.node("ServerA"))
    print(g.edge("ServerA", "ServerB"))
    ```

!!! Output

    ```python
    Graph(number_of_nodes=5, number_of_edges=7, number_of_temporal_edges=7, earliest_time=1693555200000, latest_time=1693557000000)
    Node(name=ServerA, earliest_time=1693555200000, latest_time=1693556400000, properties=Properties({OS_version: Ubuntu 20.04, primary_function: Database, uptime_days: 120, datasource: docs/data/network_traffic_edges.csv, server_name: Alpha, hardware_type: Blade Server}))
    Edge(source=ServerA, target=ServerB, earliest_time=1693555200000, latest_time=1693555200000, properties={data_size_MB: 5.6, datasource: {Critical System Request: docs/data/network_traffic_edges.csv}, is_encrypted: {Critical System Request: true}}, layer(s)=[Critical System Request])
    ```
