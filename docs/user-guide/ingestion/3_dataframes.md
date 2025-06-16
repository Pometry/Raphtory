# Ingesting from dataframes
If you prefer to initially manipulate your data in a `dataframe` before converting into a graph, Raphtory can directly ingest dataframes and convert these into node and edge updates. 

## Creating a graph from dataframes
In the example below we are ingesting some network traffic data which includes different types of interactions between servers. First we read the data from disk into two dataframes, one for the server information (nodes) and one for the server interactions (edges). Then we convert the timestamp column to datetime objects. Finally, the two dataframes are printed out so you can see the headers and values.

{{code_block('getting-started/ingestion','server_data',[])}}

!!! Output

    ```python exec="on" result="text" session="dataframes"
    --8<-- "python/getting-started/ingestion.py:server_data"
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

{{code_block('getting-started/ingestion','graph_from_dataframe',[])}}

!!! Output

    ```python exec="on" result="text" session="dataframes"
    --8<-- "python/getting-started/ingestion.py:graph_from_dataframe"
    ```

## Adding constant properties via dataframes
There may be instances where you are adding a dataset which has no timestamps. To handle this when ingesting via dataframes the graph has the `load_edge_props_from_pandas()` and `load_node_props_from_pandas()` functions.

Below we break the ingestion into a four stage process, adding the constant properties at the end. This example uses the same two dataframes for brevity but in real instances these would probably be four different dataframes, one for each function call.

!!! warning 
    Constant properties can only be added to nodes and edges which are part of the graph. If you attempt to add a constant property without first adding the node/edge then Raphtory will throw an error.

{{code_block('getting-started/ingestion','const_dataframe',[])}}

!!! Output

    ```python exec="on" result="text" session="dataframes"
    --8<-- "python/getting-started/ingestion.py:const_dataframe"
    ```
