
# Exporting to Pandas dataframes

You can ingest from a set of dataframes, work on them in Raphtory formats then convert back into dataframes. Raphtory provides the `to_df()` function on both the `Nodes` and `Edges` for this purpose. 

## Node Dataframe

To explore the use of `to_df()` on the nodes we can first we call the function with default parameters. This exports only the latest property updates and utilises epoch timestamps - the output from this can be seen below. 

To demonstrate flags, we call `to_df()` again, this time enabling the property history and utilising datetime timestamps. The output for this can also be seen below.

{{code_block('getting-started/export','node_df',['Graph'])}}

!!! Output

    ```python exec="on" result="text" session="export"
    --8<-- "python/getting-started/export.py:node_df"
    ```

## Edge Dataframe

Exporting to an edge dataframe via `to_df()` generally works the same as for the nodes. However, by default this will export the property history for each edge, split by edge layer. This is because `to_df()` has an alternative flag to explode the edges and view each update individually (which will then ignore the `include_property_history` flag). 

In the below example we first create a subgraph of the monkey interactions, selecting `ANGELE` and `FELIPE` as the monkeys we are interested in. This isn't a required step, but helps to demonstrate the export of graph views. 

Then we call `to_df()` on the subgraph edges, setting no flags. In the output you can see the property history for each interaction type (layer) between `ANGELE` and `FELIPE`.

Finally, we call `to_df()` again, turning off the property history and exploding the edges. In the output you can see each interaction that occurred between `ANGELE` and `FELIPE`.
 
!!! info 

    We have further reduced the graph to only one layer (`Grunting-Lipsmacking`) to reduce the output size.

{{code_block('getting-started/export','edge_df',['Graph'])}}

!!! Output

    ```python exec="on" result="text" session="export"
    --8<-- "python/getting-started/export.py:edge_df"
    ```
