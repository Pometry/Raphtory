# Edge metrics and functions 
Edges can be accessed by storing the object returned from a call to `add_edge()`, by directly asking for a specific edge via `edge()`, or by iterating over all edges with `in-edges`, `out-edges`, or `edges`. 

## Edge structure and update history
By default an edge object in Raphtory will contain all updates over all layers between the given source and destination nodes. As an example, we can look at the two edges between `FELIPE` and `MAKO` (one for each direction). 

In the code below we create the two edge objects by requesting them from the graph and then print out the layers each is involved in with `layer_names`. We can see that there are multiple behaviors in each direction represented within the edges.

Following this we access the history to get the earliest and latest update times. This update history consists all interactions across all layers.

!!!info 
    Note that we call `e.src.name` because `src` and `dst` return a node object, instead of just an id or name.

{{code_block('getting-started/querying','edge_history',['Node'])}}
!!! Output

    ```python exec="on" result="text" session="getting-started/querying"
    --8<-- "python/getting-started/querying.py:edge_history"
    ```

## Exploded edges
Raphtory offers you three different ways to split an edge by layer, depending on your use case:

- `.layers()`: takes a list of layer names and returns a new `Edge View` which contains updates for only the specified layers. This is discussed in more detail in the [Layer views](../views/3_layer.md) chapter
- `.explode_layers()`: returns an iterable of `Edge Views`, each containing the updates for one layer
- `.explode()`: returns an `Exploded Edge` containing only the information from one call to `add_edge()`, in this case an edge object for each update. 

In the code below you can see an example of each of these functions. We first call `explode_layers()`, to see which layer each edge object represents and output its update history. Next we fully `explode()` the edge and see each update as an individual object. Thirdly we use the `layer()` function to look at only the `Touching` and `Carrying` layers and chain this with a call to `explode()` to see the separate updates. 

!!! info
    Within the examples and in the API documentation you will see singular and plural versions what appear to be the same function. For example `.layer_names` and `.layer_name`.
    
    Singular functions such as `.layer_name` or `.time` can be called on exploded edges and plural functions such as `.layer_names` and `.history()` can be called on standard edges.

{{code_block('getting-started/querying','edge_explode_layer',['Node'])}}
!!! Output

    ```python exec="on" result="text" session="getting-started/querying"
    --8<-- "python/getting-started/querying.py:edge_explode_layer"
    ```
