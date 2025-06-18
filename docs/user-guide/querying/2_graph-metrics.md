
# Graph metrics and functions

## Basic metrics
Begining with the [previous graph](1_intro.md) we can start probing it for some basic metrics, such as how many nodes and edges it contains and the time range over which it exists. 

In the below code example  the functions `count_edges()` and `count_temporal_edges()` are called and return different results. This is because `count_edges()` returns the number of unique edges and `count_temporal_edges()` returns the total edge updates which have occurred. 
    
Using `count_temporal_edges()` is useful if you want to imagine each edge update as a separate connection between the two nodes. The edges can be accessed in this manner via `edge.explode()`, as is discussed in [edge metrics and functions](../querying/4_edge-metrics.md).

!!! info
    The property APIs are the same for the graph, nodes and edges, these are discussed together in [Property queries](../querying/5_properties.md).

{{code_block('getting-started/querying','graph_metrics',['Graph'])}}
!!! Output

    ```python exec="on" result="text" session="getting-started/querying"
    --8<-- "python/getting-started/querying.py:graph_metrics"
    ```

## Accessing nodes and edges  
Three types of functions are provided for accessing the nodes and edges within a graph: 

* **Existence check:** using `has_node()` and `has_edge()` you can check if an entity is present within the graph.
* **Direct access:** `node()` and `edge()` will return a node or edge object if the entity is present and `None` if it is not.
* **Iterable access:** `nodes` and `edges` will return iterables for all nodes/edges which can be used within a for loop or as part of a [function chain](../querying/6_chaining.md).

All of these functions are shown in the code below and will appear in several other examples throughout this tutorial.

{{code_block('getting-started/querying','graph_functions',['Graph'])}}
!!! Output

    ```python exec="on" result="text" session="getting-started/querying"
    --8<-- "python/getting-started/querying.py:graph_functions"
    ```
