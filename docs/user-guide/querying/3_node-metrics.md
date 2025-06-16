
# Node metrics and functions
Nodes can be accessed by storing the object returned from a call to `add_node()`, by directly asking for a specific entity via `node()`, or by iterating over all entities via `nodes`. Once you have a node, you can ask it some questions. 

## Update history 

Nodes have functions for querying their earliest and latest update time (as an epoch or datetime) as well as for accessing their full history (using `history()` or `history_date_time()`). In the code below we create a node object for the monkey `Felipe` and see when their updates occurred. 

{{code_block('getting-started/querying','node_metrics',['Node'])}}
!!! Output

    ```python exec="on" result="text" session="getting-started/querying"
    --8<-- "python/getting-started/querying.py:node_metrics"
    ```

## Neighbours, edges and paths
To investigate who a node is connected with we can ask for its `degree()`, `edges`, or `neighbours`. As Raphtory graphs are directed, all of these functions also have an `in_` and `out_` variation, allowing you get only incoming and outgoing connections respectively. These functions return the following:

* **degree:** A count of the number of unique connections a node has
* **edges:** An `Edges` iterable of edge objects, one for each unique `(src,dst)` pair
* **neighbours:** A `PathFromNode` iterable of node objects, one for each entity the original node shares an edge with

In the code below we call a selection of these functions to show the sort of questions you may ask. 

!!! info

    The final section of the code makes use of `v.neighbours.name.collect()` - this is a chain of functions which are run on each node in the `PathFromNode` iterable. We will discuss these sort of operations more in [Chaining functions](../querying/6_chaining.md). 

{{code_block('getting-started/querying','node_neighbours',['Node'])}}
!!! Output

    ```python exec="on" result="text" session="getting-started/querying"
    --8<-- "python/getting-started/querying.py:node_neighbours"
    ```