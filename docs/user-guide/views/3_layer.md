# Layered graphs

## Prerequisites

Before reading this topic, please ensure you are familiar with:

- [Edge layers](../ingestion/2_direct-updates.md#edge-layers)
- [Exploded Edges](../querying/4_edge-metrics.md#exploded-edges)
- [Traversing graphs](2_time.md#traversing-the-graph-with-views)

## Creating layers views

An edge object by default will contain information on all layers between its source and destination nodes. Often there are only a subset of these relationships that you are interested in. To handle this the `Graph`, `Node` and `Edge` provide the `layers()` function which takes a list of layer names and returns a view with only the edge updates that occurred on these layers. 

Layer views can also be used in combination with any other view function. In the example below, we look at the total edge weight over the full graph, then restrict this to the `Grooming` and `Resting` layers and then reduce this further by applying a window between the 13th and 20th of June.

{{code_block('getting-started/querying','layered',[])}}
!!! Output

    ```python exec="on" result="text" session="getting-started/querying"
    --8<-- "python/getting-started/querying.py:layered"
    ```

## Traversing the graph with layers

Similar to the [time based filters](2_time.md#traversing-the-graph-with-views), if a layer view is applied to the graph, all extracted entities will have this view applied to them. However, if the layer view is applied to a node or edge, it will only last until you have moved to a new node.

Expanding on the example from [the time views](2_time.md#traversing-the-graph-with-views), if you wanted to look at which neighbours LOME has groomed, followed by who those monkeys have rested with, then you could write the following query.

{{code_block('getting-started/querying','layered_hopping',[])}}
!!! Output

    ```python exec="on" result="text" session="getting-started/querying"
    --8<-- "python/getting-started/querying.py:layered_hopping"
    ```
