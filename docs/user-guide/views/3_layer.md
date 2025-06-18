# Layered graphs

## Prerequisites

Before reading this topic, please ensure you are familiar with:

- [Edge layers](../ingestion/2_direct-updates.md#edge-layers)
- [Exploded Edges](../querying/4_edge-metrics.md#exploded-edges)
- [Traversing graphs](2_time.md#traversing-the-graph-with-views)

## Creating layers views

An edge object by default will contain information on all layers between its source and destination nodes. Often there are only a subset of these relationships that you are interested in. To handle this the `Graph`, `Node` and `Edge` provide the `layers()` function which takes a list of layer names and returns a view with only the edge updates that occurred on these layers. 

Layer views can also be used in combination with any other view function. In the example below, we look at the total edge weight over the full graph, then restrict this to the `Grooming` and `Resting` layers and then reduce this further by applying a window between the 13th and 20th of June.

```python
total_weight = g.edges.properties.temporal.get("Weight").values().sum().sum()
print(f"Total weight across all edges is {total_weight}.")

total_weight = (
    g.layers(["Grooming", "Resting"])
    .edges.properties.temporal.get("Weight")
    .values()
    .sum()
    .sum()
)
print(f"Total weight across Grooming and Resting is {total_weight}.")

start_day = datetime.strptime("2019-06-13", "%Y-%m-%d")
end_day = datetime.strptime("2019-06-20", "%Y-%m-%d")
total_weight = (
    g.layers(["Grooming", "Resting"])
    .window(start_day, end_day)
    .edges.properties.temporal.get("Weight")
    .values()
    .sum()
    .sum()
)
print(
    f"Total weight across Grooming and Resting between {start_day} and {end_day} is {total_weight}."
)
```

!!! Output

    ```output
    Total weight across all edges is 2948.
    Total weight across Grooming and Resting is 1685.
    Total weight across Grooming and Resting between 2019-06-13 00:00:00 and 2019-06-20 00:00:00 is 403.
    ```

## Traversing the graph with layers

Similar to the [time based filters](2_time.md#traversing-the-graph-with-views), if a layer view is applied to the graph, all extracted entities will have this view applied to them. However, if the layer view is applied to a node or edge, it will only last until you have moved to a new node.

Expanding on the example from [the time views](2_time.md#traversing-the-graph-with-views), if you wanted to look at which neighbours LOME has groomed, followed by who those monkeys have rested with, then you could write the following query.

```python
two_hop_neighbours = set(
    g.node("LOME")
    .layer("Grooming")
    .neighbours.layer("Resting")
    .neighbours.name.collect()
)
print(
    f"When the Grooming layer is applied to the node, LOME's two hop neighbours are: {two_hop_neighbours}"
)
```

!!! Output

    ```output
    When the Grooming layer is applied to the node, LOME's two hop neighbours are: {'FEYA', 'LOME', 'HARLEM', 'FELIPE', 'PETOULETTE', 'EWINE', 'FANA', 'LIPS', 'ARIELLE', 'VIOLETTE', 'PIPO', 'BOBO', 'MALI', 'NEKKE', 'MAKO', 'MUSE', 'ANGELE', 'ATMOSPHERE', 'KALI'}
    ```
