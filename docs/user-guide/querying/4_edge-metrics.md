# Edge metrics and functions 
Edges can be accessed by storing the object returned from a call to `add_edge()`, by directly asking for a specific edge via `edge()`, or by iterating over all edges with `in-edges`, `out-edges`, or `edges`. 

## Edge structure and update history
By default an edge object in Raphtory will contain all updates over all layers between the given source and destination nodes. As an example, we can look at the two edges between `FELIPE` and `MAKO` (one for each direction). 

In the code below we create the two edge objects by requesting them from the graph and then print out the layers each is involved in with `layer_names`. We can see that there are multiple behaviors in each direction represented within the edges.

Following this we access the history to get the earliest and latest update times. This update history consists all interactions across all layers.

!!!info 
    Note that we call `e.src.name` because `src` and `dst` return a node object, instead of just an id or name.

/// tab | :fontawesome-brands-python: Python
```python
e = g.edge("FELIPE", "MAKO")
e_reversed = g.edge("MAKO", "FELIPE")
e_history = [date.strftime("%Y-%m-%d %H:%M:%S") for date in e.history_date_time()]
e_reversed_history = [
    date.strftime("%Y-%m-%d %H:%M:%S") for date in e_reversed.history_date_time()
]

print(
    f"The edge from {e.src.name} to {e.dst.name} covers the following layers: {e.layer_names}"
)
print(
    f"and has updates between {e.earliest_date_time} and {e.latest_date_time} at the following times: {e_history}\n"
)
print(
    f"The edge from {e_reversed.src.name} to {e_reversed.dst.name} covers the following layers: {e_reversed.layer_names}"
)
print(
    f"and has updates between {e_reversed.earliest_date_time} and {e_reversed.latest_date_time} at the following times: {e_reversed_history}"
)
```
///

!!! Output

    ```output
    The edge from FELIPE to MAKO covers the following layers: ['Touching', 'Grooming', 'Resting', 'Playing with', 'Grunting-Lipsmacking', 'Embracing', 'Carrying']
    and has updates between 2019-06-13 14:50:00+00:00 and 2019-07-09 11:17:00+00:00 at the following times: ['2019-06-13 14:50:00', '2019-06-13 14:54:00', '2019-06-19 09:11:00', '2019-06-20 15:08:00', '2019-06-20 15:08:00', '2019-06-20 15:09:00', '2019-06-21 11:47:00', '2019-06-24 10:58:00', '2019-06-24 10:58:00', '2019-06-24 10:59:00', '2019-06-24 10:59:00', '2019-06-24 10:59:00', '2019-06-24 10:59:00', '2019-06-24 10:59:00', '2019-06-24 15:41:00', '2019-06-24 15:41:00', '2019-06-24 15:41:00', '2019-06-24 15:42:00', '2019-06-27 13:53:00', '2019-06-28 10:18:00', '2019-06-28 10:19:00', '2019-07-01 08:46:00', '2019-07-03 10:16:00', '2019-07-03 10:16:00', '2019-07-03 10:17:00', '2019-07-03 10:17:00', '2019-07-03 10:18:00', '2019-07-09 11:17:00']

    The edge from MAKO to FELIPE covers the following layers: ['Grooming', 'Resting', 'Playing with', 'Presenting', 'Grunting-Lipsmacking', 'Embracing']
    and has updates between 2019-06-19 09:42:00+00:00 and 2019-07-09 13:40:00+00:00 at the following times: ['2019-06-19 09:42:00', '2019-06-20 15:08:00', '2019-06-20 15:08:00', '2019-06-20 15:08:00', '2019-06-24 10:58:00', '2019-06-24 11:14:00', '2019-06-24 11:14:00', '2019-06-24 16:09:00', '2019-06-24 16:11:00', '2019-06-26 09:10:00', '2019-06-26 09:11:00', '2019-06-28 10:17:00', '2019-07-02 14:35:00', '2019-07-04 14:30:00', '2019-07-04 14:30:00', '2019-07-09 13:37:00', '2019-07-09 13:37:00', '2019-07-09 13:37:00', '2019-07-09 13:38:00', '2019-07-09 13:39:00', '2019-07-09 13:39:00', '2019-07-09 13:40:00']
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

/// tab | :fontawesome-brands-python: Python
```python
print("Update history per layer:")
for e in g.edge("FELIPE", "MAKO").explode_layers():
    e_history = [date.strftime("%Y-%m-%d %H:%M:%S") for date in e.history_date_time()]
    print(
        f"{e.src.name} interacted with {e.dst.name} with the following behaviour '{e.layer_name}' at this times: {e_history}"
    )

print()
print("Individual updates as edges:")
for e in g.edge("FELIPE", "MAKO").explode():
    print(
        f"At {e.date_time} {e.src.name} interacted with {e.dst.name} in the following manner: '{e.layer_name}'"
    )

print()
print("Individual updates for 'Touching' and 'Carrying:")
for e in g.edge("FELIPE", "MAKO").layers(["Touching", "Carrying"]).explode():
    print(
        f"At {e.date_time} {e.src.name} interacted with {e.dst.name} in the following manner: '{e.layer_name}'"
    )
```
///

!!! Output

    ```output
    Update history per layer:
    FELIPE interacted with MAKO with the following behaviour 'Touching' at this times: ['2019-07-03 10:17:00']
    FELIPE interacted with MAKO with the following behaviour 'Grooming' at this times: ['2019-06-20 15:08:00', '2019-06-20 15:09:00']
    FELIPE interacted with MAKO with the following behaviour 'Resting' at this times: ['2019-06-13 14:50:00', '2019-06-13 14:54:00', '2019-06-19 09:11:00', '2019-06-21 11:47:00', '2019-06-24 10:58:00', '2019-06-24 15:41:00', '2019-06-24 15:41:00', '2019-06-24 15:41:00', '2019-06-27 13:53:00', '2019-07-01 08:46:00', '2019-07-03 10:17:00', '2019-07-09 11:17:00']
    FELIPE interacted with MAKO with the following behaviour 'Playing with' at this times: ['2019-06-24 10:58:00', '2019-06-24 10:59:00', '2019-06-24 10:59:00', '2019-06-24 15:42:00', '2019-07-03 10:16:00', '2019-07-03 10:16:00', '2019-07-03 10:18:00']
    FELIPE interacted with MAKO with the following behaviour 'Grunting-Lipsmacking' at this times: ['2019-06-24 10:59:00', '2019-06-28 10:18:00', '2019-06-28 10:19:00']
    FELIPE interacted with MAKO with the following behaviour 'Embracing' at this times: ['2019-06-24 10:59:00']
    FELIPE interacted with MAKO with the following behaviour 'Carrying' at this times: ['2019-06-20 15:08:00', '2019-06-24 10:59:00']

    Individual updates as edges:
    At 2019-06-13 14:50:00+00:00 FELIPE interacted with MAKO in the following manner: 'Resting'
    At 2019-06-13 14:54:00+00:00 FELIPE interacted with MAKO in the following manner: 'Resting'
    At 2019-06-19 09:11:00+00:00 FELIPE interacted with MAKO in the following manner: 'Resting'
    At 2019-06-20 15:08:00+00:00 FELIPE interacted with MAKO in the following manner: 'Carrying'
    At 2019-06-20 15:08:00+00:00 FELIPE interacted with MAKO in the following manner: 'Grooming'
    At 2019-06-20 15:09:00+00:00 FELIPE interacted with MAKO in the following manner: 'Grooming'
    At 2019-06-21 11:47:00+00:00 FELIPE interacted with MAKO in the following manner: 'Resting'
    At 2019-06-24 10:58:00+00:00 FELIPE interacted with MAKO in the following manner: 'Resting'
    At 2019-06-24 10:58:00+00:00 FELIPE interacted with MAKO in the following manner: 'Playing with'
    At 2019-06-24 10:59:00+00:00 FELIPE interacted with MAKO in the following manner: 'Playing with'
    At 2019-06-24 10:59:00+00:00 FELIPE interacted with MAKO in the following manner: 'Carrying'
    At 2019-06-24 10:59:00+00:00 FELIPE interacted with MAKO in the following manner: 'Embracing'
    At 2019-06-24 10:59:00+00:00 FELIPE interacted with MAKO in the following manner: 'Grunting-Lipsmacking'
    At 2019-06-24 10:59:00+00:00 FELIPE interacted with MAKO in the following manner: 'Playing with'
    At 2019-06-24 15:41:00+00:00 FELIPE interacted with MAKO in the following manner: 'Resting'
    At 2019-06-24 15:41:00+00:00 FELIPE interacted with MAKO in the following manner: 'Resting'
    At 2019-06-24 15:41:00+00:00 FELIPE interacted with MAKO in the following manner: 'Resting'
    At 2019-06-24 15:42:00+00:00 FELIPE interacted with MAKO in the following manner: 'Playing with'
    At 2019-06-27 13:53:00+00:00 FELIPE interacted with MAKO in the following manner: 'Resting'
    At 2019-06-28 10:18:00+00:00 FELIPE interacted with MAKO in the following manner: 'Grunting-Lipsmacking'
    At 2019-06-28 10:19:00+00:00 FELIPE interacted with MAKO in the following manner: 'Grunting-Lipsmacking'
    At 2019-07-01 08:46:00+00:00 FELIPE interacted with MAKO in the following manner: 'Resting'
    At 2019-07-03 10:16:00+00:00 FELIPE interacted with MAKO in the following manner: 'Playing with'
    At 2019-07-03 10:16:00+00:00 FELIPE interacted with MAKO in the following manner: 'Playing with'
    At 2019-07-03 10:17:00+00:00 FELIPE interacted with MAKO in the following manner: 'Touching'
    At 2019-07-03 10:17:00+00:00 FELIPE interacted with MAKO in the following manner: 'Resting'
    At 2019-07-03 10:18:00+00:00 FELIPE interacted with MAKO in the following manner: 'Playing with'
    At 2019-07-09 11:17:00+00:00 FELIPE interacted with MAKO in the following manner: 'Resting'

    Individual updates for 'Touching' and 'Carrying:
    At 2019-06-20 15:08:00+00:00 FELIPE interacted with MAKO in the following manner: 'Carrying'
    At 2019-06-24 10:59:00+00:00 FELIPE interacted with MAKO in the following manner: 'Carrying'
    At 2019-07-03 10:17:00+00:00 FELIPE interacted with MAKO in the following manner: 'Touching'
    ```
