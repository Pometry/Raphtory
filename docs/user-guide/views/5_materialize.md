# Materialize

All of the view functions hold zero updates of their own, simply providing a lens through which to look at a graph. This is by design so that you can have many views without expensive data duplication. 

However, if the original graph is updated, all of the views over it will also update. If you do not want this to happen you can `materialize()` a view, creating a new graph and copying all the updates the view contains into it. 

In the below example, we create a windowed view between the 17th and 18th of June and then materialize this view. After adding a new monkey interaction in the materialized graph, we can see the original graph does not contain this update, but the materialized graph does.

=== ":fontawesome-brands-python: Python"

    ```python
    start_time = datetime.strptime("2019-06-17", "%Y-%m-%d")
    end_time = datetime.strptime("2019-06-18", "%Y-%m-%d")
    windowed_view = g.window(start_time, end_time)

    materialized_graph = windowed_view.materialize()
    print(
        f"Before the update the view had {windowed_view.count_temporal_edges()} edge updates"
    )
    print(
        f"Before the update the materialized graph had {materialized_graph.count_temporal_edges()} edge updates"
    )
    print("Adding new update to materialized_graph")
    materialized_graph.add_edge(1, "FELIPE", "LOME", properties={"Weight": 1}, layer="Grooming")
    print(
        f"After the update the view had {windowed_view.count_temporal_edges()} edge updates"
    )
    print(
        f"After the update the materialized graph had {materialized_graph.count_temporal_edges()} edge updates"
    )
    ```

!!! Output

    ```output
    Before the update the view had 132 edge updates
    Before the update the materialized graph had 132 edge updates
    Adding new update to materialized_graph
    After the update the view had 132 edge updates
    After the update the materialized graph had 133 edge updates
    ```