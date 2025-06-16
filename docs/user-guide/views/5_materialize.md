# Materialize

All of the view functions hold zero updates of their own, simply providing a lens through which to look at a graph. This is by design so that you can have many views without expensive data duplication. 

However, if the original graph is updated, all of the views over it will also update. If you do not want this to happen you can `materialize()` a view, creating a new graph and copying all the updates the view contains into it. 

In the below example, we create a windowed view between the 17th and 18th of June and then materialize this view. After adding a new monkey interaction in the materialized graph, we can see the original graph does not contain this update, but the materialized graph does.

{{code_block('getting-started/querying','materialize',[])}}
!!! Output

    ```python exec="on" result="text" session="getting-started/querying"
    --8<-- "python/getting-started/querying.py:materialize"
    ```