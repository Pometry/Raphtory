# Subgraph

For some use cases you may only be interested in a subset of `nodes` within the graph. One solution to this could be to call `g.nodes` and filtering this before continuing your workflow. However, this does not remove anything from future function calls. You would have to constantly recheck these lists. 

To handle these corner cases Raphtory provides the `subgraph()` function which takes a list of `nodes` of interest. This applies a view such that all `nodes` not in the list are hidden from all future function calls. This also hides any edges linked to hidden `nodes` to keep the subgraph consistent. 

In the below example we demonstrate this by looking at the neighbours of `FELIPE` in the full graph, compared to a subgraph of `FELIPE`, `LIPS`, `NEKKE`, `LOME` and `BOBO`. We also show how that `subgraph()` can be combined with other view functions, in this case a window between the 17th and 18th of June.

{{code_block('getting-started/querying','subgraph',[])}}
!!! Output

    ```python exec="on" result="text" session="getting-started/querying"
    --8<-- "python/getting-started/querying.py:subgraph"
    ```