
# Exporting to NetworkX

When converting to a networkx graph there is only one function (`to_networkx()`), which has flags for node and edge history and for exploding edges. By default all history is included and the edges are separated by layer. 

In the below example we call `to_networkx()` on the network traffic graph, keeping all the default arguments so that it exports the full history. We extract `ServerA` from this graph and print to show how the history is modelled.

!!! info 
    The resulting graph is a networkx `MultiDiGraph` since Raphtory graphs are both directed and have multiple edges between nodes.

We call `to_networkx()` again, disabling the property and update history and reprint `ServerA` to show the difference.   

{{code_block('getting-started/export','networkX',['Graph'])}}

!!! Output

    ```python exec="on" result="text" session="export"
    --8<-- "python/getting-started/export.py:networkX"
    ```

## Visualisation

Once converted into a networkX graph you have access to their full suite of functionality. For example, using their [drawing](https://networkx.org/documentation/stable/reference/drawing.html) library for visualising graphs.

In the code snippet below we use this functionality to draw a network traffic graph, labelling the nodes with their Server ID. For more information, see the [networkx](https://networkx.org/documentation/stable/reference/drawing.html) documentation.

{{code_block('getting-started/export','networkX_vis',['Graph'])}}