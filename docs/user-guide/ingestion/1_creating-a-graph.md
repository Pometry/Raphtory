# Creating a graph

There are many ways to get data into Raphtory and start running analysis. In this tutorial we are going to cover three of the most versatile:

- Direct updating
- Pandas Dataframe
- Loading from a saved Raphtory graph. 

To get started we first need to create a graph to store our data. Printing this graph will show it as empty with no nodes, edges or update times.

{{code_block('getting-started/ingestion','new_graph',['Graph'])}}
!!! Output

    ```python exec="on" result="text" session="getting-started/ingestion"
    --8<-- "python/getting-started/ingestion.py:new_graph"
    ```
