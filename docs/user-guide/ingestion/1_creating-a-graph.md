# Creating a graph

There are many ways to get data into Raphtory and start running analysis. In this tutorial we are going to cover three of the most versatile:

- Direct updating
- Pandas Dataframe
- Loading from a saved Raphtory graph. 

To get started we first need to create a graph to store our data. Printing this graph will show it as empty with no nodes, edges or update times.

=== ":fontawesome-brands-python: Python"

    ```python
    from raphtory import Graph

    g = Graph()
    print(g)
    ```

!!! Output

    ```python
    Graph(number_of_nodes=0, number_of_edges=0, number_of_temporal_edges=0, earliest_time=None, latest_time=None)
    ```
