# Create a graph

You can create graphs from scratch in Raphtory or build a graph from existing data formatted as either a [pandas dataframe](https://pandas.pydata.org/docs/reference/frame.html#dataframe) or [Apache Parquet file](https://parquet.apache.org/).

Using these standard data formats allows you to integrate your existing data sanitisation and normalisation pipelines into creating your Raphtory graphs. It is typically easier to prepare your data before ingestion to ensure the quality of subsequent analysis.

## Creating a graph

To get started we first need to create a graph to store our data. Printing this graph will show it as empty with no nodes, edges or update times.

/// tab | :fontawesome-brands-python: Python
```python
from raphtory import Graph

g = Graph()
print(g)
```
///

```{.python continuation hide}
assert str(g) == "Graph(number_of_nodes=0, number_of_edges=0, number_of_temporal_edges=0, earliest_time=None, latest_time=None)"
```

!!! Output

    ```python
    Graph(number_of_nodes=0, number_of_edges=0, number_of_temporal_edges=0, earliest_time=None, latest_time=None)
    ```

## Updating a graph

Once you have a `graph` object you can add data by:

- [Making direct updates](./2_direct-updates.md)
- [Ingesting a dataframe or Parquet file](./3_dataframes.md)

## Saving a graph

When you have created a graph you can save it to disk to reload quickly in the future.

Raphtory can store a graph as:

- Native Raphtory graph
- Zip file
- Parquet
- Raw bytes
