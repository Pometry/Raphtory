# Property queries

In Raphtory graphs, nodes and edges can all have temporal `properties` and  constant `metadata`, consisting of a wide range of
data types. This is also discussed in the [ingestion tutorial](../ingestion/2_direct-updates.md).

This [`Properties`][raphtory.Properties] class offers several functions to access values in different formats. To demonstrate this you can
create a simple graph with one node that has a variety of different properties.

You can fetch a nodes property object and call the following functions to access data:

- `keys()`: Returns all of the property keys (names).
- `values()`: Returns the latest value for each property.
- `items()`: Combines the `keys()` and `values()` into a list of tuples.
- `get()`: Returns the latest value for a given key if the property exists or `None` if it does not.
- `as_dict()`: Converts the `Properties` object into a standard python dictionary.

!!! info

    Metadata can call the same functions as properties

/// tab | :fontawesome-brands-python: Python

```python
from raphtory import Graph

property_g = Graph()
# Create the node and add a variety of temporal properties
v = property_g.add_node(
    timestamp=1,
    id="User",
    properties={"count": 1, "greeting": "hi", "encrypted": True},
)
property_g.add_node(
    timestamp=2,
    id="User",
    properties={"count": 2, "balance": 0.6, "encrypted": False},
)
property_g.add_node(
    timestamp=3,
    id="User",
    properties={"balance": 0.9, "greeting": "hello", "encrypted": True},
)
# Add some metadata properties
v.add_metadata(
    metadata={
        "inner data": {"name": "bob", "value list": [1, 2, 3]},
        "favourite greetings": ["hi", "hello", "howdy"],
    },
)
# Call all of the functions on the properties object
properties = v.properties
print("Property keys:", properties.keys())
print("Property values:", properties.values())
print("Property tuples:", properties.items())
print("Latest value of balance:", properties.get("balance"))
print("Property keys:", properties.as_dict(), "\n")

# Access the keys of the metadata and temporal properties individually
metadata = v.metadata
temporal_properties = properties.temporal
print("Metadata keys:", metadata.keys())
print("Property keys:", temporal_properties.keys())
```

///

!!! Output

    ```output
    Property keys: ['count', 'greeting', 'encrypted', 'balance']
    Property values: [2, 'hello', True, 0.9]
    Property tuples: [('count', 2), ('greeting', 'hello'), ('encrypted', True), ('balance', 0.9)]
    Latest value of balance: 0.9
    Property keys: {'count': 2, 'balance': 0.9, 'greeting': 'hello', 'encrypted': True}  

    Metadata keys: ['inner data', 'favourite greetings']
    Constant property keys: ['count', 'greeting', 'encrypted', 'balance']
    ```

## Examining histories

Properties have a history, this means that you can do more than just look at the latest value. Calling `get()`,
`values()` or `items()` on `Properties` will return a `TemporalProp` object which contains all of the value
history.

`TemporalProp` has many helper functions to examine histories, this includes:

* `value()` and `values()`: Get the latest value or all values of the property.
* `at()`: Get the latest value of the property at the specified time.
* `history()` and `history_date_time()`: Get the timestamps of all updates to the property.
* `items()` and `items_date_time()`: Merges `values()` and `history()` or `history_date_time()` into a list of tuples.
* `mean()`, `median()`, and `average()`: If the property is orderable, get the average value for the property.
* `min()` and `max()`: If the property is orderable, get the minimum or maximum value.
* `count()`: Get the number of updates which have occurred
* `sum()`: If the property is additive, sum the values and return the result.

In the code below, we call a subset of these functions on the `Weight` property of the edge between `FELIPE` and `MAKO`
in our previous monkey graph example.

/// tab | :fontawesome-brands-python: Python

```python
import pandas as pd
from raphtory import Graph
from datetime import datetime

edges_df = pd.read_csv(
    "../data/OBS_data.txt", sep="\t", header=0, usecols=[0, 1, 2, 3, 4], parse_dates=[0]
)
edges_df["DateTime"] = pd.to_datetime(edges_df["DateTime"])
edges_df.dropna(axis=0, inplace=True)
edges_df["Weight"] = edges_df["Category"].apply(
    lambda c: 1 if (c == "Affiliative") else (-1 if (c == "Agonistic") else 0)
)

g = Graph()
g.load_edges_from_pandas(
    df=edges_df,
    src="Actor",
    dst="Recipient",
    time="DateTime",
    layer_col="Behavior",
    properties=["Weight"],
) 

properties = g.edge("FELIPE", "MAKO").properties.temporal
print("Property keys:", properties.keys())
weight_prop = properties.get("Weight")
print("Weight property history:", weight_prop.items())
print("Average interaction weight:", weight_prop.mean())
print("Total interactions:", weight_prop.count())
print("Total interaction weight:", weight_prop.sum())
```

///

```{.python continuation hide}
assert weight_prop.mean() == 0.9285714285714286
assert weight_prop.count() == 28
assert weight_prop.sum() == 26
```

!!! Output

    ```output
    Property keys: ['Weight']
    Weight property history: [(1560437400000, 1), (1560437640000, 1), (1560935460000, 1), (1561043280000, 0), (1561043280000, 1), (1561043340000, 1), (1561117620000, 1), (1561373880000, 1), (1561373880000, 1), (1561373940000, 1), (1561373940000, 0), (1561373940000, 1), (1561373940000, 1), (1561373940000, 1), (1561390860000, 1), (1561390860000, 1), (1561390860000, 1), (1561390920000, 1), (1561643580000, 1), (1561717080000, 1), (1561717140000, 1), (1561970760000, 1), (1562148960000, 1), (1562148960000, 1), (1562149020000, 1), (1562149020000, 1), (1562149080000, 1), (1562671020000, 1)]
    Average interaction weight: 0.9285714285714286
    Total interactions: 28
    Total interaction weight: 26
    ```

