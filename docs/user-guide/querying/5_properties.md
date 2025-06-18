
# Property queries
In Raphtory graphs, nodes and edges can all have `constant` and `temporal` properties, consisting of a wide range of data types. This is also discussed in the [ingestion tutorial](../ingestion/2_direct-updates.md). Raphtory provides a unified API for accessing `constant` and `temporal` data via the `Properties` object available on all classes by calling `.properties`.

This `Properties` class offers several functions to access values in different formats. To demonstrate this you can create a simple graph with one node that has a variety of different properties, both `temporal` and `constant`. 

You can fetch a nodes property object and call the following functions to access data:

- `keys()`: Returns all of the property keys (names).
- `values()`: Returns the latest value for each property.
- `items()`: Combines the `keys()` and `values()` into a list of tuples.
- `get()`: Returns the latest value for a given key if the property exists or `None` if it does not.
- `as_dict()`: Converts the `Properties` object into a standard python dictionary.

In addition, the `Properties` class also has two attributes `constant` and `temporal` which have all of the above functions, but are restricted to only the properties which fall within their respective categories. The semantics for `ConstantProperties` are exactly the same as described above. However, `TemporalProperties` allow you to do much more, as described in the next section.

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
# Add some constant properties
v.add_constant_properties(
    properties={
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

# Access the keys of the constant and temporal properties individually
constant_properties = properties.constant
temporal_properties = properties.temporal
print("Constant property keys:", constant_properties.keys())
print("Constant property keys:", temporal_properties.keys())
```

!!! Output

    ```output
    Property keys: ['count', 'greeting', 'encrypted', 'balance', 'inner data', 'favourite greetings']
    Property values: [2, 'hello', True, 0.9, {'value list': [1, 2, 3], 'name': 'bob'}, ['hi', 'hello', 'howdy']]
    Property tuples: [('count', 2), ('greeting', 'hello'), ('encrypted', True), ('balance', 0.9), ('inner data', {'value list': [1, 2, 3], 'name': 'bob'}), ('favourite greetings', ['hi', 'hello', 'howdy'])]
    Latest value of balance: 0.9
    Property keys: {'inner data': {'value list': [1, 2, 3], 'name': 'bob'}, 'balance': 0.9, 'favourite greetings': ['hi', 'hello', 'howdy'], 'greeting': 'hello', 'count': 2, 'encrypted': True} 

    Constant property keys: ['inner data', 'favourite greetings']
    Constant property keys: ['count', 'greeting', 'encrypted', 'balance']
    ```
    

## Temporal specific functions
Temporal properties have a history, this means that you can do more than just look at the latest value. Calling `get()`, `values()` or `items()` on `TemporalProperties` will return a `TemporalProp` object which contains all of the value history.

`TemporalProp` has many helper functions to examine histories, this includes:

* `value()` and `values()`: Get the latest value or all values of the property.
* `at()`: Get the latest value of the property at the specified time.
* `history()` and `history_date_time()`: Get the timestamps of all updates to the property.
* `items()` and `items_date_time()`: Merges `values()` and `history()` or `history_date_time()` into a list of tuples.
* `mean()`, `median()`, and `average()`: If the property is orderable, get the average value for the property.
* `min()` and `max()`: If the property is orderable, get the minimum or maximum value.
* `count()`: Get the number of updates which have occurred
* `sum()`: If the property is additive, sum the values and return the result.

In the code below, we call a subset of these functions on the `Weight` property of the edge between `FELIPE` and `MAKO` in our previous monkey graph example.

```python
properties = g.edge("FELIPE", "MAKO").properties.temporal
print("Property keys:", properties.keys())
weight_prop = properties.get("Weight")
print("Weight property history:", weight_prop.items())
print("Average interaction weight:", weight_prop.mean())
print("Total interactions:", weight_prop.count())
print("Total interaction weight:", weight_prop.sum())
```

!!! Output

    ```output
    Property keys: ['Weight']
    Weight property history: [(1560437400000, 1), (1560437640000, 1), (1560935460000, 1), (1561043280000, 0), (1561043280000, 1), (1561043340000, 1), (1561117620000, 1), (1561373880000, 1), (1561373880000, 1), (1561373940000, 1), (1561373940000, 0), (1561373940000, 1), (1561373940000, 1), (1561373940000, 1), (1561390860000, 1), (1561390860000, 1), (1561390860000, 1), (1561390920000, 1), (1561643580000, 1), (1561717080000, 1), (1561717140000, 1), (1561970760000, 1), (1562148960000, 1), (1562148960000, 1), (1562149020000, 1), (1562149020000, 1), (1562149080000, 1), (1562671020000, 1)]
    Average interaction weight: 0.9285714285714286
    Total interactions: 28
    Total interaction weight: 26
    ```

