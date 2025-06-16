
# Property queries
In Raphtory graphs, nodes and edges can all have `constant` and `temporal` properties, consisting of a wide range of data types. This is also discussed in the [ingestion tutorial](../ingestion/2_direct-updates.md). Raphtory provides a unified API for accessing `constant` and `temporal` data via the `Properties` object available on all classes by calling `.properties`.

This `Properties` class offers several functions to access values in different formats. To demonstrate this you can create a simple graph with one node that has a variety of different properties, both `temporal` and `constant`. 

You can fetch a nodes property object and call the following functions to access data:

* `keys()`: Returns all of the property keys (names).
* `values()`: Returns the latest value for each property.
* `items()`: Combines the `keys()` and `values()` into a list of tuples.
* `get()`: Returns the latest value for a given key if the property exists or `None` if it does not.
* `as_dict()`: Converts the `Properties` object into a standard python dictionary.

In addition, the `Properties` class also has two attributes `constant` and `temporal` which have all of the above functions, but are restricted to only the properties which fall within their respective categories. The semantics for `ConstantProperties` are exactly the same as described above. However, `TemporalProperties` allow you to do much more, as described in the next section.

{{code_block('getting-started/querying','properties',['Node'])}}
!!! Output

    ```python exec="on" result="text" session="getting-started/querying"
    --8<-- "python/getting-started/querying.py:properties"
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

{{code_block('getting-started/querying','temporal_properties',['Edge'])}}
!!! Output

    ```python exec="on" result="text" session="getting-started/querying"
    --8<-- "python/getting-started/querying.py:temporal_properties"
    ```

