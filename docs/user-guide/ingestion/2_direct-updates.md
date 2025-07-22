# Direct Updates

Now that we have a graph we can directly update it with the `add_node()` and `add_edge()` functions.

## Adding nodes
To add a node we need a unique `id` to represent it and an update `timestamp` to specify when it was added to the graph. In the below example we are going to add node `10` at timestamp `1`. 

!!! info

    If your data doesn't have any timestamps, you can just set a constant value like `1` for all additions into the graph.  


/// tab | :fontawesome-brands-python: Python
```python
from raphtory import Graph

g = Graph()
v = g.add_node(timestamp=1, id=10)

print(g)
print(v)
```
///

```{.python continuation hide}
assert str(g) == "Graph(number_of_nodes=1, number_of_edges=0, number_of_temporal_edges=0, earliest_time=1, latest_time=1)"
assert str(v) == "Node(name=10, earliest_time=1, latest_time=1)"
```

Printing out the graph and the returned node we can see the update was successful and the earliest/latest time has been updated.

!!! Output

    ```output
    Graph(number_of_nodes=1, number_of_edges=0, number_of_temporal_edges=0, earliest_time=1, latest_time=1)
    Node(name=10, earliest_time=1, latest_time=1)
    ```



## Adding edges
All graphs in raphtory are [directed](https://en.wikipedia.org/wiki/Directed_graph), meaning edge additions must specify a `timestamp` (the same as a `node_add()`), the `source` node the edge starts from and the `destination` node the edge ends at. 

As an example of this below we are adding an edge to the graph from `15` to `16` at timestamp `1`.

/// tab | :fontawesome-brands-python: Python
```python
from raphtory import Graph

g = Graph()
e = g.add_edge(timestamp=1, src=15, dst=16)

print(g)
print(e)
```
///

```{.python continuation hide}
assert str(g) == "Graph(number_of_nodes=2, number_of_edges=1, number_of_temporal_edges=1, earliest_time=1, latest_time=1)"
assert str(e) == "Edge(source=15, target=16, earliest_time=1, latest_time=1, layer(s)=[_default])"
```

!!! Output

    ```output
    Graph(number_of_nodes=2, number_of_edges=1, number_of_temporal_edges=1, earliest_time=1, latest_time=1)
    Edge(source=15, target=16, earliest_time=1, latest_time=1, layer(s)=[_default])
    ```

You will notice in the output that the graph says that it has two nodes as well as the edge. Raphtory automatically creates the source and destination nodes at the same time if they do not currently exist in the graph. This is to keep the graph consistent and avoid `hanging edges`.


## Accepted ID types
The `add_node()` and `add_edge()` functions will also accept strings for their `id`, `src` & `dst` arguments. This is useful when your node IDs are not integers. For example, node IDs could be unique strings like a person's username or a blockchain wallet hash. 

In this example, we are adding two nodes to the graph `User 1` and `User 2` and an edge between them. 

/// tab | :fontawesome-brands-python: Python
```python
from raphtory import Graph

g = Graph()

g.add_node(timestamp=123, id="User 1")
g.add_node(timestamp=456, id="User 2")
g.add_edge(timestamp=789, src="User 1", dst="User 2")

print(g.node("User 1"))
print(g.node("User 2"))
print(g.edge("User 1", "User 2"))
```
///

```{.python continuation hide}
assert str(g.node("User 1")) == "Node(name=User 1, earliest_time=123, latest_time=789)"
assert str(g.node("User 2")) == "Node(name=User 2, earliest_time=456, latest_time=789)"
assert str(g.edge("User 1", "User 2")) == "Edge(source=User 1, target=User 2, earliest_time=789, latest_time=789, layer(s)=[_default])"
```

!!! Output

    ```output
    Node(name=User 1, earliest_time=123, latest_time=789)
    Node(name=User 2, earliest_time=456, latest_time=789)
    Edge(source=User 1, target=User 2, earliest_time=789, latest_time=789, layer(s)=[_default])
    ```

!!! warning
    A graph can index nodes by either integers or strings, not both at the same time.This means, for example, you cannot have `User 1` (a string) and `200` (an integer) as ids in the same graph. 

## Accepted timestamps
While integer based timestamps can represent both [logical time](https://en.wikipedia.org/wiki/Logical_clock) and [epoch time](https://en.wikipedia.org/wiki/Unix_time), datasets often have their timestamps stored in human readable formats or special datetime objects. As such, `add_node()` and `add_edge()` can accept integers, datetime strings and datetime objects interchangeably. 

In the example below the node `10` is added into the graph at `2021-02-03 14:01:00` and `2021-01-01 12:32:00`. The first timestamp is kept as a string, with Raphtory internally handling the conversion, and the second has been converted into a python datetime object before ingestion. This datetime object can also have a timezone, with Raphtory storing everything internally in UTC.

/// tab | :fontawesome-brands-python: Python
```python
from raphtory import Graph
from datetime import datetime

g = Graph()
g.add_node(timestamp="2021-02-03 14:01:00", id=10)

# Create a python datetime object
datetime_obj = datetime(2021, 1, 1, 12, 32, 0, 0)
g.add_node(timestamp=datetime_obj, id=10)

print(g)
print(g.node(id=10).history())
print(g.node(id=10).history_date_time())
```
///

```{.python continuation hide}
assert str(g) == "Graph(number_of_nodes=1, number_of_edges=0, number_of_temporal_edges=0, earliest_time=1609504320000, latest_time=1612360860000)"
assert str(g.node(id=10).history()) == "[1609504320000 1612360860000]"
assert str(g.node(id=10).history_date_time()) == "[datetime.datetime(2021, 1, 1, 12, 32, tzinfo=datetime.timezone.utc), datetime.datetime(2021, 2, 3, 14, 1, tzinfo=datetime.timezone.utc)]"
```

!!! Output

    ```output
    Graph(number_of_nodes=1, number_of_edges=0, number_of_temporal_edges=0, earliest_time=1609504320000, latest_time=1612360860000)
    [1609504320000 1612360860000]
    [datetime.datetime(2021, 1, 1, 12, 32, tzinfo=datetime.timezone.utc), datetime.datetime(2021, 2, 3, 14, 1, tzinfo=datetime.timezone.utc)]
    ```

In the output we can see the `history` of node `10` contains the two times at which we have added it into the graph (maintained in ascending order), returned in both unix epoch (integer) and datetime format.

## Properties
Alongside the structural update history, Raphtory can maintain the changing value of properties associated with nodes and edges. Both the `add_node()` and `add_edge()` functions have an optional parameter `properties` which takes a dictionary of key value pairs to be stored at the given timestamp. 

The graph itself may also have its own `global properties` added using the `add_properties()` function which takes only a `timestamp` and a `properties` dictionary. 

Properties can consist of primitives (`Integer`, `Float`, `String`, `Boolean`, `Datetime`) and structures (`Dictionary`, `List`). This allows you to store both basic values as well as do complex hierarchical modelling depending on your use case.

In the example below, we are using all of these functions to add a mixture of properties to a node, an edge, and the graph.

!!! warning
    Please note that once a `property key` is associated with one of the above types for a given node/edge/graph, attempting to add a value of a different type under the same key will result in an error. For `Lists` the values must all be the same type and for `Dictionaries` the values for each key must always be the same type.

/// tab | :fontawesome-brands-python: Python
```python
from raphtory import Graph
from datetime import datetime

g = Graph()

# Primitive type properties added to a node
g.add_node(
    timestamp=1,
    id="User 1",
    properties={"count": 1, "greeting": "hi", "encrypted": True},
)
g.add_node(
    timestamp=2,
    id="User 1",
    properties={"count": 2, "balance": 0.6, "encrypted": False},
)
g.add_node(
    timestamp=3,
    id="User 1",
    properties={"balance": 0.9, "greeting": "hello", "encrypted": True},
)

# Dictionaries and Lists added to a graph
g.add_properties(
    timestamp=1,
    properties={
        "inner data": {"name": "bob", "value list": [1, 2, 3]},
        "favourite greetings": ["hi", "hello", "howdy"],
    },
)
datetime_obj = datetime.strptime("2021-01-01 12:32:00", "%Y-%m-%d %H:%M:%S")
g.add_properties(
    timestamp=2,
    properties={
        "inner data": {
            "date of birth": datetime_obj,
            "fruits": {"apple": 5, "banana": 3},
        }
    },
)

# Weight list on an edge
g.add_edge(timestamp=4, src="User 1", dst="User 2", properties={"weights": [1,2,3]})

# Printing everything out
v = g.node(id="User 1")
e = g.edge(src="User 1", dst="User 2")
print(g)
print(v)
print(e)
```
///

```{.python continuation hide}
assert str(e) == "Edge(source=User 1, target=User 2, earliest_time=4, latest_time=4, properties={weights: [1, 2, 3]}, layer(s)=[_default])"
```

!!! Output

    ```output
    Graph(number_of_nodes=2, number_of_edges=1, number_of_temporal_edges=1, earliest_time=1, latest_time=4, properties=Properties({inner data: {fruits: {apple: 5, banana: 3}, date of birth: 2021-01-01 12:32:00}, favourite greetings: [hi, hello, howdy]}))
    Node(name=User 1, earliest_time=1, latest_time=4, properties=Properties({count: 2, greeting: hello, encrypted: true, balance: 0.9}))
    Edge(source=User 1, target=User 2, earliest_time=4, latest_time=4, properties={weights: [1, 2, 3]}, layer(s)=[_default])
    ```

!!! info
    When the output is printed only the latest property values are shown. The older values haven't been lost, in fact the history of all of these different property types can be queried, explored and aggregated, as you will see in [Property Queries](../querying/5_properties.md).

### Constant Properties

Alongside the `temporal` properties which have a value history, Raphtory also provides `constant` properties which have an immutable value. These are useful when you know a value won't change or are adding metadata to your graph which does not need to be asossiated with a specific time. To add these into your model the `graph`, `node` and `edge` have the `add_constant_properties()` function, which takes a single `dictionary` argument for properties.

In the example below, three different constant properties are added to the `graph`, `node` and `edge`. 

/// tab | :fontawesome-brands-python: Python

```python
from raphtory import Graph
from datetime import datetime

g = Graph()
v = g.add_node(timestamp=1, id="User 1")
e = g.add_edge(timestamp=2, src="User 1", dst="User 2")

g.add_constant_properties(properties={"name": "Example Graph"})
v.add_constant_properties(
    properties={"date of birth": datetime.strptime("1990-02-03", "%Y-%m-%d")},
)
e.add_constant_properties(properties={"data source": "https://link-to-repo.com"})

print(g)
print(v)
print(e)
```
///

```{.python continuation hide}
assert str(g) == "Graph(number_of_nodes=2, number_of_edges=1, number_of_temporal_edges=1, earliest_time=1, latest_time=2, properties=Properties({name: Example Graph}))"
assert str(v) == "Node(name=User 1, earliest_time=1, latest_time=2, properties=Properties({date of birth: 1990-02-03 00:00:00}))"
assert str(e) == "Edge(source=User 1, target=User 2, earliest_time=2, latest_time=2, properties={data source: https://link-to-repo.com}, layer(s)=[_default])"
```

!!! output

    ```output
    Graph(number_of_nodes=2, number_of_edges=1, number_of_temporal_edges=1, earliest_time=1, latest_time=2, properties=Properties({name: Example Graph}))
    Node(name=User 1, earliest_time=1, latest_time=2, properties=Properties({date of birth: 1990-02-03 00:00:00}))
    Edge(source=User 1, target=User 2, earliest_time=2, latest_time=2, properties={data source: https://link-to-repo.com}, layer(s)=[_default])
    ```    

## Edge Layers
If you have worked with other graph libraries you may be expecting two calls to `add_edge()` between the same nodes to generate two distinct edge objects. In Raphtory, these calls append the information together into the history of a single edge. 

Edges can be [exploded](../querying/4_edge-metrics.md/#exploded-edges) to interact with all updates independently and Raphtory also allows you to represent totally different relationships between the same nodes via `edge layers`.

The `add_edge()` function takes a second optional parameter, `layer` that allows you to name the type of relationship being added. All calls to `add_edge` with the same `layer` value will be stored together allowing them to be accessed separately or merged with other layers as required.

You can see this in the example below where we add five updates between `Person 1` and `Person 2` across the layers `Friends`, `Co Workers` and `Family`. When we query the history of the `weight` property on the edge we initially get all of the values back. However, by applying the [`layers()` graph view](../views/3_layer.md) we can return only updates from `Co Workers` and `Family`. 

/// tab | :fontawesome-brands-python: Python
```python
from raphtory import Graph

g = Graph()
g.add_edge(
    timestamp=1,
    src="Person 1",
    dst="Person 2",
    properties={"weight": 10},
    layer="Friends",
)
g.add_edge(
    timestamp=2,
    src="Person 1",
    dst="Person 2",
    properties={"weight": 13},
    layer="Friends",
)
g.add_edge(
    timestamp=3,
    src="Person 1",
    dst="Person 2",
    properties={"weight": 20},
    layer="Co Workers",
)
g.add_edge(
    timestamp=4,
    src="Person 1",
    dst="Person 2",
    properties={"weight": 17},
    layer="Friends",
)
g.add_edge(
    timestamp=5,
    src="Person 1",
    dst="Person 2",
    properties={"weight": 35},
    layer="Family",
)

unlayered_edge = g.edge("Person 1", "Person 2")
layered_edge = g.layers(["Co Workers", "Family"]).edge("Person 1", "Person 2")
print(unlayered_edge.properties.temporal.get("weight").values())
print(layered_edge.properties.temporal.get("weight").values())
```
///

```{.python continuation hide}
assert str(unlayered_edge.properties.temporal.get("weight").values()) == "[10 13 20 17 35]"
assert str(layered_edge.properties.temporal.get("weight").values()) == "[20 35]"
```

!!! Output

    ```output
    [10 13 20 17 35]
    [20 35]
    ```