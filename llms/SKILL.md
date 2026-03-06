---
name: raphtory-python-api
description: >
  Use this skill when the user asks to write Python code using raphtory,
  create temporal graphs, run graph algorithms, query graph properties,
  use time views or layers, load graph data, or work with the raphtory
  Python library in any way. Also use when discussing raphtory API usage,
  graph analytics, or temporal network analysis with raphtory.
version: 0.17.0
---

# Raphtory Python API Examples


## Graph Creation & Basics
```python
from raphtory import Graph
```


```python
g = Graph()
print(g)
# => Graph(number_of_nodes=0, number_of_edges=0, number_of_temporal_edges=0, earliest_time=None, latest_time=None)
```


# Add nodes with timestamps and properties
```python
g.add_node(1, "alice", properties={"age": 30})
g.add_node(2, "bob", properties={"age": 25})
print(g.count_nodes())
# => 2
```


# Add edges with layers
```python
g.add_edge(3, "alice", "bob", properties={"weight": 1.0}, layer="friends")
print(g.count_edges())
# => 1
```
