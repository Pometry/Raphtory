# Running algorithms on graph views 

Both `graphwide` and `node centric` algorithms can be run on `graph views`. This allows us to see how results change over time, run algorithms on subsets of the layers, or remove specific nodes from the graph to see the impact this has. 

To demonstrate this, the following example shows how you could track Gandalf's importance over the course of the story using rolling windows and the `PageRank` algorithm. 

Within each windowed graph we use the `NodeState` api to extract Gandalf's score and record it alongside the earliest timestamp in the window, which can then be plotted via matplotlib.

/// tab | :fontawesome-brands-python: Python
```python
# mkdocs: render
import matplotlib.pyplot as plt
import pandas as pd
from raphtory import algorithms as rp
from raphtory import Graph

df = pd.read_csv("docs/data/lotr.csv")
lotr_graph = Graph()
lotr_graph.load_edges_from_pandas(
    df=df, src="src", dst="dst", time="time"
)

importance = []
time = []

for windowed_graph in lotr_graph.rolling(window=2000):
    result = rp.pagerank(windowed_graph)
    importance.append(result.get("Gandalf"))
    time.append(windowed_graph.earliest_time)

plt.plot(time, importance, marker="o")
plt.xlabel("Sentence (Time)")
plt.ylabel("Pagerank Score")
plt.title("Gandalf's importance over time")
plt.grid(True)
```
///