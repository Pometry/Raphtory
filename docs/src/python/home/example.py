# --8<-- [start:example]
from raphtory import Graph
from raphtory import algorithms as algo
import pandas as pd

# Create a new graph
graph = Graph()

# Add some data to your graph
graph.add_node(timestamp=1, id="Alice")
graph.add_node(timestamp=1, id="Bob")
graph.add_node(timestamp=1, id="Charlie")
graph.add_edge(timestamp=2, src="Bob", dst="Charlie", properties={"weight": 5.0})
graph.add_edge(timestamp=3, src="Alice", dst="Bob", properties={"weight": 10.0})
graph.add_edge(timestamp=3, src="Bob", dst="Charlie", properties={"weight": -15.0})

# Check the number of unique nodes/edges in the graph and earliest/latest time seen.
print(graph)

results = [["earliest_time", "name", "out_degree", "in_degree"]]

# Collect some simple node metrics Ran across the history of your graph with a rolling window
for graph_view in graph.rolling(window=1):
    for v in graph_view.nodes:
        results.append(
            [graph_view.earliest_time, v.name, v.out_degree(), v.in_degree()]
        )

# Print the results
print(pd.DataFrame(results[1:], columns=results[0]))

# Grab an edge, explore the history of its 'weight'
cb_edge = graph.edge("Bob", "Charlie")
weight_history = cb_edge.properties.temporal.get("weight").items()
print(
    "The edge between Bob and Charlie has the following weight history:", weight_history
)

# Compare this weight between time 2 and time 3
weight_change = cb_edge.at(2)["weight"] - cb_edge.at(3)["weight"]
print(
    "The weight of the edge between Bob and Charlie has changed by",
    weight_change,
    "pts",
)

# Run pagerank and ask for the top ranked node
top_node = algo.pagerank(graph).top_k(1).max_item()
print(
    "The most important node in the graph is",
    top_node[0].name,
    "with a score of",
    top_node[1],
)
# --8<-- [end:example]
