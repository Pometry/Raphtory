# Running algorithms on graph views 

Both `graphwide` and `node centric` algorithms can be run on `graph views`. This allows us to see how results change over time, run algorithms on subsets of the layers, or remove specific nodes from the graph to see the impact this has. 

To demonstrate this, the following example shows how you could track Gandalf's importance over the course of the story using rolling windows and the `PageRank` algorithm. 

Within each windowed graph we use the `NodeState` api to extract Gandalf's score and record it alongside the earliest timestamp in the window, which can then be plotted via matplotlib.

{{code_block('getting-started/algorithms','rolling',['Graph'])}}