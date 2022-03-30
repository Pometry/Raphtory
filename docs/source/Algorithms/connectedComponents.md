# Connected Components

---

<p style="margin-left: 1.5em;"> Returns the number of connected components.</p>

A connected component of an undirected graph is a set of vertices all of which are connected by paths to each other. This algorithm calculates the number of connected components in the graph and the size of each, and returns some statisics of these.

### Parameters 

* `output_dir` _(String)_ - Directory path where the output is written to.

### Implementation
The algorithm is similar to that of GraphX and fairly straightforward:

1. Each node is numbered by its ID, and takes this as an initial connected components label.

2. Each node forwards its own label to each of its neighbours.

3. Having received a list of labels from neighbouring nodes, each node relabels itself with the smallest label it has received (or stays the same if its starting label is smaller than any received).

4. The algorithm iterates over steps 2 and 3 until no nodes change their label within an iteration.

### Returns

This returns a CSV with the following 

* `ID` _(Long)_ - ID of the vertex.
* `Label` _(Long)_ - The value of the component it belongs to.

#### Notes
Edges here are treated as undirected. A future feature will calculate weakly/strongly connected components for directed networks.
