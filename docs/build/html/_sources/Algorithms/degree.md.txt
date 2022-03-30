# Degree
---
<p style="margin-left: 1.5em;"> Returns the degree of each node.</p>

The _degree_ of a node in an undirected networks counts the number of neighbours that node has. Its _weighted degree_ counts the number of interactions each node has. Finally the _edge weight_ counts the number of interactions which have happened across an edge.

<p align="center">
	<img src="../_static/degree.png" style="width: 10vw;" alt="node degree and weighed degree example"/>
</p>

In the graph above, with all edges aggregated between time $t_1$ and $t_3$, node $A$ has degree 2 (having neighbours $B$ and $C$) but weighted degree 3 (from the interactions at times $t_1$, $t_2$ and $t_3$). In the same way, edge $AB$ has weight 2.

### Parameter
* `output_dir` _(String)_ -- Directory path where the output is written to. 

### Returns
* `ID` _(Long)_ : Vertex ID.
* `indegree` _(Long)_ : The indegree of the node.
* `outdegree` _(Long)_ : The outdegree of the node.
* `total degree` _(Long)_ : The total degree.

