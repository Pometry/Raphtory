# Motif Alpha
--- 
<p style="margin-left: 1.5em;"> Returns the number of 2-edge temporal motifs for every node in the graph.</p>

 The algorithms identifies 2-edge-1-node temporal motifs; It detects one type of motifs:

 	_Type-1_: Detects motifs that exhibit incoming flow followed by outgoing flow in the form;

<p align="center">
	<img src="../_static/mc1.png" style="width: 11vw;" alt="mc type 1"/>
</p>


### Implementation

1. For each vertex, for each incoming edge it checks whether there are any outgoing edges that occur after

### Parameters

* `fileOutput` _(String)_ : The path where the output will be saved. If not specified, defaults to _/tmp/PageRank_ 

### Returns
* `ID` _(Long)_ : Vertex ID
* `Number of Type1 motifs` _(Long)_ : Number of _Type-1_ motifs.
