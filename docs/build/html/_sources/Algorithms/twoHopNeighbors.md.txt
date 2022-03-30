# Two-Hop Neighbours
---


<p style="margin-left: 1.5em;"> Returns the 2-hop network of a node `n`.</p>

This algorithm will return the two hop neighbours of each node
in the graph. If the user provides a node ID, then it will
only return the two hop neighbours of that node.


### Parameters
* `node` _(String)_ : The node ID to start with. If not specified, then this is run for all nodes.
* `output` _(String)_ : The path where the output will be saved. If not specified, defaults to _/tmp/twoHopNeighbour_ 

## Implementation

1. In the first step the node messages all its neighbours, saying that it is
asking for a two-hop analysis.

2. Each vertex, starting from a triangle count of zero, looks at the lists of ID requests it has received, it then finds all of its neighbours and replies to the node in the form (response, neighbour, me).

3. The requester compiles these into a list of results 

### Warning

As this sends alot of messages between nodes, running this for the
entire graph with a large number of iterations may cause you to run out of memory.
Therefore it is most optimal to run with a select node at a time.
The number of iterations makes a difference to ensure all messages have been read.

### Returns
* `First Hop ID` _(Long)_:  ID of the first hop
* `Second Hop ID`_(Long)_:  ID of the second hop
* `Third Hop ID` _(Long)_:  ID of the third hop


<p align="center">
  <img src="../_static/2hop-ex.png" style="width: 18vw;" alt="2hop-network example"/>
</p>
