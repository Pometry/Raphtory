# Page Rank
---

Page Rank algorithm ranks nodes depending on their connections to determine
how important the node is. This assumes a node is more important if it receives
more connections from others. 

## Parameters 

* `dampingFactor` _(Double)_ : Probability that a node will be randomly selected by a user traversing the graph, defaults to 0.85.
* `iterateSteps` _(Int)_ : Number of times for the algorithm to run. 
* `output` _(String)_ : The path where the output will be saved. If not specified, defaults to _/tmp/PageRank_ 


## Implementation

1. Each vertex begins with an initial state. If it has any neighbours, it sends them a message which is the inital label / the number of neighbours

2. Each vertex, checks its messages and computes a new label based on: the total value of messages received and the damping factor. This new value is 
propogated to all outgoing neighbours. A vertex will stop propogating messages if its value becomes stagnant (i.e. has a change of less than 0.00001) 
This process is repeated for a number of iterate step times. Most algorithms should converge after approx. 20 iterations. 

### Returns
* `ID` _(Long)_ : Vertex ID
* `Page Rank` _(Double)_ : Rank of the node

