`com.raphtory.algorithms.generic.centrality.WeightedPageRank`
(com.raphtory.algorithms.generic.centrality.WeightedPageRank)=
# WeightedPageRank

{s}`WeightedPageRank(dampingFactor:Double = 0.85, iterateSteps:Int = 100, weightProperty = "weight")`
 : Compute PageRank for all nodes, treating the network as weighted

Page Rank algorithm ranks nodes depending on their connections to determine how important
the node is. This assumes a node is more important if it receives more connections from others.
Each vertex begins with an initial state. If it has any neighbours, it sends them a message
which is the inital label / the number of neighbours.
Each vertex, checks its messages and computes a new label based on: the total value of
messages received and the damping factor. This new value is propogated to all outgoing neighbours.
A vertex will stop propogating messages if its value becomes stagnant (i.e. has a change of less
than 0.00001) This process is repeated for a number of iterate step times. Most algorithms should
converge after approx. 20 iterations.

## Parameters

 {s}`dampingFactor: Double = 0.85`
   : Probability that a node will be randomly selected by a user traversing the graph, defaults to 0.85.

 {s}`iterateSteps: Int = 100`
   : Maximum number of iterations for the algorithm to run.

 {s}`weightProperty: String = "weight"`
   : the property (if any) containing a numerical weight value for each edge, defaults to "weight".

 ```{note}
 If the weight property is not found, weight is treated as the number of edge occurrences.
 ```

## States

 {s}`prlabel: Double`
   : PageRank of the node

## Returns

 | vertex name       | PageRank             |
 | ----------------- | -------------------- |
 | {s}`name: String` | {s}`prlabel: Double` |

```{seealso}
[](com.raphtory.algorithms.generic.centrality.PageRank)
```