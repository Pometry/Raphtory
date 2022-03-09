`com.raphtory.algorithms.generic.centrality.WeightedDegree`
(com.raphtory.algorithms.generic.centrality.WeightedDegree)=
# WeightedDegree

{s}`WeightedDegree(weightProperty: String = "weight")`
 : compute the weighted degree (i.e. strength)

This algorithm returns the weighted degree (i.e., strength) of a node, defined by the weighted sum of incoming,
outgoing or total edges to that node. If an edge has a numerical weight property, the name of this property can
be specified as a parameter -- default is "weight". In this case, the sum of edge weights for respectively
incoming and outgoing edges respectively is returned, as well as the total
of these. Otherwise, the number of incoming and outgoing edges (including multiple edges between the same
node pairs) is returned, as well as the sum of these.

## Parameters

 {s}`weightProperty: String = "weight"`
   : the property (if any) containing a numerical weight value for each edge, defaults to "weight".

## States

 {s}`inWeight: Double`
   : Sum of weighted incoming edges

 {s}`outWeight: Double`
   : Sum of weighted outgoing edges

 {s}`totWeight: Double`
   : Sum of the above

## Returns

 | vertex name       | total incoming weight | total outgoing weight  | total weight           |
 | ----------------- | --------------------- | ---------------------- | ---------------------- |
 | {s}`name: String` | {s}`inWeight: Double` | {s}`outWeight: Double` | {s}`totWeight: Double` |

```{seealso}
[](com.raphtory.algorithms.generic.centrality.Degree)
```