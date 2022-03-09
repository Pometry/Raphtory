`com.raphtory.algorithms.generic.centrality.Degree`
(com.raphtory.algorithms.generic.centrality.Degree)=
# Degree

{s}`Degree()`
 : return in-degree, out-degree, and degree of nodes

The degree of a node in an undirected networks counts the number of neighbours that
node has. In directed networks, the in-degree of a note counts the number of incoming edges and the
out-degree the number of outgoing edges.

## States

 {s}`inDegree: Int`
   : The in-degree of the node

 {s}`outDegree: Int`
   : The out-degree of the node

 {s}`degree: Int`
   : The undirected degree (i.e. the overall number of neighbours)

## Returns

 | vertex name       | in-degree          | out-degree          | degree           |
 | ----------------- | ------------------ | ------------------- | ---------------- |
 | {s}`name: String` | {s}`inDegree: Int` | {s}`outDegree: Int` | {s}`degree: Int` |

```{seealso}
[](com.raphtory.algorithms.generic.centrality.WeightedDegree)
```