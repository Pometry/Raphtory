`com.raphtory.algorithms.generic.CBOD`
(com.raphtory.algorithms.generic.CBOD)=
# CBOD

 {s}`CBOD(label: String = "community", cutoff: Double = 0.0, labeler:GraphAlgorithm = Identity())`
 : Returns outliers detected based on the community structure of the Graph.

 The algorithm assumes that the state of each vertex contains a community label
 (e.g., set by running [LPA](com.raphtory.algorithms.generic.community.LPA) on the graph, initially)
 and then defines an outlier score based on a node's
 community membership and how it compares to its neighbors community memberships.

## Parameters

 {s}`label: String = "community"`
 : Identifier for community label (default: "community")

 {s}`cutoff: Double = 0.0`
 : Outlier score threshold (default: 0.0). Identifies the outliers with an outlier score > cutoff.

 {s}`labeler: GraphAlgorithm`
 : Community algorithm to run to get labels (does nothing by default, i.e., labels should
   be already set on the input graph, either via chaining or defined as properties of the data)

## States

 {s}`outlierscore: Double`
 : Community-based outlier score for vertex

## Returns

 (only for vertex such that `outlierscore >= cutoff`)

 | vertex name      | outlier score            |
 |------------------|--------------------------|
 |{s}`name: String` | {s}`outlierscore: Double`|

```{seealso}
[](com.raphtory.algorithms.generic.community.LPA)
```