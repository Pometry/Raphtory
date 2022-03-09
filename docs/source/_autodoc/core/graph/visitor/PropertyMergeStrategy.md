`com.raphtory.core.graph.visitor.PropertyMergeStrategy`
(com.raphtory.core.graph.visitor.PropertyMergeStrategy)=
# PropertyMergeStrategy

{s}`PropertyMergeStrategy`
 : Collection of pre-defined merge strategies.

A merge strategy is a function that takes a collection of timestamped property values and returns a single value.
Merge strategies are used to control the aggregation semantics for property access in
[{s}`EntityVisitor`](com.raphtory.core.graph.visitor.EntityVisitor).
The general signature for a merge strategy is {s}`Seq[(Long, A)] => B`.

## Methods

{s}`sum[T: Numeric](history: (history: Seq[(Long, T)]): T`
 : Return the sum of property values

{s}`max[T: Numeric](history: (history: Seq[(Long, T)]): T`
 : Return the maximum property value

{s}`min[T: Numeric](history: (history: Seq[(Long, T)]): T`
 : Return the minimum property value

{s}`product[T: Numeric](history: (history: Seq[(Long, T)]): T`
 : Return the product of property values

{s}`average[T: Numeric](history: (history: Seq[(Long, T)]): Double`
 : Return the average of property values

{s}`latest[T](history: (history: Seq[(Long, T)]): T`
 : Return the latest property value (i.e. the value corresponding to the largest timestamp)

{s}`earliest[T](history: (history: Seq[(Long, T)]): T`
 : Return the earliest property value (i.e., the value corresponding to the smallest timestamp)

```{seealso}
[](com.raphtory.core.graph.visitor.EntityVisitor),
[](com.raphtory.core.graph.visitor.Edge),
[](com.raphtory.core.graph.visitor.Vertex)
```