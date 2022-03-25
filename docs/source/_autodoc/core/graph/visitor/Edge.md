`com.raphtory.core.graph.visitor.Edge`
(com.raphtory.core.graph.visitor.Edge)=
# Edge

{s}`Edge`
 : Extends [{s}`EntityVisitor`](com.raphtory.core.graph.visitor.EntityVisitor) with edge-specific functionality

For documentation of the property access and update history methods see the
[{s}`EntityVisitor` documentation](com.raphtory.core.graph.visitor.EntityVisitor).

## Attributes

{s}`ID: Long`
 : Edge ID

{s}`src: Long`
 : ID of the source vertex of the edge

{s}`dst: Long`
 : ID of the destination vertex of the edge

## Methods

{s}`send(data: Any): Unit`
 : Send a message to the destination vertex of the edge

   {s}`data: Any`
     : Message data to send

{s}`explode(): List[ExplodedEdge]`
 : Return an [{s}`ExplodedEdge`](com.raphtory.core.graph.visitor.ExplodedEdge) instance for each time the edge is
   active in the current view.

{s}`weight[A, B](weightProperty: String = "weight", mergeStrategy: Seq[(Long, A)] => B = PropertyMergeStrategy.sum[A], default: A = 1)`
 : Compute the weight of the edge using a custom merge strategy

   {s}`A`
     : value type for the edge weight property (if {s}`mergeStrategy` is not given, this needs to be a numeric type)

   {s}`B`
     : return type of the merge strategy (only specify if using a custom merge strategy)

   {s}`weightProperty: String = "weight"`
     : edge property to use for computing edge weight

   {s}`mergeStrategy: Seq[(Long, A)] => B = PropertyMergeStrategy.sum[A]`
     : merge strategy to use for converting property history to edge weight
       (see [{s}`PropertyMergeStrategy`](com.raphtory.core.graph.visitor.PropertyMergeStrategy) for predefined
       options or provide custom function). By default this returns the sum of property values.

   {s}`default: A = 1`
     : default value for the weight property before applying the merge strategy.

       This defaults to {s}`1` if {s}`A` is a numeric type. This default value is applied before applying the
       merge strategy. In the case where, e.g., {s}`mergeStrategy =  PropertyMergeStrategy.sum[A]`, the
       computed weight is the number of times the edge was active in the current view if the weight property is not
       found.

```{seealso}
[](com.raphtory.core.graph.visitor.ExplodedEdge),
[](com.raphtory.core.graph.visitor.PropertyMergeStrategy),
[](com.raphtory.core.graph.visitor.EntityVisitor)
[](com.raphtory.core.graph.visitor.Vertex)
```