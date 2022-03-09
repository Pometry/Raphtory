`com.raphtory.algorithms.temporal.community.MultilayerLPA`
(com.raphtory.algorithms.temporal.community.MultilayerLPA)=
# MultilayerLPA

{s}`MultilayerLPA(weight: String = "", maxIter: Int = 500, layers: List[Long], layerSize: Long, omega: Double = 1.0, seed: Long = -1)`
 : find multilayer communities using synchronous label propagation

This returns the communities of the constructed multi-layer graph as detected by synchronous label propagation.
This transforms the graph into a multi-layer graph where the same vertices on different layers are handled as
distinct vertices. The algorithm then runs a version of LPA on this view of the graph and returns communities that
share the same label that can span both vertices on the same layer and other layers.

## Parameters

 {s}`weight: String = ""`
   : Edge property to be specified in case of weighted graph.

 {s}`maxIter: Int = 500`
   : Maximum iterations for LPA to run.

 {s}`layers: List[Long]`
   : List of layer timestamps.

 {s}`layerSize: Long`
   : Size of a single layer that spans all events occurring within this period.

 {s}`omega: Double = 1.0`
   : Weight of temporal edge that are created between two layers for two persisting instances of a node.
     If {s}`omega=-1`, the weights are assigned based on an average of the neighborhood of two layers.

 {s}`seed: Long`
   : Seed for random elements. Defaults to random seed.

## States

 {s}`mlpalabel: List[Long, Long]`
   : List of community labels for all instances of the vertex of the form (timestamp, label)

## Returns

 | community label  | vertex name and timestamp   |
 | ---------------- | --------------------------- |
 | {s}`label: Long` | {s}`name + "_" + timestamp` |

```{note}
  This implementation is based on LPA, which incorporates probabilistic elements. This makes it
  non-deterministic i.e., the returned communities may differ on multiple executions.
  You can set a seed if you want to make this deterministic.
```

```{seealso}
[](com.raphtory.algorithms.generic.community.LPA)
```