`com.raphtory.algorithms.generic.community.LPA`
(com.raphtory.algorithms.generic.community.LPA)=
# LPA

{s}`LPA(weight:String = "", maxIter:Int = 500, seed:Long = -1)`
   : run synchronous label propagation based community detection

  LPA returns the communities of the constructed graph as detected by synchronous label propagation.
  Every vertex is assigned an initial label at random. Looking at the labels of its neighbours, a probability is assigned
  to observed labels following an increasing function then the vertex’s label is updated with the label with the highest
  probability. If the new label is the same as the current label, the vertex votes to halt. This process iterates until
  all vertex labels have converged. The algorithm is synchronous since every vertex updates its label at the same time.

## Parameters

  {s}`weight: String = ""`
   : Edge weight property. To be specified in case of weighted graph.

  {s}`maxIter: Int = 500`
   : Maximum iterations for algorithm to run.

  {s}`seed: Long`
   : Value used for the random selection, can be set to ensure same result is returned per run.
     If not specified, it will generate a random seed.

## States

   {s}`community: Long`
     : The ID of the community the vertex belongs to

## Returns

 | vertex name       | community label      |
 | ----------------- | -------------------- |
 | {s}`name: String` | {s}`community: Long` |

```{note}
  This implementation of LPA incorporates probabilistic elements which makes it
  non-deterministic; The returned communities may differ on multiple executions.
  Which is why you may want to set the seed if testing.
```

```{seealso}
[](com.raphtory.algorithms.generic.community.SLPA), [](com.raphtory.algorithms.temporal.community.MultilayerLPA)
```