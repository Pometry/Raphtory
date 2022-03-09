`com.raphtory.algorithms.temporal.Ancestors`
(com.raphtory.algorithms.temporal.Ancestors)=
# Ancestors

{s}`Ancestors(seed:String, time:Long, delta:Long=Long.MaxValue, directed:Boolean=true)`
 : find all ancestors of a vertex at a given time point

The ancestors of a seed vertex are defined as those vertices which can reach the seed vertex via a temporal
path (in a temporal path the time of the next edge is always later than the time of the previous edge) by the
specified time.

## Parameters

 {s}`seed: String`
   : The name of the target vertex

 {s}`time: Long`
   : The time of interest

 {s}`delta: Long = Long.MaxValue`
   : The maximum timespan for the temporal path

 {s}`directed: Boolean = true`
   : whether to treat the network as directed

## States

 {s}`ancestor: Boolean`
   : flag indicating that the vertex is an ancestor of {s}`seed`

## Returns

 | vertex name       | is ancestor of seed?   |
 | ----------------- | ---------------------- |
 | {s}`name: String` | {s}`ancestor: Boolean` |