`com.raphtory.algorithms.temporal.Descendants`
(com.raphtory.algorithms.temporal.Descendants)=
# Descendants

{s}`Descendants(seed:String, time:Long, delta:Long=Long.MaxValue, directed:Boolean=true)`
 : find all descendants of a vertex at a given time point

The descendants of a seed vertex are defined as those vertices which can be reached from the seed vertex
via a temporal path (in a temporal path the time of the next edge is always later than the time of the previous edge)
starting after the specified time.

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

 {s}`descendant: Boolean`
   : flag indicating that the vertex is a descendant of {s}`seed`

## Returns

 | vertex name       | is descendant of seed?   |
 | ----------------- | ---------------------- |
 | {s}`name: String` | {s}`descendant: Boolean` |