`com.raphtory.algorithms.generic.dynamic.DiscreteSI`
(com.raphtory.algorithms.generic.dynamic.DiscreteSI)=
# DiscreteSI

{s}`DiscreteSI(infectedNode: Iterable[String], infectionProbability: Double = 0.5, maxGenerations: Int = 100, seed:Long = -1)`
   : discrete susceptible-infected (SI) model on the network

 The network is treated as directed. An infected node propagates the infection along each
 of its out-edges with probability `infectionProbability`. The propagation terminates once there are no newly infected
 nodes or `maxGenerations` is reached. The initially infected seed nodes are considered generation 0.

## Parameters

 {s}`infectedNode: Seq[String]`
   : names of initially infected nodes

 {s}`infectionProbability: Double = 0.5`
   : probability of infection propagation along each edge

 {s}`seed: Long`
   : seed for random number generator (specify for deterministic results)

 {s}`maxGenerations: Int = 100`
   : maximum number of propagation generations

## States

 {s}`infected: Boolean`
   : infection status of vertex

 {s}`generation: Int`
   : generation at which vertex became infected (unset for vertices that were never infected)

## Returns

| vertex name       | infection status       |
| ----------------- | ---------------------- |
| {s}`name: String` | {s}`infected: Boolean` |