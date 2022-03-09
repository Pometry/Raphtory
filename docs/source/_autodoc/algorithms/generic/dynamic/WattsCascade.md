`com.raphtory.algorithms.generic.dynamic.WattsCascade`
(com.raphtory.algorithms.generic.dynamic.WattsCascade)=
# WattsCascade

{s}`WattsCascade(infectedSeed:Iterable[String], threshold: T = Threshold.UNIFORM_RANDOM, seed:Long = -1, maxGenerations: Int = 100)`
 : run the Watts Cascade dynamic on the network

 This algorithm, presented by Duncan Watts in 2002, presents a method for the spread of "infectious ideas."
 In the model, people are represented as nodes and relationships are edges. Each node is given a random or
 deterministic threshold that states it will accept an idea if the fraction its in-neighbours accepting the
 idea has exceeded the threshold.

 1. In the first step the state of all nodes are set. This includes whether they are
    initially infected and their threshold.

 2. Each non-infected vertex checks whether the number of infected messages it has
    received outweighs its threshold, if so then it sets its state to be infected and
    then announces this to all of its out-neighbours.

## Parameters

 {s}`infectedSeed: Seq[String]`
   : The list of node names that begin infection.

 {s}`threshold: Double | Threshold.UNIFORM_RANDOM | Threshold.RANDOM_SAME_VAL = Threshold.UNIFORM_RANDOM`
   : fraction of infected neighbours necessary to trigger change of state. Set to {s}`UNIFORM_RANDOM` to choose
     thresholds independently at random for each vertex and {s}`RANDOM_SAME_VAL` to choose a single random threshold
     to apply to all vertices..

 {s}`seed: Long`
   : Value used for the random selection, can be set to ensure same result is returned per run.
     If not specified, it will generate a random seed.

 {s}`maxGenerations: Int = 100`
   : Maximum number of spreading generations, where seeds are at generation 0.

## States

 {s}`numInfectedNeighbours: Int`
   : Number of infected neighbours

 {s}`threshold: Double`
   : infection threshold for vertex

 {s}`infected: Boolean`
   : true if vertex is infected, else false

## Returns

 | vertex name       | infection status       |
 | ----------------- | ---------------------- |
 | {s}`name: String` | {s}`infected: Boolean` |