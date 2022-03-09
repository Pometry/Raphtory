`com.raphtory.core.algorithm.Chain`
(com.raphtory.core.algorithm.Chain)=
# Chain

{s}`Chain(algorithms: GraphAlgorithm*)`
 : Run algorithms in sequence with shared graph state

## Parameters

 {s}`algorithms: GraphAlgorithm*`
   : List of algorithms to run

## Class Signature

 {s}`Chain(algorithms: Seq[GraphAlgorithm]) extends GraphAlgorithm`

## Methods

 {s}`apply(graph: GraphPerspective): GraphPerspective`
   : Apply each algorithm to the graph in sequence, clearing messages in-between.
     This means that while the next algorithm has access to all the graph state set by previous algorithms,
     it does not receive messages sent by the previous algorithm.

 {s}`tabularise(graph: GraphPerspective): Table`
   : Uses the `tabularise` method of the last algorithm in the chain to return results (if {s}`algorithms`
      is empty, it returns an empty table).

 {s}`->(graphAlgorithm: GraphAlgorithm): Chain`
   : Append a new graph algorithm to the chain.

 ```{seealso}
 [](com.raphtory.core.algorithm.GraphAlgorithm), [](com.raphtory.core.algorithm.GraphPerspective),
 [](com.raphtory.core.algorithm.Table)
 ```