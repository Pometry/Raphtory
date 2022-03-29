package com.raphtory.algorithms.api

/**
  * {s}`Chain(algorithms: GraphAlgorithm*)`
  *  : Run algorithms in sequence with shared graph state
  *
  * ## Parameters
  *
  *  {s}`algorithms: GraphAlgorithm*`
  *    : List of algorithms to run
  *
  * ## Class Signature
  *
  *  {s}`Chain(algorithms: Seq[GraphAlgorithm]) extends GraphAlgorithm`
  *
  * ## Methods
  *
  *  {s}`apply(graph: GraphPerspective): GraphPerspective`
  *    : Apply each algorithm to the graph in sequence, clearing messages in-between.
  *      This means that while the next algorithm has access to all the graph state set by previous algorithms,
  *      it does not receive messages sent by the previous algorithm.
  *
  *  {s}`tabularise(graph: GraphPerspective): Table`
  *    : Uses the `tabularise` method of the last algorithm in the chain to return results (if {s}`algorithms`
  *       is empty, it returns an empty table).
  *
  *  {s}`->(graphAlgorithm: GraphAlgorithm): Chain`
  *    : Append a new graph algorithm to the chain.
  *
  *  ```{seealso}
  *  [](com.raphtory.algorithms.api.GraphAlgorithm), [](com.raphtory.algorithms.api.GraphPerspective),
  *  [](com.raphtory.algorithms.api.Table)
  *  ```
  */
class Chain(algorithms: Seq[GraphAlgorithm]) extends GraphAlgorithm {

  override def apply(graph: GraphPerspective): GraphPerspective = {
    var gp = graph
    if (algorithms.nonEmpty)
      for (algorithm <- algorithms)
        gp = algorithm.apply(gp).clearMessages()
    gp
  }

  override def tabularise(graph: GraphPerspective): Table =
    if (algorithms.nonEmpty)
      algorithms.last.tabularise(graph)
    else
      super.tabularise(graph)

  override def ->(graphAlgorithm: GraphAlgorithm): Chain =
    Chain(algorithms :+ graphAlgorithm: _*)
}

object Chain {
  def apply(algorithms: GraphAlgorithm*) = new Chain(algorithms)
}
