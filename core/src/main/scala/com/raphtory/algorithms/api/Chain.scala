package com.raphtory.algorithms.api

/** Run algorithms in sequence with shared graph state,
  *
  * @see
  *   [[com.raphtory.algorithms.api.GraphAlgorithm]],
  *   [[com.raphtory.algorithms.api.GraphPerspective]],
  *   [[com.raphtory.algorithms.api.Table]]
  *
  * @param algorithms Sequence of GraphAlgorithm(s) to run
  * */
class Chain(algorithms: Seq[GraphAlgorithm]) extends GraphAlgorithm {

  /**  Apply each algorithm to the graph in sequence, clearing messages in-between.
    *  This means that while the next algorithm has access to all the graph state set by previous algorithms,
    *  it does not receive messages sent by the previous algorithm.
    *
    * @param graph The graph perspective to apply the algorithm to
    * @return a new GraphPerspective that has been modified with the algorithm
    * */
  override def apply(graph: GraphPerspective): GraphPerspective = {
    var gp = graph
    if (algorithms.nonEmpty)
      for (algorithm <- algorithms)
        gp = algorithm.apply(gp).clearMessages()
    gp
  }

  /**  Uses the `tabularise` method of the last algorithm in the chain to return results (if algorithms
    *  is empty, it returns an empty table).
    *
    *  @param graph The graph perspective to apply the algorithm to
    *  @return Returns a Table with the results, if empty returns an empty table
    */
  override def tabularise(graph: GraphPerspective): Table =
    if (algorithms.nonEmpty)
      algorithms.last.tabularise(graph)
    else
      super.tabularise(graph)

  /**  Append a new graph algorithm to the chain.
    *
    * @param graphAlgorithm the graph algorithm to apply to the chain
    * @return The chain with the added algorithm
    */
  override def ->(graphAlgorithm: GraphAlgorithm): Chain =
    Chain(algorithms :+ graphAlgorithm: _*)
}

/** Factory for [[com.raphtory.algorithms.api.Chain]] objects */
object Chain {

  /**  Apply each algorithm to the chain in sequence, clearing messages in-between.
    *  This means that while the next algorithm has access to all the graph state set by previous algorithms,
    *  it does not receive messages sent by the previous algorithm.
    *
    * @param algorithms The graph algorithms to apply
    * @return a new Chain that has been modified with the algorithms
    * */
  def apply(algorithms: GraphAlgorithm*) = new Chain(algorithms)
}
