package com.raphtory.core.model.algorithm

class AlgorithmChain(algorithms: Seq[GraphAlgorithm]) extends GraphAlgorithm {

  override def graphStage(graphPerspective: GraphPerspective): GraphPerspective = {
    var gp = graphPerspective
    if(algorithms.nonEmpty) {
      for(algorithm <- algorithms)
        gp = algorithm.graphStage(gp)
    }
    gp
  }

  override def tableStage(graphPerspective: GraphPerspective): Table = {
    if(algorithms.nonEmpty)
      algorithms.last.tableStage(graphPerspective)
    else
      super.tableStage(graphPerspective)
  }

  override def write(table: Table): Unit = {
    if(algorithms.nonEmpty)
      algorithms.last.write(table)
    else
      super.write(table)
  }

}

object AlgorithmChain {
  def apply(algorithms: GraphAlgorithm*) = new AlgorithmChain(algorithms)
}
