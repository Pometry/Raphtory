package com.raphtory.algorithms.newer

import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective}

class DegreeRanking(weighted:Boolean = false, output:String = "/tmp/degreeRanking") extends GraphAlgorithm {


  override def algorithm(graph: GraphPerspective): Unit = {
    var totalVertices = 0L
    var totalIn = 0L
    var totalOut = 0L
    var top20 = scala.collection.mutable.ArrayBuffer()
    graph
      .step({
        vertex =>
          totalVertices += 1
          totalIn += vertex.getInEdges().size
          totalOut += vertex.getOutEdges().size
          val degree = (vertex.getOutEdges().size, vertex.getInEdges().size)
      })
  }

}


object DegreeRanking{
  def apply(weighted:Boolean, output:String) = new DegreeRanking(weighted, output)
}

