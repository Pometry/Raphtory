package com.raphtory.algorithms.newer

import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row}

class OutDegreeAverage(output:String) extends GraphAlgorithm {
  override def algorithm(graph: GraphPerspective): Unit = {
    var nodeCount = 0L
    graph
      .step({v => nodeCount += 1})
      .select({
        vertex =>
          val id = vertex.ID()
          val sized =
            try vertex.getOutEdges().size.toDouble / nodeCount.toDouble
            catch { case _: ArithmeticException => 0 }
          Row(id, sized)
      })
      .writeTo(output)
  }
}


object OutDegreeAverage{
  def apply(output:String= "/tmp/outDegreeAverage") = new OutDegreeAverage(output)
}

