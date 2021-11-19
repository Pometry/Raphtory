package com.raphtory.algorithms

import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row}

class OutDegreeAverage(output:String) extends GraphAlgorithm {
  override def algorithm(graph: GraphPerspective): Unit = {
    val nodeCount = graph.nodeCount()
    graph
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

