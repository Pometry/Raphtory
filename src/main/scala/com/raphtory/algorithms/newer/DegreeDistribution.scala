package com.raphtory.algorithms.newer

import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row}
import scala.collection.mutable

class DegreeDistribution(output:String= "/tmp/degreeDistribution") extends GraphAlgorithm {
  override def algorithm(graph: GraphPerspective): Unit = {
    var totalDegree = 0L
    var totalVertex = 0L
    var maxDeg = 0L
    var inDegSq = 0L
    var outDegSq = 0L
    var degSq = 0L
    var firstState = true
    graph.step({
      vertex =>
        val inDeg = vertex.getInEdges().size
        val outDeg = vertex.getOutEdges().size
        val degree = inDeg + outDeg
        totalDegree += degree
        totalVertex += 1
        maxDeg = if (degree > maxDeg) degree else maxDeg
        inDegSq += inDeg*inDeg
        outDegSq += outDeg*outDeg
        degSq += degree*degree
        if (firstState) {
          vertex.setState("start", true)
        }
        firstState = false
    }).select(
      v => Row(
        v.getStateOrElse("start", false),
        totalVertex,
        maxDeg,
        degSq/totalVertex.toDouble,
        inDegSq/totalVertex.toDouble,
        outDegSq/totalVertex.toDouble,
      )
    ).filter(
      r => r.get(0) == true
    ).explode(
      r => List(
        Row("vertices", "maxDegree", "avgSquaredDeg", "avgSquaredInDeg", "avgSquaredOutDeg"),
        Row( r.get(1), r.get(2),r.get(3),r.get(4),r.get(5))
      )
    ).writeTo(output)
  }

  object DegreeDistribution{
    def apply(output:String) = new DegreeDistribution(output)
  }
}
