package com.raphtory.algorithms

import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row}

class GraphState(path:String) extends GraphAlgorithm{
  override def algorithm(graph: GraphPerspective): Unit = {
    graph.select(vertex=> {
        val inDeg = vertex.getInEdges().size
        val outDeg = vertex.getOutEdges().size
        val deg = inDeg + outDeg
        val vdeletions = vertex.history().count(f => !f.event)
        val vcreations = vertex.history().count(f =>f.event)
        val outedgedeletions =vertex.getOutEdges().map(f=>f.history().count(f => !f.event)).sum
        val outedgecreations =vertex.getOutEdges().map(f=>f.history().count(f => f.event)).sum

        val inedgedeletions =vertex.getInEdges().map(f=>f.history().count(f => !f.event)).sum
        val inedgecreations =vertex.getInEdges().map(f=>f.history().count(f => f.event)).sum

        val properties = vertex.getPropertySet().size
        val propertyhistory = vertex.getPropertySet().toArray.map(x=> vertex.getPropertyHistory(x).size).sum
        val outedgeProperties = vertex.getOutEdges().map(edge => edge.getPropertySet().size).sum
        val outedgePropertyHistory = vertex.getOutEdges().map(edge => edge.getPropertySet().toArray.map(x=> edge.getPropertyHistory(x).size).sum).sum

        val inedgeProperties = vertex.getInEdges().map(edge => edge.getPropertySet().size).sum
        val inedgePropertyHistory = vertex.getInEdges().map(edge => edge.getPropertySet().toArray.map(x=> edge.getPropertyHistory(x).size).sum).sum

        Row(inDeg, outDeg,deg, vdeletions,vcreations,outedgedeletions,outedgecreations,inedgedeletions,inedgecreations,properties,propertyhistory,outedgeProperties,outedgePropertyHistory,inedgeProperties,inedgePropertyHistory)
    })
      .writeTo(path)
  }
}

object GraphState {
  def apply(path:String) = new GraphState(path)
}
