package com.raphtory.algorithms.newer

import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row}


/*
  // damping factor, (1-d) is restart probability


 */

class PageRank(dampingFactor:Double = 0.85, iterateSteps:Int = 100, output:String = "/tmp/PageRank") extends  GraphAlgorithm {

  override def algorithm(graph: GraphPerspective): Unit = {
    graph.step({
      vertex =>
        val outEdges = vertex.getOutEdges()
        val outDegree = outEdges.size
        if (outDegree > 0) {
          val toSend = 1.0/outDegree
          vertex.setState("prlabel",toSend)
          outEdges.foreach(edge => {
            edge.send(toSend)
          })
        } else {
          vertex.setState("prlabel",0.0)
        }
      }).iterate({ vertex =>
      val currentLabel = vertex.getState[Double]("prlabel")
      val newLabel = 1 - dampingFactor + dampingFactor * vertex.messageQueue[Double].sum
      vertex.setState("prlabel", newLabel)
      if (Math.abs(newLabel - currentLabel) / currentLabel > 0.01) {
        val outEdges = vertex.getOutEdges()
        val outDegree = outEdges.size
        if (outDegree > 0) {
          val toSend = newLabel / outDegree
          outEdges.foreach(edge => {
            edge.send(toSend)
          })
        }
      }
      else {
        vertex.voteToHalt()
      }
    }, iterateSteps)
      .select({
        vertex =>
          Row(
            vertex.ID(),
            vertex.getStateOrElse("prlabel", -1)
          )
      })
      .writeTo(output)
  }
}



object PageRank{
  def apply(dampingFactor:Double = 0.85, iterateSteps:Int = 100, output:String = "/tmp/PageRank") =
    new PageRank(dampingFactor, iterateSteps, output)
}