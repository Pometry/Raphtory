package com.raphtory.algorithms.newer

import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row}


/*
  // damping factor, (1-d) is restart probability


 */

class PageRank(dampingFactor:Double = 0.85, iterateSteps:Int = 100, output:String = "/tmp/PageRank") extends  GraphAlgorithm {

  override def algorithm(graph: GraphPerspective): Unit = {
    graph.step({
      vertex =>
        val initLabel=1.0
        val outDegree=vertex.getOutNeighbours().size
        vertex.setState("prlabel",initLabel)
        if (outDegree>0.0)
          vertex.messageAllOutgoingNeighbors(initLabel/outDegree)
      }).iterate({ vertex =>
      val currentLabel = vertex.getState[Double]("prlabel")
      val newLabel = (1 - dampingFactor) + dampingFactor * vertex.messageQueue[Double].sum
      vertex.setState("prlabel", newLabel)
      if (Math.abs(newLabel - currentLabel) / currentLabel > 0.000001) {
        val outEdges = vertex.getOutNeighbours()
        val outDegree = outEdges.size
        if (outDegree > 0) {
          vertex.messageAllOutgoingNeighbors(newLabel/outDegree)
        }
      }
      else {
        vertex.voteToHalt()
      }
    }, iterateSteps)
      .select({
        vertex =>
          Row(
            vertex.getPropertyOrElse("name", vertex.ID()),
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