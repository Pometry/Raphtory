package com.raphtory.algorithms

import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row}

class PageRank(dampingFactor:Double = 0.85, iterateSteps:Int = 100, output:String = "/tmp/PageRank") extends  GraphAlgorithm {

  override def algorithm(graph: GraphPerspective): Unit = {
    graph.step({
      vertex =>
        val initLabel=1.0
        val outDegree=vertex.getOutNeighbours().size
        vertex.setState("prlabel",initLabel)
        if (outDegree>0.0)
          vertex.messageAllOutgoingNeighbors(initLabel/outDegree)
        vertex.messageSelf(0.0) // ensure all vertices are included in subsequent iterate steps
    }).
      iterate({ vertex =>
        // for
        val vname = vertex.getPropertyOrElse("name",vertex.ID().toString) // for output/logging purposes
        val currentLabel = vertex.getState[Double]("prlabel")

        val queue = vertex.messageQueue[Double]
        val newLabel = (1 - dampingFactor) + dampingFactor * queue.sum
        vertex.setState("prlabel", newLabel)

        val outEdges = vertex.getOutNeighbours()
        val outDegree = outEdges.size
        if (outDegree > 0) {
          vertex.messageAllOutgoingNeighbors(newLabel/outDegree)
        }
        vertex.messageSelf(0.0) // ensure all vertices are included in subsequent iterate steps
        if (Math.abs(newLabel - currentLabel) / currentLabel < 0.00001) {
          vertex.voteToHalt()
        }
      }, iterateSteps,false)
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