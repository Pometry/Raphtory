package com.raphtory.algorithms.generic.centrality

import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row, Table}

class WeightedPageRank (dampingFactor:Float = 0.85F, iterateSteps:Int = 100, output:String = "/tmp/PageRank",weightProperty:String="weight") extends  GraphAlgorithm{

  override def apply(graph: GraphPerspective): GraphPerspective = {
    graph.step({
      vertex =>
        val initLabel = 1.0F
        vertex.setState("prlabel", initLabel)
        val outWeight = vertex.outWeight()
        vertex.getOutEdges().foreach({
          e =>
            vertex.messageVertex(e.ID, e.totalWeight(weightProperty=weightProperty) / outWeight)
        })
    })
      .iterate({ vertex =>
        val vname = vertex.name() // for logging purposes
        val currentLabel = vertex.getState[Float]("prlabel")

        val queue = vertex.messageQueue[Float]
        val newLabel = (1 - dampingFactor) + dampingFactor * queue.sum
        vertex.setState("prlabel", newLabel)

        val outWeight = vertex.outWeight(weightProperty = weightProperty)
        vertex.getOutEdges().foreach({
          e =>
            vertex.messageVertex(e.ID, newLabel * e.totalWeight(weightProperty=weightProperty)/ outWeight)
        })

        if (Math.abs(newLabel - currentLabel) / currentLabel < 0.00001) {
          vertex.voteToHalt()
        }
      }, iterateSteps, false)
  }

  override def tabularise(graph: GraphPerspective): Table = {
    graph.select({
      vertex =>
        Row(
          vertex.name(),
          vertex.getStateOrElse("prlabel", -1)
        )
    })
  }

  override def write(table: Table): Unit = {
    table.writeTo(output)
  }
}

object WeightedPageRank{
  def apply(dampingFactor:Float = 0.85F, iterateSteps:Int = 100, output:String = "/tmp/PageRank") =
    new WeightedPageRank(dampingFactor, iterateSteps, output)
}