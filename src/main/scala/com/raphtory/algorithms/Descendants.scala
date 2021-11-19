package com.raphtory.algorithms

import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row}

class Descendants(path:String,
                  seed:String,
                  time:Long,
                  delta:Long=Long.MaxValue,
                  directed:Boolean=true)
  extends GraphAlgorithm{

  override def algorithm(graph: GraphPerspective): Unit = {
    graph.step({
      vertex =>
        if (vertex.ID() == checkID(seed)) {
          val edges = (if (directed) vertex.getOutEdges() else vertex.getEdges())
            .filter(e => e.latestActivity().time > time)
            .filter(e => e.firstActivityAfter(time).time < time + delta)
          edges.foreach({
            e =>
              vertex.messageNeighbour(e.ID(), e.firstActivityAfter(time).time)
          })
          vertex.setState("descendant",false)
        }
    })
      .iterate({
        vertex =>
          val earliestTime = vertex.messageQueue[Long]
            .min
          vertex.setState("descendant",true)
          val outEdges = (if (directed) vertex.getOutEdges() else vertex.getEdges())
            .filter(e => e.latestActivity().time > earliestTime)
          .filter(e => e.firstActivityAfter(earliestTime).time < earliestTime + delta)
          outEdges.foreach({
            e =>
              vertex.messageNeighbour(e.ID(), e.firstActivityAfter(earliestTime).time)
          })
      },executeMessagedOnly = true, iterations = 100)
      .select(vertex => Row(vertex.getPropertyOrElse("name",vertex.ID()), vertex.getStateOrElse[Boolean]("descendant",false)))
      .writeTo(path)
  }
}

object Descendants {
  def apply(path:String,
            seed:String,
            time:Long,
            delta:Long=Long.MaxValue,
            directed:Boolean=true)
  = new Descendants(path, seed, time, delta, directed)
}
