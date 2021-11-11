package com.raphtory.algorithms

import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row}

class Ancestors(path:String,
                seed:String,
                time:Long,
                delta:Long=Long.MaxValue,
                directed:Boolean=true)
  extends GraphAlgorithm{

  override def algorithm(graph: GraphPerspective): Unit = {
    graph.step({
      vertex =>
        if (vertex.ID() == checkID(seed)) {
          val edges = (if (directed) vertex.getInEdges() else vertex.getEdges())
            .filter(e => e.earliestActivity().time < time)
            .filter(e=> e.lastActivityBefore(time).time > time - delta)
          edges.foreach({
            e =>
              vertex.messageNeighbour(e.ID(), e.lastActivityBefore(time).time)
          })
          vertex.setState("ancestor",false)
        }
    })
      .iterate({
        vertex =>
          val latestTime = vertex.messageQueue[Long]
            .max
          vertex.setState("ancestor",true)
          val inEdges = (if (directed) vertex.getInEdges() else vertex.getEdges())
            .filter(e => e.earliestActivity().time < latestTime)
            .filter(e=> e.lastActivityBefore(time).time > latestTime - delta)
          inEdges.foreach({
            e =>
              vertex.messageNeighbour(e.ID(), e.lastActivityBefore(latestTime).time)
          })
      },executeMessagedOnly = true, iterations = 100)
      .select(vertex => Row(vertex.getPropertyOrElse("name",vertex.ID()), vertex.getStateOrElse[Boolean]("ancestor",false)))
      .writeTo(path)
  }
}
object Ancestors {
  def apply(path:String, seed:String,
            time:Long,
            delta:Long=Long.MaxValue,
            directed:Boolean=true)
  = new Ancestors(path, seed, time, delta, directed)
}
