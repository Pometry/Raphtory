package com.raphtory.algorithms.temporal

import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row, Table}

class Descendants(path:String,
                  seed:String,
                  time:Long,
                  delta:Long=Long.MaxValue,
                  directed:Boolean=true)
  extends GraphAlgorithm{

  override def apply(graph: GraphPerspective): GraphPerspective = {
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
          vertex.setState("descendant", false)
        }
    })
      .iterate({
        vertex =>
          val earliestTime = vertex.messageQueue[Long]
            .min
          vertex.setState("descendant", true)
          val outEdges = (if (directed) vertex.getOutEdges() else vertex.getEdges())
            .filter(e => e.latestActivity().time > earliestTime)
            .filter(e => e.firstActivityAfter(earliestTime).time < earliestTime + delta)
          outEdges.foreach({
            e =>
              vertex.messageNeighbour(e.ID(), e.firstActivityAfter(earliestTime).time)
          })
      }, executeMessagedOnly = true, iterations = 100)
  }

  override def tabularise(graph: GraphPerspective): Table = {
    graph.select(vertex => Row(vertex.getPropertyOrElse("name", vertex.ID()), vertex.getStateOrElse[Boolean]("descendant", false)))
  }

  override def write(table: Table): Unit = {
    table.writeTo(path)
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
