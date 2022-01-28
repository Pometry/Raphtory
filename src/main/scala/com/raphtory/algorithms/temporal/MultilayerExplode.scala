package com.raphtory.algorithms.temporal

import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective}
import com.raphtory.core.model.graph.visitor.Vertex
/** This is a utility which constructs a multilayer network representation from a temporal network with edges between nodes
  * and their immediate future/past selves. */

class MultilayerExplode(layers: List[Long],
                        layerSize: Long,
                        omega: Double = 1.0,
                        weight: String="weight")
  extends GraphAlgorithm{

  override def apply(graph: GraphPerspective): GraphPerspective = {
    graph.step { vertex =>
      // Assign random labels for all instances in time of a vertex as Map(ts, lab)
      val tlabels =
        layers.filter(ts => vertex.aliveAt(ts, layerSize))
          .map(ts => (ts, vertex.name()+ts.toString))
      vertex.setState("multilabels", tlabels)
      val message = tlabels.map(x => (x._1,x._2))
        .groupBy(_._1)
        .mapValues(v => v(0)._2)
      vertex.messageOutNeighbours(message)
    }.select({
      vertex =>
        val myLabs = vertex.getState[List[(Long,String)]]("multilabels")
        val receivedLabs = vertex.messageQueue[(Long,List[(Long,String)])]
        val interEdges = myLabs.sliding(2)
          .map( e => (e(0)._2,e(1)._2))
        val intraEdges = myLabs.map({
          lab =>

        })

    })
  }

  def interLayerWeights(omega: Double, v: Vertex, ts: Long): Float =
    omega match {
      case -1 =>
        val neilabs = weightFunction(v, ts)
        neilabs.values.sum / neilabs.size
      case _ => omega.toFloat
    }

  def weightFunction(v: Vertex, ts: Long): Map[Long, Float] =
    (v.getInEdges(after = ts - layerSize, before = ts) ++ v.getOutEdges(after = ts - layerSize, before = ts))
      .map(e => (e.ID(), e.getProperty(weight).getOrElse(1.0f)))
      .groupBy(_._1)
      .mapValues(x => x.map(_._2).sum / x.size) // (ID -> Freq)

}
