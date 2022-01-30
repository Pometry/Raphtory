package com.raphtory.algorithms.temporal

import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row, Table}
import com.raphtory.core.model.graph.visitor.Vertex
/** This is a utility which constructs a multilayer network representation from a temporal network with edges between nodes
  * and their immediate future/past selves. */

class MultilayerExplode (path:String,
  layers: List[Long],
  layerSize: Long,
  omega: Double = 1.0,
  weight: String="weight")
  extends GraphAlgorithm{

  override def apply(graph: GraphPerspective): GraphPerspective = {
    graph.step { vertex =>
      val tlabels =
        layers.filter(ts => vertex.aliveAt(ts, layerSize))
          .groupBy(identity)
          .mapValues(x => vertex.name()+x.head.toString)
      vertex.setState("multilabels", tlabels)
      vertex.messageAllNeighbours(vertex.ID(),tlabels)

    }
  }

  override def tabularise(graph: GraphPerspective): Table = {
    graph.select({
      vertex =>
        val myLabs = vertex.getState[Map[Long,String]]("multilabels")
        val receivedLabs = vertex.messageQueue[(Long, Map[Long,String])]
        val interEdges = myLabs.collect {
          case (ts, lab) if myLabs.contains(ts + layerSize) => (lab, lab, interLayerWeights(omega,vertex,ts))
        }.toList

        val intraEdges = receivedLabs.flatMap {
          case (vID, labmap: Map[Long, String]) =>
            labmap.collect { case (ts, lab) if myLabs.contains(ts) => (lab, myLabs.getOrElse(ts,""), vertex.getEdge(vID) match {
              case Some(edge) => edge.getPropertyOrElse(weight,1.0f)
              case None => 1.0f
            } ) }
        }
        Row(vertex.name(), interEdges++intraEdges)
    })
      .explode({
        row => row.getAs[List[(String,String,Float)]](1)
          .map({case (src,dst,wgt) => Row(src,dst,wgt)})
      })
  }

  override def write(table: Table): Unit = {
    table.writeTo(path)
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

object MultilayerExplode {
  def apply (path:String,
             layers: List[Long],
             layerSize: Long,
             omega: Double = 1.0,
             weight: String="weight") = new MultilayerExplode(path,layers,layerSize,omega,weight)
}
