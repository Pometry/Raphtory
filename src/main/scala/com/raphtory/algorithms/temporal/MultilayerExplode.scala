package com.raphtory.algorithms.temporal

import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row, Table}
import com.raphtory.core.model.graph.visitor.Vertex
/** This is a utility which constructs a multilayer network representation from a temporal network with edges between nodes
  * and their immediate future/past selves. Copies are made of each node corresponding to the snapshots in which it is present
  * and edges of weight omega are added between a node and the copy of itself in the next layer (if present) */

class MultilayerExplode (path:String,
  layers: List[Long],
  layerSize: Long,
  omega: Float = 1.0f,
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
        val interEdges = interLayerEdges(vertex)
        val intraEdges = intraLayerEdges(vertex)

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

  def interLayerWeights(v: Vertex, ts: Long): Float =
    omega match {
      case -1 =>
        val neilabs = weightFunction(v, ts)
        neilabs.values.sum / neilabs.size
      case _ => omega
    }

  // extract inter-layer edges from self in other layers
  def interLayerEdges(v: Vertex) : List[(String,String,Float)] = {
    val myLabs = v.getState[Map[Long,String]]("multilabels")
    myLabs.collect {
      case (ts, lab) if myLabs.contains(ts + layerSize) => (lab, myLabs.getOrElse(ts + layerSize,lab), interLayerWeights(v,ts))
    }.toList
  }

  // extract intra-layer edges from adjacent vertices
  def intraLayerEdges(v: Vertex) : List[(String,String,Float)] = {
    val myLabs = v.getState[Map[Long,String]]("multilabels")
    val neighLabs = v.messageQueue[(Long, Map[Long,String])]
    neighLabs.flatMap {
      case (vID, labmap: Map[Long, String]) =>
        labmap.collect { case (ts, lab) if myLabs.contains(ts) => (lab, myLabs.getOrElse(ts, ""), v.getEdge(vID) match {
          case Some(edge) => edge.getPropertyOrElse(weight, edge.history().size)
          case None => 1.0f
        })
        }
    }
  }

  def weightFunction(v: Vertex, ts: Long): Map[Long, Float] =
    (v.getInEdges(after = ts - layerSize, before = ts) ++ v.getOutEdges(after = ts - layerSize, before = ts))
      .map(e => (e.ID(), e.getPropertyOrElse("weight", e.history().size)))
      .groupBy(_._1)
      .mapValues(x => x.map(_._2).sum / x.size) // (ID -> Freq)

}

object MultilayerExplode {
  def apply (path:String,
             layers: List[Long],
             layerSize: Long,
             omega: Float = 1.0f,
             weight: String="weight") = new MultilayerExplode(path,layers,layerSize,omega,weight)
}
