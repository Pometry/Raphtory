package com.raphtory.internals.storage.pojograph.entities.external.vertex

import com.raphtory.api.analysis.visitor.ExplodedVertex
import com.raphtory.api.analysis.visitor.ReducedVertex
import com.raphtory.internals.storage.pojograph.entities.external.edge.PojoExReducedEdgeBase

import scala.collection.View

private[pojograph] class PojoUndirectedVertexView[T](override val vertex: PojoConcreteVertexBase[T])
        extends PojoLocalVertexViewBase(vertex) {

  override type IDType = vertex.IDType
  override type Edge   = vertex.Edge#Eundir

  def outEdges: View[Edge] =
    vertex.inEdges.map { inEdge =>
      vertex.getOutEdge(inEdge.ID) match {
        case Some(outEdge) => inEdge.combineUndirected(outEdge, asInEdge = false)
        case None          => inEdge.reversed
      }
    } ++ vertex.outEdges.filterNot(outEdge => vertex.getInEdge(outEdge.ID).isDefined)

  def inEdges: View[Edge] =
    vertex.outEdges.map { outEdge =>
      vertex.getInEdge(outEdge.ID) match {
        case Some(inEdge) => outEdge.combineUndirected(inEdge, asInEdge = true)
        case None         => outEdge.reversed
      }
    } ++ vertex.inEdges.filterNot(inEdge => vertex.getOutEdge(inEdge.ID).isDefined)

  def getOutEdge(id: vertex.IDType): Option[Edge] =
    vertex.getOutEdge(id) match {
      case Some(outEdge) =>
        vertex.getInEdge(id) match {
          case Some(inEdge) => Some(outEdge.combineUndirected(inEdge, asInEdge = false))
          case None         => Some(outEdge)
        }
      case None          =>
        vertex.getInEdge(id) match {
          case Some(inEdge) => Some(inEdge.reversed)
          case None         => None
        }
    }

  def getInEdge(id: vertex.IDType): Option[Edge] =
    vertex.getInEdge(id) match {
      case Some(inEdge) =>
        vertex.getOutEdge(id) match {
          case Some(outEdge) => Some(inEdge.combineUndirected(outEdge, asInEdge = true))
          case None          => Some(inEdge)
        }
      case None         =>
        vertex.getOutEdge(id) match {
          case Some(outEdge) => Some(outEdge.reversed)
          case None          => None
        }
    }
}

private[pojograph] class PojoReducedUndirectedVertexView(override val vertex: PojoExVertex)
        extends PojoUndirectedVertexView(vertex)
        with ReducedVertex {
  override type Edge = PojoExReducedEdgeBase

  override def getOutEdges(after: Long, before: Long): List[Edge] =
    vertex.getInEdges(after, before).map { inEdge =>
      vertex.getOutEdge(inEdge.ID, after, before) match {
        case Some(outEdge) => inEdge.combineUndirected(outEdge, asInEdge = false)
        case None          => inEdge.reversed
      }
    } ++ vertex.getOutEdges(after, before).filterNot(outEdge => vertex.getInEdge(outEdge.ID, after, before).isDefined)

  override def getInEdges(after: Long, before: Long): List[Edge] =
    vertex.getOutEdges(after, before).map { outEdge =>
      vertex.getInEdge(outEdge.ID, after, before) match {
        case Some(inEdge) => outEdge.combineUndirected(inEdge, asInEdge = true)
        case None         => outEdge.reversed
      }
    } ++ vertex.getInEdges(after, before).filterNot(inEdge => vertex.getOutEdge(inEdge.ID, after, before).isDefined)

  override def getOutEdge(id: Long, after: Long, before: Long): Option[Edge] =
    getOutEdge(id).collect { case edge if edge.active(after, before) => edge.viewBetween(after, before) }

  override def getInEdge(id: Long, after: Long, before: Long): Option[Edge]  =
    getInEdge(id).collect { case edge if edge.active(after, before) => edge.viewBetween(after, before) }
}

private[pojograph] object PojoReducedUndirectedVertexView {
  def apply(vertex: PojoExVertex) = new PojoReducedUndirectedVertexView(vertex)
}

private[pojograph] class PojoExplodedUndirectedVertexView(override val vertex: PojoExplodedVertex)
        extends PojoUndirectedVertexView[(Long, Long)](vertex)
        with ExplodedVertex {

  override def timestamp: Long = vertex.timestamp
}

private[pojograph] object PojoExplodedUndirectedVertexView {
  def apply(vertex: PojoExplodedVertex) = new PojoExplodedUndirectedVertexView(vertex)
}
