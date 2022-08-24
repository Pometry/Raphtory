package com.raphtory.internals.storage.pojograph.entities.external.vertex

import com.raphtory.api.analysis.visitor.ExplodedVertex
import com.raphtory.api.analysis.visitor.ReducedVertex
import com.raphtory.internals.components.querymanager.SchemaProvider

private[pojograph] class PojoReversedVertexView[T](override val vertex: PojoConcreteVertexBase[T])
        extends PojoLocalVertexViewBase(vertex) {

  override type IDType = vertex.IDType
  override type Edge   = vertex.Edge

  def outEdges: List[Edge] =
    vertex.inEdges.map(_.reversed)

  def inEdges: List[Edge] =
    vertex.outEdges.map(_.reversed)

  def getOutEdge(id: vertex.IDType): Option[Edge] =
    vertex.getInEdge(id).map(_.reversed)

  def getInEdge(id: vertex.IDType): Option[Edge] =
    vertex.getOutEdge(id).map(_.reversed)

  override implicit val provider: SchemaProvider[T] = ??? // TODO
}

private[pojograph] class PojoReducedReversedVertexView(override val vertex: PojoExVertex)
        extends PojoReversedVertexView(vertex)
        with ReducedVertex {

  override def getOutEdges(after: Long, before: Long): List[Edge] =
    vertex.getInEdges(after, before).map(_.reversed)

  override def getInEdges(after: Long, before: Long): List[Edge] =
    vertex.getOutEdges(after, before).map(_.reversed)

  override def getOutEdge(id: Long, after: Long, before: Long): Option[Edge] =
    vertex.getInEdge(id, after, before).map(_.reversed)

  override def getInEdge(id: Long, after: Long, before: Long): Option[Edge] =
    vertex.getOutEdge(id, after, before).map(_.reversed)
}

private[pojograph] object PojoReducedReversedVertexView {
  def apply(vertex: PojoExVertex) = new PojoReducedReversedVertexView(vertex)
}

private[pojograph] class PojoExplodedReversedVertexView(override val vertex: PojoExplodedVertex)
        extends PojoReversedVertexView[(Long, Long)](vertex)
        with ExplodedVertex {

  override def timestamp: Long = vertex.timestamp
}

private[pojograph] object PojoExplodedReversedVertexView {
  def apply(vertex: PojoExplodedVertex) = new PojoExplodedReversedVertexView(vertex)
}
