package com.raphtory.internals.storage.pojograph.entities.external

import com.raphtory.api.analysis.visitor.ExplodedVertex
import com.raphtory.api.analysis.visitor.HistoricEvent
import com.raphtory.api.analysis.visitor.ReducedVertex
import com.raphtory.api.analysis.visitor.Vertex
import com.raphtory.internals.components.querymanager.GenericVertexMessage
import com.raphtory.internals.components.querymanager.VertexMessage
import com.raphtory.internals.storage.pojograph.PojoGraphLens
import com.raphtory.internals.storage.pojograph.messaging.VertexMultiQueue

import scala.collection.mutable
import scala.reflect.ClassTag

abstract class PojoVertexViewBase(vertex: PojoVertexBase) extends PojoVertexBase {
  override def lens: PojoGraphLens = vertex.lens

  /** Return the type of the entity */
  override def Type(): String = vertex.Type()

  override def firstActivityAfter(time: Long, strict: Boolean): Option[HistoricEvent] =
    vertex.firstActivityAfter(time, strict)

  override def lastActivityBefore(time: Long, strict: Boolean): Option[HistoricEvent] =
    vertex.lastActivityBefore(time, strict)

  override def latestActivity(): HistoricEvent = vertex.latestActivity()

  override def earliestActivity(): HistoricEvent = vertex.earliestActivity()

  override def getPropertySet(): List[String] = vertex.getPropertySet()

  override def getPropertyAt[T](key: String, time: Long): Option[T] = vertex.getPropertyAt(key, time)

  override def getPropertyHistory[T](key: String, after: Long, before: Long): Option[List[(Long, T)]] =
    vertex.getPropertyHistory(key, after, before)

  override def history(): List[HistoricEvent] = vertex.history()

  override def active(after: Long, before: Long): Boolean = vertex.active(after, before)

  override def aliveAt(time: Long, window: Long): Boolean = vertex.aliveAt(time, window)
}

abstract class PojoLocalVertexViewBase(val vertex: PojoVertexBase) extends PojoVertexViewBase(vertex) {
  override type IDType = vertex.IDType

  override def setState(key: String, value: Any): Unit = vertex.setState(key, value)

  override def getState[T](key: String, includeProperties: Boolean): T = vertex.getState(key, includeProperties)

  override def getStateOrElse[T](key: String, value: T, includeProperties: Boolean): T =
    vertex.getStateOrElse(key, value, includeProperties)

  override def containsState(key: String, includeProperties: Boolean): Boolean =
    vertex.containsState(key, includeProperties)

  override def getOrSetState[T](key: String, value: T, includeProperties: Boolean): T =
    vertex.getOrSetState(key, value, includeProperties)

  override def appendToState[T: ClassTag](key: String, value: T): Unit = vertex.appendToState(key, value)

  override def remove(): Unit = vertex.remove()

  override def isFiltered: Boolean = vertex.isFiltered

  implicit override val IDOrdering: Ordering[IDType] = vertex.IDOrdering

  override def ID: IDType = vertex.ID

  override def hasMessage: Boolean = vertex.hasMessage

  override def messageQueue[T]: List[T] = vertex.messageQueue

  override def clearMessageQueue(): Unit = vertex.clearMessageQueue()

  override def receiveMessage(msg: GenericVertexMessage[_]): Unit = vertex.receiveMessage(msg)

  override def executeEdgeDelete(): Unit = vertex.executeEdgeDelete()
}

class PojoUndirectedVertexView[T](override val vertex: PojoConcreteVertexBase[T])
        extends PojoLocalVertexViewBase(vertex) {

  override type IDType = vertex.IDType
  override type Edge   = vertex.Edge#Eundir

  def outEdges: List[Edge] =
    vertex.inEdges.map { inEdge =>
      vertex.getOutEdge(inEdge.ID) match {
        case Some(outEdge) => inEdge.combineUndirected(outEdge, asInEdge = false)
        case None          => inEdge.reversed
      }
    } ++ vertex.outEdges.filterNot(outEdge => vertex.getInEdge(outEdge.ID).isDefined)

  def inEdges: List[Edge] =
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

class PojoReducedUndirectedVertexView(override val vertex: PojoExVertex)
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

object PojoReducedUndirectedVertexView {
  def apply(vertex: PojoExVertex) = new PojoReducedUndirectedVertexView(vertex)
}

class PojoExplodedUndirectedVertexView(override val vertex: PojoExplodedVertex)
        extends PojoUndirectedVertexView[(Long, Long)](vertex)
        with ExplodedVertex {

  override def timestamp: Long = vertex.timestamp
}

object PojoExplodedUndirectedVertexView {
  def apply(vertex: PojoExplodedVertex) = new PojoExplodedUndirectedVertexView(vertex)
}
