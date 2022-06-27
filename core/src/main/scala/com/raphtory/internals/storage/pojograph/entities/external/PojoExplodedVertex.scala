package com.raphtory.internals.storage.pojograph.entities.external

import com.raphtory.api.analysis.visitor.ExplodedVertex
import com.raphtory.api.analysis.visitor.HistoricEvent
import com.raphtory.internals.components.querymanager.FilteredInEdgeMessage
import com.raphtory.internals.components.querymanager.FilteredOutEdgeMessage
import com.raphtory.internals.storage.pojograph.messaging.VertexMultiQueue

import scala.collection.mutable
import scala.reflect.ClassTag

private[raphtory] class PojoExplodedVertex(
    val vertex: PojoExVertex,
    override val timestamp: Long
) extends PojoVertexViewBase(vertex)
        with PojoConcreteVertexBase
        with ExplodedVertex {
  override val internalIncomingEdges = mutable.Map.empty[(Long, Long), PojoExMultilayerEdge]
  override val internalOutgoingEdges = mutable.Map.empty[(Long, Long), PojoExMultilayerEdge]

  override type Edge = PojoExMultilayerEdge

  var computationValues: Map[String, Any] =
    Map.empty //Partial results kept between supersteps in calculation

  override def ID: IDType = (vertex.ID, timestamp)

  override def Type(): String = vertex.Type()

  override def firstActivityAfter(time: Long = timestamp, strict: Boolean): Option[HistoricEvent] =
    vertex.firstActivityAfter(time, strict)

  override def lastActivityBefore(time: Long = timestamp, strict: Boolean): Option[HistoricEvent] =
    vertex.lastActivityBefore(time, strict)

  override def getPropertySet(): List[String] = vertex.getPropertySet()

  override def getPropertyHistory[T](
      key: String,
      after: Long,
      before: Long = timestamp
  ): Option[List[(Long, T)]] = vertex.getPropertyHistory[T](key, after, before)

  override def setState(key: String, value: Any): Unit = computationValues += (key -> value)

  override def getState[T](key: String, includeProperties: Boolean): T =
    computationValues.get(key) match {
      case Some(value) => value.asInstanceOf[T]
      case None        => vertex.getState[T](key, includeProperties)
    }

  override def getStateOrElse[T](key: String, value: T, includeProperties: Boolean): T =
    computationValues.get(key) match {
      case Some(value) => value.asInstanceOf[T]
      case None        => vertex.getStateOrElse[T](key, value, includeProperties)
    }

  override def containsState(key: String, includeProperties: Boolean): Boolean =
    computationValues.contains(key) || vertex.containsState(key, includeProperties)

  override def getOrSetState[T](key: String, value: T, includeProperties: Boolean): T =
    computationValues.get(key) match {
      case Some(value) => value.asInstanceOf[T]
      case None        =>
        val newValue: T = vertex.getStateOrElse[T](key, value, includeProperties)
        computationValues += (key -> newValue)
        newValue
    }

  override def appendToState[T: ClassTag](key: String, value: T): Unit =
    computationValues.get(key) match {
      case Some(arr) =>
        setState(key, arr.asInstanceOf[Array[T]] :+ value)
      case None      =>
        setState(key, Array(value))
    }

  override def remove(): Unit = {
    super.remove()
    vertex.explodedNeedsFiltering = true
  }
}
