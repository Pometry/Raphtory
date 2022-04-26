package com.raphtory.storage.pojograph.entities.external

import com.raphtory.graph.visitor.ExplodedVertex
import com.raphtory.graph.visitor.HistoricEvent
import com.raphtory.storage.pojograph.PojoGraphLens

import scala.collection.mutable
import scala.reflect.ClassTag

/** @DoNotDocument */
class PojoExplodedVertex(
    val vertex: PojoExVertex,
    override val timestamp: Long,
    override val lens: PojoGraphLens
) extends ExplodedVertex
        with PojoVertexBase {
  override val internalIncomingEdges = mutable.Map.empty[(Long, Long), PojoExMultilayerEdge]
  override val internalOutgoingEdges = mutable.Map.empty[(Long, Long), PojoExMultilayerEdge]

  override type Edge = PojoExMultilayerEdge

  implicit override val IDOrdering: Ordering[(Long, Long)] =
    Ordering.Tuple2(Ordering.Long, Ordering.Long)

  var computationValues: Map[String, Any] =
    Map.empty //Partial results kept between supersteps in calculation

  override def ID(): IDType = (vertex.ID(), timestamp)

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

  override def latestActivity(): HistoricEvent = vertex.latestActivity()

  override def earliestActivity(): HistoricEvent = vertex.earliestActivity()

  override def getPropertyAt[T](key: String, time: Long): Option[T] =
    vertex.getPropertyAt(key, time)

  override def history(): List[HistoricEvent] = vertex.history()

  override def active(after: Long, before: Long): Boolean = vertex.active(after, before)

  override def aliveAt(time: Long, window: Long): Boolean = vertex.aliveAt(time, window)

  override def remove(): Unit = {
    super.remove()
    vertex.explodedNeedsFiltering = true
  }
}
