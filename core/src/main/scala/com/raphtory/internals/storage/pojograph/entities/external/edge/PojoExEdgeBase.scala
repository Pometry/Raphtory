package com.raphtory.internals.storage.pojograph.entities.external.edge

import com.raphtory.api.analysis.visitor.ConcreteEdge
import com.raphtory.api.analysis.visitor.ConcreteExplodedEdge
import com.raphtory.api.analysis.visitor.HistoricEvent
import com.raphtory.api.analysis.visitor.IndexedValue
import com.raphtory.api.analysis.visitor.PropertyValue
import com.raphtory.api.analysis.visitor.ReducedEdge
import com.raphtory.api.analysis.visitor.TimePoint
import com.raphtory.internals.components.querymanager.FilteredInEdgeMessage
import com.raphtory.internals.components.querymanager.FilteredOutEdgeMessage
import com.raphtory.internals.components.querymanager.VertexMessage
import com.raphtory.internals.storage.pojograph.PojoGraphLens
import com.raphtory.utils.OrderingFunctions._
import com.raphtory.internals.communication.SchemaProviderInstances._

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

private[pojograph] trait PojoExEdgeBase[T] extends ConcreteEdge[T] {
  def view: PojoGraphLens

  def start: IndexedValue

  def end: IndexedValue

  def send(data: Any): Unit =
    view.sendMessage(VertexMessage(view.superStep + 1, ID, data))

  override def remove(): Unit = {
    view.needsFiltering = true
    view.sendMessage(FilteredOutEdgeMessage(view.superStep + 1, src, dst))
    view.sendMessage(FilteredInEdgeMessage(view.superStep + 1, dst, src))
  }

  private var computationValues: Map[String, Any] =
    Map.empty //Partial results kept between supersteps in calculation

  override def setState(key: String, value: Any): Unit =
    computationValues += ((key, value))

  override def getState[T](key: String, includeProperties: Boolean): T =
    if (computationValues.contains(key))
      computationValues(key).asInstanceOf[T]
    else if (includeProperties && getPropertySet().contains(key))
      getProperty[T](key).get
    else if (includeProperties)
      throw new Exception(
        s"$key not found within analytical state or properties within edge {${(src,dst)}."
      )
    else
      throw new Exception(s"$key not found within analytical state within edge {${(src,dst)}")

  override def getStateOrElse[T](key: String, value: T, includeProperties: Boolean): T =
    if (computationValues contains key)
      computationValues(key).asInstanceOf[T]
    else if (includeProperties && getPropertySet().contains(key))
      getProperty[T](key).get
    else
      value

  override def containsState(key: String, includeProperties: Boolean): Boolean =
    computationValues.contains(key) || (includeProperties && getPropertySet().contains(key))

  override def getOrSetState[T](key: String, value: T, includeProperties: Boolean): T = {
    var output_value = value
    if (containsState(key))
      output_value = getState[T](key)
    else {
      if (includeProperties && getPropertySet().contains(key))
        output_value = getProperty[T](key).get
      setState(key, output_value)
    }
    output_value
  }

  override def appendToState[T: ClassTag](key: String, value: T): Unit = //write function later
    computationValues.get(key) match {
      case Some(arr) =>
        setState(key, arr.asInstanceOf[ArrayBuffer[T]] :+ value)
      case None      =>
        setState(key, ArrayBuffer(value))
    }
}

private[pojograph] trait PojoExplodedEdgeBase[T] extends PojoExEdgeBase[T] with ConcreteExplodedEdge[T] {
  def timePoint: IndexedValue
  override def timestamp: Long = timePoint.time
}

private[pojograph] trait PojoExDirectedEdgeBase[
    Edir <: PojoExDirectedEdgeBase[Edir, T],
    T
] extends PojoExEdgeBase[T] { this: Edir =>
  type Eundir >: Edir <: PojoExEdgeBase[T]
  def reversed: Edir

  def combineUndirected(other: Edir, asInEdge: Boolean): Eundir
}

private[pojograph] trait PojoExReducedEdgeBase extends PojoExEdgeBase[Long] with ReducedEdge {

  def viewBetween(after: Long, before: Long): PojoExReducedEdgeBase
  def viewBetween(after: IndexedValue, before: IndexedValue): PojoExReducedEdgeBase
}

private[pojograph] trait PojoExReducedEdgeImplementation[E <: PojoExReducedEdgeImplementation[E]]
        extends PojoExReducedEdgeBase {
  def viewBetween(after: Long, before: Long): E = viewBetween(TimePoint.first(after), TimePoint.last(before))

  override def viewBetween(after: IndexedValue, before: IndexedValue): E
}

abstract private[pojograph] class PojoExInOutEdgeBase[Eundir <: PojoExInOutEdgeBase[
        Eundir,
        Edir,
        T
], Edir <: PojoExDirectedEdgeBase[
        Edir,
        T
], T](in: Edir, out: Edir, asInEdge: Boolean)
        extends PojoExEdgeBase[T] { this: Eundir =>
  override def ID: IDType  = in.ID
  override def src: IDType = if (asInEdge) in.src else out.src
  override def dst: IDType = if (asInEdge) in.dst else out.dst

  override val view: PojoGraphLens = in.view

  val edges = List(in, out)

  override def remove(): Unit =
    edges.foreach(_.remove())

  override def send(data: Any): Unit = view.sendMessage(VertexMessage(view.superStep + 1, ID, data))

  override def Type: String = edges.map(_.Type).distinct.mkString("_")

  override def firstActivityAfter(time: Long, strict: Boolean): Option[HistoricEvent] =
    edges
      .flatMap(_.firstActivityAfter(time, strict)) // list of HistoricEvent that are defined
      .minOption                                   // get most recent one if there are any

  override def lastActivityBefore(time: Long, strict: Boolean): Option[HistoricEvent] =
    edges
      .flatMap(_.lastActivityBefore(time, strict)) // list of HistoricEvent that are defined
      .maxOption                                   // get latest one if there are any

  override def latestActivity(): HistoricEvent =
    edges
      .map(_.latestActivity())
      .max // get most recent one

  override def earliestActivity(): HistoricEvent =
    edges
      .map(_.earliestActivity())
      .minBy(_.time) // get most recent one

  override def getPropertySet(): List[String] = edges.flatMap(_.getPropertySet()).distinct

  override def getPropertyHistory[T](
      key: String,
      after: Long,
      before: Long
  ): Option[List[PropertyValue[T]]] = {
    val histories = edges.map(_.getPropertyHistory[T](key, after, before))
    if (histories.forall(_.isEmpty))
      None
    else
      Some(histories.flatten.flatten.sorted)
  }

  override def history(): List[HistoricEvent] =
    edges.flatMap(_.history()).sorted.distinct

  override def active(after: Long, before: Long): Boolean =
    edges.exists(_.active(after, before))

  override def aliveAt(time: Long, window: Long): Boolean =
    edges.exists(_.aliveAt(time, window))

  override def start: IndexedValue = min(in.start, out.start)

  override def end: IndexedValue = max(in.end, out.end)
}
