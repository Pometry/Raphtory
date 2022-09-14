package com.raphtory.internals.storage.pojograph.entities.external.vertex

import com.raphtory.api.analysis.visitor.PropertyMergeStrategy.PropertyMerge
import com.raphtory.api.analysis.visitor.HistoricEvent
import com.raphtory.api.analysis.visitor.InterlayerEdge
import com.raphtory.api.analysis.visitor.PropertyValue
import com.raphtory.api.analysis.visitor.ReducedVertex
import com.raphtory.api.analysis.visitor.Vertex
import com.raphtory.internals.components.querymanager.GenericVertexMessage
import com.raphtory.internals.storage.pojograph.PojoGraphLens
import com.raphtory.internals.storage.pojograph.entities.external.PojoExEntity
import com.raphtory.internals.storage.pojograph.entities.external.edge.PojoExEdge
import com.raphtory.internals.storage.pojograph.entities.external.edge.PojoExMultilayerEdge
import com.raphtory.internals.storage.pojograph.entities.internal.PojoVertex

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.language.existentials
import scala.math.Ordering
import scala.reflect.ClassTag

private[raphtory] class PojoExVertex(
    private val v: PojoVertex,
    override val internalIncomingEdges: mutable.Map[Long, PojoExEdge],
    override val internalOutgoingEdges: mutable.Map[Long, PojoExEdge],
    override val lens: PojoGraphLens
) extends PojoExEntity(v, lens)
        with PojoConcreteVertexBase[Long]
        with ReducedVertex {

  override type Edge = PojoExEdge
  implicit override val IDOrdering: Ordering[Long] = Ordering.Long

  override def ID: Long = v.vertexId

  private var computationValues: Map[String, Any] =
    Map.empty //Partial results kept between supersteps in calculation

  // state related
  def setState(key: String, value: Any): Unit =
    computationValues += ((key, value))

  def getState[T](key: String, includeProperties: Boolean = false): T =
    if (computationValues.contains(key))
      computationValues(key).asInstanceOf[T]
    else if (includeProperties && v.properties.contains(key))
      getProperty[T](key).get
    else if (includeProperties)
      throw new Exception(
        s"$key not found within analytical state or properties within vertex {${v.vertexId}."
      )
    else
      throw new Exception(s"$key not found within analytical state within vertex {${v.vertexId}")

  def getStateOrElse[T](key: String, value: T, includeProperties: Boolean = false): T =
    if (computationValues contains key)
      computationValues(key).asInstanceOf[T]
    else if (includeProperties && v.properties.contains(key))
      getProperty[T](key).get
    else
      value

  def containsState(key: String, includeProperties: Boolean = false): Boolean =
    computationValues.contains(key) || (includeProperties && v.properties.contains(key))

  def getOrSetState[T](key: String, value: T, includeProperties: Boolean = false): T = {
    var output_value = value
    if (containsState(key))
      output_value = getState[T](key)
    else {
      if (includeProperties && v.properties.contains(key))
        output_value = getProperty[T](key).get
      setState(key, output_value)
    }
    output_value
  }

  def appendToState[T: ClassTag](key: String, value: T): Unit = //write function later
    computationValues.get(key) match {
      case Some(arr) =>
        setState(key, arr.asInstanceOf[ArrayBuffer[T]] :+ value)
      case None      =>
        setState(key, ArrayBuffer(value))
    }

  val exploded               = mutable.Map.empty[Long, PojoExplodedVertex]
  var explodedVertices       = Array.empty[PojoExplodedVertex]
  var explodedNeedsFiltering = false
  var interlayerEdges        = Seq.empty[InterlayerEdge]

  override def clearState(key: String): Unit =
    computationValues -= key

  def explode(
      interlayerEdgeBuilder: Option[Vertex => Seq[InterlayerEdge]]
  ): Unit = {
    if (exploded.isEmpty) {
      // exploding the view

      historyView.iterateUniqueTimes.foreach {
        case HistoricEvent(time, index, event) =>
          if (event)
            exploded += (time -> new PojoExplodedVertex(this, time))
      }
      explodedVertices = exploded.values.toArray[PojoExplodedVertex]

      explodeInEdges().foreach { edge =>
        exploded(edge.timestamp).internalIncomingEdges += (
                edge.src,
                edge.timestamp
        ) -> new PojoExMultilayerEdge(
                timestamp = edge.timestamp,
                ID = (edge.src, edge.timestamp),
                src = (edge.src, edge.timestamp),
                dst = (edge.dst, edge.timestamp),
                edge = edge,
                view = lens
        )
      }
      explodeOutEdges().foreach { edge =>
        exploded(edge.timestamp).internalOutgoingEdges += (
                edge.dst,
                edge.timestamp
        ) -> new PojoExMultilayerEdge(
                timestamp = edge.timestamp,
                ID = (edge.dst, edge.timestamp),
                src = (edge.src, edge.timestamp),
                dst = (edge.dst, edge.timestamp),
                edge = edge,
                view = lens
        )
      }
    }
    //    handle interlayer edges if provided
    interlayerEdgeBuilder.foreach { builder =>
      if (interlayerEdges.nonEmpty)
        interlayerEdges.foreach { edge =>
          exploded(edge.srcTime).internalOutgoingEdges -= ((ID, edge.dstTime))
          exploded(edge.dstTime).internalIncomingEdges -= ((ID, edge.srcTime))
        }
      interlayerEdges = builder(this)
      interlayerEdges.foreach { edge =>
        val srcID = (ID, edge.srcTime)
        val dstID = (ID, edge.dstTime)
        exploded(edge.srcTime).internalOutgoingEdges += dstID -> new PojoExMultilayerEdge(
                edge.dstTime,
                dstID,
                srcID,
                dstID,
                edge,
                lens
        )
        exploded(edge.dstTime).internalIncomingEdges += srcID -> new PojoExMultilayerEdge(
                edge.srcTime,
                srcID,
                srcID,
                dstID,
                edge,
                lens
        )
      }
    }
  }

  def reduce(
      defaultMergeStrategy: Option[PropertyMerge[_, _]],
      mergeStrategyMap: Option[Map[String, PropertyMerge[_, _]]],
      aggregate: Boolean
  ): Unit = {
    if (defaultMergeStrategy.nonEmpty || mergeStrategyMap.nonEmpty) {
      val states   = mutable.Map.empty[String, mutable.ArrayBuffer[PropertyValue[Any]]]
      val collect  = defaultMergeStrategy match {
        case Some(_) => (_: String) => true
        case None    =>
          val strategyMap = mergeStrategyMap.get
          (key: String) => strategyMap contains key
      }
      exploded.values.foreach { vertex =>
        vertex.computationValues.foreach {
          case (key, value) =>
            if (collect(key))
              states.getOrElseUpdate(
                      key,
                      mutable.ArrayBuffer.empty[PropertyValue[Any]]
              ) += (PropertyValue(vertex.timestamp, 0, value))
        }
      }
      val strategy = defaultMergeStrategy match {
        case Some(strategy) =>
          mergeStrategyMap match {
            case Some(strategyMap) => (key: String) => strategyMap.getOrElse(key, strategy)
            case None              => (_: String) => strategy
          }
        case None           =>
          val strategyMap = mergeStrategyMap.get
          (key: String) => strategyMap(key)
      }

      states.foreach {
        case (key, history) =>
          val strat = strategy(key)
          strat match {
            case v: PropertyMerge[Any, Any] => setState(key, v(history))
          }
      }
    }
    if (aggregate) {
      exploded.clear()
      interlayerEdges = Seq()
    }
  }

  def filterExplodedVertices(): Unit =
    if (explodedNeedsFiltering) {
      explodedVertices.filterNot(_.isFiltered)
      explodedNeedsFiltering = false
    }

  // implement receive in case of exploded view (normal receive is handled in PojoVertexBase)
  override def receiveMessage(msg: GenericVertexMessage[_]): Unit =
    msg.vertexId match {
      case _: Long               =>
        super.receiveMessage(msg)
      case (_: Long, time: Long) =>
        exploded(time)
          .receiveMessage(msg)
    }

  def individualEdge(
      edges: mutable.Map[Long, PojoExEdge],
      after: Long,
      before: Long,
      id: Long
  ): Option[PojoExEdge] =
    edges.get(id) match {
      case Some(edge) if edge.active(after, before) => Some(edge.viewBetween(after, before))
      case _                                        => None
    }

  def allEdge(
      edges: mutable.Map[Long, PojoExEdge],
      after: Long,
      before: Long
  ): List[PojoExEdge] =
    edges
      .collect { case (_, edge) if edge.active(after, before) => edge }
      .map(edge => edge.viewBetween(after, before))
      .toList

  def getOutEdges(after: Long, before: Long): List[PojoExEdge] = allEdge(internalOutgoingEdges, after, before)

  def getInEdges(after: Long, before: Long): List[PojoExEdge] = allEdge(internalIncomingEdges, after, before)

  override def getOutEdge(id: Long, after: Long, before: Long): Option[PojoExEdge] =
    individualEdge(internalOutgoingEdges, after, before, id)

  override def getInEdge(id: Long, after: Long, before: Long): Option[PojoExEdge] =
    individualEdge(internalIncomingEdges, after, before, id)

  override def viewUndirected: PojoReducedUndirectedVertexView = PojoReducedUndirectedVertexView(this)

  override def viewReversed: PojoReversedVertexView[Long] = PojoReducedReversedVertexView(this)
}
