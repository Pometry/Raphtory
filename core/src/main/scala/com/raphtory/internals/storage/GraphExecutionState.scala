package com.raphtory.internals.storage

import com.raphtory.internals.components.querymanager.GenericVertexMessage
import com.raphtory.internals.storage.arrow.ArrowEntityStateRepository
import com.raphtory.internals.storage.pojograph.messaging.VertexMultiQueue

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.concurrent.TrieMap
import scala.collection.View
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.EnumerationHasAsScala

class GraphExecutionState(superStep0: AtomicInteger, messageSender: GenericVertexMessage[_] => Unit)
        extends ArrowEntityStateRepository {

  private val filteredEdges    = mutable.Set.empty[Any]
  private val filteredVertices = mutable.Set.empty[Any]

  private val newFilteredVertices = new ArrayBuffer[Any]
  private val newFilteredEdges    = new VertexMultiQueue

  private val messagesPerVertex = TrieMap.empty[Any, VertexMultiQueue]

  private val state = new ConcurrentHashMap[Long, mutable.Map[String, Any]]()

  def currentStepVertices: View[Long] = View.fromIteratorProvider(() => state.keys().asScala)

  override def getState[T](getLocalId: Long, key: String): T = {
    val innerMap = state.get(getLocalId)
    // this can blow up obviously
    innerMap(key).asInstanceOf[T]
  }

  override def getStateOrElse[T](getLocalId: Long, key: String, orElse: => T): T = {
    val innerMap = state.get(getLocalId)
    if (innerMap == null) orElse
    else innerMap(key).asInstanceOf[T]
  }

  override def setState(vertexId: Long, key: String, value: Any): Unit =
    state.compute(
            vertexId,
            (_, oldv) =>
              if (oldv == null) mutable.Map(key -> value)
              else {
                oldv.update(key, value)
                oldv
              }
    )

  def hasMessage(getLocalId: Long): Boolean =
    messagesPerVertex(getLocalId).getMessageQueue(superStep).nonEmpty

  def removeOutEdge(vertexId: Long, sourceId: Long, edgeId: Option[Long]): Unit = ???

  def removeInEdge(sourceId: Long, vertexId: Long, edgeId: Option[Long]): Unit = ???

  def removeEdge(vertexId: Long, sourceId: Long, edgeId: Option[Long]): Unit =
    newFilteredEdges.receiveMessage(superStep, edgeId.get)

  def removeEdge(edgeId: Long): Unit =
    removeEdge(-1L, -1L, Option(edgeId))

  def receiveMessage(vertexId: Any, superstep: Int, data: Any): Unit =
    messagesPerVertex
      .getOrElseUpdate(vertexId, new VertexMultiQueue)
      .receiveMessage(superstep, data)

  def clearMessages(): Unit = messagesPerVertex.values.foreach(_.clearAll())

  def nextStep(superStepLocal: Int): Unit = {
    newFilteredEdges.getMessageQueue(superStepLocal + 1).foreach { v =>
      filteredEdges.add(v)
    }
    newFilteredEdges.clearQueue(superStepLocal + 1)

    newFilteredVertices.foldLeft(filteredVertices)(_ += _)
    newFilteredVertices.clear()
  }

  def isAlive(vertexId: Long, partitionId: Int): Boolean =
    !filteredVertices(vertexId)

  override def removeVertex(vertexId: Long): Unit =
    newFilteredVertices.synchronized(newFilteredVertices.addOne(vertexId))

  override def sendMessage(msg: GenericVertexMessage[_]): Unit =
    messageSender(msg)

  override def superStep: Int = superStep0.get

  override def queue[T](vertexId: Long): View[T] =
    messagesPerVertex.get(vertexId) match {
      case None    => View.empty[T]
      case Some(q) =>
        val value = q.getMessageQueue(superStep).toVector
        println(s"$vertexId $value")
        value.view.map(_.asInstanceOf[T])
    }
}

object GraphExecutionState {

  def apply(superStep: AtomicInteger, messageSender: GenericVertexMessage[_] => Unit): GraphExecutionState =
    new GraphExecutionState(superStep, messageSender)
}
