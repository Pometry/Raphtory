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

class GraphExecutionState(
    partitionId: Int,
    superStep0: AtomicInteger,
    votingMachine: VotingMachine,
    messageSender: GenericVertexMessage[_] => Unit,
    makeGlobalFn: Long => Long,
    val start: Long,
    val end: Long
) extends ArrowEntityStateRepository {

  private val filteredInEdges  = mutable.HashMap.empty[Long, mutable.Set[Long]]
  private val filteredOutEdges = mutable.HashMap.empty[Long, mutable.Set[Long]]
  private val filteredVertices = mutable.Set.empty[Any]

  private val newFilteredVertices = new ArrayBuffer[Any]
  private val newFilteredEdges    = new ArrayBuffer[EdgeRemoval]

  private val messagesPerVertex = TrieMap.empty[Any, VertexMultiQueue]

  private val state = new ConcurrentHashMap[Long, mutable.Map[String, Any]]()

  def currentStepVertices: View[Long] = View.fromIteratorProvider(() => state.keys().asScala)

  override def getState[T](getLocalId: Long, key: String): Option[T] = {
    val innerMap = state.get(getLocalId)
    Option(innerMap).flatMap(_.get(key)).asInstanceOf[Option[T]]
  }

  override def getStateOrElse[T](getLocalId: Long, key: String, orElse: => T): T = {
    val innerMap = state.get(getLocalId)
    if (innerMap == null) orElse
    else
      innerMap.getOrElse(key, orElse).asInstanceOf[T]
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

  def hasMessage(vertexId: Long): Boolean =
    messagesPerVertex.get(vertexId).exists(_.getMessageQueue(superStep).nonEmpty)

  def removeOutEdge(sourceId: Long, vertexId: Long): Unit =
    newFilteredEdges += RemoveOut(sourceId, vertexId)

  def removeInEdge(sourceId: Long, vertexId: Long): Unit =
    newFilteredEdges += RemoveInto(vertexId, sourceId)

  def removeEdge(vertexId: Long, sourceId: Long, edgeId: Option[Long]): Unit = {
    removeInEdge(sourceId, vertexId)
    removeOutEdge(vertexId, sourceId)
  }

  def removeEdge(edgeId: Long): Unit =
    removeEdge(-1L, -1L, Option(edgeId))

  def receiveMessage(vertexId: Any, localSuperStep: Int, data: Any): Unit =
    messagesPerVertex
      .getOrElseUpdate(vertexId, new VertexMultiQueue)
      .receiveMessage(localSuperStep, data)

  def clearMessages(): Unit = messagesPerVertex.values.foreach(_.clearAll())

  def nextStep(superStepLocal: Int): Unit = {
    newFilteredEdges.foreach {
      case RemoveOut(sourceId, vertexId)  =>
        filteredOutEdges.updateWith(sourceId) {
          case None      => Some(mutable.Set(vertexId))
          case Some(set) => Some(set + vertexId)
        }
      case RemoveInto(vertexId, sourceId) =>
        filteredInEdges.updateWith(vertexId) {
          case None      => Some(mutable.Set(sourceId))
          case Some(set) => Some(set + sourceId)
        }

    }
    newFilteredVertices.foldLeft(filteredVertices)(_ += _)
    newFilteredVertices.clear()
    newFilteredEdges.clear()
  }

  def isAlive(vertexId: Long): Boolean =
    !filteredVertices(vertexId)

  override def removeVertex(vertexId: Long): Unit =
    newFilteredVertices.synchronized(newFilteredVertices.addOne(vertexId))

  override def sendMessage(msg: GenericVertexMessage[_]): Unit =
    messageSender(msg)

  override def superStep: Int = superStep0.get

  override def releaseQueue[T](vertexId: Long): Seq[T] =
    messagesPerVertex.get(vertexId) match {
      case None    => Vector.empty[T]
      case Some(q) =>
        val value = q.getMessageQueue(superStep) // copies the queue
        q.clearQueue(superStep)
        value.asInstanceOf[Vector[T]]
    }

  override def asGlobal(localVertexId: Long): Long = makeGlobalFn(localVertexId)

  override def vertexVoted(): Unit = votingMachine.vote()

  override def isEdgeAlive(sourceId: Long, vertexId: Long): Boolean =
    !filteredOutEdges.get(sourceId).exists(removed => removed(vertexId)) &&
      !filteredInEdges.get(vertexId).exists(removed => removed(sourceId))

  override def deletedOutEdges(ID: Long): Int = filteredOutEdges.getOrElse(ID, mutable.Set.empty[Long]).size

  override def deletedInEdges(ID: Long): Int = filteredInEdges.getOrElse(ID, mutable.Set.empty[Long]).size
}

object GraphExecutionState {

  def apply(
      partitionId: Int,
      superStep: AtomicInteger,
      messageSender: GenericVertexMessage[_] => Unit,
      makeGlobal: Long => Long,
      votingMachine: VotingMachine,
      start: Long,
      end: Long
  ): GraphExecutionState =
    new GraphExecutionState(partitionId, superStep, votingMachine, messageSender, makeGlobal, start, end)
}

sealed trait EdgeRemoval
case class RemoveOut(sourceId: Long, vertexId: Long)  extends EdgeRemoval
case class RemoveInto(vertexId: Long, sourceId: Long) extends EdgeRemoval
