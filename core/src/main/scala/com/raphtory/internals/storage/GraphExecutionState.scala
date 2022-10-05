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
    makeGlobalFn: Long => Long
) extends ArrowEntityStateRepository {

  private val filteredEdges    = mutable.HashMap.empty[Long, mutable.Set[Long]]
  private val filteredVertices = mutable.Set.empty[Any]

  private val newFilteredVertices = new ArrayBuffer[Any]
  private val newFilteredEdges    = new ArrayBuffer[(Any, Any)]

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

  def hasMessage(vertexId: Long): Boolean = {
    val bool = messagesPerVertex.get(vertexId).exists(_.getMessageQueue(superStep).nonEmpty)
    bool
  }

  def removeOutEdge(sourceId: Long, vertexId: Long): Unit =
    newFilteredEdges += (sourceId -> vertexId)

  def removeInEdge(sourceId: Long, vertexId: Long): Unit =
    newFilteredEdges += (vertexId -> sourceId)

  def removeEdge(vertexId: Long, sourceId: Long, edgeId: Option[Long]): Unit = {
    removeInEdge(sourceId, vertexId)
    removeOutEdge(vertexId, sourceId)
  }

  def removeEdge(edgeId: Long): Unit =
    removeEdge(-1L, -1L, Option(edgeId))

  def receiveMessage(vertexId: Any, localSuperStep: Int, data: Any): Unit = {
    messagesPerVertex
      .getOrElseUpdate(vertexId, new VertexMultiQueue)
      .receiveMessage(localSuperStep, data)
  }

  def clearMessages(): Unit = messagesPerVertex.values.foreach(_.clearAll())

  def nextStep(superStepLocal: Int): Unit = {
    newFilteredEdges.foreach {
      case (a: Long, b: Long) =>
        filteredEdges.updateWith(a) {
          case None      => Some(mutable.Set(b))
          case Some(set) => Some(set + b)
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

  override def releaseQueue[T](vertexId: Long): Seq[T] = {
    val value1 = messagesPerVertex.get(vertexId) match {
      case None    => Vector.empty[T]
      case Some(q) =>
        val value = q.getMessageQueue(superStep) // copies the queue
        q.clearQueue(superStep)
        value.asInstanceOf[Vector[T]]
    }
    value1
  }

  override def asGlobal(localVertexId: Long): Long = makeGlobalFn(localVertexId)

  override def vertexVoted(): Unit = votingMachine.vote()

  override def isEdgeAlive(sourceId: Long, vertexId: Long): Boolean =
    !filteredEdges.get(sourceId).exists(removed => removed(vertexId))
}

object GraphExecutionState {

  def apply(
      partitionId: Int,
      superStep: AtomicInteger,
      messageSender: GenericVertexMessage[_] => Unit,
      makeGlobal: Long => Long,
      votingMachine: VotingMachine
  ): GraphExecutionState =
    new GraphExecutionState(partitionId, superStep, votingMachine, messageSender, makeGlobal)
}
