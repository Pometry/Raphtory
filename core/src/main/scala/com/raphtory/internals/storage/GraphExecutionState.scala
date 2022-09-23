package com.raphtory.internals.storage

import com.raphtory.internals.storage.arrow.ArrowEntityStateRepository
import com.raphtory.internals.storage.pojograph.messaging.VertexMultiQueue

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class GraphExecutionState(superStep: AtomicInteger) extends ArrowEntityStateRepository {

  private val filteredEdges                = mutable.Set.empty[Any]
  private val filteredVerticesPerPartition = Array.fill(4)(mutable.Set.empty[Any])

  private val newFilteredVerticesPerPartition = Array.fill(4)(new ArrayBuffer[Any])
  private val newFilteredEdges                = new VertexMultiQueue

  private val messagesPerVertex = TrieMap.empty[Any, VertexMultiQueue]

  private val state = new ConcurrentHashMap[Long, mutable.Map[String, Any]]()

  override def getState[T](getLocalId: Long, key: String): T = {
    val innerMap = state.get(key)
    innerMap(key).asInstanceOf[T] // this can blow up obviously
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
    messagesPerVertex(getLocalId).getMessageQueue(superStep.get).nonEmpty

  def removeOutEdge(vertexId: Long, sourceId: Long, edgeId: Option[Long]): Unit = ???

  def removeInEdge(sourceId: Long, vertexId: Long, edgeId: Option[Long]): Unit = ???

  def removeEdge(vertexId: Long, sourceId: Long, edgeId: Option[Long]): Unit =
    newFilteredEdges.receiveMessage(superStep.get, edgeId.get)

  def removeEdge(edgeId: Long): Unit =
    removeEdge(-1L, -1L, Option(edgeId))

  def receiveMessage(vertexId: Any, superstep: Int, data: Any): Unit =
    messagesPerVertex
      .getOrElseUpdate(vertexId, new VertexMultiQueue)
      .receiveMessage(superstep, data)

  def clearMessages(): Unit =
    messagesPerVertex.values.foreach(_.clearAll())

  def nextStep(superStep: Int): Unit = {
    newFilteredEdges.getMessageQueue(superStep + 1).foreach { v =>
      filteredEdges.add(v)
    }
    newFilteredEdges.clearQueue(superStep + 1)
    filteredVerticesPerPartition.indices.foreach { i =>
      val filtered = filteredVerticesPerPartition(i)
      newFilteredVerticesPerPartition(i).foreach(v => filtered.add(v))
      newFilteredVerticesPerPartition(i).clear()
    }
  }

  def isAlive(vertexId: Long, partitionId: Int): Boolean =
    !filteredVerticesPerPartition(partitionId).contains(vertexId)
}

object GraphExecutionState {
  def apply(superStep: AtomicInteger): GraphExecutionState = new GraphExecutionState(superStep)
}
