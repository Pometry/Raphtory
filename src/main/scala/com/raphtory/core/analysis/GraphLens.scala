package com.raphtory.core.analysis

import com.raphtory.core.analysis.entity.Vertex
import com.raphtory.core.model.EntityStorage
import com.raphtory.core.model.communication.VertexMessage
import kamon.Kamon

import scala.collection.parallel.ParIterable
import scala.collection.parallel.mutable.ParTrieMap

final case class GraphLens(
    jobId: String,
    timestamp: Long,
    window: Option[Long],
    var superStep: Int,
    workerId: Int,
    storage: EntityStorage
) {
  private var messages: List[VertexMessage] = List.empty
  protected var voteCount                   = 0

  private var messageFilter: Boolean = false

  private val viewTimer = Kamon
    .gauge("Raphtory_View_Build_Time")
    .withTag("Partition", storage.managerID)
    .withTag("Worker", workerId)
    .withTag("JobID", jobId)
    .withTag("timestamp", timestamp)

  private lazy val vertexMap: ParTrieMap[Long, Vertex] = {
    val startTime = System.currentTimeMillis()
    val result = window match {
      case None =>
        storage.vertices.collect {
          case (k, v) if v.aliveAt(timestamp) =>
            k -> Vertex(v.viewAt(timestamp), this)
        }
      case Some(w) => {
        println(storage.vertices.size)
        storage.vertices.collect {
          case (k, v) if v.aliveAtWithWindow(timestamp, w) =>
            k -> Vertex(v.viewAtWithWindow(timestamp, w), this)
        }
    }
    }
    viewTimer.update(System.currentTimeMillis() - startTime)
    result
  }

  def getMessagedVertices(): ParIterable[Vertex] = {
    val startTime = System.currentTimeMillis()
    val result    = getVertices.filter(_.hasMessage())
    messageFilter = true
    viewTimer.update(System.currentTimeMillis() - startTime)
    result
  }

  def getVertices(): ParIterable[Vertex] = vertexMap.values

  def checkVotes(): Boolean =
    if (messageFilter)
      getMessagedVertices().size == voteCount
    else
      vertexMap.size == voteCount

  //TODO hide away
  def sendMessage(msg: VertexMessage): Unit = messages = messages :+ msg
  def getAndCleanMessages(): Seq[VertexMessage] = {
    val temp = messages
    messages = List.empty
    temp
  }
  def vertexVoted(): Unit = voteCount += 1
  def nextStep(): Unit    = superStep += 1
  def receiveMessage(msg: VertexMessage): Unit = vertexMap(msg.vertexId).receiveMessage(msg)
}
