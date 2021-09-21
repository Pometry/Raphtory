package com.raphtory.core.implementations.objectgraph

import com.raphtory.core.implementations.objectgraph.entities.external.ObjectVertex
import com.raphtory.core.implementations.objectgraph.messaging.VertexMessageHandler
import com.raphtory.core.model.graph.visitor.Vertex
import com.raphtory.core.model.graph.{GraphPartition, GraphPerspective, VertexMessage}

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.concurrent.TrieMap

final case class ObjectGraphLens(
                                  jobId: String,
                                  timestamp: Long,
                                  window: Option[Long],
                                  var superStep: Int,
                                  private val storage: GraphPartition,
                                  private val messageHandler: VertexMessageHandler
                                ) extends GraphPerspective(jobId, timestamp, window) {
  private var voteCount = new AtomicInteger(0)
  private var vertexCount = new AtomicInteger(0)
  var t1 = System.currentTimeMillis()

  private lazy val vertexMap: TrieMap[Long, Vertex] = {
    val result = window match {
      case None =>
        storage.getVertices(this, timestamp)
      case Some(w) => {
        storage.getVertices(this, timestamp, w)
      }
    }
    result
  }

  def getVertices(): List[Vertex] = {
    vertexCount.set(vertexMap.size)
    vertexMap.values.toList
  }

  def getMessagedVertices(): List[Vertex] = {
    val result = vertexMap.collect {
      case (id, vertex) if vertex.hasMessage() => vertex
    }
    vertexCount.set(result.size)
    result.toList
  }

  def checkVotes(): Boolean = {
    //    println(s"$superStep - $workerId : \t ${vertexCount.get() - voteCount.get()} / ${vertexCount.get()}")
    vertexCount.get() == voteCount.get()
  }

  def sendMessage(msg: VertexMessage): Unit = messageHandler.sendMessage(msg)


  def vertexVoted(): Unit = voteCount.incrementAndGet()

  def nextStep(): Unit = {
    //println(s"Superstep: $superStep \t WiD: $workerId \t Exec: ${System.currentTimeMillis() - t1}")
    t1 = System.currentTimeMillis()
    voteCount.set(0)
    vertexCount.set(0)
    superStep += 1
  }

  def receiveMessage(msg: VertexMessage): Unit = {
    try {
      vertexMap(msg.vertexId).asInstanceOf[ObjectVertex].receiveMessage(msg)
    }
    catch {
      case e: Exception => e.printStackTrace()
    }

  }


}
