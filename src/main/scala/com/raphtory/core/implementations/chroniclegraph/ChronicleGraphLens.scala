package com.raphtory.core.implementations.chroniclegraph

import com.raphtory.core.implementations.chroniclegraph.external.ChronicleExVertex
import com.raphtory.core.implementations.generic.messaging.VertexMessageHandler
import com.raphtory.core.model.algorithm.Row
import com.raphtory.core.model.graph.visitor.Vertex
import com.raphtory.core.model.graph.{GraphLens, GraphPartition, VertexMessage}

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable

final case class ChronicleGraphLens(jobId: String, timestamp: Long, window: Option[Long], var superStep: Int, private val storage: GraphPartition, private val messageHandler: VertexMessageHandler) extends GraphLens(jobId, timestamp, window) {
  private val voteCount = new AtomicInteger(0)
  private val vertexCount = new AtomicInteger(0)
  var t1 = System.currentTimeMillis()

  private var fullGraphSize = 0

  def getFullGraphSize = fullGraphSize

  def setFullGraphSize(size: Int) = fullGraphSize = size

  private lazy val vertexMap: mutable.Map[Long, Vertex] = {
    val result = window match {
      case None =>
        storage.getVertices(this, timestamp)
      case Some(w) => {
        storage.getVertices(this, timestamp, w)
      }
    }
    result
  }

  def getSize() = vertexMap.size

  private var dataTable: List[Row] = List()

  def executeSelect(f: Vertex => Row): Unit = {
    dataTable = vertexMap.collect {
      case (id, vertex) => f(vertex)
    }.toList
  }

  def filteredTable(f: Row => Boolean): Unit =
    dataTable = dataTable.filter(f)

  def explodeTable(f: Row => List[Row]): Unit =
    dataTable = dataTable.flatMap(f)

  def getDataTable(): List[Row] = {
    dataTable
  }


  def runGraphFunction(f: Vertex => Unit): Unit = {
    vertexMap.foreach { case (id, vertex) => f(vertex) }
    vertexCount.set(vertexMap.size)
  }

  def runMessagedGraphFunction(f: Vertex => Unit): Unit = {
    val size = vertexMap.collect { case (id, vertex) if vertex.hasMessage() => f(vertex) }.size
    vertexCount.set(size)
  }

  def getMessageHandler(): VertexMessageHandler = {
    messageHandler
  }

  def checkVotes(): Boolean = vertexCount.get() == voteCount.get()

  def sendMessage(msg: VertexMessage): Unit = messageHandler.sendMessage(msg)

  def vertexVoted(): Unit = voteCount.incrementAndGet()

  def nextStep(): Unit = {
    t1 = System.currentTimeMillis()
    voteCount.set(0)
    vertexCount.set(0)
    superStep += 1
  }

  def receiveMessage(msg: VertexMessage): Unit = {
    try {
      vertexMap(msg.vertexId).asInstanceOf[ChronicleExVertex].receiveMessage(msg)
    }
    catch {
      case e: Exception => e.printStackTrace()
    }

  }


}
