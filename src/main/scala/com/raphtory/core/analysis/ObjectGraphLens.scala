package com.raphtory.core.analysis

import java.util.concurrent.atomic.AtomicInteger
import akka.cluster.pubsub.DistributedPubSubMediator
import com.raphtory.core.actors.partitionmanager.QueryExecutor
import com.raphtory.core.model.communication.{VertexMessage, VertexMessageHandler}
import com.raphtory.core.model.graph.{GraphPartition, GraphPerspective}
import com.raphtory.core.model.graph.visitor.Vertex
import com.raphtory.core.model.implementations.objectgraph.entities.external.ObjectVertex
import kamon.Kamon

import scala.collection.concurrent.TrieMap
import scala.collection.parallel.ParIterable
import scala.collection.parallel.mutable.ParTrieMap

final case class ObjectGraphLens(
                            jobId: String,
                            timestamp: Long,
                            window: Option[Long],
                            var superStep: Int,
                            private val storage: GraphPartition,
                            private val messageHandler:VertexMessageHandler
) extends GraphPerspective(jobId, timestamp, window){
  private var voteCount            = new AtomicInteger(0)
  private var vertexCount          = new AtomicInteger(0)
  var t1 = System.currentTimeMillis()

  private val viewTimer = Kamon
    .gauge("Raphtory_View_Build_Time")
    .withTag("Partition", storage.getPartitionID)
    .withTag("JobID", jobId)
    .withTag("timestamp", timestamp)

  private lazy val vertexMap: TrieMap[Long,Vertex] = {
    val startTime = System.currentTimeMillis()

    val result = window match {
      case None =>
        storage.getVertices(this,timestamp)
      case Some(w) => {
        storage.getVertices(this,timestamp,w)
      }
    }
    viewTimer.update(System.currentTimeMillis() - startTime)
    result
  }

  def getVertices(): List[Vertex] = {
    vertexCount.set(vertexMap.size)
    vertexMap.values.toList
  }

  def getMessagedVertices(): List[Vertex] = {
    val startTime = System.currentTimeMillis()
    val result    = vertexMap.collect {
      case (id,vertex) if vertex.hasMessage() => vertex
    }
    viewTimer.update(System.currentTimeMillis() - startTime)
    vertexCount.set(result.size)
    result.toList
  }

  def checkVotes(): Boolean = {
//    println(s"$superStep - $workerId : \t ${vertexCount.get() - voteCount.get()} / ${vertexCount.get()}")
    vertexCount.get() == voteCount.get()
  }

  def sendMessage(msg: VertexMessage): Unit = messageHandler.sendMessage(msg)


  def vertexVoted(): Unit = voteCount.incrementAndGet()

  def nextStep(): Unit    = {
    //println(s"Superstep: $superStep \t WiD: $workerId \t Exec: ${System.currentTimeMillis() - t1}")
    t1 = System.currentTimeMillis()
    voteCount.set(0)
    vertexCount.set(0)
    superStep += 1
  }

  def receiveMessage(msg: VertexMessage): Unit = {
    try{vertexMap(msg.vertexId).asInstanceOf[ObjectVertex].receiveMessage(msg)}
    catch {
      case e:Exception => e.printStackTrace()
    }

  }


}
