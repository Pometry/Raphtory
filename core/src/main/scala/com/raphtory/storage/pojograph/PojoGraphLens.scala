package com.raphtory.storage.pojograph

import com.raphtory.algorithms.api.GraphState
import com.raphtory.algorithms.api.Row
import com.raphtory.components.querymanager.FilteredEdgeMessage
import com.raphtory.components.querymanager.GenericVertexMessage
import com.raphtory.components.querymanager.VertexMessage
import com.raphtory.graph.visitor.Vertex
import com.raphtory.graph.GraphLens
import com.raphtory.graph.GraphPartition
import com.raphtory.graph.LensInterface
import com.raphtory.storage.pojograph.entities.external.PojoExVertex
import com.raphtory.storage.pojograph.messaging.VertexMessageHandler
import com.typesafe.config.Config
import org.apache.pulsar.client.api.Producer

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable

/** @DoNotDocument */
final case class PojoGraphLens(
    jobId: String,
    start: Long,
    end: Long,
    var superStep: Int,
    private val storage: GraphPartition,
    private val conf: Config,
    private val neighbours: Map[Int, Producer[Array[Byte]]],
    private val sentMessages: AtomicInteger,
    private val receivedMessages: AtomicInteger
) extends GraphLens(jobId, start, end)
        with LensInterface {
  private val voteCount     = new AtomicInteger(0)
  private val vertexCount   = new AtomicInteger(0)
  var t1                    = System.currentTimeMillis()
  private var fullGraphSize = 0

  val messageHandler: VertexMessageHandler =
    VertexMessageHandler(conf, neighbours, this, sentMessages, receivedMessages)

  val partitionID = storage.getPartitionID

  def getFullGraphSize: Int = {
    logger.trace(s"Current Graph size at '$fullGraphSize'.")
    fullGraphSize
  }

  def setFullGraphSize(size: Int): Unit = {
    fullGraphSize = size
    logger.trace(s"Set Graph Size to '$fullGraphSize'.")
  }

  private lazy val vertexMap: mutable.Map[Long, PojoExVertex] =
    storage.getVertices(this, start, end)

  private var vertices: Array[(Long, PojoExVertex)] = vertexMap.toArray

  def getSize(): Int = vertices.size

  private var dataTable: List[Row] = List()

  def executeSelect(f: Vertex => Row): Unit =
    dataTable = vertices.collect {
      case (id, vertex) => f(vertex)
    }.toList

  def executeSelect(
      f: (Vertex, GraphState) => Row,
      graphState: GraphState
  ): Unit =
    dataTable = vertices.collect {
      case (id, vertex) => f(vertex, graphState)
    }.toList

  def executeSelect(
      f: GraphState => Row,
      graphState: GraphState
  ): Unit =
    dataTable = List(f(graphState))

  def explodeSelect(f: Vertex => List[Row]): Unit =
    dataTable = vertices
      .collect {
        case (_, vertex) => f(vertex)
      }
      .flatten
      .toList

  def filteredTable(f: Row => Boolean): Unit =
    dataTable = dataTable.filter(f)

  def explodeTable(f: Row => List[Row]): Unit =
    dataTable = dataTable.flatMap(f)

  def getDataTable(): List[Row] =
    dataTable

  def runGraphFunction(f: Vertex => Unit): Unit = {
    vertices.foreach { case (id, vertex) => f(vertex) }
    vertexCount.set(vertices.size)
  }

  override def runGraphFunction(
      f: (Vertex, GraphState) => Unit,
      graphState: GraphState
  ): Unit = {
    vertices.foreach { case (id, vertex) => f(vertex, graphState) }
    vertexCount.set(vertices.size)
  }

  override def runMessagedGraphFunction(f: Vertex => Unit): Unit = {
    val size = vertices.collect { case (id, vertex) if vertex.hasMessage() => f(vertex) }.size
    vertexCount.set(size)
  }

  override def runMessagedGraphFunction(
      f: (Vertex, GraphState) => Unit,
      graphState: GraphState
  ): Unit = {
    val size = vertices.collect {
      case (id, vertex) if vertex.hasMessage() => f(vertex, graphState)
    }.size
    vertexCount.set(size)
  }

  def getMessageHandler(): VertexMessageHandler =
    messageHandler

  def checkVotes(): Boolean = vertexCount.get() == voteCount.get()

  def sendMessage(msg: GenericVertexMessage): Unit = messageHandler.sendMessage(msg)

  def vertexVoted(): Unit = voteCount.incrementAndGet()

  def nextStep(): Unit = {
    t1 = System.currentTimeMillis()
    voteCount.set(0)
    vertexCount.set(0)
    superStep += 1
    vertexMap.foreach(_._2.executeEdgeDelete())
    deleteVertices()
  }

//keep the vertices that are not being deleted
  private def deleteVertices(): Unit =
    vertices = vertices.filter(!_._2.isFiltered)

  def receiveMessage(msg: GenericVertexMessage): Unit =
    try vertexMap(msg.vertexId).receiveMessage(msg)
    catch {
      case e: java.util.NoSuchElementException =>
        logger.warn(
                s"Job '$jobId': Vertex '${msg.vertexId}' is yet to be created in " +
                  s"Partition '${storage.getPartitionID}'. Please consider rerunning computation on this perspective."
        )
    }

  override def getStart(): Long = start

  override def getEnd(): Long = end

  def clearMessages(): Unit =
    vertexMap.foreach {
      case (key, vertex) => vertex.clearMessageQueue()
    }

}
