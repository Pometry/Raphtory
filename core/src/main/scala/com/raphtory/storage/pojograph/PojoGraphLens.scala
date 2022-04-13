package com.raphtory.storage.pojograph

import com.raphtory.algorithms.api.GraphState
import com.raphtory.algorithms.api.Row
import com.raphtory.components.querymanager.GenericVertexMessage
import com.raphtory.graph.visitor.InterlayerEdge
import com.raphtory.graph.visitor.Vertex
import com.raphtory.graph.GraphLens
import com.raphtory.graph.GraphPartition
import com.raphtory.graph.LensInterface
import com.raphtory.graph.visitor.PropertyMergeStrategy.PropertyMerge
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
  private val voteCount         = new AtomicInteger(0)
  private val vertexCount       = new AtomicInteger(0)
  var t1                        = System.currentTimeMillis()
  private var fullGraphSize     = 0
  private var exploded: Boolean = false
  var needsFiltering            = false

  val messageHandler: VertexMessageHandler =
    VertexMessageHandler(conf, neighbours, this, sentMessages, receivedMessages)

  val partitionID = storage.getPartitionID

  private lazy val vertexMap: mutable.Map[Long, PojoExVertex] =
    storage.getVertices(this, start, end)

  private var vertices: Array[PojoExVertex] = vertexMap.values.toArray

  private def vertexIterator =
    if (exploded)
      vertices.iterator.flatMap(_.explodedVertices)
    else
      vertices.iterator

  def getFullGraphSize: Int = {
    logger.trace(s"Current Graph size at '$fullGraphSize'.")
    fullGraphSize
  }

  def setFullGraphSize(size: Int): Unit = {
    fullGraphSize = size
    logger.trace(s"Set Graph Size to '$fullGraphSize'.")
  }

  def getSize(): Int = vertices.size

  private var dataTable: List[Row] = List()

  def executeSelect(f: Vertex => Row): Unit =
    dataTable = vertexIterator.map(f).toList

  def executeSelect(
      f: (Vertex, GraphState) => Row,
      graphState: GraphState
  ): Unit =
    dataTable = vertexIterator.map(f(_, graphState)).toList

  def executeSelect(
      f: GraphState => Row,
      graphState: GraphState
  ): Unit =
    dataTable = List(f(graphState))

  def explodeSelect(f: Vertex => List[Row]): Unit =
    dataTable = vertexIterator.flatMap(f).toList

  def filteredTable(f: Row => Boolean): Unit =
    dataTable = dataTable.filter(f)

  def explodeTable(f: Row => List[Row]): Unit =
    dataTable = dataTable.flatMap(f)

  def getDataTable(): List[Row] =
    dataTable

  override def explodeView(
      interlayerEdgeBuilder: Option[Vertex => Seq[InterlayerEdge]]
  ): Unit =
    if (exploded) {
      if (interlayerEdgeBuilder.nonEmpty) vertexMap.values.foreach(_.explode(interlayerEdgeBuilder))
    }
    else {
      exploded = true
      vertexMap.values.foreach(_.explode(interlayerEdgeBuilder))
    }

  override def reduceView(
      defaultMergeStrategy: Option[PropertyMerge[_, _]],
      mergeStrategyMap: Option[Map[String, PropertyMerge[_, _]]],
      aggregate: Boolean
  ): Unit = {
    exploded = false
    vertexMap.values.foreach(_.reduce(defaultMergeStrategy, mergeStrategyMap, aggregate))
  }

  def runGraphFunction(f: Vertex => Unit): Unit = {
    var count: Int = 0
    vertexIterator.foreach { vertex =>
      count += 1
      f(vertex)
    }
    vertexCount.set(count)
  }

  override def runGraphFunction(
      f: (Vertex, GraphState) => Unit,
      graphState: GraphState
  ): Unit = {
    var count: Int = 0
    vertexIterator.foreach { vertex =>
      count += 1
      f(vertex, graphState)
    }
    vertexCount.set(count)
  }

  override def runMessagedGraphFunction(f: Vertex => Unit): Unit = {
    var count: Int = 0
    vertexIterator.foreach { vertex =>
      if (vertex.hasMessage()) {
        count += 1
        f(vertex)
      }
    }
    vertexCount.set(count)
  }

  override def runMessagedGraphFunction(
      f: (Vertex, GraphState) => Unit,
      graphState: GraphState
  ): Unit = {
    var count: Int = 0
    vertexIterator.foreach {
      case vertex if vertex.hasMessage() =>
        count += 1
        f(vertex, graphState)
    }
    vertexCount.set(count)
  }

  def getMessageHandler(): VertexMessageHandler =
    messageHandler

  def checkVotes(): Boolean = vertexCount.get() == voteCount.get()

  def sendMessage(msg: GenericVertexMessage[_]): Unit = messageHandler.sendMessage(msg)

  def vertexVoted(): Unit = voteCount.incrementAndGet()

  def nextStep(): Unit = {
    t1 = System.currentTimeMillis()
    voteCount.set(0)
    vertexCount.set(0)
    superStep += 1
    if (needsFiltering) {
      vertexIterator.foreach(_.executeEdgeDelete())
      deleteVertices()
      needsFiltering = false
    }
  }

//keep the vertices that are not being deleted
  private def deleteVertices(): Unit =
    if (exploded)
      vertices.foreach(_.filterExplodedVertices())
    else
      vertices = vertices.filterNot(_.isFiltered)

  def receiveMessage(msg: GenericVertexMessage[_]): Unit = {
    val vertexId = msg.vertexId match {
      case v: Long       => v
      case (id: Long, _) => id
    }
    try vertexMap(vertexId).receiveMessage(msg)
    catch {
      case e: java.util.NoSuchElementException =>
        logger.warn(
                s"Job '$jobId': Vertex '${msg.vertexId}' is yet to be created in " +
                  s"Partition '${storage.getPartitionID}'. Please consider rerunning computation on this perspective."
        )
    }
  }

  override def getStart(): Long = start

  override def getEnd(): Long = end

  def clearMessages(): Unit =
    vertexMap.foreach {
      case (key, vertex) => vertex.clearMessageQueue()
    }

}
