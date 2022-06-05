package com.raphtory.storage.pojograph

import com.raphtory.api.graphstate.GraphState
import com.raphtory.api.table.Row
import com.raphtory.api.visitor.PropertyMergeStrategy.PropertyMerge
import com.raphtory.api.visitor.InterlayerEdge
import com.raphtory.api.visitor.Vertex
import com.raphtory.communication.EndPoint
import com.raphtory.components.querymanager.GenericVertexMessage
import com.raphtory.components.querymanager.QueryManagement
import com.raphtory.config.MonixScheduler
import com.raphtory.graph.GraphLens
import com.raphtory.graph.GraphPartition
import com.raphtory.graph.LensInterface
import com.raphtory.storage.pojograph.entities.external.PojoExVertex
import com.raphtory.storage.pojograph.entities.external.PojoVertexBase
import com.raphtory.storage.pojograph.messaging.VertexMessageHandler
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import monix.eval.Task
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable

/** @note DoNotDocument */
final case class PojoGraphLens(
    jobId: String,
    start: Long,
    end: Long,
    var superStep: Int,
    private val storage: GraphPartition,
    private val conf: Config,
    private val neighbours: Option[Map[Int, EndPoint[QueryManagement]]],
    private val sentMessages: AtomicInteger,
    private val receivedMessages: AtomicInteger,
    private val errorHandler: (Throwable) => Unit,
    private val scheduler: MonixScheduler
) extends GraphLens(jobId, start, end)
        with LensInterface {
  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  private val voteCount         = new AtomicInteger(0)
  private val vertexCount       = new AtomicInteger(0)
  private var fullGraphSize     = 0
  private var exploded: Boolean = false

  var needsFiltering = false //used in PojoExEdge

  private val messageHandler: VertexMessageHandler =
    VertexMessageHandler(conf, neighbours, this, sentMessages, receivedMessages)

  val partitionID: Int = storage.getPartitionID

  private lazy val vertexMap: mutable.Map[Long, PojoExVertex] =
    storage.getVertices(this, start, end)

  private var vertices: Array[PojoExVertex] =
    vertexMap.values.toArray

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

  def getSize: Int = vertices.length

  private var dataTable: List[Row] = List()

  def executeSelect(f: _ => Row)(onComplete: => Unit): Unit = {
    dataTable = vertexIterator.map(f.asInstanceOf[PojoVertexBase => Row]).toList
    onComplete
  }

  def executeSelect(
      f: (_, GraphState) => Row,
      graphState: GraphState
  )(onComplete: => Unit): Unit = {
    dataTable =
      vertexIterator.map(f.asInstanceOf[(PojoVertexBase, GraphState) => Row](_, graphState)).toList
    onComplete
  }

  def executeSelect(
      f: GraphState => Row,
      graphState: GraphState
  )(onComplete: => Unit): Unit = {
    dataTable = List(f(graphState))
    onComplete
  }

  def explodeSelect(f: _ => IterableOnce[Row])(onComplete: => Unit): Unit = {
    dataTable = vertexIterator.flatMap(f.asInstanceOf[PojoVertexBase => List[Row]]).toList
    onComplete
  }

  def filteredTable(f: Row => Boolean)(onComplete: => Unit): Unit = {
    dataTable = dataTable.filter(f)
    onComplete
  }

  def explodeTable(f: Row => IterableOnce[Row])(onComplete: => Unit): Unit = {
    dataTable = dataTable.flatMap(f)
    onComplete
  }

  def writeDataTable(writer: Row => Unit)(onComplete: => Unit): Unit = {
    dataTable.foreach(row => writer(row))
    onComplete
  }

  override def explodeView(
      interlayerEdgeBuilder: Option[Vertex => Seq[InterlayerEdge]]
  )(onComplete: => Unit): Unit = {
    val tasks: Iterable[Task[Unit]] = {
      if (exploded)
        if (interlayerEdgeBuilder.nonEmpty)
          vertexMap.values.map { vertex =>
            Task {
              vertex.explode(interlayerEdgeBuilder)
            }
          }
        else
          Seq(Task.unit)
      else {
        exploded = true
        vertexMap.values.map { vertex =>
          Task {
            vertex.explode(interlayerEdgeBuilder)
          }
        }
      }
    }
    scheduler.executeInParallel(tasks, onComplete, errorHandler)
  }

  override def reduceView(
      defaultMergeStrategy: Option[PropertyMerge[_, _]],
      mergeStrategyMap: Option[Map[String, PropertyMerge[_, _]]],
      aggregate: Boolean
  )(onComplete: => Unit): Unit = {
    exploded = false
    val tasks = vertexMap.values.map { vertex =>
      Task(vertex.reduce(defaultMergeStrategy, mergeStrategyMap, aggregate))
    }
    scheduler.executeInParallel(tasks, onComplete, errorHandler)
  }

  def runGraphFunction(f: _ => Unit)(onComplete: => Unit): Unit = {
    var count: Int = 0
    val tasks      = vertexIterator.map { vertex =>
      count += 1
      Task(f.asInstanceOf[PojoVertexBase => Unit](vertex))
    }.toIterable
    vertexCount.set(count)
    scheduler.executeInParallel(tasks, onComplete, errorHandler)
  }

  override def runGraphFunction(
      f: (_, GraphState) => Unit,
      graphState: GraphState
  )(onComplete: => Unit): Unit = {
    var count: Int = 0
    val tasks      = vertexIterator.map { vertex =>
      count += 1
      Task(f.asInstanceOf[(PojoVertexBase, GraphState) => Unit](vertex, graphState))
    }.toIterable
    vertexCount.set(count)
    scheduler.executeInParallel(tasks, onComplete, errorHandler)
  }

  override def runMessagedGraphFunction(f: _ => Unit)(onComplete: => Unit): Unit = {
    var count: Int = 0
    val tasks      = vertexIterator.collect {
      case vertex if vertex.hasMessage() =>
        count += 1
        Task(f.asInstanceOf[PojoVertexBase => Unit](vertex))
    }.toIterable
    vertexCount.set(count)
    scheduler.executeInParallel(tasks, onComplete, errorHandler)
  }

  override def runMessagedGraphFunction(
      f: (_, GraphState) => Unit,
      graphState: GraphState
  )(onComplete: => Unit): Unit = {
    var count: Int = 0
    val tasks      = vertexIterator.collect {
      case vertex if vertex.hasMessage() =>
        count += 1
        Task(f.asInstanceOf[(PojoVertexBase, GraphState) => Unit](vertex, graphState))
    }.toIterable
    vertexCount.set(count)
    scheduler.executeInParallel(tasks, onComplete, errorHandler)
  }

  def getMessageHandler(): VertexMessageHandler = messageHandler

  def checkVotes(): Boolean = vertexCount.get() == voteCount.get()

  def sendMessage(msg: GenericVertexMessage[_]): Unit = messageHandler.sendMessage(msg)

  def vertexVoted(): Unit = voteCount.incrementAndGet()

  def nextStep(): Unit = {
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
