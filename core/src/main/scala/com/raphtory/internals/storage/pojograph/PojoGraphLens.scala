package com.raphtory.internals.storage.pojograph

import cats.effect.IO
import com.raphtory.api.analysis.graphstate.GraphState
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.RowImplementation
import com.raphtory.api.analysis.visitor.InterlayerEdge
import com.raphtory.api.analysis.visitor.Vertex
import com.raphtory.api.analysis.visitor.PropertyMergeStrategy.PropertyMerge
import com.raphtory.internals.components.querymanager.GenericVertexMessage
import com.raphtory.internals.graph.GraphPartition
import com.raphtory.internals.graph.LensInterface
import com.raphtory.internals.management.Scheduler
import com.raphtory.internals.storage.pojograph.entities.external.PojoExVertex
import com.raphtory.internals.storage.pojograph.entities.external.PojoVertexBase
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable

final private[raphtory] case class PojoGraphLens(
    jobId: String,
    start: Long,
    end: Long,
    var superStep: Int,
    private val storage: GraphPartition,
    private val conf: Config,
    private val messageSender: GenericVertexMessage[_] => Unit,
    private val errorHandler: (Throwable) => Unit,
    private val scheduler: Scheduler
) extends LensInterface {
  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  private val voteCount         = new AtomicInteger(0)
  private val vertexCount       = new AtomicInteger(0)
  private var fullGraphSize     = 0
  private var exploded: Boolean = false

  var needsFiltering = false //used in PojoExEdge

  val partitionID: Int = storage.getPartitionID

  private lazy val vertexMap: mutable.Map[Long, PojoExVertex] =
    storage.getVertices(this, start, end)

  private var vertices: Array[PojoExVertex] =
    vertexMap.values.toArray

  private var unDir: Boolean = false

  private var reversed: Boolean = false

  private def vertexIterator = {
    val it =
      if (exploded)
        vertices.iterator.flatMap(_.explodedVertices)
      else
        vertices.iterator

    if (unDir)
      it.map(_.viewUndirected)
    else if (reversed)
      it.map(_.viewReversed)
    else
      it
  }

  def getFullGraphSize: Int = {
    logger.trace(s"Current Graph size at '$fullGraphSize'.")
    fullGraphSize
  }

  def setFullGraphSize(size: Int): Unit = {
    fullGraphSize = size
    logger.trace(s"Set Graph Size to '$fullGraphSize'.")
  }

  def localNodeCount: Int = vertices.length

  private var dataTable: Iterator[RowImplementation] = Iterator()

  def executeSelect(f: _ => Row)(onComplete: => Unit): Unit = {
    dataTable = vertexIterator.flatMap { vertex =>
      f.asInstanceOf[PojoVertexBase => RowImplementation](vertex).yieldAndRelease
    }
    onComplete
  }

  def executeSelect(
      f: (_, GraphState) => Row,
      graphState: GraphState
  )(onComplete: => Unit): Unit = {
    dataTable = vertexIterator.flatMap { vertex =>
      f.asInstanceOf[(PojoVertexBase, GraphState) => RowImplementation](vertex, graphState).yieldAndRelease
    }
    onComplete
  }

  def executeSelect(
      f: GraphState => Row,
      graphState: GraphState
  )(onComplete: => Unit): Unit = {
    if (partitionID == 0)
      dataTable = Iterator
        .fill(1)(f(graphState).asInstanceOf[RowImplementation])
        .flatMap(_.yieldAndRelease)
    onComplete
  }

  def explodeSelect(f: _ => IterableOnce[Row])(onComplete: => Unit): Unit = {
    dataTable = vertexIterator
      .flatMap(f.asInstanceOf[PojoVertexBase => IterableOnce[RowImplementation]])
      .flatMap(_.yieldAndRelease)
    onComplete
  }

  def filteredTable(f: Row => Boolean)(onComplete: => Unit): Unit = {
    dataTable = dataTable.filter(f)
    onComplete
  }

  def explodeTable(f: Row => IterableOnce[Row])(onComplete: => Unit): Unit = {
    dataTable = dataTable.flatMap(_.explode(f))
    onComplete
  }

  def writeDataTable(writer: Row => Unit)(onComplete: => Unit): Unit = {
    dataTable.foreach(row => writer(row))
    onComplete
  }

  override def explodeView(
      interlayerEdgeBuilder: Option[Vertex => Seq[InterlayerEdge]]
  )(onComplete: => Unit): Unit = {
    val tasks: Iterable[IO[Unit]] = {
      if (exploded)
        if (interlayerEdgeBuilder.nonEmpty)
          vertexMap.values.map { vertex =>
            IO {
              vertex.explode(interlayerEdgeBuilder)
            }
          }
        else
          Seq(IO.unit)
      else {
        exploded = true
        vertexMap.values.map { vertex =>
          IO {
            vertex.explode(interlayerEdgeBuilder)
          }
        }
      }
    }
    scheduler.executeInParallel(tasks, onComplete, errorHandler)
  }

  def viewUndirected()(onComplete: => Unit): Unit = {
    unDir = true
    onComplete
  }

  def viewDirected()(onComplete: => Unit): Unit = {
    unDir = false
    onComplete
  }

  def viewReversed()(onComplete: => Unit): Unit = {
    reversed = !reversed
    onComplete
  }

  def reduceView(
      defaultMergeStrategy: Option[PropertyMerge[_, _]],
      mergeStrategyMap: Option[Map[String, PropertyMerge[_, _]]],
      aggregate: Boolean
  )(onComplete: => Unit): Unit = {
    exploded = false
    val tasks = vertexMap.values.map { vertex =>
      IO(vertex.reduce(defaultMergeStrategy, mergeStrategyMap, aggregate))
    }
    scheduler.executeInParallel(tasks, onComplete, errorHandler)
  }

  def runGraphFunction(f: _ => Unit)(onComplete: => Unit): Unit = {
    var count: Int = 0
    val tasks      = vertexIterator
      .map { vertex =>
        count += 1
        IO(f.asInstanceOf[PojoVertexBase => Unit](vertex))
      }
      .iterator
      .to(Iterable)
    vertexCount.set(count)
    scheduler.executeInParallel(tasks, onComplete, errorHandler)
  }

  override def runGraphFunction(
      f: (_, GraphState) => Unit,
      graphState: GraphState
  )(onComplete: => Unit): Unit = {
    var count: Int = 0
    val tasks      = vertexIterator
      .map { vertex =>
        count += 1
        IO(f.asInstanceOf[(PojoVertexBase, GraphState) => Unit](vertex, graphState))
      }
      .iterator
      .to(Iterable)
    vertexCount.set(count)
    scheduler.executeInParallel(tasks, onComplete, errorHandler)
  }

  override def runMessagedGraphFunction(f: _ => Unit)(onComplete: => Unit): Unit = {
    var count: Int = 0
    val tasks      = vertexIterator
      .collect {
        case vertex if vertex.hasMessage =>
          count += 1
          IO(f.asInstanceOf[PojoVertexBase => Unit](vertex))
      }
      .iterator
      .to(Iterable)
    vertexCount.set(count)
    scheduler.executeInParallel(tasks, onComplete, errorHandler)
  }

  override def runMessagedGraphFunction(
      f: (_, GraphState) => Unit,
      graphState: GraphState
  )(onComplete: => Unit): Unit = {
    var count: Int = 0
    val tasks      = vertexIterator
      .collect {
        case vertex if vertex.hasMessage =>
          count += 1
          IO(f.asInstanceOf[(PojoVertexBase, GraphState) => Unit](vertex, graphState))
      }
      .iterator
      .to(Iterable)
    vertexCount.set(count)
    scheduler.executeInParallel(tasks, onComplete, errorHandler)
  }

  def checkVotes(): Boolean = vertexCount.get() == voteCount.get()

  def sendMessage(msg: GenericVertexMessage[_]): Unit = messageSender(msg)

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

  def clearMessages(): Unit =
    vertexMap.foreach {
      case (key, vertex) => vertex.clearMessageQueue()
    }

}
