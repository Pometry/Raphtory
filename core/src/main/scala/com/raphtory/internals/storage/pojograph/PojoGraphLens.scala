package com.raphtory.internals.storage.pojograph

import cats.effect.IO
import com.raphtory.api.analysis.graphstate.GraphState
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.visitor.InterlayerEdge
import com.raphtory.api.analysis.visitor.Vertex
import com.raphtory.api.analysis.visitor.PropertyMergeStrategy.PropertyMerge
import com.raphtory.internals.components.querymanager.GenericVertexMessage
import com.raphtory.internals.graph.GraphPartition
import com.raphtory.internals.graph.LensInterface
import com.raphtory.internals.management.Scheduler
import com.raphtory.internals.storage.pojograph.entities.external.vertex.PojoExVertex
import com.raphtory.internals.storage.pojograph.entities.external.vertex.PojoVertexBase
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.immutable.SortedSet
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

private[raphtory] class SuperStepFlag {
  @volatile var evenFlag: Boolean = false
  @volatile var oddFlag: Boolean  = false

  def isSet(superStep: Int): Boolean =
    if (superStep % 2 == 0)
      evenFlag
    else
      oddFlag

  def set(superStep: Int): Unit =
    if (superStep % 2 == 0)
      evenFlag = true
    else
      oddFlag = true

  def clear(superStep: Int): Unit =
    if (superStep % 2 == 0)
      evenFlag = false
    else
      oddFlag = false
}

object SuperStepFlag {
  def apply() = new SuperStepFlag
}

final private[raphtory] case class PojoGraphLens(
    jobId: String,
    start: Long,
    end: Long,
    @volatile var superStep: Int,
    private val storage: GraphPartition,
    private val conf: Config,
    private val messageSender: GenericVertexMessage[_] => Unit,
    private val errorHandler: Throwable => Unit,
    private val scheduler: Scheduler
) extends LensInterface {
  private val logger: Logger    = Logger(LoggerFactory.getLogger(this.getClass))

  private val property_defaults =
    Map("name" -> { (vertex: Vertex) => vertex.name() }, "id" -> { (vertex: Vertex) => vertex.ID })
  private val EMPTY_CELL        = None

  private val voteCount         = new AtomicInteger(0)
  private val vertexCount       = new AtomicInteger(0)
  private var fullGraphSize     = 0
  private var exploded: Boolean = false
  var inferredHeader            = List.empty[String] // This needs to be accessed by the QueryExecutor

  val chunkSize = conf.getInt("raphtory.partitions.chunkSize")

  private val needsFiltering: SuperStepFlag = SuperStepFlag()

  val partitionID: Int = storage.getPartitionID

  private lazy val vertexMap: mutable.Map[Long, PojoExVertex] =
    storage.getVertices(this, start, end)

  private var vertices: Iterable[PojoExVertex] =
    vertexMap.values

  private var unDir: Boolean = false

  private var reversed: Boolean = false

  private def vertexIterator: Iterator[PojoVertexBase] = {
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

  override def getFullGraphSize: Int = {
    logger.trace(s"Current Graph size at '$fullGraphSize'.")
    fullGraphSize
  }

  override def setFullGraphSize(size: Int): Unit = {
    fullGraphSize = size
    logger.trace(s"Set Graph Size to '$fullGraphSize'.")
  }

  override def localNodeCount: Int = vertices.size

  private var dataTable: Iterator[Row] = Iterator()

  def filterAtStep(superStep: Int): Unit =
    needsFiltering.set(superStep)

  private def getVertexPropertyOrState(key: String, vertex: Vertex): Any = {
    val value = vertex.getStateOrElse[Any](
            key,
            property_defaults.get(key) match {
              case Some(f) => f(vertex)
              case None    => EMPTY_CELL
            },
            includeProperties = true
    )
    value
  }

  override def executeSelect(values: Seq[String])(onComplete: () => Unit): Unit = {
    dataTable = vertexIterator.map { vertex =>
      val keys    = if (values.nonEmpty) values else inferredHeader
      val columns =
        keys.map(key => (key, getVertexPropertyOrState(key, vertex)))
      Row(columns: _*)
    }
    onComplete()
  }

  override def executeSelect(values: Seq[String], graphState: GraphState)(onComplete: () => Unit): Unit = {
    if (partitionID == 0)
      dataTable = Iterator(Row(values.map(key => (key, graphState.apply[Any, Any](key).value)): _*))
    onComplete()
  }

  override def filteredTable(f: Row => Boolean)(onComplete: () => Unit): Unit = {
    dataTable = dataTable.filter(f)
    onComplete()
  }

  override def explodeColumns(columns: Seq[String])(onComplete: () => Unit): Unit = {
    def castToIterable(collection: Any): Iterable[Any] =
      collection match {
        case iterable: Iterable[Any] => iterable
        case array: Array[Any]       => array.iterator.to(Iterable)
      }
    try dataTable = dataTable.flatMap { row =>
      val validColumns = columns.filter(col => row.get(col) != EMPTY_CELL)
      validColumns.map(col => castToIterable(row.get(col))).transpose.map { values =>
        validColumns.zip(values).foldLeft(row) { case (row, (key, value)) => new Row(row.columns.updated(key, value)) }
      }
    }
    catch {
      case e: IllegalArgumentException =>
        throw new IllegalArgumentException(s"All iterables inside a row to be exploded need to have the same size", e)
    }
    onComplete()
  }

  override def renameColumns(columns: Seq[(String, String)])(onComplete: () => Unit): Unit = {
    val newColumnNameMap = columns.toMap
    dataTable = dataTable.map { row =>
      val newpairs = row.columns.map {
        case (key, value) =>
          if (newColumnNameMap.contains(key)) newColumnNameMap(key) -> value
          else (key, value)
      }
      new Row(newpairs)
    }
    onComplete()
  }

  override def writeDataTable(writer: Row => Unit)(onComplete: () => Unit): Unit = {
    dataTable.foreach(row => writer(row))
    onComplete()
  }

  override def explodeView(
      interlayerEdgeBuilder: Option[Vertex => Seq[InterlayerEdge]]
  )(onComplete: () => Unit): Unit = {
    val tasks = {
      if (exploded)
        if (interlayerEdgeBuilder.nonEmpty)
          prepareRun(vertexMap.valuesIterator, chunkSize, includeAllVs = true)(vertex =>
            vertex.explode(interlayerEdgeBuilder)
          )._2
        else List.empty
      else {
        exploded = true
        prepareRun(vertexMap.valuesIterator, chunkSize, includeAllVs = true)(vertex =>
          vertex.explode(interlayerEdgeBuilder)
        )._2
      }
    }
    scheduler.executeInParallel(tasks, onComplete, errorHandler)
  }

  override def viewUndirected()(onComplete: () => Unit): Unit = {
    unDir = true
    onComplete()
  }

  override def viewDirected()(onComplete: () => Unit): Unit = {
    unDir = false
    onComplete()
  }

  override def viewReversed()(onComplete: () => Unit): Unit = {
    reversed = !reversed
    onComplete()
  }

  override def reduceView(
      defaultMergeStrategy: Option[PropertyMerge[_, _]],
      mergeStrategyMap: Option[Map[String, PropertyMerge[_, _]]],
      aggregate: Boolean
  )(onComplete: () => Unit): Unit = {
    exploded = false
    val (_, tasks) = prepareRun(vertexMap.valuesIterator, chunkSize, includeAllVs = true)(vertex =>
      vertex.reduce(defaultMergeStrategy, mergeStrategyMap, aggregate)
    )

    scheduler.executeInParallel(tasks, onComplete, errorHandler)
  }

  override def runGraphFunction(f: Vertex => Unit)(onComplete: () => Unit): Unit = {
    val (count, tasks) = prepareRun(vertexIterator, chunkSize, includeAllVs = true)(f)
    vertexCount.set(count)
    scheduler.executeInParallel(tasks, onComplete, errorHandler)
  }

  override def runGraphFunction(
      f: (_, GraphState) => Unit,
      graphState: GraphState
  )(onComplete: () => Unit): Unit = {
    val (count, tasks) = prepareRun(vertexIterator, chunkSize, includeAllVs = true)(vertex =>
      f.asInstanceOf[(PojoVertexBase, GraphState) => Unit](vertex, graphState)
    )
    vertexCount.set(count)
    scheduler.executeInParallel(tasks, onComplete, errorHandler)
  }

  private def prepareRun[V <: Vertex](vs: Iterator[V], chunkSize: Int, includeAllVs: Boolean)(
      f: V => Unit
  ): (Int, List[IO[Unit]]) =
    vs.filter(v => v.hasMessage || includeAllVs)
      .grouped(chunkSize)
      .map(chunk => chunk.size -> IO.blocking(chunk.foreach(f)))
      .foldLeft(0 -> List.empty[IO[Unit]]) {
        case ((count, ll), (chunkSize, io)) =>
          (count + chunkSize, io :: ll)
      }

  override def runMessagedGraphFunction(f: Vertex => Unit)(onComplete: () => Unit): Unit = {

    val (count, tasks) =
      prepareRun(vertexIterator, chunkSize, includeAllVs = false)(f)

    vertexCount.set(count)
    scheduler.executeInParallel(tasks, onComplete, errorHandler)
  }

  override def runMessagedGraphFunction(
      f: (Vertex, GraphState) => Unit,
      graphState: GraphState
  )(onComplete: () => Unit): Unit = {

    val (count, tasks) = prepareRun(vertexIterator, chunkSize, includeAllVs = false) { vertex =>
      f(vertex, graphState)
    }

    vertexCount.set(count)
    scheduler.executeInParallel(tasks, onComplete, errorHandler)
  }

  def checkVotes(): Boolean = vertexCount.get() == voteCount.get()

  def sendMessage(msg: GenericVertexMessage[_]): Unit = messageSender(msg)

  def vertexVoted(): Unit = voteCount.incrementAndGet()

  def nextStep(): Unit = {
    voteCount.set(0)
    vertexCount.set(0)
    logger.debug(s"nextStep called at $superStep on partition $partitionID")
    superStep += 1
    if (needsFiltering.isSet(superStep)) {
      logger.debug(s"filtering triggered at step $superStep on partition $partitionID")
      vertexIterator.foreach(_.executeEdgeDelete())
      deleteVertices()
      needsFiltering.clear(superStep)
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
        logger.error(
                s"Job '$jobId': Vertex '${msg.vertexId}' is yet to be created in " +
                  s"Partition '${storage.getPartitionID}'. Please consider rerunning computation on this perspective."
        )
        throw e
    }
  }

  override def inferHeader(): Unit =
    inferredHeader = vertexIterator
      .foldLeft(SortedSet.empty[String])((set, vertex) => set ++ vertex.getPropertySet() ++ vertex.getStateSet())
      .toList

  def clearMessages(): Unit =
    vertexMap.foreach {
      case (key, vertex) => vertex.clearMessageQueue()
    }
}
