package com.raphtory.internals.storage.arrow

import com.raphtory.api.analysis.graphstate.GraphState
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.visitor.PropertyMergeStrategy.PropertyMerge
import com.raphtory.api.analysis.visitor.InterlayerEdge
import com.raphtory.api.analysis.visitor.Vertex
import com.raphtory.internals.components.querymanager._
import com.raphtory.internals.graph.LensInterface
import com.raphtory.internals.management.Scheduler
import com.raphtory.internals.storage.arrow.entities.ArrowExVertex
import com.raphtory.internals.storage.GraphExecutionState
import com.raphtory.internals.storage.VotingMachine
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.View

abstract class AbstractGraphLens(
    jobId: String,
    start: Long,
    end: Long,
    superStep: AtomicInteger,
    protected val storage: ArrowPartition,
    private val messageSender: GenericVertexMessage[_] => Unit,
    private val errorHandler: Throwable => Unit,
    private val scheduler: Scheduler
) extends LensInterface {
  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  private val vertexCount   = new AtomicInteger(0)
  private var fullGraphSize = 0
  private val votingMachine = VotingMachine()

  protected val graphState: GraphExecutionState =
    GraphExecutionState(partitionID(), superStep, messageSender, storage.asGlobal, votingMachine, start, end)

  private var dataTable: View[Row] = View.empty[Row]

  var t1: Long = System.currentTimeMillis()

  override def partitionID(): Int = storage.getPartitionID

  def checkVotes(): Boolean = votingMachine.checkVotes(vertexCount.get)

  def setFullGraphSize(size: Int): Unit = {
    fullGraphSize = size
    logger.trace(s"Set Graph Size to '$fullGraphSize'.")
  }

  def sendMessage(msg: GenericVertexMessage[_]): Unit = messageSender(msg)

  override def getFullGraphSize: Int = fullGraphSize

  override def nextStep(): Unit = {
    votingMachine.reset()
    t1 = System.currentTimeMillis()
    vertexCount.set(0)
    graphState.nextStep(superStep.getAndIncrement())
  }

  override def clearMessages(): Unit = graphState.clearMessages()

  override def receiveMessage(msg: GenericVertexMessage[_]): Unit =
    msg match {
      case msg: VertexMessage[_, _]                     =>
        graphState.receiveMessage(msg.vertexId, msg.superstep, msg.data)
      case msg: FilteredEdgeMessage[Long] @unchecked    =>
        graphState.removeEdge(msg.vertexId, msg.sourceId, None)
      case msg: FilteredInEdgeMessage[Long] @unchecked  =>
        graphState.removeInEdge(msg.sourceId, msg.vertexId)
      case msg: FilteredOutEdgeMessage[Long] @unchecked =>
        graphState.removeOutEdge(msg.sourceId, msg.vertexId)
      case _                                            =>
    }

  /**
    * Give me the vertices alive at this point
    * use the [[GraphExecutionState.isAlive]] to check
    * in the arrow case we'll be passing the local vertex id
    * these also must take into account the [[start]] and [[end]] limits
    *
    * @return
    */
  def vertices: View[Vertex]

  def currentVertices: View[Vertex] =
    graphState.currentStepVertices.map { id =>
      new ArrowExVertex(graphState, storage.getVertex(id))
    }

  override def runGraphFunction(f: Vertex => Unit)(onComplete: () => Unit): Unit = {
    vertexCount.set(0)
    val count = vertices.foldLeft(0) { (c, v) => f(v); c + 1 }
    vertexCount.set(count)
    onComplete()
  }

  override def runGraphFunction(f: (_, GraphState) => Unit, graphState: GraphState)(
      onComplete: () => Unit
  ): Unit = {
    vertexCount.set(0)
    val count = vertices.foldLeft(0) { (c, v) => f.asInstanceOf[(Vertex, GraphState) => Unit](v, graphState); c + 1 }
    vertexCount.set(count)
    onComplete()
  }

  override def runMessagedGraphFunction(f: Vertex => Unit)(onComplete: () => Unit): Unit = {
    vertexCount.set(0)
    val count = vertices.filter(_.hasMessage).foldLeft(0) { (c, v) => f(v); c + 1 }
    vertexCount.set(count)
    onComplete()
  }

  override def runMessagedGraphFunction(f: (Vertex, GraphState) => Unit, graphState: GraphState)(
      onComplete: () => Unit
  ): Unit = {

    vertexCount.set(0)
    val count = vertices.filter(_.hasMessage).foldLeft(0) { (c, v) => f(v, graphState); c + 1 }
    vertexCount.set(count)
    onComplete()
  }

  override def writeDataTable(writer: Row => Unit)(onComplete: () => Unit): Unit = {
    dataTable.foreach(writer)
    onComplete()
  }

  override def executeSelect(f: GraphState => Row, graphState: GraphState)(onComplete: () => Unit): Unit = {
    if (partitionID == 0)
      dataTable = View.fromIteratorProvider(() => Iterator.fill(1)(f(graphState)))
    onComplete()
  }

  override def filteredTable(f: Row => Boolean)(onComplete: () => Unit): Unit = {
    dataTable = dataTable.filter(f)
    onComplete()
  }

  override def reduceView(
      defaultMergeStrategy: Option[PropertyMerge[_, _]],
      mergeStrategyMap: Option[Map[String, PropertyMerge[_, _]]],
      aggregate: Boolean
  )(onComplete: () => Unit): Unit = onComplete()

  override def explodeView(interlayerEdgeBuilder: Option[Vertex => Seq[InterlayerEdge]])(onComplete: () => Unit): Unit =
    ???

  override def viewUndirected()(onComplete: () => Unit): Unit = ???

  override def viewDirected()(onComplete: () => Unit): Unit = ???

  override def viewReversed()(onComplete: () => Unit): Unit = ???

  override def executeSelect(values: Seq[String], defaults: Map[String, Any])(onComplete: () => Unit): Unit = ???
  override def explodeColumns(columns: Seq[String])(onComplete: () => Unit): Unit                           = ???
  override def renameColumn(columns: Seq[(String, String)])(onComplete: () => Unit): Unit                   = ???
  override def inferHeader(): Unit                                                                          = ???
  override def inferredHeader: List[String]                                                                 = ???
}
