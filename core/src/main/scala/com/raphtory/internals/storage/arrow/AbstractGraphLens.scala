package com.raphtory.internals.storage.arrow

import com.raphtory.api.analysis.graphstate.GraphState
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.RowImplementation
import com.raphtory.api.analysis.visitor.Vertex
import com.raphtory.internals.components.querymanager._
import com.raphtory.internals.graph.LensInterface
import com.raphtory.internals.management.Scheduler
import com.raphtory.internals.storage.GraphExecutionState
import com.raphtory.internals.storage.arrow.entities.ArrowExVertex
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
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

  private val voteCount = new AtomicInteger(0)
  private val vertexCount = new AtomicInteger(0)
  private var fullGraphSize = 0
  protected val graphState: GraphExecutionState = GraphExecutionState(superStep)

  private var dataTable: View[RowImplementation] = View.empty[RowImplementation]

  var t1: Long = System.currentTimeMillis()

  override def partitionID(): Int = storage.getPartitionID

  def checkVotes(): Boolean = vertexCount.get() == voteCount.get()

  def vertexVoted(): Unit = voteCount.incrementAndGet()

  def setFullGraphSize(size: Int): Unit = {
    fullGraphSize = size
    logger.trace(s"Set Graph Size to '$fullGraphSize'.")
  }

  def sendMessage(msg: GenericVertexMessage[_]): Unit = messageSender(msg)

  override def getFullGraphSize: Int = fullGraphSize

  override def nextStep(): Unit = {
    t1 = System.currentTimeMillis()
    voteCount.set(0)
    vertexCount.set(0)
    graphState.nextStep(superStep.incrementAndGet())
  }

  override def clearMessages(): Unit = graphState.clearMessages()

  override def receiveMessage(msg: GenericVertexMessage[_]): Unit =
    msg match {
      case msg: VertexMessage[_, _] =>
        graphState.receiveMessage(msg.vertexId, msg.superstep, msg.data)
      case msg: FilteredEdgeMessage[Long]@unchecked =>
        graphState.removeEdge(msg.vertexId, msg.sourceId, msg.edgeId)
      case msg: FilteredInEdgeMessage[Long]@unchecked =>
        graphState.removeInEdge(msg.sourceId, msg.vertexId, msg.edgeId)
      case msg: FilteredOutEdgeMessage[Long]@unchecked =>
        graphState.removeOutEdge(msg.vertexId, msg.sourceId, msg.edgeId)
      case _ =>
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

  def currentVertices: View[Vertex] = graphState.currentStepVertices.map {
    id => new ArrowExVertex(graphState, storage.getVertex(id))
  }

  override def runGraphFunction(f: Vertex => Unit)(onComplete: => Unit): Unit = {
    vertexCount.set(0)
    val count = vertices.foldLeft(0) { (c, v) => f(v); c + 1 }
    vertexCount.set(count)
    onComplete
  }

  override def runGraphFunction(f: (_, GraphState) => Unit, graphState: GraphState)(
    onComplete: => Unit
  ): Unit = {
    vertexCount.set(0)
    val count = vertices.foldLeft(0) { (c, v) => f.asInstanceOf[(Vertex, GraphState) => Unit](v, graphState); c + 1 }
    vertexCount.set(count)
    onComplete
  }

  override def executeSelect(f: _ => Row)(onComplete: => Unit): Unit = {
    dataTable = currentVertices.flatMap(v => f.asInstanceOf[Vertex => RowImplementation](v).yieldAndRelease)
    onComplete
  }

  override def writeDataTable(writer: Row => Unit)(onComplete: => Unit): Unit = {
    dataTable.foreach(row => writer(row))
    onComplete
  }
}
