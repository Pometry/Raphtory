package com.raphtory.components.querymanager

import com.raphtory.algorithms.api.Alignment
import com.raphtory.algorithms.api.GraphFunction
import com.raphtory.algorithms.api.GraphStateImplementation
import com.raphtory.algorithms.api.TableFunction
import com.raphtory.graph.Perspective
import com.raphtory.time.Interval
import com.raphtory.time.NullInterval

import scala.collection.immutable.Queue

/** @DoNotDocument */
trait QueryManagement extends Serializable

case class WatermarkTime(partitionID: Int, startTime: Long, endTime: Long, safe: Boolean)
        extends QueryManagement

case object StartAnalysis                   extends QueryManagement
case class EstablishExecutor(jobID: String) extends QueryManagement
case class ExecutorEstablished(worker: Int) extends QueryManagement

case class PerspectiveEstablished(vertices: Int) extends QueryManagement
case class SetMetaData(vertices: Int)            extends QueryManagement
case object MetaDataSet                          extends QueryManagement
case object JobDone                              extends QueryManagement

case class CreatePerspective(
    timestamp: Long,
    window: Option[Interval],
    actualStart: Long,
    actualEnd: Long
) extends QueryManagement

object CreatePerspective {

  def apply(perspective: Perspective): CreatePerspective =
    CreatePerspective(
            perspective.timestamp,
            perspective.window,
            perspective.actualStart,
            perspective.actualEnd
    )
}

case object StartGraph extends QueryManagement

case class AlgorithmFailure(exception: Throwable) extends QueryManagement

case class GraphFunctionComplete(
    partitionID: Int,
    receivedMessages: Int,
    sentMessages: Int,
    votedToHalt: Boolean = false
) extends QueryManagement

case class GraphFunctionCompleteWithState(
    partitionID: Int,
    receivedMessages: Int,
    sentMessages: Int,
    votedToHalt: Boolean = false,
    graphState: GraphStateImplementation
) extends QueryManagement

case object TableBuilt            extends QueryManagement
case object TableFunctionComplete extends QueryManagement

case object RecheckTime                 extends QueryManagement
case object RecheckEarliestTime         extends QueryManagement
case class CheckMessages(jobId: String) extends QueryManagement

sealed trait GenericVertexMessage[VertexID] extends QueryManagement {
  def superstep: Int
  def vertexId: VertexID
}

case class VertexMessage[+T, VertexID](superstep: Int, vertexId: VertexID, data: T)
        extends GenericVertexMessage[VertexID]

case class VertexMessageBatch(data: Array[GenericVertexMessage[_]]) extends QueryManagement

case class FilteredEdgeMessage[VertexID](superstep: Int, vertexId: VertexID, sourceId: VertexID)
        extends GenericVertexMessage[VertexID]

case class FilteredInEdgeMessage[VertexID](superstep: Int, vertexId: VertexID, sourceId: VertexID)
        extends GenericVertexMessage[VertexID]

case class FilteredOutEdgeMessage[VertexID](superstep: Int, vertexId: VertexID, sourceId: VertexID)
        extends GenericVertexMessage[VertexID]

case class Query(
    name: String = "",
    points: PointSet = NullPointSet,
    timelineStart: Long = Long.MinValue,
    timelineEnd: Long = Long.MaxValue,
    windows: List[Interval] = List(),
    windowAlignment: Alignment.Value = Alignment.START,
    graphFunctions: Queue[GraphFunction] = Queue(),
    tableFunctions: Queue[TableFunction] = Queue()
) extends QueryManagement

sealed trait PointSet
case object NullPointSet           extends PointSet
case class SinglePoint(time: Long) extends PointSet

case class PointPath(
    increment: Interval,
    start: Option[Long] = None,
    end: Option[Long] = None,
    offset: Interval = NullInterval
) extends PointSet

case class EndQuery(jobID: String)        extends QueryManagement
case class QueryNotPresent(jobID: String) extends QueryManagement

case class TaskFinished(result: Boolean) extends QueryManagement
case object AreYouFinished               extends QueryManagement
