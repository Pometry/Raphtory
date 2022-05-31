package com.raphtory.components.querymanager

import com.raphtory.algorithms.api.Alignment
import com.raphtory.algorithms.api.GraphFunction
import com.raphtory.algorithms.api.GraphStateImplementation
import com.raphtory.algorithms.api.Sink
import com.raphtory.algorithms.api.TableFunction
import com.raphtory.graph.Perspective
import com.raphtory.time.Interval
import com.raphtory.time.NullInterval

import scala.collection.immutable.Queue

/** @note DoNotDocument */
trait QueryManagement extends Serializable

case class WatermarkTime(partitionID: Int, startTime: Long, endTime: Long, safe: Boolean)
        extends QueryManagement

case object StartAnalysis                                       extends QueryManagement
case class EstablishExecutor(jobID: String, outputFormat: Sink) extends QueryManagement

case class SetMetaData(vertices: Int) extends QueryManagement

case object JobDone extends QueryManagement

case class CreatePerspective(id: Int, perspective: Perspective) extends QueryManagement

case object StartGraph extends QueryManagement

case object CompleteWrite extends QueryManagement

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
    timelineStart: Long = Long.MinValue, // inclusive
    timelineEnd: Long = Long.MaxValue,   // inclusive
    windows: List[Interval] = List(),
    windowAlignment: Alignment.Value = Alignment.START,
    graphFunctions: Queue[GraphFunction] = Queue(),
    tableFunctions: Queue[TableFunction] = Queue(),
    sink: Option[Sink] = None
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

// Messages for jobStatus topic
sealed trait JobStatus extends QueryManagement

case class ExecutorEstablished(worker: Int) extends JobStatus
case object WriteCompleted                  extends JobStatus

sealed trait PerspectiveStatus                                       extends JobStatus {
  def perspectiveID: Int
}
case class PerspectiveEstablished(perspectiveID: Int, vertices: Int) extends PerspectiveStatus
case class MetaDataSet(perspectiveID: Int)                           extends PerspectiveStatus

case class GraphFunctionComplete(
    perspectiveID: Int,
    partitionID: Int,
    receivedMessages: Int,
    sentMessages: Int,
    votedToHalt: Boolean = false
) extends PerspectiveStatus

case class GraphFunctionCompleteWithState(
    perspectiveID: Int,
    partitionID: Int,
    receivedMessages: Int,
    sentMessages: Int,
    votedToHalt: Boolean = false,
    graphState: GraphStateImplementation
) extends PerspectiveStatus

case class TableFunctionComplete(perspectiveID: Int) extends PerspectiveStatus
case class TableBuilt(perspectiveID: Int)            extends PerspectiveStatus

case class AlgorithmFailure(perspectiveID: Int, exception: Throwable) extends PerspectiveStatus
