package com.raphtory.internals.components.querymanager

import com.raphtory.api.analysis.graphstate.GraphStateImplementation
import com.raphtory.api.analysis.graphview.Alignment
import com.raphtory.api.analysis.graphview.GlobalGraphFunction
import com.raphtory.api.analysis.graphview.GraphFunction
import com.raphtory.api.analysis.table.TableFunction
import com.raphtory.api.input.Source
import com.raphtory.api.output.sink.Sink
import com.raphtory.api.time.Interval
import com.raphtory.api.time.NullInterval
import com.raphtory.internals.graph.Perspective
import scala.collection.immutable.Queue
import com.raphtory.internals.serialisers.DependencyFinder
import org.apache.bcel.Repository
import java.io.ByteArrayOutputStream
import scala.jdk.CollectionConverters._
import scala.util.Try
import scala.util.Using

private[raphtory] trait QueryManagement extends Serializable

object QueryManagement extends ProtoField[QueryManagement]

private[raphtory] trait Operation extends QueryManagement

private[raphtory] case class WatermarkTime(
    partitionID: Int,
    oldestTime: Long,
    latestTime: Long,
    safe: Boolean,
    sourceMessages: Array[(Long, Long)]
) extends QueryManagement

private[raphtory] case class SetMetaData(vertices: Int) extends QueryManagement

private[raphtory] case object JobDone                    extends QueryManagement
private[raphtory] case class JobFailed(error: Throwable) extends QueryManagement

private[raphtory] case class CreatePerspective(id: Int, perspective: Perspective) extends QueryManagement

sealed private[raphtory] trait PerspectiveReport                                         extends QueryManagement {
  def perspective: Perspective
}
private[raphtory] case class PerspectiveCompleted(perspective: Perspective)              extends PerspectiveReport
private[raphtory] case class PerspectiveFailed(perspective: Perspective, reason: String) extends PerspectiveReport

private[raphtory] case object StartGraph extends QueryManagement

private[raphtory] case object CompleteWrite extends QueryManagement

// We are assuming that all objects implementing this trait are GenericVertexMessage to bypass compilation problems in
// protocol.proto definitions, where we cannot use generic types so we use this one instead
sealed private[raphtory] trait VertexMessaging extends QueryManagement

object VertexMessaging extends ProtoField[VertexMessaging]

sealed private[raphtory] trait GenericVertexMessage[VertexID] extends VertexMessaging {
  def superstep: Int
  def vertexId: VertexID
}

trait SchemaProvider[T] {
  val endpoint: String = ""
}

trait ArrowFlightSchemaProvider[T] extends SchemaProvider[T]

case class VertexMessage[T, VertexID](
    superstep: Int,
    vertexId: VertexID,
    data: T
)(implicit val provider: SchemaProvider[T])
        extends GenericVertexMessage[VertexID]

// private[raphtory] case class VertexMessageBatch(data: Array[GenericVertexMessage[_]]) extends VertexMessaging

private[raphtory] case class FilteredEdgeMessage[VertexID](
    superstep: Int,
    vertexId: VertexID,
    sourceId: VertexID
)(implicit val provider: SchemaProvider[FilteredEdgeMessage[_]])
        extends GenericVertexMessage[VertexID]

private[raphtory] case class FilteredInEdgeMessage[VertexID](
    superstep: Int,
    vertexId: VertexID,
    sourceId: VertexID
)(implicit val provider: SchemaProvider[FilteredEdgeMessage[_]])
        extends GenericVertexMessage[VertexID]

private[raphtory] case class FilteredOutEdgeMessage[VertexID](
    superstep: Int,
    vertexId: VertexID,
    sourceId: VertexID
)(implicit val provider: SchemaProvider[FilteredEdgeMessage[_]])
        extends GenericVertexMessage[VertexID]

private[raphtory] case class VertexMessagesSync(partitionID: Int, count: Long)(implicit
    val provider: SchemaProvider[VertexMessagesSync]
) extends QueryManagement

sealed private[raphtory] trait Submission extends QueryManagement {
  def graphID: String
}

private[raphtory] case class Query(
    _bootstrap: DynamicLoader = DynamicLoader(), // leave the `_` this field gets deserialized first
    graphID: String,
    name: String = "",
    points: PointSet = NullPointSet,
    timelineStart: Long = Long.MinValue,         // inclusive
    timelineEnd: Long = Long.MaxValue,           // inclusive
    windows: List[Interval] = List(),
    windowAlignment: Alignment.Value = Alignment.START,
    operations: List[Operation] = List(),
    blockedBy: Array[Long] = Array(),
    earliestSeen: Long = Long.MaxValue,
    latestSeen: Long = Long.MinValue,
    sink: Option[Sink] = None,
    pyScript: Option[String] = None
) extends Submission

case class TryQuery(query: Try[Query])

object TryQuery extends TryProtoField[TryQuery, Query] {
  override def buildScala(value: Try[Query]): TryQuery = TryQuery(value)
  override def getTry(wrapper: TryQuery): Try[Query]   = wrapper.query
}

case class DynamicLoader(classes: List[Class[_]] = List.empty, resolved: Boolean = false) {
  def +(cls: Class[_]): DynamicLoader = this.copy(classes = cls :: classes)

  def resolve(searchPath: List[String]): DynamicLoader = {
    var knownDeps: Set[Class[_]] = Set.empty
    val distinctClasses          = classes.distinct
    val actualSearchPath         = resolveSearchPath(distinctClasses, searchPath)
    copy(
            classes = classes.reverse.flatMap { cls =>
              knownDeps += cls
              val deps = recursiveResolveDependencies(cls, actualSearchPath)(knownDeps)
              knownDeps = knownDeps ++ deps
              deps.reverse
            }.distinct,
            resolved = true
    )
  }

  private def resolveSearchPath(classes: List[Class[_]], searchPath: List[String]): List[String] =
    classes.map(_.getPackageName).filterNot(name => name.startsWith("com.raphtory") || name == "") ::: searchPath

  private def getByteCode(cls: Class[_]): Array[Byte] = {
    val jc = Repository.lookupClass(cls)

    Using(new ByteArrayOutputStream()) { bos =>
      jc.dump(bos)
      bos.flush()
      bos.toByteArray
    }.get
  }

  private def recursiveResolveDependencies(cls: Class[_], searchPath: List[String])(
      knownDeps: Set[Class[_]] = Set(cls)
  ): List[Class[_]] = {
    val packageName = cls.getPackageName
    if (packageName == "" || searchPath.exists(p => cls.getPackageName.startsWith(p))) {
      val dependencies =
        if (searchPath.isEmpty)
          Nil
        else
          DependencyFinder
            .getDependencies(cls, getByteCode(cls))
            .asScala
            .filter { d =>
              val depPackageName = d.getPackageName
              depPackageName == "" || searchPath.exists(path => depPackageName.startsWith(path))
            }
            .diff(knownDeps)
            .toList
      cls :: dependencies.flatMap(d => recursiveResolveDependencies(d, searchPath)(knownDeps + d))
    }
    else Nil
  }
}

sealed private[raphtory] trait PointSet
private[raphtory] case object NullPointSet           extends PointSet
private[raphtory] case class SinglePoint(time: Long) extends PointSet

private[raphtory] case class PointPath(
    increment: Interval,
    start: Option[Long] = None,
    end: Option[Long] = None,
    offset: Interval = NullInterval
) extends PointSet

private[raphtory] case class GraphFunctionWithGlobalState(
    function: GlobalGraphFunction,
    graphState: GraphStateImplementation
)                                                    extends QueryManagement
private[raphtory] case class EndQuery(jobID: String) extends QueryManagement

// Messages for jobStatus topic
sealed private[raphtory] trait JobStatus extends QueryManagement

private[raphtory] case object WriteCompleted extends JobStatus

sealed private[raphtory] trait PerspectiveStatus extends JobStatus {
  def perspectiveID: Int
}

private[raphtory] case class PerspectiveEstablished(perspectiveID: Int, vertices: Int) extends PerspectiveStatus
private[raphtory] case class MetaDataSet(perspectiveID: Int)                           extends PerspectiveStatus

private[raphtory] case class GraphFunctionComplete(
    perspectiveID: Int,
    partitionID: Int,
    receivedMessages: Long,
    sentMessages: Long,
    votedToHalt: Boolean = false
) extends PerspectiveStatus

private[raphtory] case class GraphFunctionCompleteWithState(
    perspectiveID: Int,
    partitionID: Int,
    receivedMessages: Long,
    sentMessages: Long,
    votedToHalt: Boolean = false,
    graphState: GraphStateImplementation
) extends PerspectiveStatus

private[raphtory] case class TableFunctionComplete(perspectiveID: Int) extends PerspectiveStatus
private[raphtory] case class TableBuilt(perspectiveID: Int)            extends PerspectiveStatus

private[raphtory] case class AlgorithmFailure(perspectiveID: Int, exception: Throwable) extends PerspectiveStatus

// Messages for partitionSetup topic
sealed private[raphtory] trait GraphManagement extends QueryManagement

private[raphtory] case class IngestData(
    _bootstrap: DynamicLoader,
    graphID: String,
    sourceId: Int,
    source: Source
) extends Submission
        with GraphManagement

case class TryIngestData(ingestData: Try[IngestData])

object TryIngestData extends TryProtoField[TryIngestData, IngestData] {
  override def buildScala(value: Try[IngestData]): TryIngestData = TryIngestData(value)
  override def getTry(wrapper: TryIngestData): Try[IngestData]   = wrapper.ingestData
}
