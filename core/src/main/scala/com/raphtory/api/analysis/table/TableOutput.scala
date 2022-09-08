package com.raphtory.api.analysis.table

import com.raphtory.api.output.sink.Sink
import com.raphtory.api.querytracker.QueryProgressTracker
import com.raphtory.api.querytracker.QueryProgressTrackerBase
import com.raphtory.api.time.Perspective
import com.raphtory.internals.communication.TopicRepository
import com.typesafe.config.Config

import scala.concurrent.duration.Duration

case class WriteProgressTracker(jobID: String, perspective: Perspective) extends QueryProgressTrackerBase {

  /** Returns job identifier for the query
    *
    * @return job identifier
    */
  override def getJobId: String = jobID

  /** Returns the latest `Perspective` processed by the query
    *
    * @return latest perspective
    */
  override def getLatestPerspectiveProcessed: Option[Perspective] = Some(perspective)

  /** Returns list of perspectives processed for the query so far
    *
    * @return a list of perspectives
    */
  override def getPerspectivesProcessed: List[Perspective] = List(perspective)

  /** Returns the time taken to process each perspective in milliseconds */
  override def getPerspectiveDurations: List[Long] = List(0)

  /** Checks if job is complete
    *
    * @return job status
    */
  override def isJobDone: Boolean = true

  /** Block until job is complete */
  override def waitForJob(timeout: Duration): Unit = {}
}

/** Concrete Table with computed results for a perspective */
case class TableOutput private[raphtory] (
    perspective: Perspective,
    rows: Array[Row],
    jobID: String,
    private val conf: Config,
    private val topics: TopicRepository
) extends TableBase {

  /** Add a filter operation to table
    *
    * @param f function that runs once for each row (only rows for which `f ` returns `true` are kept)
    */
  override def filter(f: Row => Boolean): TableOutput = copy(rows = rows.filter(f))

  /** Explode table rows
    *
    * This creates a new table where each row in the old table
    * is mapped to multiple rows in the new table.
    *
    * @param f function that runs once for each row of the table and maps it to new rows
    */
  override def explode(f: Row => IterableOnce[Row]): TableOutput = copy(rows = rows.flatMap(f))

  /** Write out data and
    * return [[com.raphtory.api.querytracker.QueryProgressTracker QueryProgressTracker]]
    * with custom job name
    *
    * @param sink    [[com.raphtory.api.output.sink.Sink Sink]] for writing results
    * @param jobName Name for job
    */
  override def writeTo(sink: Sink, jobName: String): WriteProgressTracker = {
    // TODO: Make this actually asynchronous
    val executor = sink.executor(jobName, -1, conf, topics)
    executor.setupPerspective(perspective)
    rows.foreach(executor.threadSafeWriteRow)
    executor.closePerspective()
    executor.close()
    WriteProgressTracker(jobName, perspective)
  }

  /** Write out data and
    * return [[com.raphtory.api.querytracker.QueryProgressTracker QueryProgressTracker]]
    * with default job name
    *
    * @param sink [[com.raphtory.api.output.sink.Sink Sink]] for writing results
    */
  override def writeTo(sink: Sink): WriteProgressTracker = writeTo(sink, jobID)
}
