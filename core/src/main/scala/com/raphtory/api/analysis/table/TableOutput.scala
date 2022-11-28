package com.raphtory.api.analysis.table

import com.raphtory.api.output.sink.Sink
import com.raphtory.api.progresstracker._
import com.raphtory.api.time.Perspective
import com.typesafe.config.Config

/** Concrete Table with computed results for a perspective */
case class TableOutput private (
    jobID: String,
    perspective: Perspective,
    rows: Array[Row],
    private val conf: Config
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
    * return [[com.raphtory.api.progresstracker.QueryProgressTracker QueryProgressTracker]]
    * with custom job name
    *
    * @param sink    [[com.raphtory.api.output.sink.Sink Sink]] for writing results
    * @param jobName Name for job
    */
  override def writeTo(sink: Sink, jobName: String): WriteProgressTracker = {
    // TODO: Make this actually asynchronous
    val executor = sink.executor(jobName, -1, conf)
    executor.setupPerspective(perspective)
    rows.foreach(executor.threadSafeWriteRow)
    executor.closePerspective()
    executor.close()
    WriteProgressTracker(jobName, perspective)
  }

  /** Write out data and
    * return [[com.raphtory.api.progresstracker.QueryProgressTracker QueryProgressTracker]]
    * with default job name
    *
    * @param sink [[com.raphtory.api.output.sink.Sink Sink]] for writing results
    */
  override def writeTo(sink: Sink): WriteProgressTracker = writeTo(sink, jobID)

  override def toString: String = {
    val printedRows =
      if (rows.length > 10)
        rows.take(10).mkString(", ") + ", ... "
      else
        rows.mkString(", ")

    s"TableOutput($jobID, $perspective, [$printedRows])"
  }
}
