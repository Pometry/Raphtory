package com.raphtory.api.analysis.table

import com.raphtory.api.output.sink.Sink
import com.raphtory.api.progresstracker._
import com.raphtory.api.time.Perspective
import com.typesafe.config.Config

import scala.collection.immutable.SeqMap
import scala.collection.immutable.SortedSet

/** Concrete Table with computed results for a perspective */
case class TableOutput private (
    jobID: String,
    perspective: Perspective,
    header: List[String],
    rows: Array[Row],
    private val conf: Config
) extends TableBase {

  /** Returns an array of rows represented as arrays of values */
  def rowsAsArrays(): Array[Array[Any]] = rows.map(row => row.columns.values.toArray)

  def actualHeader: List[String] =
    if (header.nonEmpty) header
    else rows.foldLeft(SortedSet.empty[String])((set, row) => set ++ row.columns.keys).toList

  override def filter(f: Row => Boolean): TableOutput = copy(rows = rows.filter(f))

  override def explode(columns: String*): TableOutput = {
    val explodedRows = rows.flatMap { row =>
      val validColumns = columns.filter(col => row.get(col) != "")
      validColumns.map(col => row.get(col).asInstanceOf[Iterable[Any]]).transpose.map { values =>
        validColumns.zip(values).foldLeft(row) { case (row, (key, value)) => new Row(row.columns.updated(key, value)) }
      }
    }
    this.copy(rows = explodedRows)
  }

  override def renameColumns(columns: (String, String)*): TableOutput = {
    val newNames      = columns.toMap
    val renamedHeader = header.collect {
      case key if newNames contains key => newNames(key)
      case key                          => key
    }
    val renamedRows   = rows
      .map { row =>
        val renamedColumns = row.columns.collect {
          case (key, value) if newNames contains key => (newNames(key), value)
          case tuple                                 => tuple
        }
        new Row(renamedColumns)
      }
    this.copy(rows = renamedRows, header = renamedHeader)
  }

  override def writeTo(sink: Sink, jobName: String): WriteProgressTracker = {
    // TODO: Make this actually asynchronous
    val executor = sink.executor(jobName, -1, conf)
    val columns  = actualHeader
    executor.setupPerspective(perspective, columns)
    rows.foreach(executor.threadSafeWriteRow)
    executor.closePerspective()
    executor.close()
    WriteProgressTracker(jobName, perspective)
  }

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
