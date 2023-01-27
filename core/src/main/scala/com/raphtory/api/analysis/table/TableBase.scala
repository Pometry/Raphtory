package com.raphtory.api.analysis.table

import com.raphtory.api.output.sink.Sink
import com.raphtory.api.progresstracker.QueryProgressTracker
import com.raphtory.api.progresstracker.ProgressTracker
import com.raphtory.sinks.FileSink

trait TableBase {

  /** Add a filter operation to table
    * @param f function that runs once for each row (only rows for which `f ` returns `true` are kept)
    */
  def filter(f: Row => Boolean): TableBase

  /** Returns a table with multiple rows for every row exploding the columns in `columns`
    *
    * If only one column name is provided, that column is assumed to contain a collection of values
    * and every row is mapped to multiple rows, one for every value in the collection.
    *
    * If several column names are provided, then for every single row all the collections in those
    * columns are expected to have the same length. The values in all those collections are zipped together
    * to be explode in multiple columns.
    *
    * For instance, if two column names are provided and for a particular row those columns contain
    * a collection of three elements, that row is mapped to three new columns:
    * one containing the first value of both collections,
    * a second one containing the second value of both,
    * and a third one containing the final value,
    * replacing the old values for those columns
    *
    * @param columns the names of the columns containing the lists to coordinately explode over
    */
  def explode(columns: String*): TableBase

  /** Returns a table with the columns renamed using the mappings in `columns`
    *
    * @param columns the mappings for the columns to be renamed (from the first value in the tuple to the second one)
    */
  def renameColumns(columns: (String, String)*): TableBase

  /** Write out data and
    * return [[com.raphtory.api.progresstracker.QueryProgressTracker QueryProgressTracker]]
    * with custom job name
    *
    * @param sink [[com.raphtory.api.output.sink.Sink Sink]] for writing results
    * @param jobName Name for job
    */
  def writeTo(sink: Sink, jobName: String): ProgressTracker

  /** Write out data and
    * return [[com.raphtory.api.progresstracker.QueryProgressTracker QueryProgressTracker]]
    * with default job name
    *
    * @param sink [[com.raphtory.api.output.sink.Sink Sink]] for writing results
    */
  def writeTo(sink: Sink): ProgressTracker

  /** Write out data to files and
    * return [[com.raphtory.api.progresstracker.QueryProgressTracker QueryProgressTracker]]
    * with default job name
    *
    * @param name folder path for writing results
    */
  def writeToFile(name: String): ProgressTracker =
    writeTo(FileSink(name))
}
