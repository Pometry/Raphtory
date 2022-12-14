package com.raphtory.api.time

/** Information about the graph view
  */
trait Perspective {

  /** Timestamp anchor at which the view was created */
  val timestamp: Long

  /** Time window of the view if it is set */
  val window: Option[Interval]

  /** Earliest time point included in the view */
  val actualStart: Long

  /** Latest time point included in the view */
  val actualEnd: Long

  /** Depending on the users query if the output should be formatted as a date */
  val formatAsDate: Boolean

  /** Timestamp anchor at which the view was created as a datetime string */
  val timestampAsString: String

  /** Earliest time point included in the view */
  val actualStartAsString: String

  /** Latest time point included in the view */
  val actualEndAsString: String

  /** Convenience function for sink formats to get either the datetime string or epoch long for output */
  def bestTimestamp: Any = if (formatAsDate) timestampAsString else timestamp
}
