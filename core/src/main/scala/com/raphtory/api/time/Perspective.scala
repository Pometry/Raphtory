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
}
