package com.raphtory.core.graph

import com.raphtory.core.components.querymanager.Query
import com.raphtory.core.components.querymanager.QueryManagement
import com.raphtory.core.time.DiscreteInterval
import com.raphtory.core.time.Interval
import com.raphtory.core.time.TimeConverters._

/** @DoNotDocument
  * The PerspectiveController is responsible for constructing graph views
  */
case class Perspective(timestamp: Long, window: Option[Interval]) extends QueryManagement

class PerspectiveController(timestamps: Stream[Long], windows: List[Interval]) {

  private var perspectives: Stream[Perspective] = windows match {
    case Nil => timestamps.map(Perspective(_, None))
    case ws  =>
      val descOrderedWindows = ws.sorted.reverse
      for {
        timestamp <- timestamps
        window    <- descOrderedWindows
      } yield Perspective(timestamp, Some(window))
  }

  def nextPerspective(): Option[Perspective] =
    perspectives match {
      case Stream.Empty  =>
        None
      case head #:: tail =>
        perspectives = tail
        Some(head)
    }
}

object PerspectiveController {

  val DEFAULT_PERSPECTIVE_TIME: Long             = -1L
  val DEFAULT_PERSPECTIVE_WINDOW: Some[Interval] = Some(DiscreteInterval(-1L))

  def apply(
      lastAvailableTimestamp: Long,
      query: Query
  ): PerspectiveController = {
    query.increment match {
      // Similar to PointQuery
      case None            =>
        val end     = query.endTime.getOrElse(lastAvailableTimestamp)
        val windows = query.startTime match {
          case Some(start) if(query.windows.isEmpty) => List(DiscreteInterval(end - start))
          case _ => query.windows
        }
        new PerspectiveController(Stream(end), windows)

      // Similar to RangeQuery and LiveQuery (depending on 'query.endTime' having a value)
      case Some(increment) =>
        val start           = query.startTime.getOrElse(lastAvailableTimestamp)
        val timestampStream = Stream.iterate(start)(_ + increment)
        val timestamps      = query.endTime match {
          case Some(end) => timestampStream.takeWhile(_ < end) :+ end
          case None      => timestampStream
        }
        new PerspectiveController(timestamps, query.windows)
    }
  }
}
