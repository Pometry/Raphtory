package com.raphtory.core.graph

import com.raphtory.core.components.querymanager.QueryManagement
import com.raphtory.core.time.AgnosticInterval
import com.raphtory.core.time.Interval

import scala.collection.immutable.NumericRange

/** @DoNotDocument */
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
  val DEFAULT_PERSPECTIVE_WINDOW: Some[Interval] = Some(AgnosticInterval(-1L))

  def pointQueryController(
      timestamp: Long,
      windows: List[Interval] = List()
  ): PerspectiveController =
    new PerspectiveController(Stream(timestamp), windows)

  def rangeQueryController(
      start: Long,
      end: Long,
      increment: Long,
      windows: List[Interval] = List()
  ): PerspectiveController = {
    val timestamps: Stream[Long] = {
      val raw = NumericRange.inclusive(start, end, increment).toStream
      raw.lastOption match {
        case Some(last) if last != end => raw :+ end
        case _                         => raw
      }
    }
    new PerspectiveController(timestamps, windows)
  }

  def liveQueryController(
      firstAvailableTimestamp: Long,
      repeatIncrement: Long,
      windows: List[Interval] = List()
  ): PerspectiveController = {
    val timestamps = Stream.iterate(firstAvailableTimestamp)(_ + repeatIncrement)
    new PerspectiveController(timestamps, windows)
  }

}
