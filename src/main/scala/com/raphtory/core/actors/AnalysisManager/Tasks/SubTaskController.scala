package com.raphtory.core.actors.AnalysisManager.Tasks

import scala.collection.immutable.NumericRange

case class TaskTimeRange(timestamp: Long, window: Option[Long])

final case class SubTaskController(
    timestamps: Stream[Long],
    windows: List[Long],
    waitTime: Long,
    overrideTimestamp: Boolean
) {


  private var subTasks: Stream[TaskTimeRange] = windows match {
    case Nil => timestamps.map(TaskTimeRange(_, None))
    case ws =>
      val descOrderedWindows = ws.sorted.reverse
      for {
        timestamp <- timestamps
        window    <- descOrderedWindows
      } yield TaskTimeRange(timestamp, Some(window))
  }

  def nextRange(mayOverrideTimestamp: Long): Option[TaskTimeRange] =
    subTasks match {
      case Stream.Empty => None
      case head #:: tail =>
        subTasks = tail
        val finalRange =
          if (overrideTimestamp)
            head.copy(timestamp = mayOverrideTimestamp)
          else head
        Some(finalRange)
    }
}

object SubTaskController {
  def fromViewTask(timestamp: Long, windows: List[Long]): SubTaskController =
    SubTaskController(Stream(timestamp), windows, 0L, false)

  def fromRangeTask(start: Long, end: Long, jump: Long, windows: List[Long]): SubTaskController = {
    val timestamps: List[Long] = {
      val raw = NumericRange.inclusive(start, end, jump).toList
      raw.lastOption match {
        case Some(last) if last != end => raw :+ end
        case _                         => raw
      }
    }
    SubTaskController(timestamps.toStream, windows, 0L, false)
  }

  def fromEventLiveTask(repeatTime: Long, windows: List[Long], firstAvailableTimestamp: Long): SubTaskController = {
    val timestamps = Stream.iterate(firstAvailableTimestamp)(_ + repeatTime)
    SubTaskController(timestamps, windows, 0L, false)
  }

  def fromPureLiveTask(repeatTime: Long, windows: List[Long]): SubTaskController =
    SubTaskController(Stream.continually(0L), windows, repeatTime, true)
}
