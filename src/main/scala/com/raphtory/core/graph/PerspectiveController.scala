package com.raphtory.core.graph

import com.raphtory.core.algorithm.Alignment
import com.raphtory.core.components.querymanager.NullPointSet
import com.raphtory.core.components.querymanager.PointPath
import com.raphtory.core.components.querymanager.Query
import com.raphtory.core.components.querymanager.QueryManagement
import com.raphtory.core.components.querymanager.SinglePoint
import com.raphtory.core.time.DiscreteInterval
import com.raphtory.core.time.Interval
import com.raphtory.core.time.NullInterval
import com.raphtory.core.time.TimeConverters._

import scala.annotation.tailrec
import scala.util.Try

/** @DoNotDocument
  * The PerspectiveController is responsible for constructing graph views
  */
case class Perspective(timestamp: Long, window: Option[Interval]) extends QueryManagement

class PerspectiveController(
    timestamps: LazyList[Long],
    windows: List[Interval],
    timelineStart: Long,
    timelineEnd: Long,
    alignment: Alignment.Value
) {
  println(timestamps.take(10).toList)

  /** A list of perspective streams, one stream for each window */
  private var perspectiveStreams: List[LazyList[Perspective]] = windows match {
    case Nil     =>
      List(timestamps map { timestamp =>
        alignment match {
          case Alignment.START  =>
            Perspective(timelineEnd, Some(DiscreteInterval(timelineEnd - timestamp)))
          case Alignment.MIDDLE => Perspective(timelineEnd, None)
          case Alignment.END    => Perspective(timestamp, None)
        }
      })
    case windows =>
      windows.sorted.reverse map (window =>
        timestamps map { timestamp =>
          val correctedTimestamp = alignment match {
            case Alignment.START  => timestamp + window
            case Alignment.MIDDLE => timestamp + (window / 2)
            case Alignment.END    => timestamp
          }
          Perspective(correctedTimestamp, Some(window))
        }
      )
  }

  /** Extract the earliest perspective among the head of all streams */
  def nextPerspective(): Option[Perspective] =
    perspectiveStreams map (_.head) match {
      case Nil        => None
      case candidates =>
        val (earliestPerspective, index) = candidates.zipWithIndex minBy {
          case (perspective, _) => perspective.timestamp
        }
        perspectiveStreams = perspectiveStreams
          .updated(index, perspectiveStreams(index).tail)
          .filter(_.nonEmpty)

        if (earliestPerspective.timestamp < timelineStart)
          nextPerspective()
        else if (earliestPerspective.timestamp > timelineEnd)
          None
        else
          Some(earliestPerspective)
    }
}

object PerspectiveController {

  val DEFAULT_PERSPECTIVE_TIME: Long             = -1L
  val DEFAULT_PERSPECTIVE_WINDOW: Some[Interval] = Some(DiscreteInterval(-1L))

  def apply(
      firstAvailableTimestamp: Long,
      lastAvailableTimestamp: Long,
      query: Query
  ): PerspectiveController = {
    val timelineStart = firstAvailableTimestamp max query.timelineStart
    val timelineEnd   = query.timelineEnd
    query.points match {
      // If there are no points marked by the user, we create
      // a windowless perspective that ends at the lastAvailableTimestamp
      case NullPointSet                                                  =>
        val equivalentQuery = query.copy(
                points = SinglePoint(lastAvailableTimestamp),
                windows = List(),
                windowAlignment = Alignment.END
        )
        apply(firstAvailableTimestamp, lastAvailableTimestamp, equivalentQuery)

      // Similar to PointQuery
      case SinglePoint(time)                                             =>
        new PerspectiveController(
                LazyList(time),
                query.windows,
                timelineStart,
                timelineEnd,
                query.windowAlignment
        )

      // Similar to RangeQuery and LiveQuery
      case PointPath(increment, pathStart, pathEnd, offset, customStart) =>
        val greaterWindow = Try(query.windows.max).getOrElse(NullInterval)
        val start         =
          if (customStart && pathStart >= (timelineStart - greaterWindow))
            pathStart
          else if (customStart) // and pathStart < (timelineStart - greaterWindow)
            getImmediateGreaterOrEqual(pathStart, increment, timelineStart - greaterWindow)
          else
            // The user hasn't set a custom start so we align with the epoch (0) considering the offset
            getImmediateGreaterOrEqual(0 + offset, increment, timelineStart - greaterWindow)
        val timestamps    = LazyList
          .iterate(start)(_ + increment)
          .takeWhile(_ < pathEnd)
          .appended(pathEnd)
        // TODO: the end is inclusive and is done artificially to match the former behavior, it's worth it considering a change in the future

        new PerspectiveController(
                timestamps,
                query.windows,
                timelineStart,
                timelineEnd,
                query.windowAlignment
        )
    }
  }

  /** Departing from the source point, moves forward or backward using the given
    * increment until it found the immediate greater than the inclusiveBound
    */
  @tailrec
  def getImmediateGreaterOrEqual(source: Long, increment: Interval, bound: Long): Long = {
    lazy val jumps = LazyList.iterate(1)(_ * 10)
    if (source + increment == source)
      source
    else if (source >= bound && source - increment < bound)
      source
    else if (source > bound) {
      val nextPosition = Try(
              jumps
                .map(jump => source - (increment * jump))
                .takeWhile(position => position > bound && position < source) // avoid overflow
                .last
      ).getOrElse(source - increment)
      getImmediateGreaterOrEqual(nextPosition, increment, bound)
    }
    else { // source < bound
      val nextPosition = Try(
              jumps
                .map(jump => source + (increment * jump))
                .takeWhile(position => position < bound && position > source) // avoid overflow
                .last
      ).getOrElse(source + increment)
      getImmediateGreaterOrEqual(nextPosition, increment, bound)
    }
  }
}
