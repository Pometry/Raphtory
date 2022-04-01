package com.raphtory.graph

import com.raphtory.algorithms.api.Alignment
import com.raphtory.components.querymanager.NullPointSet
import com.raphtory.components.querymanager.PointPath
import com.raphtory.components.querymanager.PointSet
import com.raphtory.components.querymanager.Query
import com.raphtory.components.querymanager.QueryManagement
import com.raphtory.components.querymanager.SinglePoint
import com.raphtory.time.DiscreteInterval
import com.raphtory.time.Interval
import com.raphtory.time.NullInterval
import com.raphtory.time.TimeConverters._
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.util.Try

/** @DoNotDocument
  * The PerspectiveController is responsible for constructing graph views
  */
case class Perspective(
    timestamp: Long,
    window: Option[Interval],
    actualStart: Long,
    actualEnd: Long
) extends QueryManagement

class PerspectiveController(
    private var perspectiveStreams: Array[LazyList[Perspective]]
) {

  /** Extract the earliest perspective among the head of all streams */
  def nextPerspective(): Option[Perspective] = {
    perspectiveStreams = perspectiveStreams.filter(_.nonEmpty)
    if (perspectiveStreams.isEmpty)
      None
    else {
      val (earliestPerspective, index) = perspectiveStreams.map(_.head).zipWithIndex minBy {
        case (perspective, _) => perspective.actualEnd
      }
      perspectiveStreams = perspectiveStreams
        .updated(index, perspectiveStreams(index).tail)
      Some(earliestPerspective)
    }
  }
}

object PerspectiveController {

  val DEFAULT_PERSPECTIVE_TIME: Long             = -1L
  val DEFAULT_PERSPECTIVE_WINDOW: Some[Interval] = Some(DiscreteInterval(-1L))

  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  def apply(
      firstAvailableTimestamp: Long,
      lastAvailableTimestamp: Long,
      query: Query
  ): PerspectiveController = {
    val timelineStart          = query.timelineStart
    val timelineEnd            = query.timelineEnd
    val perspectiveStreamStart = query.timelineStart max firstAvailableTimestamp
    logger.debug(
            s"Defining perspective list from '$timelineStart' with first available timestamp '$firstAvailableTimestamp' and start defined by the user at '${query.timelineStart}'"
    )
    val rawPerspectiveStreams  = query.points match {
      // If there are no points marked by the user, we create
      // a windowless perspective that ends at the lastAvailableTimestamp
      case NullPointSet      =>
        streamsFromTimestamps(LazyList(lastAvailableTimestamp), List(), Alignment.END)

      // Similar to PointQuery
      case SinglePoint(time) =>
        streamsFromTimestamps(LazyList(time), query.windows, query.windowAlignment)

      // Similar to RangeQuery and LiveQuery
      case path: PointPath   =>
        val maxSeparation = Try(query.windows.max).getOrElse(NullInterval)
        val timestamps    = timestampsAroundTimeline(path, maxSeparation, perspectiveStreamStart)
        streamsFromTimestamps(timestamps, query.windows, query.windowAlignment)
    }

    val perspectiveStreams = rawPerspectiveStreams map (stream =>
      stream
        .dropWhile(!perspectivePartiallyInside(_, perspectiveStreamStart, timelineEnd))
        .takeWhile(perspectivePartiallyInside(_, perspectiveStreamStart, timelineEnd))
        .map(boundPerspective(_, timelineStart, timelineEnd))
      // Note that the code above is equivalent to stream.filter(perspectivePartiallyInside...).map(...).
      // The reason to follow this approach is that otherwise a nonEmpty operation over the stream
      // needs to walk through the whole stream
    )

    if (perspectiveStreams forall (_.isEmpty))
      logger.warn(
              s"No perspectives to be generated: " +
                s"perspectiveStreamStart='$perspectiveStreamStart', " +
                s"timelineStart='$timelineStart', " +
                s"timelineEnd='$timelineEnd', " +
                s"query='$query'"
      )

    new PerspectiveController(perspectiveStreams.toArray)
  }

  private def timestampsAroundTimeline(
      path: PointPath,
      maxSeparation: Interval,
      timelineStart: Long
  ) =
    path match {
      case PointPath(increment, pathStart, pathEnd, offset, customStart) =>
        val start =
          if (customStart && pathStart >= (timelineStart - maxSeparation))
            pathStart
          else if (customStart) // and pathStart < (timelineStart - greaterWindow)
            getImmediateGreaterOrEqual(pathStart, increment, timelineStart - maxSeparation)
          else
            // The user hasn't set a custom start so we align with the epoch (0) considering the offset
            getImmediateGreaterOrEqual(0 + offset, increment, timelineStart - maxSeparation)
        LazyList
          .iterate(start)(_ + increment)
          .takeWhile(_ < pathEnd)
          .appended(pathEnd)
      // TODO: the end is inclusive and is done artificially to match the former behavior, it's worth it considering a change in the future
    }

  private def streamsFromTimestamps(
      timestamps: LazyList[Long],
      windows: Seq[Interval],
      alignment: Alignment.Value
  ) =
    windows match {
      case Seq()   => List(timestamps map (createWindowlessPerspective(_, alignment)))
      case windows =>
        windows.sorted.reverse map { window =>
          timestamps map (createPerspective(_, window, alignment))
        }
    }

  private def createWindowlessPerspective(
      timestamp: Long,
      alignment: Alignment.Value
  ) =
    alignment match {
      case Alignment.START  => Perspective(timestamp, None, timestamp, Long.MaxValue)
      case Alignment.MIDDLE => Perspective(timestamp, None, Long.MinValue, Long.MaxValue)
      case Alignment.END    => Perspective(timestamp, None, Long.MinValue, timestamp)
    }

  private def createPerspective(
      timestamp: Long,
      window: Interval,
      alignment: Alignment.Value
  ) =
    alignment match {
      case Alignment.START  =>
        Perspective(
                timestamp,
                Some(window),
                timestamp,
                timestamp + window - 1 // The end is exclusive
        )
      case Alignment.MIDDLE =>
        Perspective(
                timestamp,
                Some(window),
                timestamp - window / 2,
                timestamp - window / 2 + window - 1 // The end is exclusive
        )
      case Alignment.END    =>
        Perspective(
                timestamp,
                Some(window),
                timestamp - window + 1, // The start is exclusive
                timestamp
        )
    }

  private def perspectivePartiallyInside(perspective: Perspective, start: Long, end: Long) =
    perspective.actualEnd >= start && perspective.actualStart <= end

  private def boundPerspective(perspective: Perspective, lowerBound: Long, upperBound: Long) = {
    val lowerBounded =
      if (perspective.actualStart < lowerBound) perspective.copy(actualStart = lowerBound)
      else perspective
    if (lowerBounded.actualEnd > upperBound) lowerBounded.copy(actualEnd = upperBound)
    else lowerBounded
  }

  /** Departing from the source point, moves forward or backward using the given
    * increment until it founds the immediate greater or equal than the bound
    */
  @tailrec
  private def getImmediateGreaterOrEqual(source: Long, increment: Interval, bound: Long): Long = {
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
