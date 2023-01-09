package com.raphtory.internals.graph

import com.raphtory.api.analysis.graphview.Alignment
import com.raphtory.api.time
import com.raphtory.api.time.DiscreteInterval
import com.raphtory.api.time.Interval
import com.raphtory.api.time.NullInterval
import com.raphtory.internals.components.querymanager._
import com.raphtory.internals.time.DateTimeParser
import com.raphtory.internals.time.TimeConverters._
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.util.Try

/** The PerspectiveController is responsible for constructing graph views
  */
private[raphtory] case class Perspective(
    timestamp: Long,
    window: Option[Interval],
    actualStart: Long,
    actualEnd: Long,
    processingTime: Long,
    formatAsDate: Boolean
) extends time.Perspective {

  override val timestampAsString: String =
    if (formatAsDate) DateTimeParser().parse(timestamp) else timestamp.toString

  override val actualStartAsString: String =
    if (formatAsDate) DateTimeParser().parse(actualStart) else actualStart.toString

  override val actualEndAsString: String =
    if (formatAsDate) DateTimeParser().parse(actualEnd) else actualEnd.toString

}

object Perspective extends ProtoField[Perspective]

private[raphtory] class PerspectiveController(
    private var perspectiveStreams: Array[LazyList[Perspective]]
) {

  // Extract the earliest perspective among the head of all streams
  def nextPerspective(): Option[Perspective] = {
    perspectiveStreams = perspectiveStreams.filter(_.nonEmpty)
    if (perspectiveStreams.isEmpty)
      None
    else {
      val (earliestPerspective, index) = perspectiveStreams.map(_.head).zipWithIndex minBy {
        case (perspective, _) => perspective.actualEnd
      }
      perspectiveStreams = perspectiveStreams.updated(index, perspectiveStreams(index).tail)
      Some(earliestPerspective)
    }
  }
}

private[raphtory] object PerspectiveController {

  final val DEFAULT_PERSPECTIVE_TIME: Long             = -1L
  final val DEFAULT_PERSPECTIVE_WINDOW: Some[Interval] = Some(DiscreteInterval(-1L))

  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  def apply(firstAvailableTimestamp: Long, lastAvailableTimestamp: Long, query: Query): PerspectiveController = {
    logger.debug(
            s"Defining perspective list using: " +
              s"firstAvailableTimestamp='$firstAvailableTimestamp', " +
              s"lastAvailableTimestamp='$lastAvailableTimestamp', " +
              s"query='$query'"
    )
    if (firstAvailableTimestamp > lastAvailableTimestamp || query.timelineStart > query.timelineEnd)
      new PerspectiveController(Array.empty) // NONSENSE case
    else if (query.timelineStart > lastAvailableTimestamp || query.timelineEnd < firstAvailableTimestamp) {
      logger.warn(
              s"first/last available timestamps [$firstAvailableTimestamp, $lastAvailableTimestamp] outside of query timeline [${query.timelineStart}, ${query.timelineEnd}]"
      )
      new PerspectiveController(Array.empty) // out of bounds
    }
    else {
      val perspectives = query.points match {
        // If there are no points marked by the user, we create
        // a windowless perspective that ends at the lastAvailableTimestamp
        case NullPointSet                             =>
          perspectivesFromTimestamps(
                  query.timelineStart,
                  query.timelineEnd,
                  LazyList(query.timelineEnd min lastAvailableTimestamp),
                  List(),
                  Alignment.END,
                  query.datetimeQuery
          )

        // Similar to PointQuery
        case SinglePoint(time)                        =>
          perspectivesFromTimestamps(
                  query.timelineStart,
                  query.timelineEnd,
                  LazyList(time),
                  query.windows,
                  query.windowAlignment,
                  query.datetimeQuery
          )

        // Similar to RangeQuery and LiveQuery
        case PointPath(increment, pathStart, pathEnd) =>
          val start = pathStart.getOrElse(query.timelineStart max firstAvailableTimestamp)
          val end   = pathEnd.getOrElse(query.timelineEnd min lastAvailableTimestamp)

          val timestamps: LazyList[Long] =
            LazyList
              .iterate(start)(_ + increment)
              .takeWhile(_ < end)
              .appended(end)

          perspectivesFromTimestamps(
                  query.timelineStart,
                  query.timelineEnd,
                  timestamps,
                  query.windows,
                  query.windowAlignment,
                  query.datetimeQuery
          )
      }

      new PerspectiveController(perspectives.toArray)
    }
  }

  private def perspectivesFromTimestamps(
      timelineStart: Long,
      timelineEnd: Long,
      timestamps: LazyList[Long],
      windows: Seq[Interval],
      alignment: Alignment.Value,
      formatAsDate: Boolean
  ) =
    windows match {
      case Seq()   =>
        List(timestamps map (timestamp => {
          alignment match {
            case Alignment.START  =>
              Perspective(timestamp, None, (timestamp max timelineStart), timelineEnd, -1, formatAsDate)
            case Alignment.MIDDLE => Perspective(timestamp, None, timelineStart, timelineEnd, -1, formatAsDate)
            case Alignment.END    =>
              Perspective(timestamp, None, timelineStart, (timestamp min timelineEnd), -1, formatAsDate)
          }
        }))
      case windows =>
        windows.map { window =>
          timestamps map (timestamp => {
            alignment match {
              case Alignment.START  =>
                Perspective(
                        timestamp,
                        Some(window),
                        (timestamp max timelineStart),
                        ((timestamp + window) min timelineEnd) - 1, // The end is exclusive
                        -1,
                        formatAsDate
                )
              case Alignment.MIDDLE =>
                Perspective(
                        timestamp,
                        Some(window),
                        ((timestamp - window / 2) max timelineStart) + 1,
                        ((timestamp + window / 2) min timelineEnd) - 1, // Both are
                        -1,
                        formatAsDate
                )
              case Alignment.END    =>
                Perspective(
                        timestamp,
                        Some(window),
                        ((timestamp - window) max timelineStart) + 1, // The start is exclusive
                        (timestamp min timelineEnd),
                        -1,
                        formatAsDate
                )
            }
          })
        }
    }

}
