package com.raphtory.core.graph

import com.raphtory.core.algorithm.Alignment
import com.raphtory.core.components.querymanager.PointPath
import com.raphtory.core.components.querymanager.Query
import com.raphtory.core.time.DateTimeParser.{defaultParse => parseDateTime}
import com.raphtory.core.time.IntervalParser.{parse => parseInterval}
import org.scalatest.funsuite.AnyFunSuite
import com.raphtory.core.time.TimeConverters._

import java.time.Instant

class PerspectiveControllerTest extends AnyFunSuite {
  test("A range of perspectives is correctly generated") {
    val increment  = parseInterval("2 months")
    val startTime  = parseDateTime("2021-01-01 00:00:00")
    val firstStep  = parseDateTime("2021-03-01 00:00:00")
    val secondStep = parseDateTime("2021-05-01 00:00:00")
    val endTime    = parseDateTime("2021-06-01 00:00:00")
    val query      = Query(
            timelineStart = startTime,
            timelineEnd = endTime,
            points = PointPath(increment),
            windowAlignment = Alignment.END
    )
    val controller = PerspectiveController(0, Long.MaxValue, query)

    println(increment.size)
    println(Instant.ofEpochMilli(startTime + increment))

    parseInterval("2 years")

    assert(controller.nextPerspective().get === Perspective(startTime, None))
    assert(controller.nextPerspective().get === Perspective(firstStep, None))
    assert(controller.nextPerspective().get === Perspective(secondStep, None))
    assert(controller.nextPerspective().get === Perspective(endTime, None))
    assert(controller.nextPerspective() === None)
  }
}
