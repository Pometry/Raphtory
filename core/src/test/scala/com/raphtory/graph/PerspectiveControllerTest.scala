package com.raphtory.graph

import com.raphtory.api.analysis.graphview.Alignment
import com.raphtory.internals.components.querymanager.PointPath
import com.raphtory.internals.components.querymanager.Query
import com.raphtory.internals.graph.PerspectiveController
import munit.FunSuite
import com.raphtory.internals.time.IntervalParser.{parse => parseInterval}
import com.raphtory.internals.time.DateTimeParser.{defaultParse => parseDateTime}

class PerspectiveControllerTest extends FunSuite {
  test("A range of perspectives is correctly generated") {
    val increment  = parseInterval("2 months")
    val start      = parseDateTime("2021-01-01 00:00:00")
    val middle     = parseDateTime("2021-03-01 00:00:00")
    val end        = parseDateTime("2021-05-01 00:00:00")
    val query      = Query(
            graphID = "",
            timelineStart = start,
            timelineEnd = end - 1,
            points = PointPath(increment),
            windows = List(increment),
            windowAlignment = Alignment.START
    )
    val controller = PerspectiveController(0, Long.MaxValue, query)

    val firstPerspective = controller.nextPerspective().get
    assertEquals(firstPerspective.actualStart, start)
    assertEquals(firstPerspective.actualEnd, middle - 1)

    val secondPerspective = controller.nextPerspective().get
    assertEquals(secondPerspective.actualStart, middle)
    assertEquals(secondPerspective.actualEnd, end - 1)

    assertEquals(controller.nextPerspective(), None)
  }
}
