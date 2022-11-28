package com.raphtory.graph

import com.raphtory.api.analysis.graphview.Alignment
import com.raphtory.internals.components.querymanager.{NullPointSet, PointPath, Query}
import com.raphtory.internals.graph.PerspectiveController
import com.raphtory.internals.time.DateTimeParser.{defaultParse => parseDateTime}
import com.raphtory.internals.time.IntervalParser.{parse => parseInterval}
import munit.FunSuite

class PerspectiveControllerTest extends FunSuite {

  test("Inverted first/last should result in None nextPerspective") {
    val query = Query(
            graphID = "",
            timelineStart = Long.MinValue,
            timelineEnd = Long.MaxValue,
            points = NullPointSet,
            windows = Nil,
            windowAlignment = Alignment.START
    )

    val controller = PerspectiveController(Long.MaxValue, Long.MinValue, query)

    assertEquals(controller.nextPerspective(), None)
  }

  test("Inverted timeline start/end inverted should result in None nextPerspective") {
    val query = Query(
            graphID = "",
            timelineStart = Long.MaxValue,
            timelineEnd = Long.MinValue,
            points = NullPointSet,
            windows = Nil,
            windowAlignment = Alignment.START
    )

    val controller = PerspectiveController(0, Long.MaxValue, query)

    assertEquals(controller.nextPerspective(), None)
  }

  test("Query start/end out of bounds") {
    val query = Query(
      graphID = "",
      timelineStart = 5,
      timelineEnd = 5000,
      points = NullPointSet,
      windows = Nil,
      windowAlignment = Alignment.START
    )

    val controller = PerspectiveController(5001, Long.MaxValue, query)

    assertEquals(controller.nextPerspective(), None)
  }


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
