package com.raphtory.graph

import com.raphtory.api.analysis.graphview.Alignment
import com.raphtory.internals.components.querymanager.NullPointSet
import com.raphtory.internals.components.querymanager.PointPath
import com.raphtory.internals.components.querymanager.Query
import com.raphtory.internals.graph.PerspectiveController
import com.raphtory.internals.time.DateTimeParser
import com.raphtory.internals.time.IntervalParser.{parse => parseInterval}
import munit.FunSuite

class PerspectiveControllerTest extends FunSuite {

  test("Inverted first/last should result in None nextPerspective") {
    val query = Query(
            graphID = "",
            timelineStart = Long.MinValue,
            timelineEnd = Long.MaxValue,
            points = NullPointSet,
            windows = Array.empty,
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
            windows = Array.empty,
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
            windows = Array.empty,
            windowAlignment = Alignment.START
    )

    val controller = PerspectiveController(5001, Long.MaxValue, query)

    assertEquals(controller.nextPerspective(), None)
  }

  test("A range of perspectives is correctly generated") {
    val increment        = parseInterval("2 months")
    val start            = DateTimeParser().parse("2021-01-01 00:00:00")
    val middle           = DateTimeParser().parse("2021-03-01 00:00:00")
    val end              = DateTimeParser().parse("2021-05-01 00:00:00")
    val query            = Query(
            graphID = "",
            timelineStart = start,
            timelineEnd = end,
            points = PointPath(increment),
            windows = Array(increment),
            windowAlignment = Alignment.START
    )
    val controller       = PerspectiveController(0, Long.MaxValue, query)
    var count            = 0
    val firstPerspective = controller.nextPerspective().get
    assertEquals(firstPerspective.actualStart, start)
    assertEquals(firstPerspective.actualEnd, middle - 1)

    val secondPerspective = controller.nextPerspective().get
    assertEquals(secondPerspective.actualStart, middle)
    assertEquals(secondPerspective.actualEnd, end - 1)
    controller.nextPerspective()
    assertEquals(controller.nextPerspective(), None)
  }
}
