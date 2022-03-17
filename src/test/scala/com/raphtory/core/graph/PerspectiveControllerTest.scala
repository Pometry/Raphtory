package com.raphtory.core.graph

import com.raphtory.core.components.querymanager.Query
import com.raphtory.core.time.TimeInterval
import org.scalatest.funsuite.AnyFunSuite

import java.time.LocalDateTime
import java.time.Period
import java.time.ZoneOffset

class PerspectiveControllerTest extends AnyFunSuite {
  test("A range of perspectives is correctly generated") {
    val increment  = Some(TimeInterval(Period.ofMonths(2)))
    val startTime  = milliseconds("2021-01-01T00:00:00")
    val firstStep  = milliseconds("2021-03-01T00:00:00")
    val secondStep = milliseconds("2021-05-01T00:00:00")
    val endTime    = milliseconds("2021-06-01T00:00:00")
    val query      = Query(startTime = Some(startTime), endTime = Some(endTime), increment = increment)
    val controller = PerspectiveController(0, query)

    assert(controller.nextPerspective().get === Perspective(startTime, None))
    assert(controller.nextPerspective().get === Perspective(firstStep, None))
    assert(controller.nextPerspective().get === Perspective(secondStep, None))
    assert(controller.nextPerspective().get === Perspective(endTime, None))
    assert(controller.nextPerspective() === None)
  }

  private def milliseconds(timestamp: String) =
    LocalDateTime.parse(timestamp).toInstant(ZoneOffset.UTC).toEpochMilli
}
