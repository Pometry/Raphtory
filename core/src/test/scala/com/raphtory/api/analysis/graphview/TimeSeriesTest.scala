package com.raphtory.api.analysis.graphview

import com.raphtory.BaseCorrectnessTest
import com.raphtory.TimeSeriesGraphState
import com.raphtory.api.input.Spout
import com.raphtory.spouts.ResourceSpout

class TimeSeriesTest extends BaseCorrectnessTest(startGraph = true) {
  override def setSpout(): Spout[String] = ResourceSpout("TimeSeriesGraphState/timeSeries.csv")
  test("Time Series Test") {
    assert(
            correctnessTest(
                    TimeSeriesGraphState(),
                    "TimeSeriesGraphState/timeSeriesResult.csv",
                    4
            )
    )
  }
}
