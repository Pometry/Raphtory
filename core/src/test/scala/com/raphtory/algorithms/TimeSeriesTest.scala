package com.raphtory.algorithms

import com.raphtory.TimeSeriesGraphState

class TimeSeriesTest extends BaseCorrectnessTest {
  test("Time Series Property History Test") {
    assert(
            correctnessTest(
                    TimeSeriesGraphState(),
                    "TimeSeriesGraphState/timeSeries.csv",
                    "TimeSeriesGraphState/timeSeriesResult.csv",
                    4
            )
    )
  }
}
