package com.raphtory.api.progresstracker

import cats.effect._
import com.raphtory.api.time.Interval
import com.raphtory.internals.graph.Perspective
import com.raphtory.protocol.PerspectiveCompleted
import com.raphtory.protocol.QueryCompleted
import munit.CatsEffectSuite
import org.mockito.MockitoSugar._

class QueryProgressTrackerSuite extends CatsEffectSuite {

  private val graphId = "testGraphId"
  private val jobId   = "testJobId"

  private val queryProgressTracker = ResourceSuiteLocalFixture(
          "QueryProgressTracker",
          Resource.eval(Async[IO].delay(QueryProgressTracker(graphId, jobId, mock[com.typesafe.config.Config])))
  )

  override def munitFixtures = List(queryProgressTracker)

  test("Job done is incomplete when query progress tracker is started") {
    assert(!queryProgressTracker().isJobDone)
  }

  test("QueryProgressTracker updates list of perspectives when recieved PerspectiveReport message") {
    val tracker     = queryProgressTracker()
    val perspective = Perspective(
            timestamp = 1L,
            window = Some(mock[Interval]),
            actualStart = 2L,
            actualEnd = 3L,
            formatAsDate = false
    )

    tracker.handleQueryUpdate(PerspectiveCompleted(perspective))

    assert(tracker.getLatestPerspectiveProcessed.contains(perspective))
    assert(tracker.getPerspectivesProcessed == List(perspective))
    assert(tracker.getPerspectiveDurations.size == 1)
  }

  test("Job done is complete when query progress tracker receives \"JobDone\" message") {
    val tracker = queryProgressTracker()
    tracker.handleQueryUpdate(QueryCompleted())
    assert(tracker.isJobDone)
  }
}
