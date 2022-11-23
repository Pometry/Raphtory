package com.raphtory.unit.com.raphtory.api.progresstracker

import cats.effect._
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.TableOutput
import com.raphtory.api.progresstracker.QueryProgressTrackerWithIterator
import com.raphtory.api.time.Interval
import com.raphtory.internals.components.output.PerspectiveResult
import com.raphtory.internals.components.querymanager._
import com.raphtory.internals.graph.Perspective
import munit.CatsEffectSuite
import org.mockito.MockitoSugar.mock

import scala.concurrent.duration.Duration

class QueryProgressTrackerWithIteratorSuite extends CatsEffectSuite {

  private val graphId         = "testGraphId"
  private val jobId           = "testJobId"
  private val config          = mock[com.typesafe.config.Config]
  private val totalPartitions = 1

  private val perspective = Perspective(
          timestamp = 1L,
          window = Some(mock[Interval]),
          actualStart = 2L,
          actualEnd = 3L
  )

  private val queryProgressTrackerWithIterator = ResourceSuiteLocalFixture(
          "QueryProgressTrackerWithIterator",
          Resource.eval(
                  Async[IO].delay(
                          QueryProgressTrackerWithIterator(
                                  graphId,
                                  jobId,
                                  config,
                                  Duration.Inf
                          )
                  )
          )
  )

  override def munitFixtures = List(queryProgressTrackerWithIterator)

  test("Job done is incomplete when QueryProgressTrackerWithIterator is started") {
    assert(!queryProgressTrackerWithIterator().isJobDone)
  }

  test("""QueryProgressTrackerWithIterator updates list of perspectives when received "PerspectiveReport" message""") {
    val tracker = queryProgressTrackerWithIterator()

    tracker.handleMessage(PerspectiveCompleted(perspective))

    assert(tracker.getLatestPerspectiveProcessed.contains(perspective))
    assert(tracker.getPerspectivesProcessed == List(perspective))
    assert(tracker.getPerspectiveDurations.size == 1)
  }

  test("""QueryProgressTrackerWithIterator updates iterator when received "OutputMessages" messages""") {
    val tracker = queryProgressTrackerWithIterator()
    val row     = Row(1, 2, 3)

    tracker.handleOutputMessage(PerspectiveResult(perspective, totalPartitions, Array(row)))
    tracker.handleMessage(JobDone)

    val itr: Iterator[TableOutput] = tracker.TableOutputIterator
    while (itr.hasNext)
      assertEquals(
              itr.next().toString,
              TableOutput(
                      jobId,
                      perspective,
                      Array(row),
                      config
              ).toString
      )
  }

  test("""QueryProgressTrackerWithIterator completes job when received "JobDone" message""") {
    val tracker = queryProgressTrackerWithIterator()

    tracker.handleMessage(JobDone)

    val itr: Iterator[TableOutput] = tracker.TableOutputIterator
    while (itr.hasNext) itr.next()
    assert(tracker.isJobDone)
  }
}
