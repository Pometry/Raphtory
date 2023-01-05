package com.raphtory.api.progresstracker

import cats.effect._
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.TableOutput
import com.raphtory.api.time.Interval
import com.raphtory.internals.graph.Perspective
import com.raphtory.protocol.PerspectiveCompleted
import com.raphtory.protocol.QueryCompleted
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
          actualEnd = 3L,
          processingTime = -1,
          formatAsDate = false
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

  test("QueryProgressTrackerWithIterator updates list of perspectives and iterator when received PerspectiveCompleted") {
    val tracker = queryProgressTrackerWithIterator()
    val row     = Row(1, 2, 3)

    tracker.handleQueryUpdate(PerspectiveCompleted(perspective, Seq(row)))
    tracker.handleQueryUpdate(QueryCompleted())

    val outcome = tracker.getLatestPerspectiveProcessed.map { case p: Perspective => p.copy(processingTime = -1) }

    assertEquals(outcome, Some(perspective))

    val itr: Iterator[TableOutput] = tracker.TableOutputIterator
    while (itr.hasNext)
      assertEquals(itr.next().toString, TableOutput(jobId, perspective, Array(row), config).toString)

    assert(tracker.isJobDone)
  }
}
