package com.raphtory.api.analysis.table

import cats.effect.IO
import cats.effect.Resource
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.api.analysis.graphview.DeployedTemporalGraph
import com.raphtory.api.time.Perspective
import com.raphtory.internals.context.RaphtoryContext.RaphtoryContextBuilder
import com.raphtory.internals.graph._
import munit.CatsEffectSuite

class TestTableOutput extends CatsEffectSuite {

  private def run[R](f: DeployedTemporalGraph => R): R = {
    val out = for {
      ctxBuilder <- Resource.fromAutoCloseable(IO.delay(RaphtoryContextBuilder()))
    } yield ctxBuilder.local()

    out
      .use(ctx =>
        IO.blocking {
          ctx.runWithNewGraph() { graph =>
            f(graph)
          }
        }
      )
      .unsafeRunSync()
  }

  test("test table get()") {
    val result = run { graph =>
      graph.addVertex(0, 0)
      graph.addVertex(0, 1)
      graph.addVertex(1, 2)
      graph.addEdge(0, 0, 1)
      graph.addEdge(1, 1, 2)

      val perspectives = graph.range(0, 1, 1).past().execute(EdgeList()).get()
      perspectives.map(out => out.perspective -> Set.from(out.rows)).toMap
    }

    val correctResult = Map[Perspective, Set[Row]](
            Perspective(0, None, Long.MinValue, 0) -> Set(Row("0", "1")),
            Perspective(1, None, Long.MinValue, 1) -> Set(Row("0", "1"), Row("1", "2"))
    )

    assertEquals(result, correctResult)
  }

}
