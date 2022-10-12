import com.raphtory.Raphtory
import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.api.analysis.graphview.Alignment
import com.raphtory.api.analysis.table.Row
import com.raphtory.examples.lotr.analysis.DegreesSeparation
import com.raphtory.internals.communication.SchemaProviderInstances.genericSchemaProvider
import com.raphtory.sinks.{FileSink, PrintSink}
import munit.CatsEffectSuite

class Queries extends CatsEffectSuite {
test("Code for queries.md is updated") {
val output = PrintSink()
val graph  = Raphtory.newGraph()
val pathToYourFile = "/tmp/doc-queries-example-sink"

// [everything]
 graph
  .execute(DegreesSeparation())
  .writeTo(output)
// [everything]

// [sentence-filtering]
val first1000sentences = graph.until(1000)
val sentencesFrom4000To5000 = graph.slice(4000, 5000)
// [sentence-filtering]

// [year-slice]
graph
  .slice("2020-01-01", "2021-01-01")
  .execute(ConnectedComponents)
  .writeTo(output)
// [year-slice]

// [depart-window]
graph
  .depart("2020-01-01", "1 day") // departing from Jan 1, 2020 with steps of one day
  .window("1 day") // creates a window of one day for each increment
  .execute(ConnectedComponents)
  .writeTo(output)
// [depart-window]

// [walk-window]
graph
  .slice("2020-01-01", "2021-01-01")
  .walk("1 day")
  .window("1 week", Alignment.START)
  .execute(ConnectedComponents)
  .writeTo(output)
// [walk-window]

// [direct-style]
graph
  .slice("2020-01-01", "2021-01-01")
  .walk("1 day")
  .window("1 day")
  .vertexFilter(vertex => vertex.outDegree > 10)
  .step(vertex => vertex.messageOutNeighbours(vertex.name()))
  .select(vertex => Row(vertex.messageQueue))
  .writeTo(FileSink(pathToYourFile))
// [direct-style]

// [mixed-style]
graph
  .slice("2020-01-01", "2021-01-01")
  .walk("1 day")
  .window("1 day")
  .vertexFilter(vertex => vertex.outDegree > 10)
  .execute(ConnectedComponents)
  .writeTo(FileSink(pathToYourFile))
// [mixed-style]

}
}
