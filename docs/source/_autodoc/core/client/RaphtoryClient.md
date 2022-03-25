`com.raphtory.core.client.RaphtoryClient`
(com.raphtory.core.client.RaphtoryClient)=
# RaphtoryClient

{s}`RaphtoryClient`
   : Raphtory Client exposes query APIs for point, range and live queries

 {s}`RaphtoryClient` should not be created directly. To create a {s}`RaphtoryClient` use
 {s}`Raphtory.createClient(deploymentID: String = "", customConfig: Map[String, Any] = Map())`.

 ## Methods

   {s}`pointQuery(graphAlgorithm: GraphAlgorithm, outputFormat: OutputFormat, timestamp: Long, windows: List[Long] = List()): QueryProgressTracker`
     : Point query for querying at a specific timestamp, returns [{s}`QueryProgressTracker`](com.raphtory.core.components.querytracker.QueryProgressTracker)

       {s}`graphAlgorithm: GraphAlgorithm`
          : Graph algorithm to use for query

       {s}`outputFormat: OutputFormat`
          : Type of output format for storing results

       {s}`timestamp: Long`
          : Timestamp of query

       {s}`windows: List[Long] = List()`
          : If specified, run query for each `window` in `windows`, restricted to data between `timestamp - window` and `timestamp`

   {s}`rangeQuery(graphAlgorithm: GraphAlgorithm, outputFormat: OutputFormat, start: Long, end: Long, increment: Long, windows: List[Long] = List()): QueryProgressTracker`
     : Queries for a range of timestamps, returns [{s}`QueryProgressTracker`](com.raphtory.core.components.querytracker.QueryProgressTracker)

       {s}`graphAlgorithm: GraphAlgorithm`
          : Graph algorithm to use for query

       {s}`outputFormat: OutputFormat`
          : Type of output format for storing results

       {s}`start: Long`
          : Start timestamp of duration for query

       {s}`end: Long`
          : End timestamp of duration for query

       {s}`increment: Long`
          : Step size of duration for re-running range query

       {s}`windows: List[Long] = List()`
          : If specified, run query for each `window` in `windows`, restricted to data between `timestamp - window` and `timestamp`

   {s}`liveQuery(graphAlgorithm: GraphAlgorithm, outputFormat: OutputFormat, increment: Long, windows: List[Long] = List()): QueryProgressTracker`
     : Runs query for latest available timestamp, returns [{s}`QueryProgressTracker`](com.raphtory.core.components.querytracker.QueryProgressTracker)

       {s}`graphAlgorithm: GraphAlgorithm`
          : Graph algorithm to use for query

       {s}`outputFormat: OutputFormat`
          : Type of output format for storing results

       {s}`increment: Long`
          : Step size of duration for re-running range query

       {s}`windows: List[Long] = List()`
          : If specified, run query for each `window` in `windows`, restricted to data between `timestamp - window` and `timestamp`

   {s}`getConfig(): Config`
     : Fetch raphtory config

Usage while querying:

```{code-block} scala
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.output.FileOutputFormat
import com.raphtory.core.algorithm.OutputFormat
import com.raphtory.core.components.graphbuilder.GraphBuilder
import com.raphtory.core.components.spout.Spout

val graph = Raphtory.createGraph[T](spout: Spout[T], graphBuilder: GraphBuilder[T])
val testDir  =  "/tmp/raphtoryTest"
val outputFormat: FileOutputFormat  =  FileOutputFormat(testDir)

// Run Point Query:
graph.pointQuery(EdgeList(), outputFormat, timestamp: Long)

// Run Range Query and wait until done:
val queryProgressTracker = graph.rangeQuery(graphAlgorithm = EdgeList(), outputFormat = outputFormat, start = 1, end = 32674, increment = 10000, windows = List(500, 1000, 10000))
queryProgressTracker.waitForJob()

```

 ```{seealso}
 [](com.raphtory.core.algorithm.GraphAlgorithm), [](com.raphtory.core.components.querytracker.QueryProgressTracker),
 [](com.raphtory.output.FileOutputFormat), [](com.raphtory.output.PulsarOutputFormat),
 [](com.raphtory.core.deploy.Raphtory)
 ```