`com.raphtory.core.deploy.Raphtory`
(com.raphtory.core.deploy.Raphtory)=
# Raphtory

{s}`Raphtory`
 : `Raphtory` object for creating Raphtory Components

## Methods

  {s}`createGraph(spout: Spout[T] = new IdentitySpout[T](), graphBuilder: GraphBuilder[T], customConfig: Map[String, Any] = Map()): RaphtoryGraph[T]`
   : Creates Graph using spout, graph-builder and custom config. Returns {s}`RaphtoryGraph` object for the user to run queries. {s}`customConfig` is a key-value mapping of Raphtory parameters for Pulsar and components like partitions, etc. Refer to the example usage below.

  {s}`createClient(deploymentID: String = "", customConfig: Map[String, Any] = Map())`
   : Create Client to expose APIs for running point, range and live queries for graph algorithms in Raphtory.
     Client is for a {s}`deploymentID` and config {s}`customConfig` of parameters eg. Pulsar endpoint as illustrated in the example usage

  {s}`createSpout(spout: Spout[T])`
   : Creates {s}`Spout` to read or ingest data from resources or files, sending messages to builder producers for each row. Supported spout types are {s}FileSpout`, {s}`ResourceSpout`, {s}`StaticGraphSpout`.

  {s}`createGraphBuilder(builder: GraphBuilder[T])`
   : Creates {s}`GraphBuilder` for creating a Graph by adding and deleting vertices and edges. {s}`GraphBuilder` processes the data ingested by the spout as tuples of rows to build the graph

  {s}`createPartitionManager()`
   : Creates {s}`PartitionManager` for creating partitions as distributed storage units with readers and writers. Uses Zookeeper to create partition IDs

  {s}`createQueryManager()`
   : Creates {s}`QueryManager` for spawning, handling and tracking queries. Query types supported include {s}`PointQuery`, {s}`RangeQuery` and {s}`LiveQuery`

  {s}`getDefaultConfig(customConfig: Map[String, Any] = Map()): Config`
   : Returns default config using {s}`ConfigFactory` for initialising parameters for running Raphtory components. This uses the default application parameters

  {s}`confBuilder(customConfig: Map[String, Any] = Map()): Config`
   : Creates {s}`Config` by using the input map {s}`customConfig`

  {s}`createSpoutExecutor(spout: Spout[T], conf: Config, pulsarController: PulsarController): SpoutExecutor[T]]`
   : Create spout executor for ingesting data from resources and files. Supported executors include `FileSpoutExecutor`, `StaticGraphSpoutExecutor`


Example Usage:

```{code-block} scala

import com.raphtory.core.deploy.Raphtory
import com.raphtory.lotrtest.LOTRGraphBuilder
import com.raphtory.core.components.spout.instance.ResourceSpout
import com.raphtory.GraphState
import com.raphtory.output.FileOutputFormat

val customConfig = Map(("raphtory.pulsar.endpoint", "localhost:1234"))
Raphtory.createClient("deployment123", customConfig)
val graph = Raphtory.createGraph(ResourceSpout("resource"), LOTRGraphBuilder())
graph.rangeQuery(GraphState(),FileOutputFormat("/test_dir"),1, 32674, 10000, List(500, 1000, 10000))

```

 ```{seealso}
 [](com.raphtory.core.components.graphbuilder.GraphBuilder),
 [](com.raphtory.core.components.spout.Spout)
 ```