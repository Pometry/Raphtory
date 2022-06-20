# Deploying Raphtory Bare Metal

## Single Machine
Once you are happy that your data source is ingesting properly through the selected `Spout` and the model you are creating through your `Graph Builder` is fit for purpose, we may compile your code into a deployable jar. This is a simple process utilising SBT and allows you to deploy this code anywhere you can copy the Jar to. For this tutorial we will be building a `fat` jar which includes all of the underlying packages (including Raphtory). This is standalone and requires nothing else to run other than java/scala.

To do this we will use the sbt assembly plugin, which must be added to the `project/plugins.sbt` file in your sbt project. This has already been done inside the [raphtory-example-lotr](https://github.com/Raphtory/Raphtory/tree/master/examples/raphtory-example-lotr) project, and should look like the following:

```
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "1.1.0")
```

Once the plugin has been added you can simply run `sbt assembly` from your projects main directory. This will compile all the classes, deduplicate any class files from the underlying imports and produce you a runnable fat jar. This will be available in your project under `target/scala-2.13/PROJECT_NAME-assembly-PROJECT_VERSION.jar`. For instance you can see the jar for the LOTR example below:

```
[warn] multiple main classes detected: run 'show discoveredMainClasses' to see the list
[info] Strategy 'concat' was applied to 2 files (Run the task at debug level to see details)
[info] Strategy 'discard' was applied to 7 files (Run the task at debug level to see details)
[info] Strategy 'filterDistinctLines' was applied to 5 files (Run the task at debug level to see details)
[info] Strategy 'first' was applied to 4550 files (Run the task at debug level to see details)
[info] Strategy 'rename' was applied to 14 files (Run the task at debug level to see details)
[success] Total time: 58 s, completed 17 Jun 2022, 01:23:54

ls target/scala-2.13
classes                       example-lotr-assembly-0.5.jar
```

We can now run this jar in our terminal via scala by adding it onto the classpath and selecting the main function we wish to run. In the below example we run our modified `TutorialRunner` where we have swapped from `load` to `stream`. The output/log for this should be exactly the same as before.

```
scala -classpath examples/raphtory-example-lotr/target/scala-2.13/example-lotr-assembly-0.5.jar com.raphtory.examples.lotr.TutorialRunner

02:24:57.946 [main] INFO  com.raphtory.spouts.FileSpout - Spout: Processing file 'lotr.csv' ...
02:24:59.250 [main] INFO  com.raphtory.internals.management.ComponentFactory - Creating '1' Partition Managers for raphtory_57786302.
02:25:00.861 [main] INFO  com.raphtory.internals.management.ComponentFactory - Creating new Query Manager.
02:25:01.366 [main] INFO  com.raphtory.internals.management.ComponentFactory - Creating new Spout.
02:25:01.366 [main] INFO  com.raphtory.internals.management.ComponentFactory - Creating '1' Graph Builders.
02:25:01.506 [main] INFO  com.raphtory.internals.management.GraphDeployment - Created Graph object with deployment ID 'raphtory_57786302'.
02:25:01.506 [main] INFO  com.raphtory.internals.management.GraphDeployment - Created Graph Spout topic with name 'raphtory_data_raw_57786302'.
02:25:01.615 [main] INFO  com.raphtory.internals.management.ComponentFactory - Creating new Query Progress Tracker for 'DegreesSeparation_3184984120317965529'.
02:25:01.640 [io-compute-4] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job DegreesSeparation_3184984120317965529: Starting query progress tracker.
02:25:01.766 [pulsar-external-listener-4-1] INFO  com.raphtory.internals.components.querymanager.QueryManager - Query 'DegreesSeparation_3184984120317965529' received, your job ID is 'DegreesSeparation_3184984120317965529'.
02:25:06.891 [pulsar-external-listener-4-1] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job 'DegreesSeparation_3184984120317965529': Perspective '32674' finished in 5251 ms.
02:25:06.891 [pulsar-external-listener-4-1] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job DegreesSeparation_3184984120317965529: Running query, processed 1 perspectives.
02:25:06.992 [pulsar-external-listener-4-1] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job DegreesSeparation_3184984120317965529: Query completed with 1 perspectives and finished in 5352 ms.
```

```{note}
The deployment ID is provided in this log "Created Graph object with deployment ID 'raphtory_57786302'" -- This will be important in the next step.
```

## Attaching A Client To Submit Queries 
Once the graph is deployed and the data is ingested you may find you want to submit new queries to it, possibly requiring a code change. If we had to reassemble the jar and reingest the data every time this happened it would be a massive time sink. Instead we may deploy `client` code which connects to a running Raphtory deployment and submits new queries. 

Before running a client we must make one change when executing the TutorialRunner (or any other local deployment). To enable queries we must set the RAPHTORY_QUERY_LOCALENABLED environment variable to `true`. This swaps the control messages for analysis to Pulsar and enables clients to connect from anywhere:

```
export RAPHTORY_QUERY_LOCALENABLED=true
scala -classpath examples/raphtory-example-lotr/target/scala-2.13/example-lotr-assembly-0.5.jar com.raphtory.examples.lotr.TutorialRunner
```

Once queries are enabled and the runner is deployed, a client may connect via the `Raphtory.connect()` function. This returns a {scaladoc}`com.raphtory.api.analysis.graphview.TemporalGraphConnection` instance, exposing exactly the same API as `load()` and `stream()`. An example of such a client can be seen below from the [raphtory-example-lotr](https://github.com/Raphtory/Raphtory/tree/master/examples/raphtory-example-lotr) project. The client is setup to connect to the deployment, execute [Connected Components](com.raphtory.algorithms.generic.ConnectedComponents) on the latest time point, write the results to "/tmp/raphtory" and then disconnect and shutdown:

```scala 
object LOTRClient extends App {

  val client = Raphtory.connect()

  val output = FileSink("/tmp/raphtory")

  val progressTracker = client.execute(ConnectedComponents()).writeTo(output)
  
  progressTracker.waitForJob()
  
  client.disconnect()
}
```

To run this code (which for the purpose of this example is part of the same jar) we need to first let the client know what the deployment ID is so that it can connect to the correct Pulsar Topic. This can be done by setting the `RAPHTORY_DEPLOY_ID` environment variable and then running the class (I have used the deployment ID from above):

```
export RAPHTORY_DEPLOY_ID=raphtory_57786302
scala -classpath examples/raphtory-example-lotr/target/scala-2.13/example-lotr-assembly-0.5.jar com.raphtory.examples.lotr.LOTRClient
```

The tracking of this query will be output on the client terminal, but the deployment will also log the submission of the query. These can be seen side by side below:
<p align="center">
	<img src="../_static/deployment/serverclient.png" alt="Raphtory Client connecting to a deployment"/>
</p>

### Connecting To A Remote Deployment
If you want to run your Raphtory deployment on a remote box whilst connecting via a local client, this is completely fine, but requires a couple more configuration steps. Deployments and clients need to share a Pulsar topic in order to properly connect with each other and, therefore, need the IP/port of this service. This can achieved by setting up the following environment variables (we provide in this page the values for connecting to a local Pulsar deployment with its default values). 

```bash
export RAPHTORY_PULSAR_BROKER_ADDRESS = "pulsar://127.0.0.1:6650"
export RAPHTORY_PULSAR_ADMIN_ADDRESS = "http://127.0.0.1:8080"
export RAPHTORY_ZOOKEEPER_ADDRESS = "127.0.0.1:2181"
```

Alternatively you can do this via the `connect(customConfig)` method in the `Raphtory` object. The `customConfig` here is to provide the appropriate configuration to locate the graph (i.e. pulsar address). For instance, translating the above env vars into `customConfig` would look like the following:

```scala
val customConfig: Map[String, String] = Map(
          ("raphtory.pulsar.admin.address","http://127.0.0.1:8080"),
          ("raphtory.pulsar.broker.address","pulsar://127.0.0.1:6650"),
          ("raphtory.zookeeper.address", "127.0.0.1:2181")
  )
  
  val client = Raphtory.connect(customConfig)
```


```{note}
When the graph is deployed in the same machine using the default Raphtory configuration you can omit these configuration parameters which is why they are not in the `LOTRClient`. However, in all instances we need to set the deployment ID.
```

## Distributed
In all prior parts of this tutorial we have been discussing Raphtory running as a singular process on one machine. However, Raphtory can be deployed as a set of multiple services in order to better take advantage of distributed resources, and flexibly scale its functionality horizontally. In order to show how this works, we will go over a base distributed scenario.

In our previous examples, we have been running Raphtory via the {scaladoc}`com.raphtory.Raphtory` object. This is used for local and single machine development, as `.load()` and `.stream()` initiate all the necessary components. This, of course, does not provide you with granular control over the system. For example, you may feel that the spout requires much less RAM than the graph builder or partition manager. You may also want to run the client across a cluster, whereby each machine runs a different component, which aids with scaling to larger datasets and analytical tasks. 

For these reasons, you can run your components via {scaladoc}`com.raphtory.RaphtoryService` instead. From a coding point of view, the approach is similar to using the {scaladoc}`com.raphtory.Raphtory` object; you only have to specify the spout and builder classes required to read and parse the data. For instance, the code below will create and run the LOTR example as a distributed service. 

```scala
import com.raphtory.api.input.Spout
import com.raphtory.RaphtoryService
import com.raphtory.examples.lotrTopic.graphbuilders.LOTRGraphBuilder
import com.raphtory.spouts.FileSpout

object LOTRService extends RaphtoryService[String] {

  override def defineSpout(): Spout[String] = FileSpout("/tmp/lotr.csv")

  override def defineBuilder: LOTRGraphBuilder = new LOTRGraphBuilder()

}
```
To run the code above, rather than starting a single Raphtory program, you must deploy multiple services providing the different components. The following services must be individually started. 

* ´spout´ - Runs the specified spout.
* ´builder´ - Launches an instance of the specified Graph Builder.
* ´partitionmanager´ - Launches a partition to store a portion of the graph and run analytics on it.
* ´querymanager´ - Accepts new analysis queries and runs them across the partitions.

You can start each service through either scala or sbt, selecting your implementation of `RaphtoryService` as the main class and specifying which service you wish to start via a command-line argument . You need at least one of each service for Raphtory to work. The commands for running the LOTRService would be as follows:

```
scala -classpath examples/raphtory-example-lotr/target/scala-2.13/example-lotr-assembly-0.5.jar com.raphtory.examples.lotr.LOTRService spout
scala -classpath examples/raphtory-example-lotr/target/scala-2.13/example-lotr-assembly-0.5.jar com.raphtory.examples.lotr.LOTRService builder
scala -classpath examples/raphtory-example-lotr/target/scala-2.13/example-lotr-assembly-0.5.jar com.raphtory.examples.lotr.LOTRService partitionmanager
scala -classpath examples/raphtory-example-lotr/target/scala-2.13/example-lotr-assembly-0.5.jar com.raphtory.examples.lotr.LOTRService querymanager
```

Running these as 4 separate processes in a pseudo-distributed fashion and then submitting a query via the same client will produce the following output: 

<p align="center">
	<img src="../_static/deployment/distributed.png" alt="Pseudo-distributed Raphtory deployment"/>
</p>


```{note}
The warning "Cannot create prometheus server as port 9999 is already bound, this could be you have multiple raphtory instances running on the same machine." Is fairly self explanatory, but is caused by us running these services together for demonstration purposes when in production they would be on separate machines or containerized. 
```

As a final comment on the distributed deployment, as the partitions are stateful if you are intending to deploy more than one you need to let all components know how many there will be. This as before is done via environment variables, specifically `RAPHTORY_PARTITIONS_SERVERCOUNT`. 

### Suggested Java ops

In order to configure how many resources are allocated, each service should be run whilst specifying the following JAVA_OPTS.
Please adjust the `Xms` and `Xmx` to the amount of free memory on the box.  [This helps A LOT with GC]

```bash
export JAVA_OPTS=-XX:+UseShenandoahGC -XX:+UseStringDeduplication -Xms10G -Xmx10G -Xss128M
```




