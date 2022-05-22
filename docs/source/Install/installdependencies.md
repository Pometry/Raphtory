# Installation

Getting started with Raphtory only takes a few steps. We will first go through the different ways to install the packages required to run Raphtory.  We will then download the Raphtory library and attach this to an example project to check that everything is working correctly. This quick start tutorial is based around a Lord of the Rings example, but there are several other examples available that it will work for, all of which are explored in the *Examples Projects* section of this tutorial.

## Installing Java, Scala and SBT (Scala Build Tool)
Raphtory is a framework written in the [Scala](https://www.scala-lang.org) programming language, which runs on the JVM (Java virtual machine). As such both Java and Scala are required to run Raphtory. We additionally require SBT, the scala build tool, to compile and run your first Raphtory project. 

### Using SDK Man
Java, Scala and SBT are all very easy to install, but we must make sure that the correct versions are installed. To manage this we recommend the Software Development Kit Manager [SDK Man](https://sdkman.io/). This allows you to install and switch between versions of all these libraries. SDK Man is available for Mac, Linux and Windows and can be installed by following their simple tutorial [here](https://sdkman.io/install). 

Once you have this installed we will need to install a distribution of Java 11, Scala 13 and the latest version of SBT. Starting with java we can list the versions available via:

```bash
sdk list java
```

This will look something like the following:
<p align="center">
	<img src="../_static/install/sdkmanjava.png" alt="Sdk Man java lists"/>
</p>

The Vendor here (i.e. Corretto, GrallVM) doesn't matter really, we just need to make sure that the major version (the first number) is 11. You can then copy the Identifier into the following command. 

```bash
sdk install java IDENTIFIER
```

For instance: ```sdk install java 11.0.11.hs-adpt``` is the version I use

Once this is installed it should set this version as your default. You can then do the same for Scala and SBT  - which only have one distributor and hence only version numbers. I have picked the latest version of Scala 13 and SBT below at the time of writing.  

```bash 
sdk install scala 2.13.7
sdk install sbt 1.6.2
```

To test that these are installed and working correctly you can run them with the  `--version` argument to see if they are available on your class path and the correct version prints out. 

```bash 
java --version
scala -version
```

If the correct version hasn't been set as default you can do this explicitly via sdkman. This is also how you can change back to another version of these libraries for other projects.

```bash 
sdk use java 11.0.11.hs-adpt
sdk use scala 2.13.7
sdk use sbt 1.6.2
```

Everything should now be installed and ready for us to get your first Raphtory Job underway!

## Running the latest Raphtory release in an Example Project
All the example projects can be found in the [Raphtory repo](https://github.com/Raphtory/Raphtory). 

```bash
git clone https://github.com/Raphtory/Raphtory.git
git checkout THE_BRANCH_YOU_ARE_WORKING_ON
```

As we are using the Lord of the Rings example, we should now move into this directory - this is a totally independent sbt project. 

```bash
cd examples/raphtory-example-lotr
```

We need to firstly build the Raphtory jar into the Maven folder on your local computer by running this command in your root Raphtory directory:

```bash
sbt "core/publishLocal"
```


If you are working on this via Intellij you should give your sbt a refresh to reload the project locally and make sure it is using the new Raphtory jar. The button for this is the two rotating arrows located on the on the right hand side window within the sbt tab.  

If you make any changes to the core Raphtory code, you can go into your local ivy repo to delete the old jar and then re-publish using `sbt "core/publishLocal"`

```bash
cd .ivy2/local/com.raphtory
ls
rm (whichever jar you want to delete)
```

 Alternatively, if you are making several iterative changes, you can add a dependency `version := "0.1-SNAPSHOT"` in the examples `build.sbt` file and subsequent calls to `sbt publishLocal` will not require you to manually delete jars from your local repo. 

## Running Raphtory via SBT

### Compiling
 You can now use the command `sbt` to start the Scala Build Tool. Once you see either the `>` or `sbt:example-lotr>` prompt, it means that you are in the SBT interactive shell. You can now run `compile` to build the project. This should produce output similar to below if working correctly:

```bash
sbt:example-lotr> compile
[info] compiling 3 Scala sources to /Users/YOUR_USERNAME/github/Examples/raphtory-example-lotr/target/scala-2.13/classes ...
[success] Total time: 3 s, completed 2 Feb 2022, 13:30:49
sbt:example-lotr>
```

**Note:** If there are a million errors saying that classes are not part of the package `com.raphtory` this is probably because your `raphtory.jar` did not publish correctly or your refresh of the sbt project did not occur. Alternatively if you have errors saying that something cannot be referenced as a URI, this is a Java version issue (the version you are using is higher than 11) and you should set the correct version as above.


### Running
To test that you have Raphtory working properly on your machine, use the command `run` when prompted again with `>`. 

And you're done!  This will run the Lord of the Rings example that we will come back to in the next few tutorials. 

----

### Understanding the execution logs

First of all as Raphtory begins executing we should see some messages showing that the ingestion components of Raphtory are online and as such the system is ready for analysis to be performed. Don't worry about what these are for the second, we will go through this in the next section.

```bash
18:33:20.091 [ScalaTest-run] INFO  com.raphtory.core.config.ComponentFactory - Creating '2' Partition Managers.
18:33:23.095 [ScalaTest-run] INFO  com.raphtory.core.config.ComponentFactory - Creating new Query Manager.
18:33:23.444 [ScalaTest-run] INFO  com.raphtory.core.config.ComponentFactory - Creating new Spout 'raphtory_data_raw_1602187403'.
18:33:23.444 [ScalaTest-run] INFO  com.raphtory.core.config.ComponentFactory - Creating '2' Graph Builders.
18:46:13.393 [monix-computation-56] INFO  com.raphtory.core.components.spout.executor.StaticGraphSpoutExecutor - Reading data from '/tmp/facebook.csv'.
18:33:24.779 [ScalaTest-run] INFO  com.raphtory.core.client.RaphtoryGraph - Created Graph object with deployment ID 'raphtory_1602187403'.
18:33:24.779 [ScalaTest-run] INFO  com.raphtory.core.client.RaphtoryGraph - Created Graph Spout topic with name 'raphtory_data_raw_1602187403'.
18:33:25.438 [ScalaTest-run] INFO  com.raphtory.generic.PulsarOutputTest - Consumer created.
```

We should then see that the Partitions have started to ingest messages, meaning the data has been picked up and is being turned into a graph. In this example there are two partitions running, so we get output for both. 

Next, we should see that our query has been submitted. If the time we have asked for within the query has yet to be ingested, or is busy synchronising we will get a message informing us so, but that it will be resubmitted soon. Once the required timestamp is available, the analysis will be run. To manage these times Raphtory maintains a global watermark which reports the status of the partitions, and the time they believe is safe to execute on. These individual times are then aggregated into a global minimum time to make sure the results are always correct. The timestamp chosen for this query (`32670`) is just before the final timestamp in the file. 

Query submitted:
``` 
18:59:58.307 [pulsar-external-listener-9-1] INFO  com.raphtory.core.components.querymanager.QueryManager - Point Query 'EdgeList_1645815597845' received, your job ID is 'EdgeList_1645815597845'.
```

Data not fully ingested yet (only shows when logger level is set to 'debug'):
```
18:59:59.504 [pulsar-external-listener-9-1] DEBUG com.raphtory.core.components.querymanager.QueryHandler - Job 'EdgeList_1645815597845': Perspective 'Perspective(32674,None)' is not ready, currently at '270'.
```

Data was fully ingested and the query completed:
```
19:00:07.990 [pulsar-external-listener-9-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job 'EdgeList_1645815597845': Perspective '30000' finished in 9788 ms.
````

Finally we can see logs containing information about the Job ID, topics, perspectives, windows and the time it has taken to run.

```bash
18:59:58.202 [main] INFO  com.raphtory.core.config.ComponentFactory - Creating new Query Progress Tracker for deployment 'raphtory_1279440800' and job 'EdgeList_1645815597845' at topic 'raphtory_1279440800_EdgeList_1645815597845'.
18:59:58.202 [main] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Starting query progress tracker.
```
```
19:00:29.906 [pulsar-external-listener-9-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job 'PageRank_1645815598203': Perspective '20000' with window '10000' finished in 31390 ms.
19:00:29.907 [pulsar-external-listener-9-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job PageRank_1645815598203: Running query, processed 1 perspectives.
19:00:34.304 [pulsar-external-listener-9-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job 'PageRank_1645815598203': Perspective '20000' with window '1000' finished in 4397 ms.
19:00:34.304 [pulsar-external-listener-9-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job PageRank_1645815598203: Running query, processed 2 perspectives.
19:00:38.284 [pulsar-external-listener-9-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job 'PageRank_1645815598203': Perspective '20000' with window '500' finished in 3980 ms.
19:00:38.284 [pulsar-external-listener-9-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job PageRank_1645815598203: Running query, processed 3 perspectives.
19:01:02.343 [pulsar-external-listener-9-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job 'PageRank_1645815598203': Perspective '30000' with window '10000' finished in 24059 ms.
19:01:02.343 [pulsar-external-listener-9-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job PageRank_1645815598203: Running query, processed 4 perspectives.
19:01:08.120 [pulsar-external-listener-9-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job 'PageRank_1645815598203': Perspective '30000' with window '1000' finished in 5777 ms.
19:01:08.120 [pulsar-external-listener-9-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job PageRank_1645815598203: Running query, processed 5 perspectives.
19:01:12.685 [pulsar-external-listener-9-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job 'PageRank_1645815598203': Perspective '30000' with window '500' finished in 4565 ms.
19:01:12.686 [pulsar-external-listener-9-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job PageRank_1645815598203: Running query, processed 6 perspectives.
```

### Checking your output
Once the query has finished executing Raphtory will not stop running. This is because we may submit more queries to the running instance, now that it has ingested the graph. However, you may kill the Raphtory job and check out the output. For the example, queries should begin being saved to `/tmp` or the directory specified in the `Runner` class if you have changed it. Below is an example of the CSV file that has been output. This means that Raphtory is working as it should and you can move onto creating your first graph for analysis. The meaning of this output is only a couple pages away, so don't threat if it looks a little odd right now!


````
32670,  Odo,        2
32670,  Samwise,    1
32670,  Elendil,    2
32670,  Valandil,   2
32670,  Angbor,     2
32670,  Arwen,      2
32670,  Treebeard,  1
32670,  Óin,        3
32670,  Butterbur,  1
32670,  Finduilas,  2
32670,  Celebrimbor,2
32670,  Grimbeorn,  2
32670,  Lobelia,    2
32670,  Helm,       1
````
