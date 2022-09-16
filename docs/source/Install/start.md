# Installation

Raphtory has support for graph analytics in both Scala and Python.

The Scala library is called Raphtory. 

The Python library is called PyRaphtory. 

## Python (PyRaphtory)

Pyraphtory is the python client for Raphtory.

[Install with conda](python/install_conda.md)

[Install without conda](python/install_no_conda.md)

## Scala (Raphtory)

[Install with scala](scala/install.md)

For this tutorial we are using the Lord of the Rings example, so we should now move into this directory - this is a totally independent sbt project. 

```bash
cd examples/raphtory-example-lotr
```

### Compiling
 You can now use the command `sbt` to start the Scala Build Tool. Once you see either the `>` or `sbt:example-lotr>` prompt, it means that you are in the SBT interactive shell. You can now run `compile` to build the project. This should produce output similar to below if working correctly:

```bash
sbt:example-lotr> compile
[info] compiling 3 Scala sources to /Users/YOUR_USERNAME/github/Examples/raphtory-example-lotr/target/scala-2.13/classes ...
[success] Total time: 3 s, completed 2 Feb 2022, 13:30:49
sbt:example-lotr>
```

```{note} 
If there are a million errors saying that classes are not part of the package `com.raphtory` this is probably because your `raphtory.jar` did not publish correctly or your refresh of the sbt project did not occur. Alternatively if you have errors saying that something cannot be referenced as a URI, this is a Java version issue (the version you are using is higher than 11) and you should set the correct version as above.
```

### Running
To test that you have Raphtory working properly on your machine, use the command `run` when prompted again with `>`. From this you should get a request to select a main class (as can be seen below). Ignore the `PulsarRunner`,  `LOTRClient` and `LOTRService` for now, these are used in the distributed deployment. We want to select number 4, the `TutorialRunner`.

<p align="center">
	<img src="../_static/install/sbtrunselect.png" alt="Main Classes within the LOTR example"/>
</p>

And you're done!  This will run the Lord of the Rings example that we will come back to in the next few tutorials. 

----

### Understanding the execution logs

First of all as Raphtory begins executing we should see some messages showing that the ingestion components of Raphtory are online and as such the system is ready for analysis to be performed. Don't worry about what these are for the second, we will go through this in the next section.

```bash
Enter number: 4
[info] running com.raphtory.examples.lotrTopic.TutorialRunner
23:11:33.381 [run-main-0] INFO  com.raphtory.spouts.FileSpout - Spout: Processing file 'lotr.csv' ...
23:11:33.884 [spawner-akka.actor.default-dispatcher-3] INFO  akka.event.slf4j.Slf4jLogger - Slf4jLogger started
23:11:34.808 [run-main-0] INFO  com.raphtory.internals.management.ComponentFactory - Creating '1' Partition Managers for raphtory_181811870.
23:11:34.856 [run-main-0] INFO  com.raphtory.internals.management.ComponentFactory - Creating new Query Manager.
23:11:34.862 [run-main-0] INFO  com.raphtory.internals.management.GraphDeployment - Created Graph object with deployment ID 'raphtory_181811870'.
23:11:34.862 [run-main-0] INFO  com.raphtory.internals.management.GraphDeployment - Created Graph Spout topic with name 'raphtory_data_raw_181811870'.
```

Next, we should see that our query has been submitted:
``` 
23:11:35.043 [run-main-0] INFO  com.raphtory.internals.management.ComponentFactory - Creating new Query Progress Tracker for 'DegreesSeparation_8822143620455242909'.
23:11:35.080 [spawner-akka.actor.default-dispatcher-3] INFO  com.raphtory.internals.components.querymanager.QueryManager - Query 'DegreesSeparation_8822143620455242909' received, your job ID is 'DegreesSeparation_8822143620455242909'.
23:11:35.157 [monix-computation-138] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job DegreesSeparation_8822143620455242909: Starting query progress tracker.
```

Finally, once the data has completed ingesting, the query can run. The logs for this contain information about the Job ID, topics, perspectives, windows and the time it has taken to run.
```
23:11:37.247 [spawner-akka.actor.default-dispatcher-9] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job 'DegreesSeparation_8822143620455242909': Perspective '32674' finished in 2202 ms.
23:11:37.247 [spawner-akka.actor.default-dispatcher-9] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job DegreesSeparation_8822143620455242909: Running query, processed 1 perspectives.
23:11:37.255 [spawner-akka.actor.default-dispatcher-3] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job DegreesSeparation_8822143620455242909: Query completed with 1 perspectives and finished in 2210 ms.
````


### Checking your output
Once the query has finished executing Raphtory will not stop running. This is because we may submit more queries to the running instance, now that it has ingested the graph. However, you may now kill the Raphtory job and check out the output. For the example, results will be saved to `/tmp/raphtory` or the directory specified in the `TutorialRunner` class if you have changed it. Below is an example of the CSV file that has been output. This means that Raphtory is working as it should and you can move onto creating your first graph for analysis. The meaning of this output is only a couple pages away, so don't threat if it looks a little odd right now!


````
32674,Hirgon,1
32674,Hador,3
32674,Horn,2
32674,Galadriel,1
32674,Isildur,1
32674,Mablung,2
32674,Gram,2
32674,Thingol,2
32674,Celebrían,3
32674,Gamling,2
32674,Déagol,2
32674,Findegil,2
32674,Brand,-1
32674,Baldor,2
32674,Helm,1
32674,Thengel,1
32674,Gil-galad,2
````

## Editing Raphtory core

```{note}
If you are just building applications on top of Raphtory this section can be skipped as it discusses how to publish a new local version of Raphtory-core which can be used by your apps. 
```

Once you have cloned the raphtory repo and made your changes you may build the new Raphtory jar and publish into the Maven folder on your local computer by running this command in your root Raphtory directory:

```bash
sbt "core/publishLocal"
```

If you are working on your application via Intellij you should give your sbt a refresh to reload the project locally and make sure it is using the new Raphtory jar. The button for this is the two rotating arrows located on the right hand side window within the sbt tab.  

If you make any more changes to the core Raphtory code, you can go into your local ivy repo to delete the old jar and then re-publish using `sbt "core/publishLocal"`.  Alternatively, if you are making several iterative changes, you can add a dependency `version := "0.1-SNAPSHOT"` in the core `build.sbt` file and subsequent calls to `sbt publishLocal` will not require you to manually delete jars from your local repo. The Raphtory dependency in your project will need to be updated accordingly however.

```bash
cd ~/.ivy2/local/com.raphtory
ls
rm (whichever jar you want to delete)
```