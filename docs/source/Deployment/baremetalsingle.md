# Running Raphtory distributed

Raphtory can be deployed as a set of multiple services in order to better take advantage of distributed resources, and flexibly scale horizontally its functionality. In order to show how this setup works, we will go over a base distributed scenario.


## Bare metal distributed -- Raphtory services

In our previous examples, we have been running Raphtory via the `RaphtoryGraph`
instance. This is used for local and single machine development, as the `RaphtoryGraph` instance
initiates all the necessary components. This, of course, does not provide you with granular control over the system. For example, you may feel that the spout requires
much less RAM than the graph builder or partition manager. You may also want to 
run the client across a cluster, whereby each machine runs a different component, which aids
with scaling to larger datasets and analytical tasks. 

For these reasons, you can run your programs as `RaphtoryService`(s) instead. 
The service file gives the user more granular control over each individual component. 
With the service, the user is also able to send analysis queries on-demand. 
For example, you can run the service, ingest the entire graph, and then run multiple
queries or leave the machine running for other users. 

From a coding point of view, the approach is similar to using `RaphtoryGraph`; you only have to specify the spout and builder classes required to read and parse the data. For instance, the code below will create and run the LOTR example as a distributed service. 

```scala

import com.raphtory.core.build.server.RaphtoryService
import com.raphtory.core.components.graphbuilder.GraphBuilder
import com.raphtory.core.components.spout.Spout
import com.raphtory.spouts.FileSpout

object LOTRDistributed extends RaphtoryService[String]{

  override def defineSpout: Spout[String] = 
    new FileSpout("src/main/scala/com/raphtory/dev/lotr", "lotr.csv")

  override def defineBuilder: GraphBuilder[String] = 
    new LOTRGraphBuilder()
}
```
To run the code above, rather than starting a single Raphtory program, you will deploy multiple services providing the different components. The following services must be individually started. 

* ´spout´ - Ingests the data
* ´builder´ - Defines how to build the graph from the data
* ´partitionmanager´ - Stores the live graph and all the history 
* ´querymanager´ - Accepts new analysis queries and runs them across the partitions

You start each service through either scala or sbt, selecting your implementation of `RaphtoryService` as the main class, and specifying one additional command-line argument selecting what service you wish to start. You need all these services running for Raphtory to work, although if the data will be loaded in batch mode, you can start the ´alinonepm´ service that combines spout, builder and partitionmanager in the same service deployment.  

The Raphtory services  communicate with each other using Pulsar. If you do not have a pulsar broker, you can run one locally. See [pulsar local deployment](pulsarlocal.md) for information on different deployment options and the required install steps.

## Service configuration Variables

Raphtory Services need the location of Pulsar in order to properly connect among each other. This is achieved by setting up the following environment variables (we provide in this page the values for connecting to a local Pulsar deployment with its default values). 

```bash
export RAPHTORY_PULSAR_BROKER_ADDRESS = "pulsar://127.0.0.1:6650"
export RAPHTORY_PULSAR_ADMIN_ADDRESS = "http://127.0.0.1:8080"
export RAPHTORY_ZOOKEEPER_ADDRESS = "127.0.0.1:2181"
```

### Optional Variables

In order to configure how many resources are allocated, each service should be run whilst specifying the following JAVA_OPTS.
Please adjust the `Xms` and `Xmx` to the amount of free memory on the box.  [This helps A LOT with GC]

```bash
export JAVA_OPTS=-XX:+UseShenandoahGC -XX:+UseStringDeduplication -Xms10G -Xmx10G -Xss128M
```

If using the parquet reader, we also recommend installing hadoop and adding the HOME and OPTs to the system environment ;

```bash
Environment='HADOOP_HOME=/usr/local/bin/hadoop-3.3.1'
Environment="HADOOP_OPTS=-Djava.library.path=/usr/local/bin/hadoop-3.3.1/lib/native"
Environment="PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin:/usr/local/bin/hadoop-3.3.1/bin/:/usr/local/bin/hadoop-3.3.1/sbin/
```

# Bare metal

The difference between the two from an analysis perspective is that `load` will block any submitted queries until the data has finished ingesting - allowing it to handle completely out of order data. On the other hand when using `stream` it is assumed new data is continuously arriving (in roughly chronological order) which Raphtory handles with a watermarking heuristic to decide what time is safe to analyse across all the partitions. Therefore, queries where the perspective time fully ingested and synchronised allowed to progress and which should be blocked until the time they are set to run at has arrived and has been synchronised.

## Bare metal single node

Set up dependencies

To run Raphtory locally on a macbook/laptop there are several ways this can be achieved
- Intelij IDE
- Local java process 
- Minikube - See [ kubernetes deployment ](kubernetes.md)



## Sending analysis queries to Raphtory with a client


Finally, if you have a graph deployed somewhere else and want to submit new queries to it you can do this via the `connect(customConfig)` method in the `Raphtory` object. The `customConfig` here is to provide the appropriate configuration to locate the graph (i.e. the akka/pulsar address). If the graph is deployed in the same machine using the default Raphtory configuration you can omit this configuration parameter:

```scala
val graph = Raphtory.connect()
```

From this point, you can keep working with your graph as we have done so far.
