# Bare metal

The difference between the two from an analysis perspective is that `batchLoad` will block any submitted queries until the data has finished ingesting - allowing it to handle completely out of order data. On the other hand when using `stream` it is assumed new data is continuously arriving (in roughly chronological order) which Raphtory handles with a watermarking heuristic to decide what time is safe to analyse across all the partitions. Therefore, queries where the perspective time   fully ingested and synchronised allowed to progress and which should be blocked until the time they are set to run at has arrived and has been synchronised.


## Bare metal single node

Set up dependencies

If you do not have a pulsar broker, you can run one locally. See [ pulsar local deployment ](pulsarlocal.md) for information on different deployment options and the required install steps.

To run Raphtory locally on a macbook/laptop there are several ways this can be achieved
- Intelij IDE
- Local java process 
- Minikube - See [ kubernetes deployment ](kubernetes.md)



## Using Raphtory as a client

Finally, if you have a graph deployed somewhere else and want to submit new queries to it you can do this via the `deployedGraph(customConfig)` method in the `Raphtory` object. The `customConfig` here is to provide the appropriate configuration to locate the graph (i.e. the akka/pulsar address). If the graph is deployed in the same machine using the default Raphtory configuration you can omit this configuration parameter:

```scala
val graph = Raphtory.deployedGraph()
```

From this point, you can keep working with your graph as we have done so far.

Additionally, you still have access to the `RaphtoryClient` class provided in previous releases of Raphtory. This is, however, deprecated and will be removed in later versions:

```scala
val client = Raphtory.createClient()
client.pointQuery(ConnectedComponents(), output, 10000)
```

## Bare metal distributed -- Raphtory services

In our previous examples, we have been running Raphtory via the `RaphtoryGraph`
instance. This is used for local and single machine development, as the `RaphtoryGraph` instance
initiates all the necessary components. This, of course, does not provide
you with granular control over the system. For example, you may feel that the spout requires
much less RAM than the graph builder or partition manager. You may also want to 
run the client across a cluster, whereby each machine runs a different component, which aids
with scaling to larger datasets and analytical tasks. 

For these reasons, you can run your programs as `RaphtoryService`(s) instead. 
The service file gives the user more granular control over each individual component. 
With the service, the user is also able to send analysis queries on-demand. 
For example, you can run the service, ingest the entire graph, and then run multiple
queries or leave the machine running for other users. 


Similar to the `RaphtoryGraph`, the user only has to specify the spout and builder classes required to read and parse the data. For instance, the code below will create and run the LOTR example as a distributed service. 

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

To run the code above, one must start up the following components individually. 

* Leader - Manages new members joining and maintaining the global watermark. 
* Spout - Ingests the data
* GraphBuilders - Build the graph
* PartitionManager - Stores the live graph and all the history 
* QueryManager - Accepts new analysis queries and runs them across the partitions

## Environment Variables 

Each of the above components also expects a set of environment variables which must be present on all of the machines; 

```yaml
    RAPHTORY_BUILD_SERVERS = 2
    RAPHTORY_PARTITION_SERVERS = 2
    RAPHTORY_BUILDERS_PER_SERVER = 5
    RAPHTORY_PARTITIONS_PER_SERVER = 5
    
    RAPHTORY_BIND_ADDRESS= <The local machine IP>
    RAPHTORY_BIND_PORT=1600
    
    RAPHTORY_LEADER_ADDRESS = <The leader Machines IP>
    RAPHTORY_LEADER_PORT = 1600
```
The number of `RAPHTORY_BUILD_SERVERS` refers to how many graph builder services you have deployed - Similarly `RAPHTORY_PARTITION_SERVERS` refers to how many Partition Managers you have. These must be set properly or Raphtory may not work correctly. `RAPHTORY_BUILDERS_PER_SERVER` and `RAPHTORY_PARTITIONS_PER_SERVER` can be thought of as how many threads are running on each service and can be experimented with for performance. 

`RAPHTORY_BIND_ADDRESS` and `RAPHTORY_BIND_PORT` refer to the IP and port you wish the service to bind onto on the machine hosting it respectively. The value of these for the leader should be set in all machines via `RAPHTORY_LEADER_ADDRESS` and `RAPHTORY_LEADER_PORT`.

### Optional Variables

Each component should be run whilst specifying the following JAVA_OPTS.
Please adjust the `Xms` and `Xmx` to the amount of free memory on the box.  [This helps A LOT with GC]

```bash
JAVA_OPTS=-XX:+UseShenandoahGC -XX:+UseStringDeduplication -Xms10G -Xmx10G -Xss128M
```

If using the parquet reader, we also recommend installing hadoop and adding the HOME and OPTs to the system environment ;

```bash
Environment='HADOOP_HOME=/usr/local/bin/hadoop-3.3.1'
Environment="HADOOP_OPTS=-Djava.library.path=/usr/local/bin/hadoop-3.3.1/lib/native"
Environment="PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin:/usr/local/bin/hadoop-3.3.1/bin/:/usr/local/bin/hadoop-3.3.1/sbin/
```
