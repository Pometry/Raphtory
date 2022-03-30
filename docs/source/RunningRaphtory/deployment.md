# Raphtory Cluster Deployment

Here we will describe how to deploy Raphtory onto a distributed cluster. 


## Raphtory Graph VS Raphtory Service

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

```json
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
## Running Each Component
Once you have your Service and machines defined and set up, you can run each service with the example commands below. Note that we use args(0) in the `RaphtoryService` superclass to define which component is being deployed, so do not use this for anything else.

#### Leader

```bash
scala -classpath raphtory.jar:YOUR_CODE.jar the.path.to.your.service leader
```

#### Spout

```bash
scala -classpath raphtory.jar:YOUR_CODE.jar the.path.to.your.service spout
```

#### QueryManager 

```bash
scala -classpath raphtory.jar:YOUR_CODE.jar the.path.to.your.service analysisManager
```

#### GraphBuilder 

```bash
scala -classpath raphtory.jar:YOUR_CODE.jar the.path.to.your.service builder
```

#### ParitionManager

```bash
scala -classpath raphtory.jar:YOUR_CODE.jar the.path.to.your.service partitionManager
```

Once your services have been deployed, you should see exactly the same output when running in `RaphtoryGraph` mode. The head node should report that all components have come online and that you are ready to perform analysis.

## Raphtory Client
The `RaphtoryClient` is very simple to implement and can be run compiled, as the Service code above, or via the Scala REPL. An example client may be seen below:

```scala
import com.raphtory.algorithms.ConnectedComponents
import com.raphtory.core.build.client.RaphtoryClient

object ExampleClient extends App {
  val client = new RaphtoryClient("leaderIP/hostname:1600",1700)
  client.pointQuery(ConnectedComponents("/tmp"),10000,List(10000, 1000,100))
}
```
Here we can see that the client has only two arguments. The first is the leader's `RAPHTORY_BIND_ADDRESS` and `RAPHTORY_BIND_PORT` combined into one string, seperated by a colon. Secondly is a port on the local machine with which to bind to - in this instance 1700 is chosen. 

The client then has the same `pointQuery` and `rangeQuery` functionality available to it as the RaphtoryGraph, and can be interacted with as such. Algorithms submitted this way will be sent across to the QueryManager and the results saved into each partition for retrieval. 

**Note:** Unfortunately you cannot submit brand new algorithms in this manner, only those which are availble within the Jars submitted with the Components previously. This is because of the manner in which closures (anonymous functions) work in Scala 2.12 and currently has no work around. This should hopefully be fixed in later versions of Raphtory!

## Whats next?
You have reached the end of the quick start tutorial! For any discussion of it, or help with your own projects using Raphtory, please join the [slack](https://join.slack.com/t/raphtory/shared_invite/zt-xbebws9j-VgPIFRleJFJBwmpf81tvxA).  