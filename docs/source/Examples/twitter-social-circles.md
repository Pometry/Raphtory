# Social Circles in Twitter Networks

## Overview
We have created an [example](https://github.com/Raphtory/Examples/tree/0.5.0/raphtory-example-twittercircles) to identify Twitter social circles in Raphtory. The data is from [SNAP](https://snap.stanford.edu/data/ego-Twitter.html). This dataset has 81306 nodes and 1768149 edges, containing 1000 ego-networks, 4869 circles and 81362 users.

The ability to organise vertices based on connections can be very useful. For example, being able to filter content so that you can only see your close friend's updates, or for privacy reasons such as hiding information from specific people. Currently, social media platforms such as Facebook, Instagram and Twitter, require users to identify their social circles manually. Raphtory has the ability to form social circles automatically as a user adds more friends. In our [Twitter](https://github.com/Raphtory/Examples/tree/0.5.0/raphtory-example-twittercircles) example, we are using `ConnectedComponents` to organise vertices that are connected to each other. `EdgeList` writes out the edge list of the ingested data. After data ingestion, a network graph of all the individual nodes and edges is created in Jupyter notebook for further analysis.

## Project Overview

This example builds a temporal graph from the [Snap Social Circles: Twitter dataset](https://snap.stanford.edu/data/ego-Twitter.html).

We have uploaded the Snap Social Circles Twitter dataset [here](https://github.com/Raphtory/Data/blob/main/snap-twitter.csv). Each line contains user A and user B, where user A follows user B. In the examples folder, you will find StaticGraphSpout.scala which pulls and downloads the data file from github into a temporary path on your local drive /tmp/twitter.csv. This file will then be read via Raphtory and built into a graph.

Also, in the examples folder you will find `TwitterCirclesGraphBuilder.scala` and `Runner.scala`.

* `TwitterCirclesGraphBuilder.scala` builds the graph
* `Runner.scala` runs the application including the analysis

We have also included python scripts in directory`src/main/python` to output the dataframes straight into Jupyter notebook.
The example python scripts uses PyMotif and outputs all the connections in the Twitter social circles onto a graph.

## IntelliJ setup guide

As of 9th February 2022. This is a guide to run this within IntelliJ.

1. From https://adoptopenjdk.net/index.html download OpenJDK 11 (LTS) with the HotSpot VM.
2. Enable this as the project SDK under File > Project Structure > Project Settings > Project > Project SDK.
3. Create a new configuration as an `Application` , select Java 11 as the build, and `com.raphtory.examples.twittercircles.Runner` as the class, add the Environment Variables too.

## Running this example

1. This example project is up on Github: [raphtory-example-twittercircles](https://github.com/Raphtory/Examples/tree/0.5.0/raphtory-example-twittercircles). If you have downloaded the Examples folder from the installation guide previously, then the Twitter Circles example will already be set up. If not, please return [there](../Install/installdependencies.md) and complete this step first. 
2. In the Examples folder, open up the directory `raphtory-example-twittercircles` to get this example running.
3. Install all the python libraries necessary for visualising your data via the [Jupyter Notebook Tutorial](../PythonClient/tutorial_pulsar.md). Once you have Jupyter Notebook up and running on your local machine, you can open up the Jupyter Notebook specific for this project, with all the commands needed to output your graph. This can be found by following the path `src/main/python/TwitterCirclesJupyterNotebook.ipynb`.
4. You are now ready to run this example. You can either run this example via Intellij by running the class `Runner.scala` or [via sbt](../Install/installdependencies.md#running-raphtory-via-sbt).
5. Once your job has finished, you are ready to go onto Jupyter Notebook and run your analyses/output.

## Output

Terminal output when starting up your job:
```bash
15:00:55.352 [main] INFO  com.raphtory.core.config.ComponentFactory - Creating '2' Partition Managers.
15:00:58.402 [main] INFO  com.raphtory.core.config.ComponentFactory - Creating new Query Manager.
15:00:58.699 [main] INFO  com.raphtory.core.config.ComponentFactory - Creating new Spout 'raphtory_data_raw_1056048736'.
15:00:58.699 [main] INFO  com.raphtory.core.config.ComponentFactory - Creating '2' Graph Builders.
15:00:58.699 [monix-computation-42] INFO  com.raphtory.core.components.spout.executor.StaticGraphSpoutExecutor - Reading data from '/tmp/snap-twitter.csv'.
15:00:59.826 [main] INFO  com.raphtory.core.client.RaphtoryGraph - Created Graph object with deployment ID 'raphtory_1056048736'.
15:00:59.826 [main] INFO  com.raphtory.core.client.RaphtoryGraph - Created Graph Spout topic with name 'raphtory_data_raw_1056048736'.
15:01:20.240 [main] INFO  com.raphtory.core.config.ComponentFactory - Creating new Query Progress Tracker for deployment 'raphtory_1056048736' and job 'EdgeList_1646233279838' at topic 'raphtory_1056048736_EdgeList_1646233279838'.
15:01:20.241 [main] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Starting query progress tracker.
15:01:20.371 [pulsar-external-listener-9-1] INFO  com.raphtory.core.components.querymanager.QueryManager - Point Query 'EdgeList_1646233279838' received, your job ID is 'EdgeList_1646233279838'.
15:01:20.600 [main] INFO  com.raphtory.core.config.ComponentFactory - Creating new Query Progress Tracker for deployment 'raphtory_1056048736' and job 'ConnectedComponents_1646233280241' at topic 'raphtory_1056048736_ConnectedComponents_1646233280241'.
15:01:20.600 [main] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Starting query progress tracker.
```
Terminal output to indicate that your Connected Components algorithm is running:
```bash
15:01:21.797 [pulsar-external-listener-9-1] INFO  com.raphtory.core.components.querymanager.QueryManager - Range Query 'ConnectedComponents_1646233280241' received, your job ID is 'ConnectedComponents_1646233280241'.
15:01:33.803 [pulsar-external-listener-9-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job 'ConnectedComponents_1646233280241': Perspective '10000' with window '10000' finished in 13203 ms.
15:01:33.803 [pulsar-external-listener-9-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job ConnectedComponents_1646233280241: Running query, processed 1 perspectives.
15:01:37.921 [pulsar-external-listener-9-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job 'ConnectedComponents_1646233280241': Perspective '10000' with window '1000' finished in 4118 ms.
15:01:37.921 [pulsar-external-listener-9-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job ConnectedComponents_1646233280241: Running query, processed 2 perspectives.
15:01:41.769 [pulsar-external-listener-9-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job 'ConnectedComponents_1646233280241': Perspective '10000' with window '500' finished in 3848 ms.
15:01:41.769 [pulsar-external-listener-9-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job ConnectedComponents_1646233280241: Running query, processed 3 perspectives.
15:01:46.741 [pulsar-external-listener-9-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job 'ConnectedComponents_1646233280241': Perspective '20000' with window '10000' finished in 4972 ms.
15:01:46.742 [pulsar-external-listener-9-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job ConnectedComponents_1646233280241: Running query, processed 4 perspectives.
15:01:50.909 [pulsar-external-listener-9-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job 'ConnectedComponents_1646233280241': Perspective '20000' with window '1000' finished in 4167 ms.
15:01:50.910 [pulsar-external-listener-9-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job ConnectedComponents_1646233280241: Running query, processed 5 perspectives.
15:01:55.073 [pulsar-external-listener-9-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job 'ConnectedComponents_1646233280241': Perspective '20000' with window '500' finished in 4163 ms.
15:01:55.073 [pulsar-external-listener-9-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job ConnectedComponents_1646233280241: Running query, processed 6 perspectives.
...
15:03:41.081 [pulsar-external-listener-9-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job ConnectedComponents_1646233280241: Running query, processed 27 perspectives.
15:03:41.165 [pulsar-external-listener-9-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job ConnectedComponents_1646233280241: Query completed with 27 perspectives and finished in 140565 ms.
```
This log indicates that your job has finished and you are ready to analyse the output in Jupyter:
```bash
15:03:41.165 [pulsar-external-listener-9-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job ConnectedComponents_1646233280241: Query completed with 27 perspectives and finished in 140565 ms.
```

EdgeList Sample Data
```python
time    src_id          dst_id
88234	65647594	50115087
88234	65647594	119897041
88234	65647594	65707359
88234	65647594	11348282
88234	65647594	11180212
...	...	...	...
88234	171145913	14169740
88234	171145913	17902348
88234	171145913	163937752
88234	233336165	31443503
88234	233336165	34868965
```
ConnectedComponents Sample Data
```python
time     window  vertex_id      component_size
10000	 10000	 203827751	2705
10000	 10000	 31457243	2367911
10000	 10000	 408060505      2367911
10000	 10000	 27077521	2367911
10000	 10000	 349256925	2705
...	...	...	...	...
88234	 500	 12340562	684023
88234	 500	 79011908	684023
88234	 500	 20437704	684023
88234	 500	 33235204	684023
88234	 500	 153823700      684023
```

Network graph of EdgeList
<p>
 <img src="../_static/twitter-egonets.png" width="700px" style="padding: 15px" alt="Twitter Raphtory Network Graph"/>
</p>
