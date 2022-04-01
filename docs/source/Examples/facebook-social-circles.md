# Social Circles in Facebook Networks

## Overview

We have created an [example](https://github.com/Raphtory/Examples/tree/0.5.0/raphtory-example-facebook) to identify Facebook social circles in Raphtory. The data is from [SNAP](https://snap.stanford.edu/data/ego-Facebook.html). This dataset has 4039 nodes and 88234 edges, collected from survey participants using the Facebook app.

The ability to organise vertices based on connections can be very useful. For example, being able to filter content so that you can only see your close friend's updates, or for privacy reasons such as hiding information from specific people. Currently, social media platforms such as Facebook, Instagram and Twitter, require users to identify their social circles manually. Raphtory has the ability to form social circles automatically as a user adds more friends. In our [Facebook](https://github.com/Raphtory/Examples/tree/0.5.0/raphtory-example-facebook) example, we are using `ConnectedComponents` to organise vertices that are connected to each other. `EdgeList` writes out the edge list of the ingested data. After data ingestion, a network graph of all the individual nodes and edges is created in Jupyter notebook for further analysis.

## Project Overview

This example builds a graph from the [Snap Facebook dataset](https://snap.stanford.edu/data/ego-Facebook.html).

We have uploaded the combined facebook dataset of all 10 networks [here](https://github.com/Raphtory/Data/blob/main/facebook.csv). Each line contains user A and user B, where user A is friends with user B (undirected). In the examples folder, you will find `StaticGraphSpout.scala` which pulls and downloads the data file from github into a temporary path on your local drive `/tmp/facebook.csv`. This file will then be read via Raphtory and built into a graph.

Also in the examples folder, you will find `FacebookGraphBuilder.scala` and `Runner.scala`.

* `FacebookGraphBuilder.scala` builds the graph
* `Runner.scala` runs the application including the analysis

We have also included python scripts in directory`src/main/python` to output the dataframes straight into Jupyter notebook.

## IntelliJ setup guide

As of February 2022, this is a guide to run this within IntelliJ.

1. From https://adoptopenjdk.net/index.html download OpenJDK 11 (LTS) with the HotSpot VM.
2. Enable this as the project SDK under File > Project Structure > Project Settings > Project > Project SDK.
3. Create a new configuration as an `Application` , select Java 11 as the build, and `com.raphtory.examples.facebook.Runner` as the class, add the Environment Variables too.

## Running this example

1. This example project is up on Github: [raphtory-example-facebook](https://github.com/Raphtory/Examples/tree/0.5.0/raphtory-example-facebook). If you have downloaded the Examples folder from the installation guide previously, then the Facebook example will already be set up. If not, please return [there](../Install/installdependencies.md) and complete this step first. 
2. In the Examples folder, open up the directory `raphtory-example-facebook` to get this example running.
3. Install all the python libraries necessary for visualising your data via the [Jupyter Notebook Tutorial](../PythonClient/tutorial.md). Once you have Jupyter Notebook up and running on your local machine, you can open up the Jupyter Notebook specific for this project, with all the commands needed to output your graph. This can be found by following the path `src/main/python/FacebookJupyterNotebook.ipynb`.
4. You are now ready to run this example. You can either run this example via Intellij by running the class `Runner.scala` or [via sbt](../Install/installdependencies.md#running-raphtory-via-sbt).
5. Once your job has finished, you are ready to go onto Jupyter Notebook and run your analyses/output.

## Output

Terminal Output snippet from the queries:
```bash
16:49:22.453 [main] INFO  com.raphtory.core.config.ComponentFactory - Creating '2' Partition Managers.
16:49:25.332 [main] INFO  com.raphtory.core.config.ComponentFactory - Creating new Query Manager.
16:49:25.606 [main] INFO  com.raphtory.core.config.ComponentFactory - Creating new Spout 'raphtory_data_raw_350150610'.
16:49:25.607 [main] INFO  com.raphtory.core.config.ComponentFactory - Creating '2' Graph Builders.
16:49:25.607 [monix-computation-46] INFO  com.raphtory.core.components.spout.executor.StaticGraphSpoutExecutor - Reading data from '/tmp/facebook.csv'.
16:49:26.673 [main] INFO  com.raphtory.core.client.RaphtoryGraph - Created Graph object with deployment ID 'raphtory_350150610'.
16:49:26.673 [main] INFO  com.raphtory.core.client.RaphtoryGraph - Created Graph Spout topic with name 'raphtory_data_raw_350150610'.
16:49:46.939 [main] INFO  com.raphtory.core.config.ComponentFactory - Creating new Query Progress Tracker for deployment 'raphtory_350150610' and job 'EdgeList_1646239786675' at topic 'raphtory_350150610_EdgeList_1646239786675'.
16:49:46.948 [main] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Starting query progress tracker.
16:49:47.034 [pulsar-external-listener-27-1] INFO  com.raphtory.core.components.querymanager.QueryManager - Point Query 'EdgeList_1646239786675' received, your job ID is 'EdgeList_1646239786675'.
16:49:47.198 [main] INFO  com.raphtory.core.config.ComponentFactory - Creating new Query Progress Tracker for deployment 'raphtory_350150610' and job 'ConnectedComponents_1646239786948' at topic 'raphtory_350150610_ConnectedComponents_1646239786948'.
16:49:47.199 [main] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Starting query progress tracker.
16:49:48.376 [pulsar-external-listener-27-1] INFO  com.raphtory.core.components.querymanager.QueryManager - Range Query 'ConnectedComponents_1646239786948' received, your job ID is 'ConnectedComponents_1646239786948'.
16:49:56.411 [pulsar-external-listener-27-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job 'ConnectedComponents_1646239786948': Perspective '10000' with window '10000' finished in 9212 ms.
16:49:56.412 [pulsar-external-listener-27-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job ConnectedComponents_1646239786948: Running query, processed 1 perspectives.
16:50:00.613 [pulsar-external-listener-27-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job 'ConnectedComponents_1646239786948': Perspective '10000' with window '1000' finished in 4201 ms.
16:50:00.613 [pulsar-external-listener-27-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job ConnectedComponents_1646239786948: Running query, processed 2 perspectives.
16:50:03.838 [pulsar-external-listener-27-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job 'ConnectedComponents_1646239786948': Perspective '10000' with window '500' finished in 3225 ms.
16:50:03.838 [pulsar-external-listener-27-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job ConnectedComponents_1646239786948: Running query, processed 3 perspectives.
16:50:09.502 [pulsar-external-listener-27-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job 'ConnectedComponents_1646239786948': Perspective '20000' with window '10000' finished in 5664 ms.
16:50:09.502 [pulsar-external-listener-27-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job ConnectedComponents_1646239786948: Running query, processed 4 perspectives.
16:50:14.066 [pulsar-external-listener-27-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job 'ConnectedComponents_1646239786948': Perspective '20000' with window '1000' finished in 4564 ms.
16:50:14.067 [pulsar-external-listener-27-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job ConnectedComponents_1646239786948: Running query, processed 5 perspectives.
16:50:16.269 [pulsar-external-listener-27-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job 'EdgeList_1646239786675': Perspective '88234' finished in 29321 ms.
16:50:16.269 [pulsar-external-listener-27-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job EdgeList_1646239786675: Running query, processed 1 perspectives.
16:50:16.353 [pulsar-external-listener-27-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job EdgeList_1646239786675: Query completed with 1 perspectives and finished in 29405 ms.
16:50:18.431 [pulsar-external-listener-27-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job 'ConnectedComponents_1646239786948': Perspective '20000' with window '500' finished in 4364 ms.
16:50:18.431 [pulsar-external-listener-27-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job ConnectedComponents_1646239786948: Running query, processed 6 perspectives.
...
16:51:39.237 [pulsar-external-listener-27-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job ConnectedComponents_1646239786948: Running query, processed 26 perspectives.
16:51:45.983 [pulsar-external-listener-27-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job 'ConnectedComponents_1646239786948': Perspective '88234' with window '500' finished in 6746 ms.
16:51:45.984 [pulsar-external-listener-27-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job ConnectedComponents_1646239786948: Running query, processed 27 perspectives.
16:51:46.065 [pulsar-external-listener-27-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job ConnectedComponents_1646239786948: Query completed with 27 perspectives and finished in 118866 ms.
```
Terminal Output log to show query and job has finished: 

```bash
16:51:46.065 [pulsar-external-listener-27-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job ConnectedComponents_1646239786948: Query completed with 27 perspectives and finished in 118866 ms.
```
EdgeList Sample Data

```python
time    src_id  dst_id
88234	1	   194
88234	1	   322
88234	1	   133
88234	1	   73
88234	1	   299
...	...	...	...
88234	116	   149
88234	116	   214
88234	116	   326
88234	116	   343
88234	116	   312
```
ConnectedComponents Sample Data
```python
time    window  vertex_id   component_size
10000	10000	1	    0
10000	10000	3	    0
10000	10000	5	    0
10000	10000	7	    0
10000	10000	9	    0
...	...	...	...	...
88234	500	3961	    3925
88234	500	3835	    3833
88234	500	3963	    3863
88234	500	3837	    3833
88234	500	3967	    3833
```
Edge List Network Graphs:

<p>
 <img src="../_static/facebook-ego-networks.png" width="700px" style="padding: 15px" alt="Facebook Raphtory Network Graph"/>
</p>

Zoomed In View of Graph:
<p>
 <img src="../_static/facebook-ego-networks-2.png" width="700px" style="padding: 15px" alt="Facebook Raphtory Network Graph Zoomed in"/>
</p>