# Lord of the Rings Character Interactions

## Overview
This example is a dataset that tells us when two characters have some type of interaction in the Lord of the Rings trilogy books. It's a great dataset to test different algorithms or even your own written algorithms.

## Project Overview

This example has two outputs:
* FileOutputRunner builds a graph and uses the algorithm `DegreesSeparation.scala` which is explained [here](../Analysis/LOTR_six_degrees.md#six-degrees-of-gandalf). 
* PulsarOutputRunner builds a graph and runs two queries. The first is an edge list (`EdgeList.scala`) listing out all the edges that join source nodes and destination nodes of this dataset, this will be needed for graph visualisation in python. The second is a query to show us the various ranks of the characters across time (`PageRank.scala`). 

The data is a `csv` file (comma-separated values) is located in the `resources` folder. 
Each line contains two characters that appeared in the same sentence in the 
book, along with which sentence they appeared as indicated by a number. 
In the example, the first line of the file is `Gandalf,Elrond,33` which tells
us that Gandalf and Elrond appears together in sentence 33.

Also, in the examples folder you will find `LOTRGraphBuilder.scala`,`PulsarOutputRunner.scala` and `FileOutputRunner.scala`.

* `PulsarOutputRunner.scala` runs the application including the analysis and outputs to Pulsar.
* `FileOutputRunner.scala` runs the application including the analysis and outputs to a file directory.
* `LOTRGraphBuilder.scala` builds the graph.

Refer back to [Building a graph from your data](../Ingestion/sprouter.md) for a more in-depth explanation on how these parts work to build your graph.

## IntelliJ setup guide

As of February 2022, this is a guide to run this within IntelliJ.

1. From https://adoptopenjdk.net/index.html download OpenJDK 11 (LTS) with the HotSpot VM.
2. Enable this as the project SDK under File > Project Structure > Project Settings > Project > Project SDK.
3. Create a new configuration as an `Application` , select Java 11 as the build, and `com.raphtory.examples.lotr.FileOutputRunner` or `com.raphtory.examples.lotr.PulsarOutputRunner` as the class, add the Environment Variables too.

## Running this example

1. This example project is up on Github: [raphtory-example-lotr](https://github.com/Raphtory/Raphtory/tree/master/examples/raphtory-example-lotr). If you have downloaded the Examples folder from the installation guide previously, then the Lotr example will already be set up. If not, please return [there](../Install/installdependencies.md) and complete this step first. 
2. In the Examples folder, open up the directory `raphtory-example-lotr` to get this example running.
3. Install all the python libraries necessary for visualising your data via the [Jupyter Notebook Tutorial](../PythonClient/tutorial_py_raphtory.md). Once you have Jupyter Notebook up and running on your local machine, you can open up the Jupyter Notebook specific for this project, with all the commands needed to output your graph. This can be found by following the path `python/LOTR_demo.ipynb`.
4. You are now ready to run this example. You can either run this example via Intellij by running the class `com.raphtory.examples.lotr.FileOutputRunner` or `com.raphtory.examples.lotr.PulsarOutputRunner` or [via sbt](../Install/installdependencies.md#running-raphtory-via-sbt).
5. Once your job has finished, you are ready to go onto Jupyter Notebook and run your analyses/output.

## Output

Output in terminal when running `PulsarOutputRunner.scala`
```bash
12:23:03.765 [main] INFO  com.raphtory.core.config.ComponentFactory - Creating '2' Partition Managers.
12:23:07.241 [main] INFO  com.raphtory.core.config.ComponentFactory - Creating new Query Manager.
12:23:07.567 [main] INFO  com.raphtory.core.config.ComponentFactory - Creating new Spout 'raphtory_data_raw_887356734'.
12:23:07.567 [main] INFO  com.raphtory.core.config.ComponentFactory - Creating '2' Graph Builders.
12:23:08.849 [main] INFO  com.raphtory.core.client.RaphtoryGraph - Created Graph object with deployment ID 'raphtory_887356734'.
12:23:08.849 [main] INFO  com.raphtory.core.client.RaphtoryGraph - Created Graph Spout topic with name 'raphtory_data_raw_887356734'.
12:23:29.195 [main] INFO  com.raphtory.core.config.ComponentFactory - Creating new Query Progress Tracker for deployment 'raphtory_887356734' and job 'EdgeList_1646310208857' at topic 'raphtory_887356734_EdgeList_1646310208857'.
12:23:29.196 [main] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Starting query progress tracker.
12:23:29.302 [pulsar-external-listener-9-1] INFO  com.raphtory.core.components.querymanager.QueryManager - Point Query 'EdgeList_1646310208857' received, your job ID is 'EdgeList_1646310208857'.
12:23:29.541 [main] INFO  com.raphtory.core.config.ComponentFactory - Creating new Query Progress Tracker for deployment 'raphtory_887356734' and job 'PageRank_1646310209197' at topic 'raphtory_887356734_PageRank_1646310209197'.
12:23:29.542 [main] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Starting query progress tracker.
12:23:30.959 [pulsar-external-listener-9-1] INFO  com.raphtory.core.components.querymanager.QueryManager - Range Query 'PageRank_1646310209197' received, your job ID is 'PageRank_1646310209197'.
12:23:38.859 [pulsar-external-listener-9-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job 'EdgeList_1646310208857': Perspective '30000' finished in 9663 ms.
12:23:38.860 [pulsar-external-listener-9-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job EdgeList_1646310208857: Running query, processed 1 perspectives.
12:23:38.953 [pulsar-external-listener-9-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job EdgeList_1646310208857: Query completed with 1 perspectives and finished in 9757 ms.
12:24:00.772 [pulsar-external-listener-9-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job 'PageRank_1646310209197': Perspective '20000' with window '10000' finished in 31229 ms.
12:24:00.772 [pulsar-external-listener-9-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job PageRank_1646310209197: Running query, processed 1 perspectives.
12:24:05.232 [pulsar-external-listener-9-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job 'PageRank_1646310209197': Perspective '20000' with window '1000' finished in 4460 ms.
...
12:24:42.408 [pulsar-external-listener-9-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job PageRank_1646310209197: Running query, processed 6 perspectives.
12:24:42.513 [pulsar-external-listener-9-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job PageRank_1646310209197: Query completed with 6 perspectives and finished in 72970 ms.
```

Terminal output to show job has finished and is ready for visualising the data in Jupyter Notebook
```bash
12:24:42.513 [pulsar-external-listener-9-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job PageRank_1646310209197: Query completed with 6 perspectives and finished in 72970 ms.
```

The results will be saved to two Topics `EdgeList` and `PageRank`, producing the following results

EdgeList sample dataframe
```python
pulsar_timestamp character character
30000	          Hador	    Húrin
30000	          Ungoliant Shelob
30000	          Isildur   Gil-galad
30000	          Isildur   Frodo
30000	          Isildur   Valandil
...	...	...	...
30000	          Bain	    Brand
30000	          Celeborn  Galadriel
30000	          Celeborn  Aragorn
30000	          Barahir   Faramir
30000	          Barahir   Thingol
```

PageRank sample dataframe
```python
pulsar_timestamp window     character      pagerank_score
20000	        10000	    Elrond	   0.7093758378329211
20000	        10000	    Glóin	   0.2273675721514219
20000	        10000	    Gimli	   1.092245800535391
20000	        10000	    Grimbold       0.15000000000000002
20000	        10000	    Galadriel      0.9402083216861185
...	...	...	...	...
30000	        500	    Gorbag	   0.15000000000000002
30000	        500	    Shagrat	   0.24403125000000003
30000	        500	    Galadriel      0.18028125000000003
30000	        500	    Faramir	   0.18028125000000003
30000	        500	    Shelob	   0.25690078125000004
```

### EdgeList
```python
30000,Hirgon,Denethor
30000,Hirgon,Gandalf
30000,Horn,Harding
30000,Galadriel,Elrond
30000,Galadriel,Faramir
...
```

This says that at time 30,000 Hirgon was connected to Denethor. 
Hirgon was connected to Gandalf etc. 

### PageRank
```python
20000,10000,Balin,0.15000000000000002
20000,10000,Orophin,0.15000000000000002
20000,10000,Arwen,0.15000000000000002
20000,10000,Isildur,0.15000000000000002
20000,10000,Samwise,0.15000000000000002
...
```

This data tells us that at time 20,000, window 10,000 that Balin had a rank of 0.15. 
etc. 

Graph visualisation output of Lord of the Ring characters and their Pagerank
<p>
 <img src="../_static/lotr_pagerank.png" width="700px" style="padding: 15px" alt="Graph Output of LOTR Pagerank"/>
</p>