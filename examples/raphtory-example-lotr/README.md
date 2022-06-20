# Lord of the Rings Character Interactions

## Overview
This example is a dataset that tells us when two characters have some type of interaction in the Lord of the Rings trilogy books. It's a great dataset to test different algorithms or even your own written algorithms.

## Project Overview

This example has two outputs:
* FileOutputRunner builds a graph and uses the algorithm `DegreesSeparation.scala` which is explained [here](https://raphtory.readthedocs.io/en/development/Analysis/analysis-explained.html#six-degrees-of-gandalf). 
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

Refer back to [Building a graph from your data](https://raphtory.readthedocs.io/en/development/Ingestion/sprouter.html#) for a more in-depth explanation on how these parts work to build your graph.

## IntelliJ setup guide

As of February 2022, this is a guide to run this within IntelliJ.

1. From https://adoptopenjdk.net/index.html download OpenJDK 11 (LTS) with the HotSpot VM.
2. Enable this as the project SDK under File > Project Structure > Project Settings > Project > Project SDK.
3. Create a new configuration as an `Application` , select Java 11 as the build, and `com.raphtory.examples.lotr.FileOutputRunner` or `com.raphtory.examples.lotr.PulsarOutputRunner` as the class, add the Environment Variables too.

## Running this example

1. This example project is up on Github: [raphtory-example-lotr](https://github.com/Raphtory/Raphtory/tree/development/examples/raphtory-example-lotr). If you have downloaded the Examples folder from the installation guide previously, then the Lotr example will already be set up. If not, please return [there](https://raphtory.readthedocs.io/en/development/Install/installdependencies.html) and complete this step first.
2. Download the _lotr.csv_ data from [raphtory-data](https://github.com/Raphtory/Data) repository and place it inside of the resources folder.
3. In the Examples folder, open up the directory `raphtory-example-lotr` to get this example running.
4. Install all the python libraries necessary for visualising your data via the [Jupyter Notebook Tutorial](https://raphtory.readthedocs.io/en/development/PythonClient/tutorial.html). Once you have Jupyter Notebook up and running on your local machine, you can open up the Jupyter Notebook specific for this project, with all the commands needed to output your graph. This can be found by following the path `python/LOTR_demo.ipynb`.
5. You are now ready to run this example. You can either run this example via Intellij by running the class `com.raphtory.examples.lotr.FileOutputRunner` or `com.raphtory.examples.lotr.PulsarOutputRunner` or [via sbt](https://raphtory.readthedocs.io/en/development/Install/installdependencies.html#running-raphtory-via-sbt).
6. Once your job has finished, you are ready to go onto Jupyter Notebook and run your analyses/output.

## Output

Output in terminal when running `PulsarOutputRunner.scala`
```json
Enter number: 3
[info] running com.raphtory.examples.lotrTopic.PulsarOutputRunner
14:01:17.044 [run-main-0] INFO  com.raphtory.spouts.FileSpout - Spout: Processing file 'lotr.csv' ...
14:01:17.930 [spawner-akka.actor.default-dispatcher-3] INFO  akka.event.slf4j.Slf4jLogger - Slf4jLogger started
14:01:19.555 [run-main-0] INFO  com.raphtory.internals.management.ComponentFactory - Creating '1' Partition Managers for raphtory_1072124052.
14:01:19.749 [run-main-0] INFO  com.raphtory.internals.management.ComponentFactory - Creating new Query Manager.
14:01:19.763 [run-main-0] INFO  com.raphtory.internals.management.GraphDeployment - Created Graph object with deployment ID 'raphtory_1072124052'.
14:01:19.763 [run-main-0] INFO  com.raphtory.internals.management.GraphDeployment - Created Graph Spout topic with name 'raphtory_data_raw_1072124052'.
14:01:20.103 [run-main-0] INFO  com.raphtory.internals.management.ComponentFactory - Creating new Query Progress Tracker for 'EdgeList_8100741964076353527'.
14:01:20.105 [io-compute-3] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job EdgeList_8100741964076353527: Starting query progress tracker.
14:01:20.121 [run-main-0] INFO  com.raphtory.internals.management.ComponentFactory - Creating new Query Progress Tracker for 'PageRank_8567638990663365259'.
14:01:20.122 [io-compute-6] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job PageRank_8567638990663365259: Starting query progress tracker.
14:01:20.152 [spawner-akka.actor.default-dispatcher-3] INFO  com.raphtory.internals.components.querymanager.QueryManager - Query 'EdgeList_8100741964076353527' received, your job ID is 'EdgeList_8100741964076353527'.
14:01:20.180 [spawner-akka.actor.default-dispatcher-3] INFO  com.raphtory.internals.components.querymanager.QueryManager - Query 'PageRank_8567638990663365259' received, your job ID is 'PageRank_8567638990663365259'.
14:01:23.613 [spawner-akka.actor.default-dispatcher-7] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job 'PageRank_8567638990663365259': Perspective '20000' with window '10000' finished in 3492 ms.
14:01:23.614 [spawner-akka.actor.default-dispatcher-7] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job PageRank_8567638990663365259: Running query, processed 1 perspectives.
14:01:23.681 [spawner-akka.actor.default-dispatcher-10] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job 'PageRank_8567638990663365259': Perspective '20000' with window '1000' finished in 67 ms.
14:01:23.682 [spawner-akka.actor.default-dispatcher-10] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job PageRank_8567638990663365259: Running query, processed 2 perspectives.
14:01:23.705 [spawner-akka.actor.default-dispatcher-6] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job 'PageRank_8567638990663365259': Perspective '20000' with window '500' finished in 23 ms.
14:01:23.706 [spawner-akka.actor.default-dispatcher-6] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job PageRank_8567638990663365259: Running query, processed 3 perspectives.
14:01:23.738 [spawner-akka.actor.default-dispatcher-6] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job 'EdgeList_8100741964076353527': Perspective '30000' finished in 3633 ms.
14:01:23.739 [spawner-akka.actor.default-dispatcher-6] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job EdgeList_8100741964076353527: Running query, processed 1 perspectives.
14:01:23.853 [spawner-akka.actor.default-dispatcher-6] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job 'PageRank_8567638990663365259': Perspective '30000' with window '10000' finished in 147 ms.
14:01:23.853 [spawner-akka.actor.default-dispatcher-6] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job PageRank_8567638990663365259: Running query, processed 4 perspectives.
14:01:23.879 [spawner-akka.actor.default-dispatcher-10] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job 'PageRank_8567638990663365259': Perspective '30000' with window '1000' finished in 26 ms.
14:01:23.879 [spawner-akka.actor.default-dispatcher-10] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job PageRank_8567638990663365259: Running query, processed 5 perspectives.
14:01:23.893 [spawner-akka.actor.default-dispatcher-7] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job 'PageRank_8567638990663365259': Perspective '30000' with window '500' finished in 14 ms.
14:01:23.894 [spawner-akka.actor.default-dispatcher-7] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job PageRank_8567638990663365259: Running query, processed 6 perspectives.
14:01:26.144 [spawner-akka.actor.default-dispatcher-8] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job EdgeList_8100741964076353527: Query completed with 1 perspectives and finished in 6039 ms.
14:01:26.145 [spawner-akka.actor.default-dispatcher-5] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job PageRank_8567638990663365259: Query completed with 6 perspectives and finished in 6024 ms.
```

Terminal output to show job has finished and is ready for visualising the data in Jupyter Notebook
```json

14:01:26.144 [spawner-akka.actor.default-dispatcher-8] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job EdgeList_8100741964076353527: Query completed with 1 perspectives and finished in 6039 ms.
14:01:26.145 [spawner-akka.actor.default-dispatcher-5] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job PageRank_8567638990663365259: Query completed with 6 perspectives and finished in 6024 ms.
```

The results will be saved to two Topics `EdgeList` and `PageRank`, producing the following results

EdgeList sample dataframe
```json
pulsar_timestamp character character
30000	        Hador	    Húrin
30000	        Ungoliant   Shelob
30000	        Isildur	    Gil-galad
30000	        Isildur	    Frodo
30000	        Isildur	    Valandil
...	...	...	...
30000	        Bain	    Brand
30000	        Celeborn    Galadriel
30000	        Celeborn    Aragorn
30000	        Barahir	    Faramir
30000	        Barahir	    Thingol
```

PageRank sample dataframe
```json
pulsar_timestamp window     character      pagerank_score
20000	        10000	  Elrond	    0.7093758378329211
20000	        10000	  Glóin	            0.2273675721514219
20000	        10000	  Gimli	            1.092245800535391
20000	        10000	  Grimbold              0.15000000000000002
20000	        10000	  Galadriel             0.9402083216861185
...	...	...	...	...
30000	        500	    Gorbag	    0.15000000000000002
30000	        500	    Shagrat	    0.24403125000000003
30000	        500	    Galadriel       0.18028125000000003
30000	        500	    Faramir	    0.18028125000000003
30000	        500	    Shelob	    0.25690078125000004
```

#### EdgeList
```json
30000,Hirgon,Denethor
30000,Hirgon,Gandalf
30000,Horn,Harding
30000,Galadriel,Elrond
30000,Galadriel,Faramir
...
```

This says that at time 30,000 Hirgon was connected to Denethor. 
Hirgon was connected to Gandalf etc. 

#### PageRank
```json
20000,10000,Balin,0.15000000000000002
20000,10000,Orophin,0.15000000000000002
20000,10000,Arwen,0.15000000000000002
20000,10000,Isildur,0.15000000000000002
20000,10000,Samwise,0.15000000000000002
...
```

This data tells us that at time 20,000, window 10,000 that Balin had a rank of 0.15. 
etc. 

Graph visualisation output of Lord of the Ring characters and their Pagerank:

<img width="1411" alt="lotrpagerank" src="https://user-images.githubusercontent.com/25484244/156590723-66ac284f-9df1-46e8-933b-8635f4a79114.png">

