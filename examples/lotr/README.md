# Lord of the Rings Character Interactions

## Overview
This example is a dataset that tells us when two characters have some type of interaction in the Lord of the Rings trilogy books. It's a great dataset to test different algorithms or even your own written algorithms.

## Project Overview

This example has two outputs:
* TutorialRunner builds a graph and several algorithms explained [here](https://docs.raphtory.com/en/development/Examples/lotr.html). 
* PulsarOutputRunner builds a graph and runs two queries. The first is an edge list (`EdgeList.scala`) listing out all the edges that join source nodes and destination nodes of this dataset, this will be needed for graph visualisation in python. The second is a query to show us the various ranks of the characters across time (`PageRank.scala`). What makes this example runner different is that the results is saved into Pulsar rather than your local files. 

The data is a `csv` file (comma-separated values) is located in the Raphtory [Data repository](https://github.com/Raphtory/Data). 
Each line contains two characters that appeared in the same sentence in the 
book, along with which sentence they appeared as indicated by a number. 
In the example, the first line of the file is `Gandalf,Elrond,33` which tells
us that Gandalf and Elrond appears together in sentence 33.

Also, in the examples folder you will find `DegreesSeparation.scala` and `TutorialRunner.scala`.

* `DegreesSeparation.scala` is an algorithm we wrote for the LOTR dataset, explained [here](https://docs.raphtory.com/en/development/Analysis/LOTR_six_degrees.html).
* `LOTRGraphBuilder.scala` builds the graph.

Refer back to [Building a graph from your data](https://raphtory.readthedocs.io/en/development/Ingestion/sprouter.html#) for a more in-depth explanation on how these parts work to build your graph.

## Running this example

Scala Installation Guide: https://docs.raphtory.com/en/development/Install/scala/install.html

Python Installation Guide: [With Conda](https://docs.raphtory.com/en/development/Install/python/install_conda.html) and [Without Conda](https://docs.raphtory.com/en/development/Install/python/install_no_conda.html)

## Output

Output in terminal when running `TutorialRunner.scala` collecting simple metrics.
```bash
11:10:42.583 [spawner-akka.actor.default-dispatcher-6] INFO  com.raphtory.internals.components.querymanager.QueryManager - Source '0' is unblocking analysis for Graph 'violent_rose_mastodon' with 7947 messages sent.
11:10:42.585 [io-compute-8] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job 722153918_5338049307951797618: Starting query progress tracker.
11:10:42.590 [spawner-akka.actor.default-dispatcher-6] INFO  com.raphtory.internals.components.querymanager.QueryManager - Query '722153918_5338049307951797618' received, your job ID is '722153918_5338049307951797618'.
11:10:42.596 [spawner-akka.actor.default-dispatcher-9] INFO  com.raphtory.internals.components.partition.QueryExecutor - 722153918_5338049307951797618_0: Starting QueryExecutor.
11:10:43.395 [spawner-akka.actor.default-dispatcher-3] INFO  com.raphtory.internals.components.querymanager.QueryHandler - Job '722153918_5338049307951797618': Perspective at Time '32674' took 790 ms to run.
11:10:43.395 [spawner-akka.actor.default-dispatcher-11] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job '722153918_5338049307951797618': Perspective '32674' finished in 810 ms.
11:10:43.395 [spawner-akka.actor.default-dispatcher-11] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job 722153918_5338049307951797618: Running query, processed 1 perspectives.
11:10:43.397 [spawner-akka.actor.default-dispatcher-3] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job 722153918_5338049307951797618: Query completed with 1 perspectives and finished in 812 ms.
```

Terminal output to show job has finished and is ready for visualising the data in Jupyter Notebook
```bash

11:10:43.395 [spawner-akka.actor.default-dispatcher-11] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job 722153918_5338049307951797618: Running query, processed 1 perspectives.
11:10:43.397 [spawner-akka.actor.default-dispatcher-3] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job 722153918_5338049307951797618: Query completed with 1 perspectives and finished in 812 ms.
```

#### EdgeList Results
```bash
30000,Hirgon,Denethor
30000,Hirgon,Gandalf
30000,Horn,Harding
30000,Galadriel,Elrond
30000,Galadriel,Faramir
...
```

This says that at time 30,000 Hirgon was connected to Denethor. 
Hirgon was connected to Gandalf etc. 

#### PageRank Results
```bash
20000,10000,Balin,0.15000000000000002
20000,10000,Orophin,0.15000000000000002
20000,10000,Arwen,0.15000000000000002
20000,10000,Isildur,0.15000000000000002
20000,10000,Samwise,0.15000000000000002
...
```

This data tells us that at time 20,000, window 10,000 that Balin had a rank of 0.15. 
etc. 

Graph visualisation output of Lord of the Ring characters:

https://docs.raphtory.com/en/development/Examples/lotr.html#show-the-html-file-of-the-visualisation
