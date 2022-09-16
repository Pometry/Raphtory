# Lord of the Rings Character Interactions

## Overview
This example takes a dataset that tells us when two characters have some type of interaction in the Lord of the Rings trilogy books and builds a graph of these interactions in Raphtory. It's a great dataset to test different algorithms or even your own written algorithms. You can run this example using either our Python or Scala client.

## Pre-requisites

Follow our Installation guide: [Scala](../Install/scala/install.md) or [Python (with Conda)](../Install/python/install_conda.md), [Python (without Conda)](../Install/python/install_no_conda.md).

## Data

The data is a `csv` file (comma-separated values) and is pulled from our Github data repository. 
Each line contains two characters that appeared in the same sentence in the 
book, along with which sentence they appeared as indicated by a number. 
In the example, the first line of the file is `Gandalf,Elrond,33` which tells
us that Gandalf and Elrond appears together in sentence 33.

<p>
 <img src="../_static/lotr-graphic.png" width="700px" style="padding: 15px" alt="Intro Graphic of LOTR slices"/>
</p>

## Lord Of The Rings Example 🧝🏻‍♀️🧙🏻‍♂️💍

### Setup environment 🌍

Import all necessary dependencies needed to build a graph from your data in Raphtory. 

````{tabs}

```{code-tab} py
from pathlib import Path
from pyraphtory.context import PyRaphtory
from pyraphtory.vertex import Vertex
from pyraphtory.spouts import FileSpout
from pyraphtory.builder import *
from pyvis.network import Network

```
```{code-tab} scala

import com.raphtory.Raphtory
import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.api.input.Graph.assignID
import com.raphtory.api.input.ImmutableProperty
import com.raphtory.api.input.Properties
import com.raphtory.api.input.Type
import com.raphtory.sinks.FileSink
import com.raphtory.utils.FileUtils
import scala.language.postfixOps

```
````

### Download csv data from Github 💾

````{tabs}

```{code-tab} py
!curl -o /tmp/lotr.csv https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv
```
```{code-tab} scala
val path = "/tmp/lotr.csv"
val url  = "https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv"
FileUtils.curlFile(path, url)
```
````

#### Terminal Output
```bash
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100 52206  100 52206    0     0   147k      0 --:--:-- --:--:-- --:--:--  149k
```

### Create a new Raphtory graph 📊

Turn on logs to see what is going on in PyRaphtory. Initialise Raphtory by creating a PyRaphtory object and create your new graph.

````{tabs}

```{code-tab} py
pr = PyRaphtory(logging=True).open()
rg = pr.new_graph()
```
```{code-tab} scala
val graph = Raphtory.newGraph()
```
````

#### Terminal Output
```bash
11:10:18.664 [Thread-12] INFO  com.raphtory.internals.context.LocalContext$ - Creating Service for 'violent_rose_mastodon'
11:10:18.678 [io-compute-1] INFO  com.raphtory.internals.management.Prometheus$ - Prometheus started on port /0:0:0:0:0:0:0:0:9999
11:10:19.491 [io-compute-1] INFO  com.raphtory.internals.components.partition.PartitionOrchestrator$ - Creating '1' Partition Managers for 'violent_rose_mastodon'.
11:10:21.700 [io-compute-4] INFO  com.raphtory.internals.components.partition.PartitionManager - Partition 0: Starting partition manager for 'violent_rose_mastodon'.
```

### Ingest the data into a graph 😋

Write a parsing method to parse your csv file and ultimately create a graph.

````{tabs}

```{code-tab} py
with open(filename, 'r') as csvfile:
    datareader = csv.reader(csvfile)
    for row in datareader:
        source_node = row[0]
        src_id = rg.assign_id(source_node)
        target_node = row[1]
        tar_id = rg.assign_id(target_node)
        time_stamp = int(row[2])
        rg.add_vertex(time_stamp, src_id, Properties(ImmutableProperty("name", source_node)), Type("Character"))
        rg.add_vertex(time_stamp, tar_id, Properties(ImmutableProperty("name", target_node)), Type("Character"))
        rg.add_edge(time_stamp, src_id, tar_id, Type("Character_Co-occurence"))
```
```{code-tab} scala
  val file = scala.io.Source.fromFile(path)
  file.getLines.foreach { line =>
// split csv line by comma
    val fileLine   = line.split(",").map(_.trim)
// assign parts to source and target node variable and convert to Long, also assign time to timestamp variable.
    val sourceNode = fileLine(0)
    val srcID      = assignID(sourceNode)
    val targetNode = fileLine(1)
    val tarID      = assignID(targetNode)
    val timeStamp  = fileLine(2).toLong
// add vertex and edges to graph
    graph.addVertex(timeStamp, srcID, Properties(ImmutableProperty("name", sourceNode)), Type("Character"))
    graph.addVertex(timeStamp, tarID, Properties(ImmutableProperty("name", targetNode)), Type("Character"))
    graph.addEdge(timeStamp, srcID, tarID, Type("Character Co-occurence"))
  }
```
````


### Collect simple metrics 📈

Select certain metrics to show in your output dataframe. Here we have selected vertex name, degree, out degree and in degree. 

````{tabs}

```{code-tab} py
from pyraphtory.graph import Row
df = rg \
      .select(lambda vertex: Row(vertex.name(), vertex.degree(), vertex.out_degree(), vertex.in_degree())) \
      .write_to_dataframe(["name", "degree", "out_degree", "in_degree"])
```
```{code-tab} scala
val graph = Raphtory.newGraph()
  graph
    .execute(Degree())
    .writeTo(FileSink("/tmp/raphtory"))
    .waitForJob()
```
````

#### Terminal output
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

### Clean dataframe 🧹 and preview 👀

In Python, we need to clean the dataframe and we can preview it. In Scala, we can preview the saved csv file in the /tmp directory, which we set in the .writeTo method, this can be done in the bash terminal.

````{tabs}

```{code-tab} py
df.drop(columns=['window'], inplace=True)
df
```
```{code-tab} bash
cd /tmp/raphtory
cd Degree_JOBID
cat partition-0.csv
```
````
#### Terminal Output
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

### Sort by highest degree, top 10

````{tabs}

```{code-tab} py
df.sort_values(['degree'], ascending=False)[:10]
```
````
```{table} Top 10 Highest Degree Results
|     | timestamp |    name | degree | out_degree | in_degree |
|----:|----------:|--------:|-------:|-----------:|----------:|
|  55 | 32674     | Frodo   | 51     | 37         | 22        |
|  54 | 32674     | Gandalf | 49     | 35         | 24        |
|  97 | 32674     | Aragorn | 45     | 5          | 45        |
|  63 | 32674     | Merry   | 34     | 23         | 18        |
|  32 | 32674     | Pippin  | 34     | 30         | 10        |
|  56 | 32674     | Elrond  | 32     | 18         | 24        |
|  52 | 32674     | Théoden | 30     | 22         | 9         |
| 134 | 32674     | Faramir | 29     | 3          | 29        |
| 118 | 32674     | Sam     | 28     | 20         | 17        |
| 129 | 32674     | Gimli   | 25     | 22         | 11        |
```

### Sort by highest in-degree, top 10

````{tabs}

```{code-tab} py
df.sort_values(['in_degree'], ascending=False)[:10]
```
````

```{table} Top 10 Highest In Degree Results
|     | timestamp |      name | degree | out_degree | in_degree |
|----:|----------:|----------:|-------:|-----------:|-----------|
|  97 |     32674 |   Aragorn |     45 |          5 |        45 |
| 134 |     32674 |   Faramir |     29 |          3 |        29 |
|  54 |     32674 |   Gandalf |     49 |         35 |        24 |
|  56 |     32674 |    Elrond |     32 |         18 |        24 |
|  55 |     32674 |     Frodo |     51 |         37 |        22 |
|  63 |     32674 |     Merry |     34 |         23 |        18 |
| 138 |     32674 |   Boromir |     18 |          6 |        17 |
| 118 |     32674 |       Sam |     28 |         20 |        17 |
|   3 |     32674 | Galadriel |     19 |          6 |        16 |
| 132 |     32674 |   Legolas |     25 |         18 |        16 |
```
### Sort by highest out-degree, top 10

````{tabs}

```{code-tab} py
df.sort_values(['out_degree'], ascending=False)[:10]
```
````
```{table} Top 10 Highest Out Degree Results
|     | timestamp |    name | degree | out_degree | in_degree |
|----:|----------:|--------:|-------:|-----------:|-----------|
|  55 |     32674 |   Frodo |     51 |         37 |        22 |
|  54 |     32674 | Gandalf |     49 |         35 |        24 |
|  32 |     32674 |  Pippin |     34 |         30 |        10 |
|  63 |     32674 |   Merry |     34 |         23 |        18 |
|  52 |     32674 | Théoden |     30 |         22 |         9 |
| 129 |     32674 |   Gimli |     25 |         22 |        11 |
| 118 |     32674 |     Sam |     28 |         20 |        17 |
|  56 |     32674 |  Elrond |     32 |         18 |        24 |
|   4 |     32674 | Isildur |     18 |         18 |         0 |
| 132 |     32674 | Legolas |     25 |         18 |        16 |
```
### Run a PageRank algorithm 📑

Run your selected algorithm on your graph, here we run PageRank and then NodeList. Specify where you write the result of your algorithm to, e.g. the additional column results in your dataframe.

````{tabs}

```{code-tab} py
cols = ["prlabel"]

df_pagerank = rg.at(32674) \
                .past() \
                .transform(pr.algorithms.generic.centrality.PageRank())\
                .execute(pr.algorithms.generic.NodeList(*cols)) \
                .write_to_dataframe(["name"] + cols)
```
```{code-tab} scala
  graph
    .at(32674)
    .past()
    .execute(PageRank())
    .writeTo(FileSink("/tmp/raphtory"))
    .waitForJob()
```
````
#### Terminal Output
```bash
11:10:57.397 [io-compute-9] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job PageRank:NodeList_7431747532744364308: Starting query progress tracker.
11:10:57.414 [spawner-akka.actor.default-dispatcher-11] INFO  com.raphtory.internals.components.querymanager.QueryManager - Query 'PageRank:NodeList_7431747532744364308' received, your job ID is 'PageRank:NodeList_7431747532744364308'.
11:10:57.416 [spawner-akka.actor.default-dispatcher-6] INFO  com.raphtory.internals.components.partition.QueryExecutor - PageRank:NodeList_7431747532744364308_0: Starting QueryExecutor.
11:10:57.597 [spawner-akka.actor.default-dispatcher-7] INFO  com.raphtory.internals.components.querymanager.QueryHandler - Job 'PageRank:NodeList_7431747532744364308': Perspective at Time '32674' took 179 ms to run. 
11:10:57.597 [spawner-akka.actor.default-dispatcher-9] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job 'PageRank:NodeList_7431747532744364308': Perspective '32674' finished in 200 ms.
11:10:57.597 [spawner-akka.actor.default-dispatcher-9] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job PageRank:NodeList_7431747532744364308: Running query, processed 1 perspectives.
11:10:57.597 [spawner-akka.actor.default-dispatcher-9] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job PageRank:NodeList_7431747532744364308: Query completed with 1 perspectives and finished in 200 ms.
```

### Clean dataframe 🧹 and preview 👀

````{tabs}

```{code-tab} py
df_pagerank.drop(columns=['window'], inplace=True)
df_pagerank
```
```{code-tab} bash
cd /tmp/raphtory
cd PageRank:NodeList_JOBID
cat partition-0.csv
```
````
```{table} Preview Dataframe
|     | timestamp |      name |  prlabel |
|----:|----------:|----------:|---------:|
|   0 |     32674 |    Hirgon | 0.277968 |
|   1 |     32674 |     Hador | 0.459710 |
|   2 |     32674 |      Horn | 0.522389 |
|   3 |     32674 | Galadriel | 2.228852 |
|   4 |     32674 |   Isildur | 0.277968 |
| ... |       ... |       ... |      ... |
| 134 |     32674 |   Faramir | 8.551166 |
| 135 |     32674 |      Bain | 0.396105 |
| 136 |     32674 |     Walda | 0.817198 |
| 137 |     32674 | Thranduil | 0.761719 |
| 138 |     32674 |   Boromir | 4.824014 |
```

### The top ten highest Page Rank

````{tabs}
```{code-tab} py
df_pagerank.sort_values(['prlabel'], ascending=False)[:10]
```
````
```{table} Top Ten Highest Page Rank characters in LOTR
|     | timestamp |    name |   prlabel |
|----:|----------:|--------:|----------:|
|  97 |     32674 | Aragorn | 13.246457 |
| 134 |     32674 | Faramir |  8.551166 |
|  56 |     32674 |  Elrond |  5.621548 |
| 138 |     32674 | Boromir |  4.824014 |
| 132 |     32674 | Legolas |  4.622590 |
| 110 |     32674 | Imrahil |  4.095600 |
|  65 |     32674 |   Éomer |  3.473897 |
|  42 |     32674 | Samwise |  3.292762 |
| 118 |     32674 |     Sam |  2.826140 |
|  55 |     32674 |   Frodo |  2.806475 |
```
### Run a connected components algorithm 

````{tabs}

```{code-tab} py
cols = ["cclabel"]
df_cc = rg.at(32674) \
                .past() \
                .transform(pr.algorithms.generic.ConnectedComponents)\
                .execute(pr.algorithms.generic.NodeList(*cols)) \
                .write_to_dataframe(["name"] + cols)
```
```{code-tab} scala
  graph
    .at(32674)
    .past()
    .execute(ConnectedComponents)
    .writeTo(FileSink("/tmp/raphtory"))
    .waitForJob()
```
````
#### Terminal Output
```bash
11:14:59.742 [io-compute-3] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job ConnectedComponents:NodeList_5614237107038973005: Starting query progress tracker.
11:14:59.744 [spawner-akka.actor.default-dispatcher-9] INFO  com.raphtory.internals.components.querymanager.QueryManager - Query 'ConnectedComponents:NodeList_5614237107038973005' received, your job ID is 'ConnectedComponents:NodeList_5614237107038973005'.
11:14:59.745 [spawner-akka.actor.default-dispatcher-7] INFO  com.raphtory.internals.components.partition.QueryExecutor - ConnectedComponents:NodeList_5614237107038973005_0: Starting QueryExecutor.
11:14:59.772 [spawner-akka.actor.default-dispatcher-7] INFO  com.raphtory.internals.components.querymanager.QueryHandler - Job 'ConnectedComponents:NodeList_5614237107038973005': Perspective at Time '32674' took 26 ms to run. 
11:14:59.772 [spawner-akka.actor.default-dispatcher-3] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job 'ConnectedComponents:NodeList_5614237107038973005': Perspective '32674' finished in 30 ms.
11:14:59.772 [spawner-akka.actor.default-dispatcher-3] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job ConnectedComponents:NodeList_5614237107038973005: Running query, processed 1 perspectives.
11:14:59.773 [spawner-akka.actor.default-dispatcher-7] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job ConnectedComponents:NodeList_5614237107038973005: Query completed with 1 perspectives and finished in 31 ms.
```

### Clean dataframe 🧹 and preview 👀

````{tabs}

```{code-tab} py
df_cc.drop(columns=['window'], inplace=True)
df_cc
```
```{code-tab} bash
cd /tmp/raphtory
cd ConnectedComponents_JOBID
cat partition-0.csv
```
````
```{table} Preview dataframe
|     | timestamp |      name | cclabel              |
|----:|----------:|----------:|----------------------|
|   0 |     32674 |    Hirgon | -8637342647242242534 |
|   1 |     32674 |     Hador | -8637342647242242534 |
|   2 |     32674 |      Horn | -8637342647242242534 |
|   3 |     32674 | Galadriel | -8637342647242242534 |
|   4 |     32674 |   Isildur | -8637342647242242534 |
| ... |       ... |       ... |                  ... |
| 134 |     32674 |   Faramir | -8637342647242242534 |
| 135 |     32674 |      Bain | -6628080393138316116 |
| 136 |     32674 |     Walda | -8637342647242242534 |
| 137 |     32674 | Thranduil | -8637342647242242534 |
| 138 |     32674 |   Boromir | -8637342647242242534 |
```
### Number of distinct components 

Extract number of distinct components, which is 3 in this dataframe.

````{tabs}

```{code-tab} py
len(set(df_cc['cclabel']))
```
````
#### Terminal Output
```py
Out[19]: 3
```

### Size of components 


Calculate the size of the 3 connected components.

````{tabs}

```{code-tab} py
df_cc.groupby(['cclabel']).count().reset_index().drop(columns=['timestamp'])
```
````
```{table} Size of the 3 distinct connected components
|   |              cclabel | name |
|--:|---------------------:|-----:|
| 0 | -8637342647242242534 |  134 |
| 1 | -6628080393138316116 |    3 |
| 2 | -5499479516525190226 |    2 |
```

### Run chained algorithms at once 

In this example, we chain PageRank, Connected Components and Degree algorithms, running them one after another on the graph. Specify all the columns in the output dataframe, including an output column for each algorithm in the chain.

````{tabs}

```{code-tab} py
cols = ["inDegree", "outDegree", "degree","prlabel","cclabel"]

df_chained = rg.at(32674) \
                .past() \
                .transform(pr.algorithms.generic.centrality.PageRank())\
                .transform(pr.algorithms.generic.ConnectedComponents)\
                .transform(pr.algorithms.generic.centrality.Degree())\
                .execute(pr.algorithms.generic.NodeList(*cols)) \
                .write_to_dataframe(["name"] + cols)
```
```{code-tab} scala
  graph
    .at(32674)
    .past()
    .transform(PageRank())
    .transform(ConnectedComponents)
    .transform(Degree())
    .execute(NodeList(Seq("prlabel","cclabel", "inDegree", "outDegree", "degree")))
    .writeTo(FileSink("/tmp/raphtory"))
    .waitForJob()
```
````
```bash
11:15:08.397 [io-compute-7] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job PageRank:ConnectedComponents:Degree:NodeList_8956930356200985413: Starting query progress tracker.
11:15:08.401 [spawner-akka.actor.default-dispatcher-6] INFO  com.raphtory.internals.components.querymanager.QueryManager - Query 'PageRank:ConnectedComponents:Degree:NodeList_8956930356200985413' received, your job ID is 'PageRank:ConnectedComponents:Degree:NodeList_8956930356200985413'.
11:15:08.402 [spawner-akka.actor.default-dispatcher-7] INFO  com.raphtory.internals.components.partition.QueryExecutor - PageRank:ConnectedComponents:Degree:NodeList_8956930356200985413_0: Starting QueryExecutor.
11:15:08.457 [spawner-akka.actor.default-dispatcher-10] INFO  com.raphtory.internals.components.querymanager.QueryHandler - Job 'PageRank:ConnectedComponents:Degree:NodeList_8956930356200985413': Perspective at Time '32674' took 52 ms to run. 
11:15:08.457 [spawner-akka.actor.default-dispatcher-5] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job 'PageRank:ConnectedComponents:Degree:NodeList_8956930356200985413': Perspective '32674' finished in 60 ms.
11:15:08.457 [spawner-akka.actor.default-dispatcher-5] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job PageRank:ConnectedComponents:Degree:NodeList_8956930356200985413: Running query, processed 1 perspectives.
11:15:08.458 [spawner-akka.actor.default-dispatcher-5] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job PageRank:ConnectedComponents:Degree:NodeList_8956930356200985413: Query completed with 1 perspectives and finished in 61 ms.
```

### Clean dataframe 🧹 and preview 👀

````{tabs}

```{code-tab} py
df_chained.drop(columns=['window'])
df_chained
```
```{code-tab} bash
cd /tmp/raphtory
cd PageRank:ConnectedComponents:Degree:NodeList_JOBID
cat partition-0.csv
```
````
```{table} Preview chained algorithm output
|     | timestamp |      name | inDegree | outDegree | degree |  prlabel |              cclabel |
|----:|----------:|----------:|---------:|----------:|-------:|---------:|---------------------:|
|   0 |     32674 |    Hirgon |        0 |         2 |      2 | 0.277968 | -8637342647242242534 |
|   1 |     32674 |     Hador |        2 |         1 |      3 | 0.459710 | -8637342647242242534 |
|   2 |     32674 |      Horn |        3 |         1 |      4 | 0.522389 | -8637342647242242534 |
|   3 |     32674 | Galadriel |       16 |         6 |     19 | 2.228852 | -8637342647242242534 |
|   4 |     32674 |   Isildur |        0 |        18 |     18 | 0.277968 | -8637342647242242534 |
| ... |       ... |       ... |      ... |       ... |    ... |      ... |                  ... |
| 134 |     32674 |   Faramir |       29 |         3 |     29 | 8.551166 | -8637342647242242534 |
| 135 |     32674 |      Bain |        1 |         1 |      2 | 0.396105 | -6628080393138316116 |
| 136 |     32674 |     Walda |       10 |         3 |     13 | 0.817198 | -8637342647242242534 |
| 137 |     32674 | Thranduil |        2 |         0 |      2 | 0.761719 | -8637342647242242534 |
| 138 |     32674 |   Boromir |       17 |         6 |     18 | 4.824014 | -8637342647242242534 |

```

### Create visualisation by adding nodes 🔎

````{tabs}

```{code-tab} py
def visualise(rg, df_chained):
    # Create network object
    net = Network(notebook=True, height='750px', width='100%', bgcolor='#222222', font_color='white')
    # Set visualisation tool
    net.force_atlas_2based()
    # Get the node list 
    df_node_list = rg.at(32674) \
                .past() \
                .execute(pr.algorithms.generic.NodeList()) \
                .write_to_dataframe(['name'])
    
    nodes = df_node_list['name'].tolist()
    
    node_data = []
    ignore_items = ['timestamp', 'name', 'window']
    for node_name in nodes:
        for i, row in df_chained.iterrows():
            if row['name']==node_name:
                data = ''
                for k,v in row.iteritems():
                    if k not in ignore_items:
                        data = data+str(k)+': '+str(v)+'\n'
                node_data.append(data)
                continue
    # Add the nodes
    net.add_nodes(nodes, title=node_data)
    # Get the edge list
    df_edge_list = rg.at(32674) \
            .past() \
            .execute(pr.algorithms.generic.EdgeList()) \
            .write_to_dataframe(['from', 'to'])
    edges = []
    for i, row in df_edge_list[['from', 'to']].iterrows():
        edges.append([row['from'], row['to']])
    # Add the edges
    net.add_edges(edges)
    # Toggle physics
    net.toggle_physics(True)
    return net

net = visualise(rg, df_chained)
```
````

### Show the html file of the visualisation

````{tabs}

```{code-tab} py
net.show('preview.html')
```
````

<html>
<head>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/vis-network@latest/styles/vis-network.css" type="text/css" />
<script type="text/javascript" src="https://cdn.jsdelivr.net/npm/vis-network@latest/dist/vis-network.min.js"> </script>
<center>
<h1></h1>
</center>

<!-- <link rel="stylesheet" href="../node_modules/vis/dist/vis.min.css" type="text/css" />
<script type="text/javascript" src="../node_modules/vis/dist/vis.js"> </script>-->

<style type="text/css">

        #mynetwork {
            width: 100%;
            height: 750px;
            background-color: #222222;
            border: 1px solid lightgray;
            position: relative;
            float: left;
        }

        
        #loadingBar {
            position:relative;
            top:0px;
            left:0px;
            width: 100%;
            height: 750px;
            background-color:rgba(200,200,200,0.8);
            -webkit-transition: all 0.5s ease;
            -moz-transition: all 0.5s ease;
            -ms-transition: all 0.5s ease;
            -o-transition: all 0.5s ease;
            transition: all 0.5s ease;
            opacity:1;
        }

        #bar {
            position:absolute;
            top:0px;
            left:0px;
            width:20px;
            height:20px;
            margin:auto auto auto auto;
            border-radius:11px;
            border:2px solid rgba(30,30,30,0.05);
            background: rgb(0, 173, 246); /* Old browsers */
            box-shadow: 2px 0px 4px rgba(0,0,0,0.4);
        }

        #border {
            position:absolute;
            top:10px;
            left:10px;
            width:500px;
            height:23px;
            margin:auto auto auto auto;
            box-shadow: 0px 0px 4px rgba(0,0,0,0.2);
            border-radius:10px;
        }

        #text {
            position:absolute;
            top:8px;
            left:530px;
            width:30px;
            height:50px;
            margin:auto auto auto auto;
            font-size:22px;
            color: #000000;
        }

        div.outerBorder {
            position:relative;
            top:400px;
            width:600px;
            height:44px;
            margin:auto auto auto auto;
            border:8px solid rgba(0,0,0,0.1);
            background: rgb(252,252,252); /* Old browsers */
            background: -moz-linear-gradient(top,  rgba(252,252,252,1) 0%, rgba(237,237,237,1) 100%); /* FF3.6+ */
            background: -webkit-gradient(linear, left top, left bottom, color-stop(0%,rgba(252,252,252,1)), color-stop(100%,rgba(237,237,237,1))); /* Chrome,Safari4+ */
            background: -webkit-linear-gradient(top,  rgba(252,252,252,1) 0%,rgba(237,237,237,1) 100%); /* Chrome10+,Safari5.1+ */
            background: -o-linear-gradient(top,  rgba(252,252,252,1) 0%,rgba(237,237,237,1) 100%); /* Opera 11.10+ */
            background: -ms-linear-gradient(top,  rgba(252,252,252,1) 0%,rgba(237,237,237,1) 100%); /* IE10+ */
            background: linear-gradient(to bottom,  rgba(252,252,252,1) 0%,rgba(237,237,237,1) 100%); /* W3C */
            filter: progid:DXImageTransform.Microsoft.gradient( startColorstr='#fcfcfc', endColorstr='#ededed',GradientType=0 ); /* IE6-9 */
            border-radius:72px;
            box-shadow: 0px 0px 10px rgba(0,0,0,0.2);
        }
        

        

        
</style>

</head>

<body>
<div id = "mynetwork"></div>

<div id="loadingBar">
    <div class="outerBorder">
        <div id="text">0%</div>
        <div id="border">
            <div id="bar"></div>
        </div>
    </div>
</div>


<script type="text/javascript">

    // initialize global variables.
    var edges;
    var nodes;
    var network; 
    var container;
    var options, data;

    
    // This method is responsible for drawing the graph, returns the drawn network
    function drawGraph() {
        var container = document.getElementById('mynetwork');
        
        

        // parsing and collecting nodes and edges from the python
        nodes = new vis.DataSet([{"font": {"color": "white"}, "id": "Hirgon", "label": "Hirgon", "shape": "dot", "title": "inDegree: 0\noutDegree: 2\ndegree: 2\nprlabel: 0.2779681771402487\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Hador", "label": "Hador", "shape": "dot", "title": "inDegree: 2\noutDegree: 1\ndegree: 3\nprlabel: 0.45970990769536524\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Horn", "label": "Horn", "shape": "dot", "title": "inDegree: 3\noutDegree: 1\ndegree: 4\nprlabel: 0.5223890361301453\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Galadriel", "label": "Galadriel", "shape": "dot", "title": "inDegree: 16\noutDegree: 6\ndegree: 19\nprlabel: 2.2288522436129856\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Isildur", "label": "Isildur", "shape": "dot", "title": "inDegree: 0\noutDegree: 18\ndegree: 18\nprlabel: 0.2779681771402487\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Mablung", "label": "Mablung", "shape": "dot", "title": "inDegree: 2\noutDegree: 2\ndegree: 4\nprlabel: 0.46255212383700733\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Gram", "label": "Gram", "shape": "dot", "title": "inDegree: 13\noutDegree: 0\ndegree: 13\nprlabel: 2.3853927904446253\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Thingol", "label": "Thingol", "shape": "dot", "title": "inDegree: 4\noutDegree: 4\ndegree: 7\nprlabel: 1.2624252246686543\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Celebr\u00edan", "label": "Celebr\u00edan", "shape": "dot", "title": "inDegree: 1\noutDegree: 0\ndegree: 1\nprlabel: 0.3158233678925172\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Gamling", "label": "Gamling", "shape": "dot", "title": "inDegree: 2\noutDegree: 1\ndegree: 3\nprlabel: 0.38533564445859053\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "D\u00e9agol", "label": "D\u00e9agol", "shape": "dot", "title": "inDegree: 3\noutDegree: 0\ndegree: 3\nprlabel: 0.5888134473209017\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Findegil", "label": "Findegil", "shape": "dot", "title": "inDegree: 1\noutDegree: 0\ndegree: 1\nprlabel: 0.47102990268903866\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Brand", "label": "Brand", "shape": "dot", "title": "inDegree: 2\noutDegree: 0\ndegree: 2\nprlabel: 0.7327938326292689\ncclabel: -6628080393138316116\n"}, {"font": {"color": "white"}, "id": "Baldor", "label": "Baldor", "shape": "dot", "title": "inDegree: 5\noutDegree: 9\ndegree: 13\nprlabel: 0.4210003210300653\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Helm", "label": "Helm", "shape": "dot", "title": "inDegree: 3\noutDegree: 16\ndegree: 19\nprlabel: 0.5354086724831855\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Thengel", "label": "Thengel", "shape": "dot", "title": "inDegree: 15\noutDegree: 2\ndegree: 17\nprlabel: 2.0963337323687696\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Gil-galad", "label": "Gil-galad", "shape": "dot", "title": "inDegree: 3\noutDegree: 4\ndegree: 7\nprlabel: 0.39132903752916226\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Valandil", "label": "Valandil", "shape": "dot", "title": "inDegree: 1\noutDegree: 4\ndegree: 5\nprlabel: 0.29109445674786844\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Fengel", "label": "Fengel", "shape": "dot", "title": "inDegree: 1\noutDegree: 12\ndegree: 13\nprlabel: 0.2961430258277222\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Gl\u00f3in", "label": "Gl\u00f3in", "shape": "dot", "title": "inDegree: 11\noutDegree: 3\ndegree: 13\nprlabel: 1.7184314514133603\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "D\u00e9or", "label": "D\u00e9or", "shape": "dot", "title": "inDegree: 5\noutDegree: 8\ndegree: 13\nprlabel: 0.41230009148111696\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Peregrin", "label": "Peregrin", "shape": "dot", "title": "inDegree: 9\noutDegree: 18\ndegree: 22\nprlabel: 1.2374107547257762\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Grimbold", "label": "Grimbold", "shape": "dot", "title": "inDegree: 0\noutDegree: 6\ndegree: 6\nprlabel: 0.2779681771402487\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Fredegar", "label": "Fredegar", "shape": "dot", "title": "inDegree: 6\noutDegree: 0\ndegree: 6\nprlabel: 0.5671929418801681\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Blanco", "label": "Blanco", "shape": "dot", "title": "inDegree: 0\noutDegree: 1\ndegree: 1\nprlabel: 0.2779681771402487\ncclabel: -5499479516525190226\n"}, {"font": {"color": "white"}, "id": "D\u00fanhere", "label": "D\u00fanhere", "shape": "dot", "title": "inDegree: 2\noutDegree: 6\ndegree: 8\nprlabel: 0.34758861963720655\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Folca", "label": "Folca", "shape": "dot", "title": "inDegree: 11\noutDegree: 2\ndegree: 13\nprlabel: 1.0487369118557055\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "\u00d3in", "label": "\u00d3in", "shape": "dot", "title": "inDegree: 2\noutDegree: 1\ndegree: 2\nprlabel: 0.5416227505908192\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Halfast", "label": "Halfast", "shape": "dot", "title": "inDegree: 1\noutDegree: 0\ndegree: 1\nprlabel: 0.39807906638175283\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "R\u00famil", "label": "R\u00famil", "shape": "dot", "title": "inDegree: 2\noutDegree: 0\ndegree: 2\nprlabel: 0.5404937692926433\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Dwalin", "label": "Dwalin", "shape": "dot", "title": "inDegree: 0\noutDegree: 3\ndegree: 3\nprlabel: 0.2779681771402487\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Sm\u00e9agol", "label": "Sm\u00e9agol", "shape": "dot", "title": "inDegree: 0\noutDegree: 2\ndegree: 2\nprlabel: 0.2779681771402487\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Pippin", "label": "Pippin", "shape": "dot", "title": "inDegree: 10\noutDegree: 30\ndegree: 34\nprlabel: 0.7647473206630622\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Aldor", "label": "Aldor", "shape": "dot", "title": "inDegree: 9\noutDegree: 4\ndegree: 13\nprlabel: 0.6739772692121865\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Nimloth", "label": "Nimloth", "shape": "dot", "title": "inDegree: 1\noutDegree: 1\ndegree: 2\nprlabel: 0.3313417356425763\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Fr\u00e9a", "label": "Fr\u00e9a", "shape": "dot", "title": "inDegree: 0\noutDegree: 13\ndegree: 13\nprlabel: 0.2779681771402487\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Angbor", "label": "Angbor", "shape": "dot", "title": "inDegree: 0\noutDegree: 1\ndegree: 1\nprlabel: 0.2779681771402487\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Barliman", "label": "Barliman", "shape": "dot", "title": "inDegree: 3\noutDegree: 0\ndegree: 3\nprlabel: 0.4812914025914956\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Nob", "label": "Nob", "shape": "dot", "title": "inDegree: 0\noutDegree: 4\ndegree: 4\nprlabel: 0.2779681771402487\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Elessar", "label": "Elessar", "shape": "dot", "title": "inDegree: 6\noutDegree: 3\ndegree: 9\nprlabel: 1.0921396993616848\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Arwen", "label": "Arwen", "shape": "dot", "title": "inDegree: 2\noutDegree: 13\ndegree: 14\nprlabel: 0.5789619450667451\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Arvedui", "label": "Arvedui", "shape": "dot", "title": "inDegree: 0\noutDegree: 1\ndegree: 1\nprlabel: 0.2779681771402487\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Samwise", "label": "Samwise", "shape": "dot", "title": "inDegree: 9\noutDegree: 4\ndegree: 11\nprlabel: 3.2927619939271104\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Sauron", "label": "Sauron", "shape": "dot", "title": "inDegree: 14\noutDegree: 3\ndegree: 17\nprlabel: 1.7869326018800442\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Ecthelion", "label": "Ecthelion", "shape": "dot", "title": "inDegree: 5\noutDegree: 0\ndegree: 5\nprlabel: 0.694807229468506\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Ori", "label": "Ori", "shape": "dot", "title": "inDegree: 2\noutDegree: 3\ndegree: 3\nprlabel: 0.780781784439225\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Dior", "label": "Dior", "shape": "dot", "title": "inDegree: 5\noutDegree: 0\ndegree: 5\nprlabel: 1.181141236197166\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "An\u00e1rion", "label": "An\u00e1rion", "shape": "dot", "title": "inDegree: 4\noutDegree: 0\ndegree: 4\nprlabel: 0.6527370388965335\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "C\u00edrdan", "label": "C\u00edrdan", "shape": "dot", "title": "inDegree: 5\noutDegree: 0\ndegree: 5\nprlabel: 0.9727299721992566\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Lotho", "label": "Lotho", "shape": "dot", "title": "inDegree: 6\noutDegree: 0\ndegree: 6\nprlabel: 0.6126960299457864\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Damrod", "label": "Damrod", "shape": "dot", "title": "inDegree: 3\noutDegree: 1\ndegree: 4\nprlabel: 0.6591367696238479\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Grimbeorn", "label": "Grimbeorn", "shape": "dot", "title": "inDegree: 2\noutDegree: 0\ndegree: 2\nprlabel: 0.63351634167151\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Th\u00e9oden", "label": "Th\u00e9oden", "shape": "dot", "title": "inDegree: 9\noutDegree: 22\ndegree: 30\nprlabel: 0.6981903533419308\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Maggot", "label": "Maggot", "shape": "dot", "title": "inDegree: 0\noutDegree: 4\ndegree: 4\nprlabel: 0.2779681771402487\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Gandalf", "label": "Gandalf", "shape": "dot", "title": "inDegree: 24\noutDegree: 35\ndegree: 49\nprlabel: 2.19773481286299\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Frodo", "label": "Frodo", "shape": "dot", "title": "inDegree: 22\noutDegree: 37\ndegree: 51\nprlabel: 2.8064749122888895\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Elrond", "label": "Elrond", "shape": "dot", "title": "inDegree: 24\noutDegree: 18\ndegree: 32\nprlabel: 5.621547602921244\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Shagrat", "label": "Shagrat", "shape": "dot", "title": "inDegree: 5\noutDegree: 0\ndegree: 5\nprlabel: 1.0012665673542525\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Brego", "label": "Brego", "shape": "dot", "title": "inDegree: 6\noutDegree: 8\ndegree: 13\nprlabel: 0.4561069889100549\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Denethor", "label": "Denethor", "shape": "dot", "title": "inDegree: 14\noutDegree: 6\ndegree: 19\nprlabel: 1.8504021876179562\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Butterbur", "label": "Butterbur", "shape": "dot", "title": "inDegree: 5\noutDegree: 5\ndegree: 10\nprlabel: 0.5028034556140774\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Amroth", "label": "Amroth", "shape": "dot", "title": "inDegree: 12\noutDegree: 5\ndegree: 16\nprlabel: 1.7651693715720893\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Anborn", "label": "Anborn", "shape": "dot", "title": "inDegree: 0\noutDegree: 3\ndegree: 3\nprlabel: 0.2779681771402487\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Merry", "label": "Merry", "shape": "dot", "title": "inDegree: 18\noutDegree: 23\ndegree: 34\nprlabel: 1.570019955614523\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Lobelia", "label": "Lobelia", "shape": "dot", "title": "inDegree: 0\noutDegree: 3\ndegree: 3\nprlabel: 0.2779681771402487\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "\u00c9omer", "label": "\u00c9omer", "shape": "dot", "title": "inDegree: 10\noutDegree: 16\ndegree: 19\nprlabel: 3.473896575838601\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Treebeard", "label": "Treebeard", "shape": "dot", "title": "inDegree: 0\noutDegree: 7\ndegree: 7\nprlabel: 0.2779681771402487\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Celeborn", "label": "Celeborn", "shape": "dot", "title": "inDegree: 9\noutDegree: 2\ndegree: 10\nprlabel: 1.497625828867752\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Barahir", "label": "Barahir", "shape": "dot", "title": "inDegree: 2\noutDegree: 2\ndegree: 4\nprlabel: 0.5306156437423766\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Marcho", "label": "Marcho", "shape": "dot", "title": "inDegree: 1\noutDegree: 0\ndegree: 1\nprlabel: 0.5142412100774039\ncclabel: -5499479516525190226\n"}, {"font": {"color": "white"}, "id": "Shadowfax", "label": "Shadowfax", "shape": "dot", "title": "inDegree: 6\noutDegree: 8\ndegree: 12\nprlabel: 0.5696134108253041\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Elendil", "label": "Elendil", "shape": "dot", "title": "inDegree: 4\noutDegree: 12\ndegree: 16\nprlabel: 0.595935920910432\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Daeron", "label": "Daeron", "shape": "dot", "title": "inDegree: 1\noutDegree: 0\ndegree: 1\nprlabel: 0.3313417356425763\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Beregond", "label": "Beregond", "shape": "dot", "title": "inDegree: 1\noutDegree: 12\ndegree: 12\nprlabel: 0.299636022206967\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Ungoliant", "label": "Ungoliant", "shape": "dot", "title": "inDegree: 0\noutDegree: 1\ndegree: 1\nprlabel: 0.2779681771402487\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Goldwine", "label": "Goldwine", "shape": "dot", "title": "inDegree: 7\noutDegree: 6\ndegree: 13\nprlabel: 0.5045683718113326\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Bergil", "label": "Bergil", "shape": "dot", "title": "inDegree: 4\noutDegree: 0\ndegree: 4\nprlabel: 0.6335734205328489\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Gollum", "label": "Gollum", "shape": "dot", "title": "inDegree: 9\noutDegree: 7\ndegree: 14\nprlabel: 1.4789143299042746\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Nazg\u00fbl", "label": "Nazg\u00fbl", "shape": "dot", "title": "inDegree: 0\noutDegree: 5\ndegree: 5\nprlabel: 0.2779681771402487\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Robin", "label": "Robin", "shape": "dot", "title": "inDegree: 0\noutDegree: 1\ndegree: 1\nprlabel: 0.2779681771402487\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Radagast", "label": "Radagast", "shape": "dot", "title": "inDegree: 1\noutDegree: 0\ndegree: 1\nprlabel: 0.3898547460670817\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Goldberry", "label": "Goldberry", "shape": "dot", "title": "inDegree: 2\noutDegree: 0\ndegree: 2\nprlabel: 0.36869379381074263\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Meneldor", "label": "Meneldor", "shape": "dot", "title": "inDegree: 0\noutDegree: 1\ndegree: 1\nprlabel: 0.2779681771402487\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Fastred", "label": "Fastred", "shape": "dot", "title": "inDegree: 2\noutDegree: 2\ndegree: 4\nprlabel: 0.3665887519675741\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "F\u00ebanor", "label": "F\u00ebanor", "shape": "dot", "title": "inDegree: 1\noutDegree: 0\ndegree: 1\nprlabel: 0.3313417356425763\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Erkenbrand", "label": "Erkenbrand", "shape": "dot", "title": "inDegree: 4\noutDegree: 1\ndegree: 5\nprlabel: 0.6174529927011518\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Bombur", "label": "Bombur", "shape": "dot", "title": "inDegree: 1\noutDegree: 2\ndegree: 3\nprlabel: 0.3567258547859671\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "\u00c9owyn", "label": "\u00c9owyn", "shape": "dot", "title": "inDegree: 1\noutDegree: 13\ndegree: 13\nprlabel: 0.46251879779844135\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Gh\u00e2n-buri-Gh\u00e2n", "label": "Gh\u00e2n-buri-Gh\u00e2n", "shape": "dot", "title": "inDegree: 1\noutDegree: 0\ndegree: 1\nprlabel: 0.3049437070873484\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Fundin", "label": "Fundin", "shape": "dot", "title": "inDegree: 1\noutDegree: 0\ndegree: 1\nprlabel: 0.320399673820735\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Glorfindel", "label": "Glorfindel", "shape": "dot", "title": "inDegree: 1\noutDegree: 13\ndegree: 13\nprlabel: 0.5434299957444693\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Forlong", "label": "Forlong", "shape": "dot", "title": "inDegree: 1\noutDegree: 1\ndegree: 2\nprlabel: 0.299636022206967\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Erestor", "label": "Erestor", "shape": "dot", "title": "inDegree: 4\noutDegree: 4\ndegree: 7\nprlabel: 0.7648889049226\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "H\u00farin", "label": "H\u00farin", "shape": "dot", "title": "inDegree: 6\noutDegree: 2\ndegree: 8\nprlabel: 1.4133814804285367\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "E\u00e4rnur", "label": "E\u00e4rnur", "shape": "dot", "title": "inDegree: 0\noutDegree: 1\ndegree: 1\nprlabel: 0.2779681771402487\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Bard", "label": "Bard", "shape": "dot", "title": "inDegree: 0\noutDegree: 2\ndegree: 2\nprlabel: 0.2779681771402487\ncclabel: -6628080393138316116\n"}, {"font": {"color": "white"}, "id": "Celebrimbor", "label": "Celebrimbor", "shape": "dot", "title": "inDegree: 0\noutDegree: 1\ndegree: 1\nprlabel: 0.2779681771402487\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Aragorn", "label": "Aragorn", "shape": "dot", "title": "inDegree: 45\noutDegree: 5\ndegree: 45\nprlabel: 13.246457152723524\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Beren", "label": "Beren", "shape": "dot", "title": "inDegree: 4\noutDegree: 12\ndegree: 15\nprlabel: 0.6301437923656918\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Gorbag", "label": "Gorbag", "shape": "dot", "title": "inDegree: 0\noutDegree: 2\ndegree: 2\nprlabel: 0.2779681771402487\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Balin", "label": "Balin", "shape": "dot", "title": "inDegree: 1\noutDegree: 10\ndegree: 10\nprlabel: 0.4991912539103329\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Bob", "label": "Bob", "shape": "dot", "title": "inDegree: 2\noutDegree: 1\ndegree: 3\nprlabel: 0.42251304486820235\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Orophin", "label": "Orophin", "shape": "dot", "title": "inDegree: 0\noutDegree: 1\ndegree: 1\nprlabel: 0.2779681771402487\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Thorin", "label": "Thorin", "shape": "dot", "title": "inDegree: 0\noutDegree: 3\ndegree: 3\nprlabel: 0.2779681771402487\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Elwing", "label": "Elwing", "shape": "dot", "title": "inDegree: 2\noutDegree: 3\ndegree: 5\nprlabel: 0.5908690242734329\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "T\u00farin", "label": "T\u00farin", "shape": "dot", "title": "inDegree: 1\noutDegree: 2\ndegree: 3\nprlabel: 0.32260339618443423\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Harding", "label": "Harding", "shape": "dot", "title": "inDegree: 4\noutDegree: 0\ndegree: 4\nprlabel: 0.9664199512234719\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Bofur", "label": "Bofur", "shape": "dot", "title": "inDegree: 3\noutDegree: 0\ndegree: 3\nprlabel: 0.9952230832486243\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "L\u00fathien", "label": "L\u00fathien", "shape": "dot", "title": "inDegree: 10\noutDegree: 6\ndegree: 14\nprlabel: 1.4683201618492872\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Bill", "label": "Bill", "shape": "dot", "title": "inDegree: 7\noutDegree: 0\ndegree: 7\nprlabel: 1.0974848873445304\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Imrahil", "label": "Imrahil", "shape": "dot", "title": "inDegree: 15\noutDegree: 10\ndegree: 18\nprlabel: 4.095599894020988\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Folcwine", "label": "Folcwine", "shape": "dot", "title": "inDegree: 8\noutDegree: 5\ndegree: 13\nprlabel: 0.5760489158054838\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Galdor", "label": "Galdor", "shape": "dot", "title": "inDegree: 4\noutDegree: 4\ndegree: 7\nprlabel: 0.6968085610243271\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Haldir", "label": "Haldir", "shape": "dot", "title": "inDegree: 0\noutDegree: 9\ndegree: 9\nprlabel: 0.2779681771402487\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Bilbo", "label": "Bilbo", "shape": "dot", "title": "inDegree: 16\noutDegree: 7\ndegree: 21\nprlabel: 1.5899193131870386\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Hamfast", "label": "Hamfast", "shape": "dot", "title": "inDegree: 1\noutDegree: 2\ndegree: 3\nprlabel: 0.34244123459550313\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Finduilas", "label": "Finduilas", "shape": "dot", "title": "inDegree: 2\noutDegree: 0\ndegree: 2\nprlabel: 0.6082883722417468\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Tom", "label": "Tom", "shape": "dot", "title": "inDegree: 0\noutDegree: 9\ndegree: 9\nprlabel: 0.2779681771402487\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Sam", "label": "Sam", "shape": "dot", "title": "inDegree: 17\noutDegree: 20\ndegree: 28\nprlabel: 2.8261396204962206\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "E\u00e4rendil", "label": "E\u00e4rendil", "shape": "dot", "title": "inDegree: 3\noutDegree: 3\ndegree: 6\nprlabel: 0.7582821032958648\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Saruman", "label": "Saruman", "shape": "dot", "title": "inDegree: 12\noutDegree: 8\ndegree: 18\nprlabel: 1.0530496666370888\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Ohtar", "label": "Ohtar", "shape": "dot", "title": "inDegree: 3\noutDegree: 0\ndegree: 3\nprlabel: 0.39516416689801415\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Beorn", "label": "Beorn", "shape": "dot", "title": "inDegree: 1\noutDegree: 1\ndegree: 2\nprlabel: 0.34244123459550313\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Smaug", "label": "Smaug", "shape": "dot", "title": "inDegree: 0\noutDegree: 1\ndegree: 1\nprlabel: 0.2779681771402487\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Meriadoc", "label": "Meriadoc", "shape": "dot", "title": "inDegree: 3\noutDegree: 14\ndegree: 17\nprlabel: 0.5477228477122122\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Shelob", "label": "Shelob", "shape": "dot", "title": "inDegree: 4\noutDegree: 2\ndegree: 5\nprlabel: 0.8784076308786182\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Thr\u00f3r", "label": "Thr\u00f3r", "shape": "dot", "title": "inDegree: 1\noutDegree: 0\ndegree: 1\nprlabel: 0.320399673820735\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Meneldil", "label": "Meneldil", "shape": "dot", "title": "inDegree: 0\noutDegree: 1\ndegree: 1\nprlabel: 0.2779681771402487\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Goldilocks", "label": "Goldilocks", "shape": "dot", "title": "inDegree: 2\noutDegree: 0\ndegree: 2\nprlabel: 0.35765848735249517\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Gimli", "label": "Gimli", "shape": "dot", "title": "inDegree: 11\noutDegree: 22\ndegree: 25\nprlabel: 2.0427344133448773\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Wormtongue", "label": "Wormtongue", "shape": "dot", "title": "inDegree: 5\noutDegree: 1\ndegree: 6\nprlabel: 0.5926993571172917\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Halbarad", "label": "Halbarad", "shape": "dot", "title": "inDegree: 8\noutDegree: 1\ndegree: 9\nprlabel: 1.4155816195130049\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Legolas", "label": "Legolas", "shape": "dot", "title": "inDegree: 16\noutDegree: 18\ndegree: 25\nprlabel: 4.622590485531273\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Odo", "label": "Odo", "shape": "dot", "title": "inDegree: 0\noutDegree: 1\ndegree: 1\nprlabel: 0.2779681771402487\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Faramir", "label": "Faramir", "shape": "dot", "title": "inDegree: 29\noutDegree: 3\ndegree: 29\nprlabel: 8.551166347981415\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Bain", "label": "Bain", "shape": "dot", "title": "inDegree: 1\noutDegree: 1\ndegree: 2\nprlabel: 0.3961046936088263\ncclabel: -6628080393138316116\n"}, {"font": {"color": "white"}, "id": "Walda", "label": "Walda", "shape": "dot", "title": "inDegree: 10\noutDegree: 3\ndegree: 13\nprlabel: 0.8171975016277457\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Thranduil", "label": "Thranduil", "shape": "dot", "title": "inDegree: 2\noutDegree: 0\ndegree: 2\nprlabel: 0.7617188370130762\ncclabel: -8637342647242242534\n"}, {"font": {"color": "white"}, "id": "Boromir", "label": "Boromir", "shape": "dot", "title": "inDegree: 17\noutDegree: 6\ndegree: 18\nprlabel: 4.824013505788677\ncclabel: -8637342647242242534\n"}]);
        edges = new vis.DataSet([{"from": "Hirgon", "to": "Denethor"}, {"from": "Hirgon", "to": "Gandalf"}, {"from": "Hador", "to": "H\u00farin"}, {"from": "Horn", "to": "Harding"}, {"from": "Galadriel", "to": "Elrond"}, {"from": "Galadriel", "to": "Sam"}, {"from": "Galadriel", "to": "Faramir"}, {"from": "Galadriel", "to": "Celeborn"}, {"from": "Galadriel", "to": "Aragorn"}, {"from": "Galadriel", "to": "Amroth"}, {"from": "Isildur", "to": "Gil-galad"}, {"from": "Isildur", "to": "Frodo"}, {"from": "Isildur", "to": "Valandil"}, {"from": "Isildur", "to": "Elrond"}, {"from": "Isildur", "to": "Denethor"}, {"from": "Isildur", "to": "Sauron"}, {"from": "Isildur", "to": "Gollum"}, {"from": "Isildur", "to": "An\u00e1rion"}, {"from": "Isildur", "to": "D\u00e9agol"}, {"from": "Isildur", "to": "Halbarad"}, {"from": "Isildur", "to": "Th\u00e9oden"}, {"from": "Isildur", "to": "Saruman"}, {"from": "Isildur", "to": "Ohtar"}, {"from": "Isildur", "to": "Gandalf"}, {"from": "Isildur", "to": "Boromir"}, {"from": "Isildur", "to": "Aragorn"}, {"from": "Isildur", "to": "Elendil"}, {"from": "Isildur", "to": "Elessar"}, {"from": "Mablung", "to": "Damrod"}, {"from": "Mablung", "to": "Faramir"}, {"from": "Thingol", "to": "Dior"}, {"from": "Thingol", "to": "Elwing"}, {"from": "Thingol", "to": "E\u00e4rendil"}, {"from": "Thingol", "to": "L\u00fathien"}, {"from": "Gamling", "to": "Saruman"}, {"from": "Baldor", "to": "Goldwine"}, {"from": "Baldor", "to": "D\u00e9or"}, {"from": "Baldor", "to": "Folcwine"}, {"from": "Baldor", "to": "Aldor"}, {"from": "Baldor", "to": "Brego"}, {"from": "Baldor", "to": "Walda"}, {"from": "Baldor", "to": "Folca"}, {"from": "Baldor", "to": "Thengel"}, {"from": "Baldor", "to": "Gram"}, {"from": "Helm", "to": "Goldwine"}, {"from": "Helm", "to": "D\u00e9or"}, {"from": "Helm", "to": "Erkenbrand"}, {"from": "Helm", "to": "Brego"}, {"from": "Helm", "to": "Folca"}, {"from": "Helm", "to": "Gram"}, {"from": "Helm", "to": "Gamling"}, {"from": "Helm", "to": "Folcwine"}, {"from": "Helm", "to": "Halbarad"}, {"from": "Helm", "to": "Aldor"}, {"from": "Helm", "to": "Baldor"}, {"from": "Helm", "to": "Walda"}, {"from": "Helm", "to": "Th\u00e9oden"}, {"from": "Helm", "to": "Saruman"}, {"from": "Helm", "to": "Thengel"}, {"from": "Helm", "to": "Gandalf"}, {"from": "Thengel", "to": "Gram"}, {"from": "Thengel", "to": "Aragorn"}, {"from": "Gil-galad", "to": "Sauron"}, {"from": "Gil-galad", "to": "Elrond"}, {"from": "Gil-galad", "to": "An\u00e1rion"}, {"from": "Gil-galad", "to": "C\u00edrdan"}, {"from": "Valandil", "to": "Ohtar"}, {"from": "Valandil", "to": "Aragorn"}, {"from": "Valandil", "to": "Elendil"}, {"from": "Valandil", "to": "Elessar"}, {"from": "Fengel", "to": "Goldwine"}, {"from": "Fengel", "to": "D\u00e9or"}, {"from": "Fengel", "to": "Brego"}, {"from": "Fengel", "to": "Folca"}, {"from": "Fengel", "to": "Gram"}, {"from": "Fengel", "to": "Folcwine"}, {"from": "Fengel", "to": "Aldor"}, {"from": "Fengel", "to": "Baldor"}, {"from": "Fengel", "to": "Helm"}, {"from": "Fengel", "to": "Walda"}, {"from": "Fengel", "to": "Th\u00e9oden"}, {"from": "Fengel", "to": "Thengel"}, {"from": "Gl\u00f3in", "to": "Frodo"}, {"from": "Gl\u00f3in", "to": "Bofur"}, {"from": "Gl\u00f3in", "to": "Aragorn"}, {"from": "D\u00e9or", "to": "Goldwine"}, {"from": "D\u00e9or", "to": "Folcwine"}, {"from": "D\u00e9or", "to": "Aldor"}, {"from": "D\u00e9or", "to": "Brego"}, {"from": "D\u00e9or", "to": "Walda"}, {"from": "D\u00e9or", "to": "Folca"}, {"from": "D\u00e9or", "to": "Thengel"}, {"from": "D\u00e9or", "to": "Gram"}, {"from": "Peregrin", "to": "Elrond"}, {"from": "Peregrin", "to": "Fredegar"}, {"from": "Peregrin", "to": "Denethor"}, {"from": "Peregrin", "to": "Imrahil"}, {"from": "Peregrin", "to": "Ecthelion"}, {"from": "Peregrin", "to": "Pippin"}, {"from": "Peregrin", "to": "Merry"}, {"from": "Peregrin", "to": "Bilbo"}, {"from": "Peregrin", "to": "Lotho"}, {"from": "Peregrin", "to": "Legolas"}, {"from": "Peregrin", "to": "\u00c9omer"}, {"from": "Peregrin", "to": "Sam"}, {"from": "Peregrin", "to": "Gandalf"}, {"from": "Peregrin", "to": "Boromir"}, {"from": "Peregrin", "to": "Aragorn"}, {"from": "Peregrin", "to": "Shadowfax"}, {"from": "Peregrin", "to": "Elendil"}, {"from": "Peregrin", "to": "Elessar"}, {"from": "Grimbold", "to": "Fastred"}, {"from": "Grimbold", "to": "Erkenbrand"}, {"from": "Grimbold", "to": "Horn"}, {"from": "Grimbold", "to": "Halbarad"}, {"from": "Grimbold", "to": "Harding"}, {"from": "Grimbold", "to": "D\u00fanhere"}, {"from": "Blanco", "to": "Marcho"}, {"from": "D\u00fanhere", "to": "Fastred"}, {"from": "D\u00fanhere", "to": "Horn"}, {"from": "D\u00fanhere", "to": "\u00c9omer"}, {"from": "D\u00fanhere", "to": "Harding"}, {"from": "D\u00fanhere", "to": "Th\u00e9oden"}, {"from": "D\u00fanhere", "to": "Gandalf"}, {"from": "Folca", "to": "Thengel"}, {"from": "Folca", "to": "Gram"}, {"from": "\u00d3in", "to": "Ori"}, {"from": "Dwalin", "to": "Gl\u00f3in"}, {"from": "Dwalin", "to": "Bombur"}, {"from": "Dwalin", "to": "Bofur"}, {"from": "Sm\u00e9agol", "to": "Gollum"}, {"from": "Sm\u00e9agol", "to": "D\u00e9agol"}, {"from": "Pippin", "to": "Galadriel"}, {"from": "Pippin", "to": "L\u00fathien"}, {"from": "Pippin", "to": "Imrahil"}, {"from": "Pippin", "to": "Bergil"}, {"from": "Pippin", "to": "Samwise"}, {"from": "Pippin", "to": "Ecthelion"}, {"from": "Pippin", "to": "Gollum"}, {"from": "Pippin", "to": "Bilbo"}, {"from": "Pippin", "to": "Lotho"}, {"from": "Pippin", "to": "Sam"}, {"from": "Pippin", "to": "Saruman"}, {"from": "Pippin", "to": "Gandalf"}, {"from": "Pippin", "to": "Frodo"}, {"from": "Pippin", "to": "Elrond"}, {"from": "Pippin", "to": "Fredegar"}, {"from": "Pippin", "to": "Denethor"}, {"from": "Pippin", "to": "Forlong"}, {"from": "Pippin", "to": "Butterbur"}, {"from": "Pippin", "to": "Amroth"}, {"from": "Pippin", "to": "Goldilocks"}, {"from": "Pippin", "to": "Merry"}, {"from": "Pippin", "to": "Gimli"}, {"from": "Pippin", "to": "Legolas"}, {"from": "Pippin", "to": "Faramir"}, {"from": "Pippin", "to": "Boromir"}, {"from": "Pippin", "to": "Aragorn"}, {"from": "Pippin", "to": "Shadowfax"}, {"from": "Pippin", "to": "Beregond"}, {"from": "Pippin", "to": "Beren"}, {"from": "Aldor", "to": "Walda"}, {"from": "Aldor", "to": "Folca"}, {"from": "Aldor", "to": "Thengel"}, {"from": "Aldor", "to": "Gram"}, {"from": "Nimloth", "to": "Aragorn"}, {"from": "Fr\u00e9a", "to": "Fengel"}, {"from": "Fr\u00e9a", "to": "Goldwine"}, {"from": "Fr\u00e9a", "to": "D\u00e9or"}, {"from": "Fr\u00e9a", "to": "Brego"}, {"from": "Fr\u00e9a", "to": "Folca"}, {"from": "Fr\u00e9a", "to": "Gram"}, {"from": "Fr\u00e9a", "to": "Folcwine"}, {"from": "Fr\u00e9a", "to": "Aldor"}, {"from": "Fr\u00e9a", "to": "Baldor"}, {"from": "Fr\u00e9a", "to": "Helm"}, {"from": "Fr\u00e9a", "to": "Walda"}, {"from": "Fr\u00e9a", "to": "Th\u00e9oden"}, {"from": "Fr\u00e9a", "to": "Thengel"}, {"from": "Angbor", "to": "Aragorn"}, {"from": "Nob", "to": "Bob"}, {"from": "Nob", "to": "Merry"}, {"from": "Nob", "to": "Gandalf"}, {"from": "Nob", "to": "Butterbur"}, {"from": "Elessar", "to": "Halbarad"}, {"from": "Elessar", "to": "Faramir"}, {"from": "Elessar", "to": "Aragorn"}, {"from": "Arwen", "to": "Frodo"}, {"from": "Arwen", "to": "Elrond"}, {"from": "Arwen", "to": "Galadriel"}, {"from": "Arwen", "to": "L\u00fathien"}, {"from": "Arwen", "to": "Imrahil"}, {"from": "Arwen", "to": "Amroth"}, {"from": "Arwen", "to": "Erestor"}, {"from": "Arwen", "to": "Celebr\u00edan"}, {"from": "Arwen", "to": "Bilbo"}, {"from": "Arwen", "to": "Faramir"}, {"from": "Arwen", "to": "Celeborn"}, {"from": "Arwen", "to": "Aragorn"}, {"from": "Arwen", "to": "Elessar"}, {"from": "Arvedui", "to": "Aragorn"}, {"from": "Samwise", "to": "Frodo"}, {"from": "Samwise", "to": "Faramir"}, {"from": "Samwise", "to": "Boromir"}, {"from": "Samwise", "to": "Aragorn"}, {"from": "Sauron", "to": "Thengel"}, {"from": "Sauron", "to": "Aragorn"}, {"from": "Sauron", "to": "Thingol"}, {"from": "Ori", "to": "Balin"}, {"from": "Ori", "to": "Gimli"}, {"from": "Damrod", "to": "Faramir"}, {"from": "Th\u00e9oden", "to": "Goldwine"}, {"from": "Th\u00e9oden", "to": "D\u00e9or"}, {"from": "Th\u00e9oden", "to": "Gh\u00e2n-buri-Gh\u00e2n"}, {"from": "Th\u00e9oden", "to": "Brego"}, {"from": "Th\u00e9oden", "to": "Folca"}, {"from": "Th\u00e9oden", "to": "Denethor"}, {"from": "Th\u00e9oden", "to": "Gram"}, {"from": "Th\u00e9oden", "to": "Imrahil"}, {"from": "Th\u00e9oden", "to": "Sauron"}, {"from": "Th\u00e9oden", "to": "Merry"}, {"from": "Th\u00e9oden", "to": "Folcwine"}, {"from": "Th\u00e9oden", "to": "Gimli"}, {"from": "Th\u00e9oden", "to": "Wormtongue"}, {"from": "Th\u00e9oden", "to": "Legolas"}, {"from": "Th\u00e9oden", "to": "\u00c9omer"}, {"from": "Th\u00e9oden", "to": "Aldor"}, {"from": "Th\u00e9oden", "to": "Faramir"}, {"from": "Th\u00e9oden", "to": "Baldor"}, {"from": "Th\u00e9oden", "to": "Walda"}, {"from": "Th\u00e9oden", "to": "Saruman"}, {"from": "Th\u00e9oden", "to": "Thengel"}, {"from": "Th\u00e9oden", "to": "Aragorn"}, {"from": "Maggot", "to": "Pippin"}, {"from": "Maggot", "to": "Frodo"}, {"from": "Maggot", "to": "Merry"}, {"from": "Maggot", "to": "Bilbo"}, {"from": "Gandalf", "to": "Galadriel"}, {"from": "Gandalf", "to": "Bill"}, {"from": "Gandalf", "to": "Imrahil"}, {"from": "Gandalf", "to": "Samwise"}, {"from": "Gandalf", "to": "Sauron"}, {"from": "Gandalf", "to": "Ecthelion"}, {"from": "Gandalf", "to": "Galdor"}, {"from": "Gandalf", "to": "Bilbo"}, {"from": "Gandalf", "to": "Lotho"}, {"from": "Gandalf", "to": "Sam"}, {"from": "Gandalf", "to": "Saruman"}, {"from": "Gandalf", "to": "Thengel"}, {"from": "Gandalf", "to": "F\u00ebanor"}, {"from": "Gandalf", "to": "Frodo"}, {"from": "Gandalf", "to": "Elrond"}, {"from": "Gandalf", "to": "Gl\u00f3in"}, {"from": "Gandalf", "to": "Erkenbrand"}, {"from": "Gandalf", "to": "Fredegar"}, {"from": "Gandalf", "to": "Denethor"}, {"from": "Gandalf", "to": "Butterbur"}, {"from": "Gandalf", "to": "Amroth"}, {"from": "Gandalf", "to": "Merry"}, {"from": "Gandalf", "to": "Gimli"}, {"from": "Gandalf", "to": "Wormtongue"}, {"from": "Gandalf", "to": "Legolas"}, {"from": "Gandalf", "to": "\u00c9omer"}, {"from": "Gandalf", "to": "Faramir"}, {"from": "Gandalf", "to": "Nimloth"}, {"from": "Gandalf", "to": "Boromir"}, {"from": "Gandalf", "to": "Barliman"}, {"from": "Gandalf", "to": "Aragorn"}, {"from": "Gandalf", "to": "Shadowfax"}, {"from": "Gandalf", "to": "Daeron"}, {"from": "Frodo", "to": "Galadriel"}, {"from": "Frodo", "to": "L\u00fathien"}, {"from": "Frodo", "to": "Mablung"}, {"from": "Frodo", "to": "Bill"}, {"from": "Frodo", "to": "Imrahil"}, {"from": "Frodo", "to": "Gollum"}, {"from": "Frodo", "to": "Galdor"}, {"from": "Frodo", "to": "Bilbo"}, {"from": "Frodo", "to": "Hamfast"}, {"from": "Frodo", "to": "Lotho"}, {"from": "Frodo", "to": "Sam"}, {"from": "Frodo", "to": "Damrod"}, {"from": "Frodo", "to": "Grimbeorn"}, {"from": "Frodo", "to": "Saruman"}, {"from": "Frodo", "to": "Goldberry"}, {"from": "Frodo", "to": "Beorn"}, {"from": "Frodo", "to": "Elrond"}, {"from": "Frodo", "to": "Peregrin"}, {"from": "Frodo", "to": "Shagrat"}, {"from": "Frodo", "to": "Fredegar"}, {"from": "Frodo", "to": "Shelob"}, {"from": "Frodo", "to": "Denethor"}, {"from": "Frodo", "to": "Butterbur"}, {"from": "Frodo", "to": "Amroth"}, {"from": "Frodo", "to": "Merry"}, {"from": "Frodo", "to": "Gimli"}, {"from": "Frodo", "to": "Wormtongue"}, {"from": "Frodo", "to": "Legolas"}, {"from": "Frodo", "to": "Faramir"}, {"from": "Frodo", "to": "Boromir"}, {"from": "Frodo", "to": "Barliman"}, {"from": "Frodo", "to": "Aragorn"}, {"from": "Frodo", "to": "Beren"}, {"from": "Elrond", "to": "Glorfindel"}, {"from": "Elrond", "to": "Imrahil"}, {"from": "Elrond", "to": "Amroth"}, {"from": "Elrond", "to": "Erestor"}, {"from": "Elrond", "to": "Sauron"}, {"from": "Elrond", "to": "Galdor"}, {"from": "Elrond", "to": "C\u00edrdan"}, {"from": "Elrond", "to": "Halbarad"}, {"from": "Elrond", "to": "Legolas"}, {"from": "Elrond", "to": "\u00c9omer"}, {"from": "Elrond", "to": "Sam"}, {"from": "Elrond", "to": "Faramir"}, {"from": "Elrond", "to": "Thranduil"}, {"from": "Elrond", "to": "Celeborn"}, {"from": "Elrond", "to": "Boromir"}, {"from": "Elrond", "to": "Aragorn"}, {"from": "Brego", "to": "Goldwine"}, {"from": "Brego", "to": "Folcwine"}, {"from": "Brego", "to": "Aldor"}, {"from": "Brego", "to": "Walda"}, {"from": "Brego", "to": "Folca"}, {"from": "Brego", "to": "Thengel"}, {"from": "Brego", "to": "Gram"}, {"from": "Denethor", "to": "Sauron"}, {"from": "Denethor", "to": "Ecthelion"}, {"from": "Denethor", "to": "Elrond"}, {"from": "Denethor", "to": "Faramir"}, {"from": "Denethor", "to": "Boromir"}, {"from": "Denethor", "to": "Aragorn"}, {"from": "Butterbur", "to": "Bob"}, {"from": "Butterbur", "to": "Merry"}, {"from": "Butterbur", "to": "Bilbo"}, {"from": "Butterbur", "to": "Bill"}, {"from": "Butterbur", "to": "Barliman"}, {"from": "Amroth", "to": "Finduilas"}, {"from": "Amroth", "to": "Faramir"}, {"from": "Amroth", "to": "Celeborn"}, {"from": "Amroth", "to": "Imrahil"}, {"from": "Amroth", "to": "Aragorn"}, {"from": "Anborn", "to": "Frodo"}, {"from": "Anborn", "to": "Gollum"}, {"from": "Anborn", "to": "Faramir"}, {"from": "Merry", "to": "Gil-galad"}, {"from": "Merry", "to": "Galadriel"}, {"from": "Merry", "to": "Fredegar"}, {"from": "Merry", "to": "Denethor"}, {"from": "Merry", "to": "L\u00fathien"}, {"from": "Merry", "to": "Bill"}, {"from": "Merry", "to": "Bergil"}, {"from": "Merry", "to": "Goldilocks"}, {"from": "Merry", "to": "Bilbo"}, {"from": "Merry", "to": "Gimli"}, {"from": "Merry", "to": "Wormtongue"}, {"from": "Merry", "to": "Lotho"}, {"from": "Merry", "to": "Legolas"}, {"from": "Merry", "to": "Sam"}, {"from": "Merry", "to": "Faramir"}, {"from": "Merry", "to": "Saruman"}, {"from": "Merry", "to": "Boromir"}, {"from": "Merry", "to": "Aragorn"}, {"from": "Merry", "to": "Shadowfax"}, {"from": "Merry", "to": "Beren"}, {"from": "Lobelia", "to": "Frodo"}, {"from": "Lobelia", "to": "Lotho"}, {"from": "Lobelia", "to": "Sam"}, {"from": "\u00c9omer", "to": "Gl\u00f3in"}, {"from": "\u00c9omer", "to": "\u00c9owyn"}, {"from": "\u00c9omer", "to": "Denethor"}, {"from": "\u00c9omer", "to": "Imrahil"}, {"from": "\u00c9omer", "to": "Amroth"}, {"from": "\u00c9omer", "to": "H\u00farin"}, {"from": "\u00c9omer", "to": "Merry"}, {"from": "\u00c9omer", "to": "Gimli"}, {"from": "\u00c9omer", "to": "Halbarad"}, {"from": "\u00c9omer", "to": "Legolas"}, {"from": "\u00c9omer", "to": "Faramir"}, {"from": "\u00c9omer", "to": "Aragorn"}, {"from": "\u00c9omer", "to": "Elendil"}, {"from": "Treebeard", "to": "Pippin"}, {"from": "Treebeard", "to": "Merry"}, {"from": "Treebeard", "to": "Galadriel"}, {"from": "Treebeard", "to": "Saruman"}, {"from": "Treebeard", "to": "Celeborn"}, {"from": "Treebeard", "to": "Gandalf"}, {"from": "Treebeard", "to": "Aragorn"}, {"from": "Celeborn", "to": "Aragorn"}, {"from": "Barahir", "to": "Faramir"}, {"from": "Barahir", "to": "Thingol"}, {"from": "Shadowfax", "to": "Samwise"}, {"from": "Shadowfax", "to": "Frodo"}, {"from": "Shadowfax", "to": "Gimli"}, {"from": "Shadowfax", "to": "Legolas"}, {"from": "Shadowfax", "to": "Faramir"}, {"from": "Shadowfax", "to": "Aragorn"}, {"from": "Shadowfax", "to": "Amroth"}, {"from": "Elendil", "to": "Gil-galad"}, {"from": "Elendil", "to": "Frodo"}, {"from": "Elendil", "to": "Elrond"}, {"from": "Elendil", "to": "Denethor"}, {"from": "Elendil", "to": "Sauron"}, {"from": "Elendil", "to": "An\u00e1rion"}, {"from": "Elendil", "to": "Th\u00e9oden"}, {"from": "Elendil", "to": "Ohtar"}, {"from": "Elendil", "to": "Thengel"}, {"from": "Elendil", "to": "Boromir"}, {"from": "Elendil", "to": "Aragorn"}, {"from": "Elendil", "to": "Elessar"}, {"from": "Beregond", "to": "Frodo"}, {"from": "Beregond", "to": "Meriadoc"}, {"from": "Beregond", "to": "Peregrin"}, {"from": "Beregond", "to": "Denethor"}, {"from": "Beregond", "to": "Imrahil"}, {"from": "Beregond", "to": "Bergil"}, {"from": "Beregond", "to": "Ecthelion"}, {"from": "Beregond", "to": "Faramir"}, {"from": "Beregond", "to": "Gandalf"}, {"from": "Beregond", "to": "Aragorn"}, {"from": "Beregond", "to": "Shadowfax"}, {"from": "Ungoliant", "to": "Shelob"}, {"from": "Goldwine", "to": "Folcwine"}, {"from": "Goldwine", "to": "Aldor"}, {"from": "Goldwine", "to": "Walda"}, {"from": "Goldwine", "to": "Folca"}, {"from": "Goldwine", "to": "Thengel"}, {"from": "Goldwine", "to": "Gram"}, {"from": "Gollum", "to": "D\u00e9agol"}, {"from": "Gollum", "to": "Bilbo"}, {"from": "Gollum", "to": "Faramir"}, {"from": "Gollum", "to": "Shelob"}, {"from": "Gollum", "to": "Denethor"}, {"from": "Gollum", "to": "Gandalf"}, {"from": "Gollum", "to": "Aragorn"}, {"from": "Nazg\u00fbl", "to": "Sauron"}, {"from": "Nazg\u00fbl", "to": "Shagrat"}, {"from": "Nazg\u00fbl", "to": "Faramir"}, {"from": "Nazg\u00fbl", "to": "Denethor"}, {"from": "Nazg\u00fbl", "to": "Aragorn"}, {"from": "Robin", "to": "Sam"}, {"from": "Meneldor", "to": "Gandalf"}, {"from": "Fastred", "to": "Horn"}, {"from": "Fastred", "to": "Harding"}, {"from": "Erkenbrand", "to": "Aragorn"}, {"from": "Bombur", "to": "Gl\u00f3in"}, {"from": "Bombur", "to": "Bofur"}, {"from": "\u00c9owyn", "to": "Meriadoc"}, {"from": "\u00c9owyn", "to": "Elrond"}, {"from": "\u00c9owyn", "to": "D\u00fanhere"}, {"from": "\u00c9owyn", "to": "Imrahil"}, {"from": "\u00c9owyn", "to": "Amroth"}, {"from": "\u00c9owyn", "to": "H\u00farin"}, {"from": "\u00c9owyn", "to": "Merry"}, {"from": "\u00c9owyn", "to": "Finduilas"}, {"from": "\u00c9owyn", "to": "Faramir"}, {"from": "\u00c9owyn", "to": "Th\u00e9oden"}, {"from": "\u00c9owyn", "to": "Gandalf"}, {"from": "\u00c9owyn", "to": "Aragorn"}, {"from": "Glorfindel", "to": "Frodo"}, {"from": "Glorfindel", "to": "Gl\u00f3in"}, {"from": "Glorfindel", "to": "Arwen"}, {"from": "Glorfindel", "to": "Galadriel"}, {"from": "Glorfindel", "to": "Erestor"}, {"from": "Glorfindel", "to": "Sauron"}, {"from": "Glorfindel", "to": "Galdor"}, {"from": "Glorfindel", "to": "C\u00edrdan"}, {"from": "Glorfindel", "to": "Saruman"}, {"from": "Glorfindel", "to": "Celeborn"}, {"from": "Glorfindel", "to": "Gandalf"}, {"from": "Glorfindel", "to": "Aragorn"}, {"from": "Forlong", "to": "Bergil"}, {"from": "Erestor", "to": "C\u00edrdan"}, {"from": "Erestor", "to": "Galadriel"}, {"from": "Erestor", "to": "Celeborn"}, {"from": "H\u00farin", "to": "Faramir"}, {"from": "H\u00farin", "to": "Elessar"}, {"from": "E\u00e4rnur", "to": "Faramir"}, {"from": "Bard", "to": "Brand"}, {"from": "Bard", "to": "Bain"}, {"from": "Celebrimbor", "to": "Sauron"}, {"from": "Aragorn", "to": "Legolas"}, {"from": "Aragorn", "to": "Faramir"}, {"from": "Aragorn", "to": "Boromir"}, {"from": "Beren", "to": "Hador"}, {"from": "Beren", "to": "Elwing"}, {"from": "Beren", "to": "T\u00farin"}, {"from": "Beren", "to": "L\u00fathien"}, {"from": "Beren", "to": "Thingol"}, {"from": "Beren", "to": "Sauron"}, {"from": "Beren", "to": "Dior"}, {"from": "Beren", "to": "H\u00farin"}, {"from": "Beren", "to": "Bilbo"}, {"from": "Beren", "to": "Sam"}, {"from": "Beren", "to": "E\u00e4rendil"}, {"from": "Beren", "to": "Barahir"}, {"from": "Gorbag", "to": "Shagrat"}, {"from": "Gorbag", "to": "Sam"}, {"from": "Balin", "to": "Frodo"}, {"from": "Balin", "to": "Bilbo"}, {"from": "Balin", "to": "Gimli"}, {"from": "Balin", "to": "Legolas"}, {"from": "Balin", "to": "Fundin"}, {"from": "Balin", "to": "Thr\u00f3r"}, {"from": "Balin", "to": "\u00d3in"}, {"from": "Balin", "to": "Celeborn"}, {"from": "Balin", "to": "Aragorn"}, {"from": "Bob", "to": "Bill"}, {"from": "Orophin", "to": "R\u00famil"}, {"from": "Thorin", "to": "Gl\u00f3in"}, {"from": "Thorin", "to": "Bilbo"}, {"from": "Thorin", "to": "Gandalf"}, {"from": "Elwing", "to": "Dior"}, {"from": "Elwing", "to": "E\u00e4rendil"}, {"from": "Elwing", "to": "L\u00fathien"}, {"from": "T\u00farin", "to": "Hador"}, {"from": "T\u00farin", "to": "H\u00farin"}, {"from": "L\u00fathien", "to": "Sauron"}, {"from": "L\u00fathien", "to": "Dior"}, {"from": "L\u00fathien", "to": "Elrond"}, {"from": "L\u00fathien", "to": "Barahir"}, {"from": "Imrahil", "to": "Gl\u00f3in"}, {"from": "Imrahil", "to": "H\u00farin"}, {"from": "Imrahil", "to": "Gimli"}, {"from": "Imrahil", "to": "Legolas"}, {"from": "Imrahil", "to": "Faramir"}, {"from": "Imrahil", "to": "Aragorn"}, {"from": "Folcwine", "to": "Aldor"}, {"from": "Folcwine", "to": "Walda"}, {"from": "Folcwine", "to": "Folca"}, {"from": "Folcwine", "to": "Thengel"}, {"from": "Folcwine", "to": "Gram"}, {"from": "Galdor", "to": "Erestor"}, {"from": "Galdor", "to": "Gl\u00f3in"}, {"from": "Galdor", "to": "C\u00edrdan"}, {"from": "Haldir", "to": "Pippin"}, {"from": "Haldir", "to": "Frodo"}, {"from": "Haldir", "to": "Merry"}, {"from": "Haldir", "to": "Legolas"}, {"from": "Haldir", "to": "Galadriel"}, {"from": "Haldir", "to": "Celeborn"}, {"from": "Haldir", "to": "Aragorn"}, {"from": "Haldir", "to": "R\u00famil"}, {"from": "Haldir", "to": "Amroth"}, {"from": "Bilbo", "to": "Elrond"}, {"from": "Bilbo", "to": "Findegil"}, {"from": "Bilbo", "to": "Sam"}, {"from": "Bilbo", "to": "L\u00fathien"}, {"from": "Bilbo", "to": "Boromir"}, {"from": "Bilbo", "to": "Aragorn"}, {"from": "Hamfast", "to": "Samwise"}, {"from": "Hamfast", "to": "Bilbo"}, {"from": "Tom", "to": "Frodo"}, {"from": "Tom", "to": "Elrond"}, {"from": "Tom", "to": "Merry"}, {"from": "Tom", "to": "Bilbo"}, {"from": "Tom", "to": "Sam"}, {"from": "Tom", "to": "Galadriel"}, {"from": "Tom", "to": "Goldberry"}, {"from": "Tom", "to": "Gandalf"}, {"from": "Tom", "to": "Butterbur"}, {"from": "Sam", "to": "Shagrat"}, {"from": "Sam", "to": "Shelob"}, {"from": "Sam", "to": "L\u00fathien"}, {"from": "Sam", "to": "Mablung"}, {"from": "Sam", "to": "Bill"}, {"from": "Sam", "to": "Halfast"}, {"from": "Sam", "to": "Samwise"}, {"from": "Sam", "to": "Gollum"}, {"from": "Sam", "to": "Legolas"}, {"from": "Sam", "to": "Damrod"}, {"from": "Sam", "to": "Faramir"}, {"from": "Sam", "to": "Boromir"}, {"from": "Sam", "to": "Aragorn"}, {"from": "E\u00e4rendil", "to": "Dior"}, {"from": "E\u00e4rendil", "to": "Elrond"}, {"from": "E\u00e4rendil", "to": "L\u00fathien"}, {"from": "Saruman", "to": "Sauron"}, {"from": "Saruman", "to": "Wormtongue"}, {"from": "Saruman", "to": "Galadriel"}, {"from": "Saruman", "to": "Radagast"}, {"from": "Saruman", "to": "Boromir"}, {"from": "Saruman", "to": "Aragorn"}, {"from": "Beorn", "to": "Grimbeorn"}, {"from": "Smaug", "to": "Bilbo"}, {"from": "Meriadoc", "to": "Frodo"}, {"from": "Meriadoc", "to": "Peregrin"}, {"from": "Meriadoc", "to": "Fredegar"}, {"from": "Meriadoc", "to": "Imrahil"}, {"from": "Meriadoc", "to": "Pippin"}, {"from": "Meriadoc", "to": "Merry"}, {"from": "Meriadoc", "to": "\u00c9omer"}, {"from": "Meriadoc", "to": "Sam"}, {"from": "Meriadoc", "to": "Faramir"}, {"from": "Meriadoc", "to": "Th\u00e9oden"}, {"from": "Meriadoc", "to": "Saruman"}, {"from": "Meriadoc", "to": "Gandalf"}, {"from": "Meriadoc", "to": "Boromir"}, {"from": "Meriadoc", "to": "Aragorn"}, {"from": "Shelob", "to": "Shagrat"}, {"from": "Meneldil", "to": "An\u00e1rion"}, {"from": "Gimli", "to": "Elrond"}, {"from": "Gimli", "to": "Gl\u00f3in"}, {"from": "Gimli", "to": "Peregrin"}, {"from": "Gimli", "to": "Galadriel"}, {"from": "Gimli", "to": "Bill"}, {"from": "Gimli", "to": "Amroth"}, {"from": "Gimli", "to": "Samwise"}, {"from": "Gimli", "to": "Sauron"}, {"from": "Gimli", "to": "Gamling"}, {"from": "Gimli", "to": "Halbarad"}, {"from": "Gimli", "to": "Legolas"}, {"from": "Gimli", "to": "Sam"}, {"from": "Gimli", "to": "Saruman"}, {"from": "Gimli", "to": "Boromir"}, {"from": "Gimli", "to": "Aragorn"}, {"from": "Wormtongue", "to": "Aragorn"}, {"from": "Halbarad", "to": "Aragorn"}, {"from": "Legolas", "to": "Meriadoc"}, {"from": "Legolas", "to": "Gl\u00f3in"}, {"from": "Legolas", "to": "Erkenbrand"}, {"from": "Legolas", "to": "Galadriel"}, {"from": "Legolas", "to": "Samwise"}, {"from": "Legolas", "to": "Gollum"}, {"from": "Legolas", "to": "Halbarad"}, {"from": "Legolas", "to": "Helm"}, {"from": "Legolas", "to": "Thranduil"}, {"from": "Legolas", "to": "Boromir"}, {"from": "Odo", "to": "Frodo"}, {"from": "Bain", "to": "Brand"}, {"from": "Walda", "to": "Folca"}, {"from": "Walda", "to": "Thengel"}, {"from": "Walda", "to": "Gram"}, {"from": "Boromir", "to": "Faramir"}]);

        // adding nodes and edges to the graph
        data = {nodes: nodes, edges: edges};

        var options = {
    "configure": {
        "enabled": false
    },
    "edges": {
        "color": {
            "inherit": true
        },
        "smooth": {
            "enabled": true,
            "type": "dynamic"
        }
    },
    "interaction": {
        "dragNodes": true,
        "hideEdgesOnDrag": false,
        "hideNodesOnDrag": false
    },
    "physics": {
        "enabled": true,
        "forceAtlas2Based": {
            "avoidOverlap": 0,
            "centralGravity": 0.01,
            "damping": 0.4,
            "gravitationalConstant": -50,
            "springConstant": 0.08,
            "springLength": 100
        },
        "solver": "forceAtlas2Based",
        "stabilization": {
            "enabled": true,
            "fit": true,
            "iterations": 1000,
            "onlyDynamicEdges": false,
            "updateInterval": 50
        }
    }
};
        
        

        

        network = new vis.Network(container, data, options);
	 
        


        
        network.on("stabilizationProgress", function(params) {
      		document.getElementById('loadingBar').removeAttribute("style");
	        var maxWidth = 496;
	        var minWidth = 20;
	        var widthFactor = params.iterations/params.total;
	        var width = Math.max(minWidth,maxWidth * widthFactor);

	        document.getElementById('bar').style.width = width + 'px';
	        document.getElementById('text').innerHTML = Math.round(widthFactor*100) + '%';
	    });
	    network.once("stabilizationIterationsDone", function() {
	        document.getElementById('text').innerHTML = '100%';
	        document.getElementById('bar').style.width = '496px';
	        document.getElementById('loadingBar').style.opacity = 0;
	        // really clean the dom element
	        setTimeout(function () {document.getElementById('loadingBar').style.display = 'none';}, 500);
	    });
        

        return network;

    }

    drawGraph();

</script>
</body>
</html>
</br>

### Shut down PyRaphtory  🛑
````{tabs}

```{code-tab} py
pr.shutdown()
```
````
