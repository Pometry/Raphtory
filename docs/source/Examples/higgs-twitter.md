# Detecting bot activity on Twitter 

## Overview

This is an example to show how Raphtory can run a chain of algorithms to analyse a Twitter dataset from [SNAP](https://snap.stanford.edu/data/higgs-twitter.html), 
collected during and after the announcement of the discovery of a new particle with the features of the elusive Higgs boson on 4th July 2012. 

A Twitter bot 🤖 typically uses the Twitter API to interact and engage with Twitter users. They can autonomously tweet, retweet, like, follow, unfollow, or DM other accounts.

In our example, we took the retweet network dataset from SNAP to see whether Raphtory could detect bot activity during and after the announcement of the elusive Higgs boson.

<p>
 <img src="../_static/higgstwittergraph.png" width="1000px" style="padding: 15px" alt="Retweet Network Graph"/>
</p>

## Pre-requisites

Follow our Installation guide: [Scala](../Install/start.md) or [Python](../Install/setup.md).

## Data

The data is a `csv` file (comma-separated values) and can be found in <a href="https://github.com/Raphtory/Data/blob/main/higgs-retweet-activity.csv" target="_blank">Raphtory's data repo</a>. 
Each line contains user A and user B, where user B is being retweeted by user A. The last value in the line is the time of the retweet in Unix epoch time.

## Higgs Boson Retweet Example 💥

## Setup environment 🌍

Import all necessary dependencies needed to build a graph from your data in PyRaphtory. 

````{tabs}

```{code-tab} py
from pathlib import Path
from pyraphtory.context import PyRaphtory
from pyraphtory.vertex import Vertex
from pyraphtory.spouts import FileSpout
from pyraphtory.builder import *
from pyvis.network import Network
import csv
```
```{code-tab} scala

import com.raphtory.Raphtory
import com.raphtory.algorithms.generic.centrality.PageRank
import com.raphtory.api.input.Source
import com.raphtory.examples.twitter.higgsdataset.analysis.MemberRank
import com.raphtory.examples.twitter.higgsdataset.analysis.TemporalMemberRank
import com.raphtory.examples.twitter.higgsdataset.graphbuilders.TwitterGraphBuilder
import com.raphtory.sinks.FileSink
import com.raphtory.spouts.FileSpout
import com.raphtory.utils.FileUtils

```
````

### Download csv data from Github 💾

````{tabs}

```{code-tab} py
!curl -o /tmp/twitter.csv https://raw.githubusercontent.com/Raphtory/Data/main/higgs-retweet-activity.csv
```
```{code-tab} scala
val path = "/tmp/higgs-retweet-activity.csv"
val url  = "https://raw.githubusercontent.com/Raphtory/Data/main/higgs-retweet-activity.csv"
FileUtils.curlFile(path, url)
```
````

#### Terminal output

```bash
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100 8022k  100 8022k    0     0  4745k      0  0:00:01  0:00:01 --:--:-- 4758k
```
## Preview data 👀

Preview the retweet twitter data: each line includes the source user A (the retweeter), the destination user B (the user being retweeted) and the time at which the retweet occurs.

````{tabs}

```{code-tab} py
!head /tmp/twitter.csv
```
````

    376989,50329,1341101181
    376989,13813,1341101192
    453850,8,1341101208
    99258,50329,1341101263
    75083,84647,1341101732
    325821,8,1341102141
    104321,238279,1341102794
    408376,8,1341102802
    247125,463,1341103262
    224480,93318,1341103333


## Create a new Raphtory graph 📊

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

### Terminal Output
```bash
WARNING: sun.reflect.Reflection.getCallerClass is not supported. This will impact performance.
15:13:01.077 [io-compute-1] INFO  com.raphtory.internals.management.Py4JServer - Starting PythonGatewayServer...
15:13:01.696 [Thread-12] INFO  com.raphtory.internals.context.LocalContext$ - Creating Service for 'nervous_gold_finch'
15:13:01.711 [io-compute-2] INFO  com.raphtory.internals.management.Prometheus$ - Prometheus started on port /0:0:0:0:0:0:0:0:9999
15:13:02.328 [io-compute-2] INFO  com.raphtory.internals.components.partition.PartitionOrchestrator$ - Creating '1' Partition Managers for 'nervous_gold_finch'.
15:13:04.340 [io-compute-5] INFO  com.raphtory.internals.components.partition.PartitionManager - Partition 0: Starting partition manager for 'nervous_gold_finch'.
```
### Ingest the data into a graph 😋

Write a parsing method to parse your csv file and ultimately create a graph. This example is slightly different to the LOTR example. We insert a parse method into a GraphBuilder() object. Then we create a spout object to ingest the data from the csv file. We load both the spout and graph builder into Source() and load the source into the graph.

````{tabs}

```{code-tab} py
def parse(graph, tuple: str):
    parts = [v.strip() for v in tuple.split(",")]
    source_node = parts[0]
    src_id = graph.assign_id(source_node)
    target_node = parts[1]
    tar_id = graph.assign_id(target_node)
    time_stamp = int(parts[2])

    graph.add_vertex(time_stamp, src_id, Properties(ImmutableProperty("name", source_node)), Type("User"))
    graph.add_vertex(time_stamp, tar_id, Properties(ImmutableProperty("name", target_node)), Type("User"))
    graph.add_edge(time_stamp, src_id, tar_id, Type("Tweet"))

twitter_builder = GraphBuilder(parse)
twitter_spout = FileSpout("/tmp/twitter.csv")
rg.load(Source(twitter_spout, twitter_builder))
```
```{code-tab} scala
object TwitterGraphBuilder extends GraphBuilder[String] {

  def apply(graph: Graph, tuple: String): Unit = {
    val fileLine   = tuple.split(",").map(_.trim)
    val sourceNode = fileLine(0).trim
    val srcID      = sourceNode.toLong
    val targetNode = fileLine(1).trim
    val tarID      = targetNode.toLong
    val timeStamp  = fileLine(2).toLong

    graph.addVertex(timeStamp, srcID, Properties(ImmutableProperty("name", sourceNode)), Type("User"))
    graph.addVertex(timeStamp, tarID, Properties(ImmutableProperty("name", targetNode)), Type("User"))
    //Edge shows srcID retweets tarID's tweet
    graph.addEdge(timeStamp, srcID, tarID, Type("Retweet"))
  }
}

  val spout  = FileSpout(path)
  val source = Source(spout, TwitterGraphBuilder)
  graph.load(source)
```
````

#### Terminal Output
```bash 
15:13:47.923 [spawner-akka.actor.default-dispatcher-3] INFO  com.raphtory.internals.components.ingestion.IngestionManager - Ingestion Manager for 'nervous_gold_finch' establishing new data source
com.raphtory.api.analysis.graphview.DeployedTemporalGraph@51790aa0
15:13:48.521 [io-compute-3] INFO  com.raphtory.spouts.FileSpoutInstance - Spout: Processing file 'twitter.csv' ...
15:13:48.532 [spawner-akka.actor.default-dispatcher-3] INFO  com.raphtory.internals.components.querymanager.QueryManager - Source '0' is blocking analysis for Graph 'nervous_gold_finch'
```

### Collect simple metrics 📈

Select certain metrics to show in your output dataframe. Here we have selected vertex name, degree, out degree and in degree. **Time to finish: ~2 to 3 minutes**

````{tabs}

```{code-tab} py
from pyraphtory.graph import Row
df = rg \
      .select(lambda vertex: Row(vertex.name(), vertex.degree(), vertex.out_degree(), vertex.in_degree())) \
      .write_to_dataframe(["twitter_id", "degree", "out_degree", "in_degree"])
```
```{code-tab} scala
val graph = Raphtory.newGraph()
  graph
    .execute(Degree())
    .writeTo(FileSink("/tmp/higgsoutput"))
    .waitForJob()
```
````

```bash
15:14:52.623 [io-compute-1] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job 60798274_5254107002656625895: Starting query progress tracker.
15:14:52.625 [spawner-akka.actor.default-dispatcher-3] INFO  com.raphtory.internals.components.querymanager.QueryManager - Query '60798274_5254107002656625895' currently blocked, waiting for ingestion to complete.
15:19:00.751 [spawner-akka.actor.default-dispatcher-3] INFO  com.raphtory.internals.components.querymanager.QueryManager - Source '0' is unblocking analysis for Graph 'nervous_gold_finch' with 1064790 messages sent.
15:19:01.181 [spawner-akka.actor.default-dispatcher-3] INFO  com.raphtory.internals.components.querymanager.QueryManager - Query '60798274_5254107002656625895' received, your job ID is '60798274_5254107002656625895'.
15:19:01.192 [spawner-akka.actor.default-dispatcher-9] INFO  com.raphtory.internals.components.partition.QueryExecutor - 60798274_5254107002656625895_0: Starting QueryExecutor.
15:19:15.365 [spawner-akka.actor.default-dispatcher-3] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job '60798274_5254107002656625895': Perspective '1341705593' finished in 262742 ms.
15:19:15.365 [spawner-akka.actor.default-dispatcher-7] INFO  com.raphtory.internals.components.querymanager.QueryHandler - Job '60798274_5254107002656625895': Perspective at Time '1341705593' took 14163 ms to run. 
15:19:15.365 [spawner-akka.actor.default-dispatcher-3] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job 60798274_5254107002656625895: Running query, processed 1 perspectives.
15:19:15.368 [spawner-akka.actor.default-dispatcher-10] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job 60798274_5254107002656625895: Query completed with 1 perspectives and finished in 262745 ms.
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
```{table} Preview simple metrics dataframe
|        |  timestamp | twitter_id | degree | out_degree | in_degree |
|-------:|-----------:|-----------:|-------:|-----------:|-----------|
|    0   | 1341705593 | 247216     | 1      | 1          | 0         |
|    1   | 1341705593 | 61013      | 4      | 3          | 1         |
|    2   | 1341705593 | 161960     | 1      | 1          | 0         |
|    3   | 1341705593 | 422612     | 1      | 1          | 0         |
|    4   | 1341705593 | 396362     | 1      | 1          | 0         |
|   ...  | ...        | ...        | ...    | ...        | ...       |
| 256486 | 1341705593 | 293395     | 1      | 1          | 0         |
| 256487 | 1341705593 | 30364      | 5      | 5          | 0         |
| 256488 | 1341705593 | 84292      | 1      | 1          | 0         |
| 256489 | 1341705593 | 324348     | 2      | 0          | 2         |
| 256490 | 1341705593 | 283130     | 1      | 1          | 0         |
```

**Sort by highest degree, top 10**

````{tabs}

```{code-tab} py
df.sort_values(['degree'], ascending=False)[:10]
```
````

```{table} Top 10 Highest Degree Output
|        |  timestamp | twitter_id | degree | out_degree | in_degree |
|-------:|-----------:|-----------:|-------:|-----------:|-----------|
|  77232 | 1341705593 |         88 |  14061 |          3 |     14060 |
|  95981 | 1341705593 |      14454 |   6190 |          0 |      6190 |
| 120807 | 1341705593 |        677 |   5621 |          8 |      5613 |
| 142755 | 1341705593 |       1988 |   4336 |          2 |      4335 |
| 237149 | 1341705593 |        349 |   2803 |          1 |      2802 |
|  95879 | 1341705593 |        283 |   2039 |          0 |      2039 |
|  83229 | 1341705593 |       3571 |   1981 |          1 |      1980 |
|  32393 | 1341705593 |       6948 |   1959 |          0 |      1959 |
| 240523 | 1341705593 |      14572 |   1692 |          0 |      1692 |
| 138723 | 1341705593 |      68278 |   1689 |          0 |      1689 |
```

**Sort by highest in-degree, top 10**

````{tabs}

```{code-tab} py
df.sort_values(['in_degree'], ascending=False)[:10]
```
````
```{table} Top 10 Highest In Degree Output
|        |  timestamp | twitter_id | degree | out_degree | in_degree |
|-------:|-----------:|-----------:|-------:|-----------:|-----------|
|  77232 | 1341705593 |         88 |  14061 |          3 |     14060 |
|  95981 | 1341705593 |      14454 |   6190 |          0 |      6190 |
| 120807 | 1341705593 |        677 |   5621 |          8 |      5613 |
| 142755 | 1341705593 |       1988 |   4336 |          2 |      4335 |
| 237149 | 1341705593 |        349 |   2803 |          1 |      2802 |
|  95879 | 1341705593 |        283 |   2039 |          0 |      2039 |
|  83229 | 1341705593 |       3571 |   1981 |          1 |      1980 |
|  32393 | 1341705593 |       6948 |   1959 |          0 |      1959 |
| 240523 | 1341705593 |      14572 |   1692 |          0 |      1692 |
| 138723 | 1341705593 |      68278 |   1689 |          0 |      1689 |
```

### Sort by highest out-degree, top 10

````{tabs}

```{code-tab} py
df.sort_values(['out_degree'], ascending=False)[:10]
```
````
```{table} Top 10 Highest Out Degree Output
|        |  timestamp | twitter_id | degree | out_degree | in_degree |
|-------:|-----------:|-----------:|-------:|-----------:|----------:|
|  27504 | 1341705593 |      38535 |    134 |        134 |         0 |
| 151314 | 1341705593 |     181190 |     84 |         84 |         0 |
| 199289 | 1341705593 |      81405 |     67 |         66 |         1 |
| 191563 | 1341705593 |      64911 |    230 |         49 |       192 |
| 188514 | 1341705593 |      54301 |     49 |         49 |         0 |
| 156270 | 1341705593 |      27705 |     57 |         48 |        11 |
|  78066 | 1341705593 |      53508 |     43 |         42 |         1 |
| 123157 | 1341705593 |     232850 |     41 |         41 |         0 |
|   6841 | 1341705593 |      62391 |     38 |         38 |         0 |
|  92951 | 1341705593 |       2237 |     38 |         38 |         0 |
```

### Run a PageRank algorithm 📑

Run your selected algorithm on your graph, here we run PageRank. Your algorithms can be obtained from the PyRaphtory object you created at the start. Specify where you write the result of your algorithm to, e.g. the additional column results in your dataframe. **Time to finish: ~3 to 4 minutes**

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
    11:41:58.681 [io-compute-1] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job PageRank_3498013686461469106: Starting query progress tracker.
    11:41:58.697 [spawner-akka.actor.default-dispatcher-6] INFO  com.raphtory.internals.components.querymanager.QueryManager - Query 'PageRank_3498013686461469106' received, your job ID is 'PageRank_3498013686461469106'.
    11:41:58.699 [spawner-akka.actor.default-dispatcher-6] INFO  com.raphtory.internals.components.partition.QueryExecutor - PageRank_3498013686461469106_0: Starting QueryExecutor.
    11:45:49.953 [spawner-akka.actor.default-dispatcher-6] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job 'PageRank_3498013686461469106': Perspective '1341705593' finished in 231271 ms.
    11:45:49.953 [spawner-akka.actor.default-dispatcher-9] INFO  com.raphtory.internals.components.querymanager.QueryHandler - Job 'PageRank_3498013686461469106': Perspective at Time '1341705593' took 231251 ms to run. 
    11:45:49.954 [spawner-akka.actor.default-dispatcher-6] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job PageRank_3498013686461469106: Running query, processed 1 perspectives.
    11:45:49.956 [spawner-akka.actor.default-dispatcher-9] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job PageRank_3498013686461469106: Query completed with 1 perspectives and finished in 231275 ms.
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
```{table} Preview PageRank Results
|        |  timestamp | twitter_id | prlabel  |
|-------:|-----------:|-----------:|----------|
|      0 | 1341705593 |     247216 | 0.410038 |
|      1 | 1341705593 |      61013 | 0.758570 |
|      2 | 1341705593 |     161960 | 0.410038 |
|      3 | 1341705593 |     422612 | 0.410038 |
|      4 | 1341705593 |     396362 | 0.410038 |
|    ... |        ... |        ... |      ... |
| 256486 | 1341705593 |     293395 | 0.410038 |
| 256487 | 1341705593 |      30364 | 0.410038 |
| 256488 | 1341705593 |      84292 | 0.410038 |
| 256489 | 1341705593 |     324348 | 1.107102 |
| 256490 | 1341705593 |     283130 | 0.410038 |
```

**The top ten highest Page Rank users**

````{tabs}
```{code-tab} py
df_pagerank.sort_values(['prlabel'], ascending=False)[:10]
```
````
```{table} Preview Top 10 Highest Page Rank Results
|        |  timestamp | twitter_id | prlabel  |
|-------:|-----------:|-----------:|----------|
|      0 | 1341705593 |     247216 | 0.410038 |
|      1 | 1341705593 |      61013 | 0.758570 |
|      2 | 1341705593 |     161960 | 0.410038 |
|      3 | 1341705593 |     422612 | 0.410038 |
|      4 | 1341705593 |     396362 | 0.410038 |
|    ... |        ... |        ... |      ... |
| 256486 | 1341705593 |     293395 | 0.410038 |
| 256487 | 1341705593 |      30364 | 0.410038 |
| 256488 | 1341705593 |      84292 | 0.410038 |
| 256489 | 1341705593 |     324348 | 1.107102 |
| 256490 | 1341705593 |     283130 | 0.410038 |
```


### Run chained algorithms at once

In this example, we chain PageRank and two custom algorithms - MemberRank() and TemporalMemberRank(), running them one after another on the graph. Specify all the columns in the output dataframe, including an output column for each algorithm in the chain. **Time to finish: ~4 minutes**

````{tabs}

```{code-tab} py
Coming soon...
```
```{code-tab} scala
    graph
      .at(1341705593)
      .past()
      .transform(PageRank())
      .execute(MemberRank() -> TemporalMemberRank())
      .writeTo(output)
      .waitForJob()
```
````
```bash
15:31:09.205 [io-compute-8] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job PageRank:ConnectedComponents:Degree:NodeList_4333300725965970191: Starting query progress tracker.
15:31:09.223 [spawner-akka.actor.default-dispatcher-9] INFO  com.raphtory.internals.components.querymanager.QueryManager - Query 'PageRank:ConnectedComponents:Degree:NodeList_4333300725965970191' received, your job ID is 'PageRank:ConnectedComponents:Degree:NodeList_4333300725965970191'.
15:31:09.228 [spawner-akka.actor.default-dispatcher-9] INFO  com.raphtory.internals.components.partition.QueryExecutor - PageRank:ConnectedComponents:Degree:NodeList_4333300725965970191_0: Starting QueryExecutor.
15:35:34.030 [spawner-akka.actor.default-dispatcher-11] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job 'PageRank:ConnectedComponents:Degree:NodeList_4333300725965970191': Perspective '1341705593' finished in 264826 ms.
15:35:34.030 [spawner-akka.actor.default-dispatcher-11] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job PageRank:ConnectedComponents:Degree:NodeList_4333300725965970191: Running query, processed 1 perspectives.
15:35:34.030 [spawner-akka.actor.default-dispatcher-8] INFO  com.raphtory.internals.components.querymanager.QueryHandler - Job 'PageRank:ConnectedComponents:Degree:NodeList_4333300725965970191': Perspective at Time '1341705593' took 264800 ms to run. 
15:35:34.031 [spawner-akka.actor.default-dispatcher-8] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job PageRank:ConnectedComponents:Degree:NodeList_4333300725965970191: Query completed with 1 perspectives and finished in 264827 ms.
```
## Create visualisation by adding nodes 🔎

```python
def visualise(rg, df_chained):
    # Create network object
    net = Network(notebook=True, height='750px', width='100%', bgcolor='#222222', font_color='white')
    # Set visualisation tool
    net.force_atlas_2based()
    # Get the node list 
    df_node_list = rg.at(1341705593) \
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
    df_edge_list = rg.at(1341705593) \
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
```


```python
net = visualise(rg, df_chained)
```

```python
net.show('preview.html')
```

```python
pr.shutdown()
```
## Running and writing custom algorithms in Raphtory

We used the PageRank algorithm, which is already readily available in Raphtory. We wrote two more algorithms which we've named: MemberRank and TemporalMemberRank, to further our investigations.

[PageRank](https://en.wikipedia.org/wiki/PageRank) is an algorithm used to rank web pages during a Google search. 
The PageRank algorithm ranks nodes depending on their connections to determine how important the node is. 
This assumes a node is more important if it receives more connections from others. Each vertex begins with an initial state. If it has any neighbours, it sends them a message which is the initial label / the number of neighbours. 
Each vertex, checks its messages and computes a new label based on: the total value of messages received and the damping factor. 
This new value is propagated to all outgoing neighbours. A vertex will stop propagating messages if its value becomes stagnant (i.e. has a change of less than 0.00001). 
This process is repeated for a number of iterate step times. Most algorithms should converge after approx. 20 iterations.

MemberRank is an algorithm that takes the PageRank score from the vertex neighbours and the original ranking of the vertex from the raw dataset and multiplies the two scores together to form the 'MemberRank score'. A vertex receiving a high MemberRank score means that other people with high PageRank scores have ranked them highly. If the Twitter user is influential, MemberRank will bump their score higher. If the Twitter user is non-influential (potentially a bot) the MemberRank score will be low. This should dampen the effect of bots further.

TemporalMemberRank is an algorithm that filters users with big differences in their raw dataset scores and MemberRank scores (potential bots) and checks their in-edge creations over time. Bot activity usually occurs in a very short timeframe (spamming) or at regular intervals. This algorithm will be able to give more evidence on whether a particular user is a bot or not. 

## What the algorithms do to our data:
* `PageRank.scala`  based on the Google Pagerank algorithm, ranks nodes (Twitter Users) depending on their connections to determine how important the node is. 
* `MemberRank.scala` takes the Page Rank score and ranks users further by taking into account the number of retweets they received.
* `TemporalMemberRank.scala` filters nodes with big differences in the number of retweets they received on Twitter and the final rank given to them by MemberRank, outputting the suspected retweet bot IDs, along with the user ID they retweeted and the time of the retweet. The first element in each line is the Pulsar Output Timestamp, this can be dropped or ignored as it is irrelevant to the bot activity results.

In Raphtory, you can run multiple algorithms by chaining them together with an arrow: e.g. `PageRank() -> MemberRank()`.
You can also write your own algorithms to output your desired analysis/results.

We have also included python scripts in the directory `src/main/python` to output the results from Raphtory into a Jupyter notebook.
