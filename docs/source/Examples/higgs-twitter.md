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

Follow our Installation guide: [Scala](../Install/installdependencies.md) or [Python](../PythonClient/setup.md).

## Data

The data is a `csv` file (comma-separated values) and can be found in [Raphtory's data repo](https://github.com/Raphtory/Data/blob/main/higgs-retweet-activity.csv). 
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
object TwitterGraphBuilder {
  def parse(graph: Graph, tuple: String): Unit = {
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
  val source = Source(spout, TwitterGraphBuilder.parse)
  graph.load(source)
```
````

### Collect simple metrics 📈

Select certain metrics to show in your output dataframe. Here we have selected vertex name, degree, out degree and in degree. **Time to finish: ~2 to 3 minutes**

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

<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>timestamp</th>
      <th>name</th>
      <th>degree</th>
      <th>out_degree</th>
      <th>in_degree</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1341705593</td>
      <td>247216</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1341705593</td>
      <td>61013</td>
      <td>4</td>
      <td>3</td>
      <td>1</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1341705593</td>
      <td>161960</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>1341705593</td>
      <td>422612</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>1341705593</td>
      <td>396362</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>256486</th>
      <td>1341705593</td>
      <td>293395</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
    </tr>
    <tr>
      <th>256487</th>
      <td>1341705593</td>
      <td>30364</td>
      <td>5</td>
      <td>5</td>
      <td>0</td>
    </tr>
    <tr>
      <th>256488</th>
      <td>1341705593</td>
      <td>84292</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
    </tr>
    <tr>
      <th>256489</th>
      <td>1341705593</td>
      <td>324348</td>
      <td>2</td>
      <td>0</td>
      <td>2</td>
    </tr>
    <tr>
      <th>256490</th>
      <td>1341705593</td>
      <td>283130</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
    </tr>
  </tbody>
</table>
<p>256491 rows × 5 columns</p>
</div>

**Sort by highest degree, top 10**

````{tabs}

```{code-tab} py
df.sort_values(['degree'], ascending=False)[:10]
```
````
<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>timestamp</th>
      <th>name</th>
      <th>degree</th>
      <th>out_degree</th>
      <th>in_degree</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>77232</th>
      <td>1341705593</td>
      <td>88</td>
      <td>14061</td>
      <td>3</td>
      <td>14060</td>
    </tr>
    <tr>
      <th>95981</th>
      <td>1341705593</td>
      <td>14454</td>
      <td>6190</td>
      <td>0</td>
      <td>6190</td>
    </tr>
    <tr>
      <th>120807</th>
      <td>1341705593</td>
      <td>677</td>
      <td>5621</td>
      <td>8</td>
      <td>5613</td>
    </tr>
    <tr>
      <th>142755</th>
      <td>1341705593</td>
      <td>1988</td>
      <td>4336</td>
      <td>2</td>
      <td>4335</td>
    </tr>
    <tr>
      <th>237149</th>
      <td>1341705593</td>
      <td>349</td>
      <td>2803</td>
      <td>1</td>
      <td>2802</td>
    </tr>
    <tr>
      <th>95879</th>
      <td>1341705593</td>
      <td>283</td>
      <td>2039</td>
      <td>0</td>
      <td>2039</td>
    </tr>
    <tr>
      <th>83229</th>
      <td>1341705593</td>
      <td>3571</td>
      <td>1981</td>
      <td>1</td>
      <td>1980</td>
    </tr>
    <tr>
      <th>32393</th>
      <td>1341705593</td>
      <td>6948</td>
      <td>1959</td>
      <td>0</td>
      <td>1959</td>
    </tr>
    <tr>
      <th>240523</th>
      <td>1341705593</td>
      <td>14572</td>
      <td>1692</td>
      <td>0</td>
      <td>1692</td>
    </tr>
    <tr>
      <th>138723</th>
      <td>1341705593</td>
      <td>68278</td>
      <td>1689</td>
      <td>0</td>
      <td>1689</td>
    </tr>
  </tbody>
</table>
</div>



**Sort by highest in-degree, top 10**

````{tabs}

```{code-tab} py
df.sort_values(['in_degree'], ascending=False)[:10]
```
````

<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>timestamp</th>
      <th>name</th>
      <th>degree</th>
      <th>out_degree</th>
      <th>in_degree</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>77232</th>
      <td>1341705593</td>
      <td>88</td>
      <td>14061</td>
      <td>3</td>
      <td>14060</td>
    </tr>
    <tr>
      <th>95981</th>
      <td>1341705593</td>
      <td>14454</td>
      <td>6190</td>
      <td>0</td>
      <td>6190</td>
    </tr>
    <tr>
      <th>120807</th>
      <td>1341705593</td>
      <td>677</td>
      <td>5621</td>
      <td>8</td>
      <td>5613</td>
    </tr>
    <tr>
      <th>142755</th>
      <td>1341705593</td>
      <td>1988</td>
      <td>4336</td>
      <td>2</td>
      <td>4335</td>
    </tr>
    <tr>
      <th>237149</th>
      <td>1341705593</td>
      <td>349</td>
      <td>2803</td>
      <td>1</td>
      <td>2802</td>
    </tr>
    <tr>
      <th>95879</th>
      <td>1341705593</td>
      <td>283</td>
      <td>2039</td>
      <td>0</td>
      <td>2039</td>
    </tr>
    <tr>
      <th>83229</th>
      <td>1341705593</td>
      <td>3571</td>
      <td>1981</td>
      <td>1</td>
      <td>1980</td>
    </tr>
    <tr>
      <th>32393</th>
      <td>1341705593</td>
      <td>6948</td>
      <td>1959</td>
      <td>0</td>
      <td>1959</td>
    </tr>
    <tr>
      <th>240523</th>
      <td>1341705593</td>
      <td>14572</td>
      <td>1692</td>
      <td>0</td>
      <td>1692</td>
    </tr>
    <tr>
      <th>138723</th>
      <td>1341705593</td>
      <td>68278</td>
      <td>1689</td>
      <td>0</td>
      <td>1689</td>
    </tr>
  </tbody>
</table>
</div>

### Sort by highest out-degree, top 10

````{tabs}

```{code-tab} py
df.sort_values(['out_degree'], ascending=False)[:10]
```
````

<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>timestamp</th>
      <th>name</th>
      <th>degree</th>
      <th>out_degree</th>
      <th>in_degree</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>27504</th>
      <td>1341705593</td>
      <td>38535</td>
      <td>134</td>
      <td>134</td>
      <td>0</td>
    </tr>
    <tr>
      <th>151314</th>
      <td>1341705593</td>
      <td>181190</td>
      <td>84</td>
      <td>84</td>
      <td>0</td>
    </tr>
    <tr>
      <th>199289</th>
      <td>1341705593</td>
      <td>81405</td>
      <td>67</td>
      <td>66</td>
      <td>1</td>
    </tr>
    <tr>
      <th>191563</th>
      <td>1341705593</td>
      <td>64911</td>
      <td>230</td>
      <td>49</td>
      <td>192</td>
    </tr>
    <tr>
      <th>188514</th>
      <td>1341705593</td>
      <td>54301</td>
      <td>49</td>
      <td>49</td>
      <td>0</td>
    </tr>
    <tr>
      <th>156270</th>
      <td>1341705593</td>
      <td>27705</td>
      <td>57</td>
      <td>48</td>
      <td>11</td>
    </tr>
    <tr>
      <th>78066</th>
      <td>1341705593</td>
      <td>53508</td>
      <td>43</td>
      <td>42</td>
      <td>1</td>
    </tr>
    <tr>
      <th>123157</th>
      <td>1341705593</td>
      <td>232850</td>
      <td>41</td>
      <td>41</td>
      <td>0</td>
    </tr>
    <tr>
      <th>6841</th>
      <td>1341705593</td>
      <td>62391</td>
      <td>38</td>
      <td>38</td>
      <td>0</td>
    </tr>
    <tr>
      <th>92951</th>
      <td>1341705593</td>
      <td>2237</td>
      <td>38</td>
      <td>38</td>
      <td>0</td>
    </tr>
  </tbody>
</table>
</div>


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

    11:41:58.681 [io-compute-1] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job PageRank_3498013686461469106: Starting query progress tracker.
    11:41:58.697 [spawner-akka.actor.default-dispatcher-6] INFO  com.raphtory.internals.components.querymanager.QueryManager - Query 'PageRank_3498013686461469106' received, your job ID is 'PageRank_3498013686461469106'.
    11:41:58.699 [spawner-akka.actor.default-dispatcher-6] INFO  com.raphtory.internals.components.partition.QueryExecutor - PageRank_3498013686461469106_0: Starting QueryExecutor.
    11:45:49.953 [spawner-akka.actor.default-dispatcher-6] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job 'PageRank_3498013686461469106': Perspective '1341705593' finished in 231271 ms.
    11:45:49.953 [spawner-akka.actor.default-dispatcher-9] INFO  com.raphtory.internals.components.querymanager.QueryHandler - Job 'PageRank_3498013686461469106': Perspective at Time '1341705593' took 231251 ms to run. 
    11:45:49.954 [spawner-akka.actor.default-dispatcher-6] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job PageRank_3498013686461469106: Running query, processed 1 perspectives.
    11:45:49.956 [spawner-akka.actor.default-dispatcher-9] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job PageRank_3498013686461469106: Query completed with 1 perspectives and finished in 231275 ms.

<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }

</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>timestamp</th>
      <th>window</th>
      <th>name</th>
      <th>prlabel</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1341705593</td>
      <td>None</td>
      <td>247216</td>
      <td>0.410038</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1341705593</td>
      <td>None</td>
      <td>61013</td>
      <td>0.758570</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1341705593</td>
      <td>None</td>
      <td>161960</td>
      <td>0.410038</td>
    </tr>
    <tr>
      <th>3</th>
      <td>1341705593</td>
      <td>None</td>
      <td>422612</td>
      <td>0.410038</td>
    </tr>
    <tr>
      <th>4</th>
      <td>1341705593</td>
      <td>None</td>
      <td>396362</td>
      <td>0.410038</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>256486</th>
      <td>1341705593</td>
      <td>None</td>
      <td>293395</td>
      <td>0.410038</td>
    </tr>
    <tr>
      <th>256487</th>
      <td>1341705593</td>
      <td>None</td>
      <td>30364</td>
      <td>0.410038</td>
    </tr>
    <tr>
      <th>256488</th>
      <td>1341705593</td>
      <td>None</td>
      <td>84292</td>
      <td>0.410038</td>
    </tr>
    <tr>
      <th>256489</th>
      <td>1341705593</td>
      <td>None</td>
      <td>324348</td>
      <td>1.107102</td>
    </tr>
    <tr>
      <th>256490</th>
      <td>1341705593</td>
      <td>None</td>
      <td>283130</td>
      <td>0.410038</td>
    </tr>
  </tbody>
</table>
<p>256491 rows × 4 columns</p>
</div>

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

<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>timestamp</th>
      <th>name</th>
      <th>prlabel</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1341705593</td>
      <td>247216</td>
      <td>0.410038</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1341705593</td>
      <td>61013</td>
      <td>0.758570</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1341705593</td>
      <td>161960</td>
      <td>0.410038</td>
    </tr>
    <tr>
      <th>3</th>
      <td>1341705593</td>
      <td>422612</td>
      <td>0.410038</td>
    </tr>
    <tr>
      <th>4</th>
      <td>1341705593</td>
      <td>396362</td>
      <td>0.410038</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>256486</th>
      <td>1341705593</td>
      <td>293395</td>
      <td>0.410038</td>
    </tr>
    <tr>
      <th>256487</th>
      <td>1341705593</td>
      <td>30364</td>
      <td>0.410038</td>
    </tr>
    <tr>
      <th>256488</th>
      <td>1341705593</td>
      <td>84292</td>
      <td>0.410038</td>
    </tr>
    <tr>
      <th>256489</th>
      <td>1341705593</td>
      <td>324348</td>
      <td>1.107102</td>
    </tr>
    <tr>
      <th>256490</th>
      <td>1341705593</td>
      <td>283130</td>
      <td>0.410038</td>
    </tr>
  </tbody>
</table>
<p>256491 rows × 3 columns</p>
</div>



**The top ten most ranked users**

````{tabs}
```{code-tab} py
df_pagerank.sort_values(['prlabel'], ascending=False)[:10]
```
````

<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>timestamp</th>
      <th>name</th>
      <th>prlabel</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>77232</th>
      <td>1341705593</td>
      <td>88</td>
      <td>6512.050333</td>
    </tr>
    <tr>
      <th>93521</th>
      <td>1341705593</td>
      <td>2342</td>
      <td>3746.267274</td>
    </tr>
    <tr>
      <th>191563</th>
      <td>1341705593</td>
      <td>64911</td>
      <td>2335.452547</td>
    </tr>
    <tr>
      <th>2955</th>
      <td>1341705593</td>
      <td>39420</td>
      <td>1885.321866</td>
    </tr>
    <tr>
      <th>95981</th>
      <td>1341705593</td>
      <td>14454</td>
      <td>1828.696595</td>
    </tr>
    <tr>
      <th>120807</th>
      <td>1341705593</td>
      <td>677</td>
      <td>1790.105521</td>
    </tr>
    <tr>
      <th>73101</th>
      <td>1341705593</td>
      <td>2567</td>
      <td>1649.711162</td>
    </tr>
    <tr>
      <th>62742</th>
      <td>1341705593</td>
      <td>134095</td>
      <td>1599.569856</td>
    </tr>
    <tr>
      <th>116004</th>
      <td>1341705593</td>
      <td>169287</td>
      <td>1593.242617</td>
    </tr>
    <tr>
      <th>142755</th>
      <td>1341705593</td>
      <td>1988</td>
      <td>1473.269535</td>
    </tr>
  </tbody>
</table>
</div>



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


```python

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

## Output

When you start `Runner.scala` you should see logs in your terminal like this:


Output for Vertex 4 (potential bot): 

We singled out vertex 4 as an example user that may show bot activity since after the final step of the chaining: `TemporalMemberRank.scala`, the score that vertex 4 originally had in the raw dataset significantly decreased after running PageRank and MemberRank. The output shows retweet activity happening within minutes and seconds of each other, this could be an indication of suspicious activity and further analysis on this data can be performed.

Feel free to play around with the example code and Jupyter notebook analysis. For example, in `TemporalMemberRank.scala`, you can change line 32:
`val difference: Boolean = (positiveRaw > (positiveNew * 4))`
`4` is the multiplication factor to see if the raw scores and new scores are significantly different. This can be increased or decreased depending on your desired analysis. 
