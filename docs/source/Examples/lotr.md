# Lord of the Rings Character Interactions

## Overview
This example takes a dataset that tells us when two characters have some type of interaction in the Lord of the Rings trilogy books and builds a graph of these interactions in Raphtory. It's a great dataset to test different algorithms or even your own written algorithms. You can run this example using either our Python or Scala client.

## Pre-requisites

Follow our Installation guide: [Scala](../Install/installdependencies.md) or [Python](../PythonClient/setup.md).

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

### Sort by highest degree, top 10

````{tabs}

```{code-tab} py
df.sort_values(['degree'], ascending=False)[:10]
```
````

### Sort by highest in-degree, top 10

````{tabs}

```{code-tab} py
df.sort_values(['in_degree'], ascending=False)[:10]
```
````

### Sort by highest out-degree, top 10

````{tabs}

```{code-tab} py
df.sort_values(['out_degree'], ascending=False)[:10]
```
````

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

### The top ten most ranked

````{tabs}
```{code-tab} py
df_pagerank.sort_values(['prlabel'], ascending=False)[:10]
```
````

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

### Number of distinct components 

Extract number of distinct components, which is 3 in this dataframe.

````{tabs}

```{code-tab} py
len(set(df_cc['cclabel']))
```
````

### Size of components 


Calculate the size of the 3 connected components.

````{tabs}

```{code-tab} py
df_cc.groupby(['cclabel']).count().reset_index().drop(columns=['timestamp'])
```
````

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

### Shut down PyRaphtory  🛑
````{tabs}

```{code-tab} py
pr.shutdown()
```
````
