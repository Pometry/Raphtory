---
jupyter:
  jupytext:
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
      jupytext_version: 1.14.0
  kernelspec:
    display_name: Python 3 (ipykernel)
    language: python
    name: python3
---

# Lord Of The Rings PyRaphtory Example Notebook 🧝🏻‍♀️🧙🏻‍♂️💍


## Setup environment and download data 💾


Import all necessary dependencies needed to build a graph from your data in PyRaphtory. Download csv data from github into your tmp folder (file path: /tmp/lotr.csv).

```python jupyter={"outputs_hidden": false} pycharm={"name": "#%%\n"}
from pathlib import Path
from pyraphtory.context import PyRaphtory
from pyraphtory.vertex import Vertex
from pyraphtory.spouts import FileSpout
from pyraphtory.builder import *
from pyvis.network import Network


!curl -o /tmp/lotr.csv https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv
```

## Preview data  👀


Preview the head of the dataset.

```python
!head /tmp/lotr.csv
```

## Create a new raphtory graph 📊


Turn on logs to see what is going on in PyRaphtory. Initialise Raphtory by creating a PyRaphtory object. Create your new graph.

```python jupyter={"outputs_hidden": false} pycharm={"name": "#%%\n"}
pr = PyRaphtory(logging=True).open()
rg = pr.new_graph()
```

## Ingest the data into a graph 😋


Write a parsing method to parse your csv file and ultimately create a graph.

**1)** We first need to extract the characters and time as individual variables to build our graph. We state how we are going to split each line of data into individual items. As we have a csv file, we split by comma. We also get rid of any spaces by using the strip() method.

**2)** Identify which index of the line of data is the source node and target node. 

**3)** Create an ID for the source and target node by using the assign_id() method. Make sure the source/target node you are putting in assign_id() is of type String - use str() to convert the ID to String if it is not already.

**4)** State which index of the line of data is the time at which source and target meet.

**5)** Create your add vertex and add edge methods to build your graph. For the vertex method, add the time, id, properties (such as name of character and whether it's the source or target node), and the node type such as "Character". For the edge method, add time, source node and target node ID and the type of edge it is.

**6)** Wrap your parse method in a GraphBuilder() method to create a LOTR builder variable.

**7)** Create a file spout method by adding the file path to a FileSpout() method, assign it to a variable. This ingests your csv data.

**8)** Add the spout and builder to a load() method, this will load your data and build your graph in PyRaphtory.

```python jupyter={"outputs_hidden": false} pycharm={"name": "#%%\n"}
def parse(graph, tuple: str):
    parts = [v.strip() for v in tuple.split(",")]
    source_node = parts[0]
    src_id = graph.assign_id(source_node)
    target_node = parts[1]
    tar_id = graph.assign_id(target_node)
    time_stamp = int(parts[2])

    graph.add_vertex(time_stamp, src_id, Properties(ImmutableProperty("name", source_node)), Type("Character"))
    graph.add_vertex(time_stamp, tar_id, Properties(ImmutableProperty("name", target_node)), Type("Character"))
    graph.add_edge(time_stamp, src_id, tar_id, Type("Character_Co-occurence"))

lotr_builder = GraphBuilder(parse)
lotr_spout = FileSpout("/tmp/lotr.csv")
rg.load(Source(lotr_spout, lotr_builder))
```

### Collect simple metrics 📈


Select certain metrics to show in your output dataframe. Here we have selected vertex name, degree, out degree and in degree. 

```python
from pyraphtory.graph import Row
df = rg \
      .select(lambda vertex: Row(vertex.name(), vertex.degree(), vertex.out_degree(), vertex.in_degree())) \
      .write_to_dataframe(["name", "degree", "out_degree", "in_degree"])
```

**Clean the dataframe, we have deleted the unused window column.** 🧹

```python
## clean
df.drop(columns=['window'], inplace=True)
```

**Preview the dataframe.**

```python
df
```

#### Sort by highest degree, top 10

```python
df.sort_values(['degree'], ascending=False)[:10]
```

**Sort by highest in-degree, top 10**

```python
df.sort_values(['in_degree'], ascending=False)[:10]
```

**Sort by highest out-degree, top 10**

```python
df.sort_values(['out_degree'], ascending=False)[:10]
```

# Run a PageRank algorithm 📑


Run your selected algorithm on your graph, here we run PageRank. Your algorithms can be obtained from the PyRaphtory object you created at the start. Specify where you write the result of your algorithm to, e.g. the additional column results in your dataframe.

```python
cols = ["prlabel"]

df_pagerank = rg.at(32674) \
                .past() \
                .transform(pr.algorithms.generic.centrality.PageRank())\
                .execute(pr.algorithms.generic.NodeList(*cols)) \
                .write_to_dataframe(["name"] + cols)
```

**Clean your dataframe** 🧹

```python
## clean
df_pagerank.drop(columns=['window'], inplace=True)
```

```python
df_pagerank
```

**The top ten most ranked**

```python
df_pagerank.sort_values(['prlabel'], ascending=False)[:10]
```

## Run a connected components algorithm 


Example running connected components algorithm on the graph.

```python jupyter={"outputs_hidden": false} pycharm={"name": "#%%\n"}
cols = ["cclabel"]
df_cc = rg.at(32674) \
                .past() \
                .transform(pr.algorithms.generic.ConnectedComponents)\
                .execute(pr.algorithms.generic.NodeList(*cols)) \
                .write_to_dataframe(["name"] + cols)
```

**Clean dataframe.**

```python
## clean
df_cc.drop(columns=['window'], inplace=True)
```

**Preview dataframe.**

```python
df_cc
```

### Number of distinct components 


Extract number of distinct components, which is 3 in this dataframe.

```python
len(set(df_cc['cclabel']))
```

### Size of components 


Calculate the size of the 3 connected components.

```python
df_cc.groupby(['cclabel']).count().reset_index().drop(columns=['timestamp'])
```

### Run chained algorithms at once 


In this example, we chain PageRank, Connected Components and Degree algorithms, running them one after another on the graph. Specify all the columns in the output dataframe, including an output column for each algorithm in the chain.

```python
cols = ["inDegree", "outDegree", "degree","prlabel","cclabel"]

df_chained = rg.at(32674) \
                .past() \
                .transform(pr.algorithms.generic.centrality.PageRank())\
                .transform(pr.algorithms.generic.ConnectedComponents)\
                .transform(pr.algorithms.generic.centrality.Degree())\
                .execute(pr.algorithms.generic.NodeList(*cols)) \
                .write_to_dataframe(["name"] + cols)
```

```python
df_chained.drop(columns=['window'])
```

```python
df_chained
```

<!-- #region tags=[] -->
### Create visualisation by adding nodes 🔎
<!-- #endregion -->

```python tags=[]
def visualise(rg, df_chained):
    # Create network object
    net = Network(notebook=True, height='750px', width='100%', bgcolor='#222222', font_color='white')
    # Set visuasiation tool
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
```

```python tags=[]
net = visualise(rg, df_chained)
```

## Show the html file of the visualisation

```python
net.show('preview.html')
```

## Shut down PyRaphtory  🛑

```python jupyter={"outputs_hidden": false} pycharm={"name": "#%%\n"}
pr.shutdown()
```
