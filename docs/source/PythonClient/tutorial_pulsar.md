# Getting your data into Jupyter

## Pre-requisites

Please ensure you have followed the Python Client Setup guide before continuing.

# Run Example Lab - Raphtory Python Client - LOTR DEMO

This guide will show you how to use the client. This will walk through connecting to Raphtory, exporting a graph, exporting results, appending results to a graph and visualising the results. 

Within the example `python` there is a copy of this process as a jupyter notebook called `LOTR_demo.ipynb`.

In this demo we will
* Create a Python Raphtory client
* Create a Graph from data in Raphtory
* Pull results from an algorithm in Raphtory
* Add these results to the Graph
* Visualise the graph with pymotif

## Setup Code

First we setup the various libraries we will need


```python
from raphtoryclient import client
from pymotif import Motif
```

## Create Client

Now we create a client, and then create the readers which read from topics.

If the connection fails, the code with automatically retry.

This can occur when you have not closed previous pulsar connections.

In this case we are reading the topics: `EdgeList`, `PageRank`, `ConnectedComponents`

Note: Prior to this you should have already run these algorithms in Raphtory.


```python
raphtoryClient = client()
```

    Connecting to RaphtoryClient...
    Connected.



```python
edgeListReader = raphtoryClient.createReader("EdgeList", subscription_name='edgelist_reader')
pageRankReader = raphtoryClient.createReader("PageRank", subscription_name='pagerank_reader')
conCompReader  = raphtoryClient.createReader("ConnectedComponents", subscription_name='concomp_reader')
```

    2022-02-16 16:54:38.038 INFO  [0x113dd9e00] ClientConnection:190 | [<none> -> pulsar://127.0.0.1:6650] Create ClientConnection, timeout=10000
    ...    
    Connected to topic: ConnectedComponents


## Obtain dataframes

Now we will run the getDataframe function to retrieve results as dataframes.


```python
df_edge = raphtoryClient.getDataframe(edgeListReader)
df_page = raphtoryClient.getDataframe(pageRankReader)
df_con  = raphtoryClient.getDataframe(conCompReader)
```

    Obtaining dataframe...
    
    Converting to columns...
    Completed.



## Create a graph

Next we create a graph by pulling the edge list from Raphtory.

In this case we would like to create a graph from the LOTR dataset.

So we run the `createGraphFromEdgeList` method on the `EdgeList` dataframe.


```python
G = raphtoryClient.createGraphFromEdgeList(df_edge, isMultiGraph=False)
G.number_of_nodes(), G.number_of_edges()
```

    Creating graph...
    Done.
    (124, 538)



## Adding properties to our graph

Now we merge these as node properties into our graph


```python
raphtoryClient.add_node_attributes(G, [df_page], ['PageRank'])
```

## Visualisation

Finally we plot the graph with an open source visualisation tool .


```python
motif_nx = Motif(nx_graph=G, title='NetworkX')
motif_nx.plot()
```


    Motif(value=None, state={'data': [{'nodes': [{'id': 'Hador', 'value': 'Hador', 'name': 'Hador'}, {'PageRank_30…

