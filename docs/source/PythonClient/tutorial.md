# Getting your data into Jupyter

## Pre-requisites

In order to run the example notebook you must first have 

* Pulsar running 
* Example running from [raphtory-example-lotr](https://github.com/Raphtory/Examples/tree/0.5.0/raphtory-example-lotr)
* Set up a python environment

To run Raphtory and Pulsar, please view the Raphtory setup/installation guide. 

###  Data for the Python Demo

Once you have pulsar running and Raphtory setup, we can now run the [raphtory-example-lotr](https://github.com/Raphtory/Examples/tree/0.5.0/raphtory-example-lotr) Runner. 
This will run a `spout` and `graphbuilder` that ingests and creates a LOTR graph. 
Then will run the `EdgeList` and `PageRank` algorithms. The `EdgeList` algorithm will produce an edge list that can be ingested into the graph. `PageRank` will run a page rank algorithm that will run page rank as a range query over specific times in the data. 
More information can be found in the readme


# Example Notebook

## Setup Python Environment 

- Install Python3 and Pip
- Either 
  - Install the requirements file via
    - `pip install -r requirements.txt` 
    - This will include Jupyter if you do not have it  
  - `pip install raphtory-client`  and `pip install jupyter` 
- install the addons for pymotif
```
  # Jupyter Lab
  jupyter labextension install @jupyter-widgets/jupyterlab-manager

  # For Jupyter Lab <= 2, you may need to install the extension manually
  jupyter labextension install @cylynx/pymotif

  # For Jupyter Notebook <= 5.2, you may need to enable nbextensions
  jupyter nbextension enable --py [--sys-prefix|--user|--system] pymotif
```
- then run jupyter via `jupyter notebook`

## Run Example Lab - Raphtory Python Client - LOTR DEMO

This guide will show you how to use the client. This will walk through connecting to Raphtory, exporting a graph, exporting results, appending results to a graph and visualising the results. 

Within the example `python` there is a copy of this process as a jupyter notebook called `LOTR_demo.ipynb`.

In this demo we will
* Create a Python Raphtory client
* Create a Graph from data in Raphtory
* Pull results from an algorithm in Raphtory
* Add these results to the Graph
* Visualise the graph with pymotif

### Setup Code

First we setup the various libraries we will need


```python
from raphtoryclient import raphtoryclient
from pymotif import Motif
```

### Create Client

Now we create a client, and then create the readers which read from topics.

If the connection fails, the code with automatically retry.

This can occur when you have not closed previous pulsar connections.

In this case we are reading the topics: `EdgeList`, `PageRank`, `ConnectedComponents`

Note: Prior to this you should have already run these algorithms in Raphtory.


```python
raphtoryClient = raphtoryclient()
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


### Obtain dataframes

Now we will run the getDataframe function to retrieve results as dataframes.


```python
df_edge = raphtoryClient.getDataframe(edgeListReader)
df_page = raphtoryClient.getDataframe(pageRankReader)
df_con  = raphtoryClient.getDataframe(conCompReader)
```

    Obtaining dataframe...
    
    Converting to columns...
    Completed.



### Create a graph

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



### Adding properties to our graph

Now we merge these as node properties into our graph


```python
raphtoryClient.add_node_attributes(G, [df_page], ['PageRank'])
```

### Visualisation

Finally we plot the graph with an open source visualisation tool .


```python
motif_nx = Motif(nx_graph=G, title='NetworkX')
motif_nx.plot()
```


    Motif(value=None, state={'data': [{'nodes': [{'id': 'Hador', 'value': 'Hador', 'name': 'Hador'}, {'PageRank_30…

