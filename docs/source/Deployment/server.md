# Deploying Raphtory on a server

A remote running instance of Raphtory may be wanted if you wish to run in a separate process on your machine or if you wish to have a raphtory server running on remote infrastructure with more resources.

## Prerequisites
You will need python3.9 or later and we recommend that you use conda to install python 3.9 and packages

1. Install conda by following the instructions at https://docs.conda.io/en/latest/miniconda.html
2. Create conda virtual environment
```
conda create -n raphtory-conda python=3.9
```
3. Activate conda environment
```
conda activate raphtory-conda
```

## Running Raphtory
To run Raphtory as a remote process
1. For any firewalls or network security groups between your client installation and the server, a rule must be added to allow traffic into the server on port 1736 from your client public IP address. 
2. If the deployment infrastructure is on another machine, log into the machine
3. Run python commands
    1. pip install pyraphtory pyraphtory-jvm
    2. raphtory-standalone

## Create context to connect to remote Raphtory
To create a remote context to connect your local client to a remote running instance of Raphtory, you simply call PyRaphtory.remote and pass in the IP address of the host and the port that Raphtory is running on as arguments.
```
ctx = PyRaphtory.remote("127.0.0.1",1736)
```

## Create new graph using context
Now that the context is set to a remote raphtory server, you can use the new_graph function as usual to create a graph
graph = ctx.new_graph(“graph_id”)

## Get existing graph using context
If the raphtory server already has a graph that you would like to retrieve
graph = ctx.get_graph(“graph_id”)


