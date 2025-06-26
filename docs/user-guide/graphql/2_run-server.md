# Running the GraphQL Server

## Prerequisites

Before reading this topic, please ensure you are familiar with:

- [Ingesting data](../ingestion/1_creating-a-graph.md)

## Saving your Raphtory graph into a directory

You will need some test data to complete the following examples. This can be your own data or one of the examples in the Raphtory documentation.

Once your data is loaded into a Raphtory graph, the graph needs to be saved into your working directory. This can be done with the following code, where `g` is your graph:

/// tab | :fontawesome-brands-python: Python
```{.python notest}
import os
working_dir = "graphs/"

if not os.path.exists(working_dir):
    os.makedirs(working_dir)
g.save_to_file(working_dir + "your_graph")
```
///

## Starting a server with .run()

To run the GraphQL server with `.run()`, create a python file `run_server.py` with the following code:

/// tab | :fontawesome-brands-python: Python
```{.python notest}
from raphtory import graphql

import argparse
parser = argparse.ArgumentParser(description="For passing the working_dir")
parser.add_argument(
    "--working_dir",
    type=str,
    help="path for the working directory of the raphtory server",
)
args = parser.parse_args()

server = graphql.GraphServer(args.working_dir)

server.run()
```
///

To run the server:

```bash
python run_server.py --working_dir ../your_working_dir
```

## Starting a server with .start()

It is also possible to start the server in Python with `.start()`. Below is an example of how to start the server and send a Raphtory graph to the server, where `new_graph` is your Raphtory graph object.

/// tab | :fontawesome-brands-python: Python
```{.python notest}
tmp_work_dir = tempfile.mkdtemp()
with GraphServer(tmp_work_dir, tracing=True).start():
    client = RaphtoryClient("http://localhost:1736")
    client.send_graph(path="g", graph=new_graph)

    query = """{graph(path: "g") {nodes {list {name}}}}"""
    client.query(query)
```
///

You can set the port in `RaphtoryClient()` to the port the GraphQL server should run on.

The `path` parameter is always the graph in your server that you would like to read or update. So in this example, we want to send `new_graph` to graph `g` on the server to update it.

The `graph` parameter is set to the Raphtory graph that you would like to send. An additional `overwrite` parameter can be stated if we want this new graph to overwrite the old graph.
