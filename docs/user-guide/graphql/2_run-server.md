# Running the GraphQL Server

## Prerequisites

Before reading this topic, please ensure you are familiar with:

- [Ingesting data](../ingestion/1_intro.md)

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

## Starting a server

You can start the raphtory GraphQL in multiple ways depending on your usecase.

### Using the CLI

You can use the [Raphtory CLI](../getting-started/3_cli.md)  with the `server` command by running:

```sh
raphtory server --port 1736
```

This option is the simplist and provides the most configuration options.

### Start a server in Python

If you have a [`GraphServer`][raphtory.graphql.GraphServer] object you can use either the [`.run()`][raphtory.graphql.GraphServer.run] or [`.start()`][raphtory.graphql.GraphServer.start] functions to start a GraphQL sever and Raphtory UI.

Below is an example of how to start the server and send a Raphtory graph to the server, where `new_graph` is your Raphtory graph object.

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
