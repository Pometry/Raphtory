# Quick Start Guide
In this section, we will see how we can work with the Raphtory Scala APIs to create and analyse graphs. The foundations that we have built in the introductory chapters are as much applicable to this section therefore we won't be revisiting them here again!

## Installing Dependencies
To work with a Scala project you would need Scala, Java and SBT as development dependencies. The dependencies to work with Scala Raphtory APIs are Scala version `2.13.7`, Java version `11.0.11.hs-adpt` and SBT version `1.8.0`.

**Note**: You can manage these dependencies using [SDKMAN](https://sdkman.io/), if you like.

The Raphtory version that we will be discussing in this section is a "pre-release" version `v0.2.0a7`.

## Create Project
We can create a new project from Raphtory giter8 template like so:
```
$ sbt new Raphtory/raphtory.g8
```

Besides downloading some of the required dependencies and creating a new project from the template, there are a couple of project details (with some sensible defaults) presented for you to change:
```
name [raphtory-quickstart]:
version [0.1.0-SNAPSHOT]:
scala_version [2.13.7]:
sbt_version [1.8.0]:
organization [com.pometry]:
package [com.pometry]:
raphtory_version [0.2.0a7]:
```

## Create/Retrieve Graph
To start working with a graph, we can either create one with a user-specified graph ID or get it to auto-generate an ID. We can also retrieve an existing graph from its graph id.

- To create a new graph 

    ```scala
    def runWithNewGraph[T](graphID: String = createName, persist: Boolean = false)
    ```
    **Note**: The default behaviour is to destroy any new graph upon exiting the scope of raphtory context unless chosen to persist explicitly.

- To retrieve an existing graph

    ```scala
    def runWithGraph[T](graphID: String, destroy: Boolean = false)
    ```
    **Note**: User can choose to destroy the graph upon exiting the raphtory context by set the `destroy` flag to `true`.

- Destroy graphs

    Should user choose to destroy any of the persisted graphs for a given graph id
    ```scala
    def destroyGraph(graphID: String)
    ```

These APIs are available on `RaphtoryContext` which could be summoned differently for local and remote deployments.

- For Local Deployment

    The main application needs to extend the `Raphtory.Local` abstract class. We also need to override the `run` method that brings "local" `RaphtoryContext` in scope, which in turn is used to create or retrieve graphs:
    ```scala
    object Runner extends RaphtoryApp.Local {
        override def run(args: Array[String], ctx: RaphtoryContext): Unit = ???
    }
    ```

- For Remote Deployment

     The main application needs to extend the `Raphtory.Remote("127.0.0.1", 1736)` abstract class. Since the hosts and ports mentioned here are already configured as default values in the `application.conf` file, we need not provide them explicitly. We also need to override the `run` method that brings "remote" `RaphtoryContext` in scope, which in turn is used to create or retrieve graphs:
    ```scala
    object RemoteRunner extends RaphtoryApp.Remote() {
        override def run(args: Array[String], ctx: RaphtoryContext): Unit = ???
    }
    ```

## Load Data
- Should you choose to work with a newly created graph, you must load it with some data:
    ```scala
    val path = "/tmp/lotr.csv"
    val url = "https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv"
    FileUtils.curlFile(path, url)

    val source = Source(FileSpout(path), new LotrGraphBuilder())
    graph.load(source)
    ```

    Here we download raw data to `/tmp` and then transform it to vertices and edges using graph builders.

- If you are already working with an existing graph, you may not need to load any new data and you can jump straight into analysis


## Analyse Graph
Once you have created or retrieved an already existing graph against a `graphId`, you can run analysis on it and choose to write output to a sink of your choice. 

```scala
graph
    .execute(Degree())
    .writeTo(FileSink("/tmp/raphtory"))
    .waitForJob()
```

Here, we run `Degree` algorithm on the graph which returns "in-degree" and "out-degree" of nodes in case of a directed graph i.e., incoming or outgoing edges respectively and "degree" of nodes in case of undirected graph representing the total number of edges. Additionally, we chose to write the output to a directory `/tmp/raphtory` using `FileSink`.

## Puting it all together
- Working with Local Deployment
    ```scala
    object Runner extends RaphtoryApp.Local {
        override def run(args: Array[String], ctx: RaphtoryContext): Unit =
            ctx.runWithNewGraph() { graph =>
            val path = "/tmp/lotr.csv"
            val url = "https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv"
            FileUtils.curlFile(path, url)

            val source = Source(FileSpout(path), new LotrGraphBuilder())
            graph.load(source)

            graph
                .execute(Degree())
                .writeTo(FileSink("/tmp/raphtory"))
                .waitForJob()

            }
    }
    ```

- Working with Remote Deployment
    ```scala
    object RemoteRunner extends RaphtoryApp.Remote() {
        override def run(args: Array[String], ctx: RaphtoryContext): Unit =
            ctx.runWithNewGraph() { graph =>
            val path = "/tmp/lotr.csv"
            val url = "https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv"
            FileUtils.curlFile(path, url)

            val source = Source(FileSpout(path), new LotrGraphBuilder())
            graph.load(source)

            graph
                .execute(Degree())
                .writeTo(FileSink("/tmp/raphtory"))
                .waitForJob()

            }
    }
    ```

## Run
### Local Deployment
- To create graph locally and run analysis 
    ```sh
    $ sbt "runMain com.pometry.Runner"
    ```

### Remote Deployment (Standalone)
- Start the standalone server instance
    ```sh
    $ sbt "runMain com.raphtory.service.Standalone"
    ```

- To create graph on the remote standalone server instance and run analysis
    ```sh
    $ sbt "runMain com.pometry.RemoteRunner"
    ```
