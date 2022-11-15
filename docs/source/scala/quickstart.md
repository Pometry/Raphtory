# Quick Start Guide
In this section, we will see how we can work with the Raphtory Scala APIs to create and analyse graphs. The foundations that we have build in the introductory chapters are as much applicable to this section therefore we won't be revisiting them here again!

## Installing Dependencies
To work with a Scala project you would need Scala, Java and SBT as development dependencies. The dependencies that we have tested the project discussed in this section to work with Raphtory are Scala version `2.13.7`, Java version `11.0.11.hs-adpt` and SBT version `1.8.0`.

**Note**: You can manage these dependencies using [SDKMAN](https://sdkman.io/), if you like.

The Raphtory version that we will be discussing in this section is a "pre-release" version `v0.2.0a7`.

## Create Project
We can create a new project from Raphtory giter8 template like so:
```
$ sbt new Raphtory/raphtory.g8
```

Besides downloading some of the required dependencies and creating a new project from the template, a couple of project details (with some sensible defaults) are asked, as part of above step, which you might want to change if you would like:
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
To start working with a graph, we can either create it with a user-specified graph id (or get it auto-generate) or retrieve an existing graph for a given graph id. We can make use of following APIs:

- To create a new graph 

    ```scala
    def runWithNewGraph[T](graphID: String = createName, persist: Boolean = false)
    ```
    **Note**: The default behaviour is to destory any new graph upon exiting the scope of raphtory context unless chosen to persist explicitly.

- To retreive an existing graph

    ```scala
    def runWithGraph[T](graphID: String, destroy: Boolean = false)
    ```
    **Note**: User can chose to destroy the graph upon exiting the raphtory context by set the `destory` flag to `true`.

- Destroy graphs

    Should user choose to destory any of the persisted graphs for a given graph id
    ```scala
    def destroyGraph(graphID: String)
    ```

These APIs are availabe on `RaphtoryContext` which could be summoned differently for local and remote deployments.

- For Local Deployment

    In order to bring "local" raphtory context in scope, the main application needs to extend `Raphtory.Local` abstract class which requires user to override the `run` method that brings `RaphtoryContext` in scope which could be used to create or retrieve graphs:
    ```scala
    object Runner extends RaphtoryApp.Local {
        override def run(args: Array[String], ctx: RaphtoryContext): Unit = ???
    }
    ```

- For Remote Deployment

    In order to bring "remote" raphtory context in scope, the main application should extend `Raphtory.Remote("127.0.0.1", 1736)` abstract class. Since hosts and ports here are already configured as defaults, we need not provide them explicitly:
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

- If you are already working with an existing graph, you may not need to load any new data and you can directly jump onto analysis it


## Analyse Graph
Once you have created or retrieved an already existing graph against a `graphId`, you could run analysis on it and choose to write output to a sink of your choice. 

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
