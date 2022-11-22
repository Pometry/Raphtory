# Building a graph from your data

The initial step to getting your first temporal graph analysis up and running is to tell Raphtory how to read your data source and how to build it into a graph. 

Three classes help with this:

- `Spouts` connect to the outside world, reading the data files and outputting individual tuples.
- `Builders` are used to define how your graph is built from the individual tuples.
- `Sources` wrap the Spout and Builder together. It is necessary to define a Spout in the Source so that Raphtory knows where to pull the data from. However, it is not mandatory to state a builder if your data fits certain formats including CSV files, JSON files (with no nesting) and Network X JSON graph files.

Once the Source is defined along with the Spout and optionally a graph builder, they can be passed to the `stream()` or `load()` methods on 
the {scaladoc}`com.raphtory.Raphtory` object, which will use both components to build the 
{scaladoc}`com.raphtory.api.analysis.graphview.TemporalGraph`. The difference here is that `stream()` will 
launch the full pipeline on top of [Apache Pulsar](https://pulsar.apache.org) (which you will see later in the tutorial) 
and assume new data can continuously arrive. `load()` on the other hand will compress the Spout and Graph Builder functions together, running as fast as possible, but only on a static dataset which does not change. For these initial examples we will only run `load()` as the data is static and we can set it going out of the box!

If you have the LOTR example already set up from the installation guide previously (<a href="https://github.com/Raphtory/Raphtory/tree/master/examples/lotr" target="_blank">examples/lotr</a>) then please continue. If not, YOU SHALL NOT PASS! Please return there and complete this step first.  

For this tutorial section we will use the `lotr` project located in the `examples` folder. We will be using the dataset of interactions between characters in the Lord of the Rings trilogy. The `csv` file (comma-separated values) in the examples folder can be found [here](https://github.com/Raphtory/Data/blob/main/lotr.csv). Each line contains two characters that appear in the same sentence, along with which sentence they appeared in, indicated by a number (sentence count). In the example, the first line of the file is `Gandalf,Elrond,33` which tells us that Gandalf and Elrond appear together in sentence 33.  

```
Gandalf,Elrond,33
Frodo,Bilbo,114
Blanco,Marcho,146
Frodo,Bilbo,205
Thorin,Gandalf,270
Thorin,Bilbo,270
Gandalf,Bilbo,270
Gollum,Bilbo,286
Gollum,Bilbo,306
Gollum,Bilbo,308
```

Also, in the examples folder you will find `DegreesSeparation.scala` and `TutorialRunner.scala` which we will go through in detail. 

## Local Deployment
First lets open `TutorialRunner.scala`, which is our `main` class i.e. the file which we actually run. 
To do this we have made our class a Scala app via `extends RaphtoryApp.Local`. Here, `Local` is an abstract class that implements the `main` method, making any class/object extending it "runnable". (This is a short hand for creating your runnable main class (if you come from a Java background) or can be viewed as a script if you are more comfortable with Python.) In adddition, it brings in local raphtory context in current scope. What it means is that overriding the `run` method from `RaphtoryApp` would provide you with a `RaphtoryContext` which creates (using `ctx.runWithNewGraph`) or returns already created graphs (`ctx.runWithGraph`) from the local machine itself. This is a pretty convenient way quickly try Raphtory.

We then pull data from our data source defined in the `url` variable, this is where the Lord of the Rings CSV file is located. A `foreach` function is called which basically reads each line of the CSV file - this is essentially what the Spout does in Raphtory. For each file line, we split the line into its respective variables: source, target and timestamp. These variables are then added as nodes and edges on the graph object we created at the start - this is essentially what the Graph Builder does in Raphtory. 

The graph object can then be used to run analysis on the graph you have built.

````scala
object TutorialRunner extends RaphtoryApp.Local {

  override def run(args: Array[String], ctx: RaphtoryContext): Unit = {
    ctx.runWithNewGraph() { graph =>  // creates graphId, if not provided
      val path = "/tmp/lotr.csv"
      val url  = "https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv"
      FileUtils.curlFile(path, url)

      val file = scala.io.Source.fromFile(path)
      file.getLines.foreach { line =>
        val fileLine   = line.split(",").map(_.trim)
        val sourceNode = fileLine(0)
        val srcID      = assignID(sourceNode)
        val targetNode = fileLine(1)
        val tarID      = assignID(targetNode)
        val timeStamp  = fileLine(2).toLong

        graph.addVertex(timeStamp, srcID, Properties(ImmutableProperty("name", sourceNode)), Type("Character"))
        graph.addVertex(timeStamp, tarID, Properties(ImmutableProperty("name", targetNode)), Type("Character"))
        graph.addEdge(timeStamp, srcID, tarID, Type("Character Co-occurrence"))
      }

      graph
        .at(32674)
        .past()
        .execute(DegreesSeparation())
        .writeTo(FileSink("/tmp/raphtory"))
        .waitForJob()

    }
  }
}
````

This can also be written using self-type annotations, as shown in the `TutorialRunner.scala`. You can read more about "Self-Type Annotations" [here](https://docs.scala-lang.org/tour/self-types.html), if you like.

````scala
object TutorialRunner extends RaphtoryApp.Local with LocalRunner

trait LocalRunner { self: RaphtoryApp =>

  override def run(args: Array[String], ctx: RaphtoryContext): Unit = ???
  }
}
````

## Spout 

Instead of iterating over the file with a for loop, Raphtory has a Spout function that can be used to read lines of data, this enables you to feed graphs from many different data sources into Raphtory, including real time streaming. For this example, we will make use of the `FileSpout`. This takes a file on your machine and pushes it into our Sources to parse. We automatically download the lotr.csv file from the [Raphtory data repository](https://github.com/Raphtory/Data) (hence why it run in the last tutorial), so you shouldn't have to set anything if using it. If you want to swap this file out for your own data, simply remove this download and change the `FileSpout` path to point to where your files are.

```scala 
val spout  = FileSpout("YOUR_FILE_HERE")
```

## Source

Raphtory includes different types of Source's to reduce the need of writing a graph builder. You also have the option of creating your own graph builder (this is explained in the next section). If your data is in CSV format, non-nested JSON format or NetworkX JSON format, it is likely you will not need to write your own graph builder. Simply wrap your spout in the Source object you would like to use: `CSVEdgeListSource`, `JSONEdgeListSource` or `JSONSource` (NetworkX JSON). The Source takes each ingested line of data and converts it into one or more graph updates.

```scala
val source = CSVEdgeListSource(spout)
```

## Graph Builder (Optional)

For more complicated graphs, we may need to create our own graph builder class.  Here is an example of a graph builder for the LOTR CSV data:

```scala
class LOTRGraphBuilder extends GraphBuilder[String]{

  def apply(graph: Graph, tuple: String): Unit = {
    val fileLine   = tuple.split(",").map(_.trim)
    val sourceNode = fileLine(0)
    val srcID      = assignID(sourceNode)
    val targetNode = fileLine(1)
    val tarID      = assignID(targetNode)
    val timeStamp  = fileLine(2).toLong

    addVertex(timeStamp, srcID, Properties(ImmutableProperty("name",sourceNode)), Type("Character"))
    addVertex(timeStamp, tarID, Properties(ImmutableProperty("name",targetNode)), Type("Character"))
    addEdge(timeStamp, srcID, tarID, Type("Character Co-occurrence"))
  }
}
```

First, we take the String output from the Spout. We then break up the line into the relevant components, splitting on commas (as it was a csv file ingested) via `.split(",")`. This means, for example, the line `Gandalf,Elrond,33` becomes a tuple `(Gandalf, Elrond, 33)` where each is accessible. For each of the characters seen, we give them a node ID of type `Long` via the `assignID` function - if your data already has numerical node id's you can skip this step. Finally, we send an update adding both of the vertices to the graph as well as the edge joining them, each with a timestamp of when the update occurred.

There are a few things worth pointing out here:

* We added a `name` property to each of the nodes. If we had reason to, we could have added any other property that might be appropriate. 
  We set this as an `ImmutableProperty` in this case, as character names are treated as fixed, but this could be a mutable
  property if it were required to change later. The different types of properties that can be added into a Raphtory
  graph can be found here: {scaladoc}`com.raphtory.api.input`.

* We didn't check whether either vertices exist before sending an `addVertex` update. Another component deals with this so we don't have to worry about that.

* The spout wasn't doing much heavy lifting here, just reading in the file line by line. The spout is used for pulling data from external sources (potentially streams) or extracting the useful parts of less structured data to send as a record to the graph builder.

To summarise, the spout takes an external source of data and turns it into a _stream of records_ and the graph builder converts each item from this stream of records into one or more _graph updates_.

## What next?

In the next parts of this tutorial, we will take a look at [implementing your own graph algorithms](../Analysis/LOTR_six_degrees.md) 
and [running algorithms](../Analysis/queries.md) using the query interface. Alternatively, you can take a look at the 
[detailed overview of the Analysis API](../Analysis/analysis-explained.md).



