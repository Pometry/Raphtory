# Building a graph from your data

The initial step to getting your first temporal graph analysis up and running is to tell Raphtory how to read your data source and how to build it into a graph. 

Two classes help with this:

- `Spouts` connect to the outside world, reading the data files and outputting individual tuples.
- `Graph builders`, as the name suggests, convert these tuples into updates, building the graph.

Once these classes are defined, they can be passed to the `stream()` or `load()` methods on 
the {scaladoc}`com.raphtory.Raphtory` object, which will use both components to build the 
{scaladoc}`com.raphtory.api.analysis.graphview.TemporalGraph`. The difference here is that `stream()` will 
launch the full pipeline on top of [Apache Pulsar](https://pulsar.apache.org) (which you will see later in the tutorial) 
and assume new data can continuously arrive. `load()` on the other hand will compress the Spout and Graph Builder functions together, running as fast as possible, but only on a static dataset which does not change. For these initial examples we will only run `load()` as the data is static and we can set it going out of the box!

If you have the LOTR example already set up from the installation guide previously ([raphtory-example-lotr](https://github.com/Raphtory/Raphtory/tree/master/examples/raphtory-example-lotr)) then please continue. If not, YOU SHALL NOT PASS! Please return there and complete this step first.  

For this tutorial section we will continue to use the `raphtory-example-lotr` project and the dataset of interactions between characters in the Lord of the Rings trilogy. The `csv` file (comma-separated values) in the examples folder can be found [here](https://github.com/Raphtory/Data/blob/main/lotr.csv). Each line contains two characters that appear in the same sentence, along with which sentence they appeared in, indicated by a number (sentence count). In the example, the first line of the file is `Gandalf,Elrond,33` which tells us that Gandalf and Elrond appear together in sentence 33.  

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

Also, in the examples folder you will find `LOTRGraphBuilder.scala`, `DegreesSeparation.scala` and `TutorialRunner.scala` which we will go through in detail. 

## Local Deployment
First lets open `TutorialRunner.scala`, which is our `main` class i.e. the file which we actually run. 
To do this we have made our class a scala app via `extends App`. 
This is a short hand for creating your runnable main class (if you come from a Java background) or can be viewed as a script if you are more comfortable with Python. 
Inside of this we can create spout and graphbuilder objects and combine them into a 
{scaladoc}`com.raphtory.api.analysis.graphview.TemporalGraph` (via `load()`), which can be used to make queries:

````scala
object TutorialRunner extends App {
  val path = "/tmp/lotr.csv"
  val url  = "https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv"

  FileUtils.curlFile(path, url)

  val source  = FileSpout(path)
  val builder = new LOTRGraphBuilder()
  val graph   = Raphtory.load(spout = source, graphBuilder = builder)
  val output  = FileSink("/tmp/raphtory")

  val queryHandler = graph
    .at(32674)
    .past()
    .execute(DegreesSeparation())
    .writeTo(output)

  queryHandler.waitForJob()

}
}
````

```{note} 
Once `Raphtory.load` is called, we can start submitting queries to it - which can be seen in the snippet above. Don't worry about this yet as we will dive into it in the next section.
```

## Spout 

There are many data sources that may be used to feed graphs in Raphtory, for this example we will make use of the `FileSpout`. This takes a file on your machine and pushes it into our graph builders to parse. We automatically download the lotr.csv file from the [Raphtory data repository](https://github.com/Raphtory/Data) (hence why it run in the last tutorial), so you shouldn't have to set anything if using it. If you want to swap this file out for your own data, simply remove this download and change the `FileSpout` path to point to where your files are.

```scala 
val source  = FileSpout("YOUR_FILE_HERE")
```

## Graph Builder

Now that our data is flowing in we can have a look at the graph builder class. This takes each ingested line of data and converts it into one or more graph updates. The function which does this (and in this case is the only part we need to define in this class) is `parseTuple`. Let's look at the code:

```scala
class LOTRGraphBuilder extends GraphBuilder[String]{

  override def parseTuple(tuple: String): Unit = {
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



