# Building a graph from your data

The first step to getting your first temporal graph analysis up and running is to tell Raphtory how to read your data source and how to build it into a graph. 

Two classes help with this:

- `Spouts` help with reading the data file and output tuples.
- `Graph builders`, as the name suggests, convert these tuples into updates, building the graph.

Once these classes are defined, they can be passed to the `stream()` or `batchLoad()` methods on the `Raphtory` object, which will use both components to build the temporal graph.  

If you have downloaded the [Examples](https://github.com/Raphtory/Examples.git) folder from the installation guide previously, then the LOTR example is already set up. If not, please return there and complete this step first.  

For this tutorial section we will continue to use the `raphtory-example-lotr` project and a dataset that tells us when two characters have some type of interaction in the Lord of the Rings trilogy books. The `csv` file (comma-separated values) in the examples folder can be found [here](https://github.com/Raphtory/Examples/blob/0.5.0/raphtory-example-lotr/resources/lotr.csv). Each line contains two characters that appear in the same sentence, along with which sentence they appeared in, indicated by a number (sentence count). In the example, the first line of the file is `Gandalf,Elrond,33` which tells us that Gandalf and Elrond appear together in sentence 33.  

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

Also, in the examples folder you will find `LOTRGraphBuilder.scala`, `DegreesSeparation.scala` and `FileOutputRunner.scala` which we will go through in detail. 

## Local Deployment
First lets open `FileOutputRunner.scala`, which is our `main` class i.e. the file which we actually run.
To do this we have made our class a scala app via `extends App`.
This is a short hand for creating your runnable main class (if you come from a Java background)
or can be viewed as a script if you are more comfortable with Python.
Inside of this we can create spout and graphbuilder objects and combine them into a graph object,
which can be further used to make queries from:

````scala
object FileOutputRunner extends App {
  val source  = FileSpout(path)
  val builder = new LOTRGraphBuilder()
  val graph   = Raphtory.stream(spout = source, graphBuilder = builder)
  val output  = FileOutputFormat("/tmp/raphtory")

  val queryHandler = graph
    .at(32674)
    .past()
    .execute(DegreesSeparation())
    .writeTo(output)
}
````

**Note:** Once `Raphtory.stream` is called,
we can start submitting queries to it - which can be seen in the snippet above.
Don't worry about this yet as we will dive into it in the next section.

## Spout

### Resource Spout
There are many data sources that may be used to feed graphs in Raphtory, for this example we will make use of the simplest of these, the `ResourceSpout`. This takes a file which is inside of the `resources` directory of an SBT project and pushes it into Pulsar for our graph builders to parse. We have already put the lotr.csv file into this folder (hence why it run in the last tutorial), so you shouldn't have to set anything if using it. If you are want to swap this file out for your own data, simply put your file into the resources directory and change the file name in the code.

```scala 
val source  = ResourceSpout("YOUR_FILE_HERE")
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
    addEdge(timeStamp, srcID, tarID, Type("Character Co-occurence"))
  }
}
```

First, we take the String output from the Spout. We then break up the line into the relevant components, splitting on commas (as it was a csv file ingested) via `.split(",")`. This means, for example, the line `Gandalf,Elrond,33` becomes a tuple `(Gandalf, Elrond, 33)` where each is accessible. For each of the characters seen, we give them a node ID of type `Long` via the `assignID` function - if your data already has numerical node id's you could skip this step. Finally, we send an update adding both of the vertices to the graph as well as the edge joining them, each with a timestamp of when that update occurred.

There are a few things worth pointing out here:

* We added a `name` property to each of the nodes. If we had reason to, we could have added any other property that might be appropriate. We set this as an `ImmutableProperty` in this case, as character names are treated as fixed, but this could be a mutable property if it were required to change later.

* We didn't check whether either vertices exist before sending an `addVertex` update. Another class deals with this so we don't have to worry about that.

* The spout wasn't doing much heavy lifting here, just reading in the file line by line. The spout is used for pulling data from external sources (potentially streams) or extracting the useful parts of less structured data to send as a record to the graph builder.

To summarise, the spout takes an external source of data and turns it into a _stream of records_ and the graph builder converts each item from this stream of records into a _graph updates_.

## What next?

In the next parts of this tutorial, we will take a look at [implementing your own graph algorithms](../Analysis/LOTR_six_degrees.md) 
and [running algorithms](../Analysis/queries.md) using the query interface. Alternatively, you can take a look at the 
[detailed overview of the Analysis API](../Analysis/analysis-explained.md).



