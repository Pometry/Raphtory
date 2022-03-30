# Building a graph from your data

The first step to getting your first temporal graph analysis up and running is to tell Raphtory how to read your data source and how to build it into a graph. 

Two classes help with this:

- `Spouts` help with reading the data file and output tuples.
- `Graph builders`, as the name suggests, convert these tuples into updates, building the graph.

Once these classes are defined, they can be passed to `RaphtoryGraph` which will use both components to build the temporal graph.  

If you have downloaded the [Examples](https://github.com/Raphtory/Examples.git) folder from the [installation](https://raphtory.github.io/documentation/install) guide, then the below LOTR example is already set up. If not, please return there and complete this step first.  
There are two examples in this repository, `raphtory-example-gab` and `raphtory-example-lotr`.  

For this example, we will use the `raphtory-example-lotr` and a dataset that tells us when two characters have some type of interation in the Lord of the Rings trilogy books. The `csv` file (comma-separated values) in the examples folder can be found [here](https://github.com/Raphtory/Examples/blob/master/raphtory-example-lotr/src/main/scala/com/raphtory/examples/lotr/data/lotr.csv). Each line contains two characters that appear in the same sentence, along with which sentence they appeared in, indicated by a number (sentence count). In the example, the first line of the file is `Gandalf,Elrond,33` which tells us that Gandalf and Elrond appear together in sentence 33.  

Also, in the examples folder you will find `LOTRGraphBuilder.scala`, `DegreesSeperation.scala`, `LOTRSpout.scala` and `LOTRDeployment.scala` which we will go through in detail.


## Spout

Let's continue with the LOTR example. There are two ways to get our data in; We can use our in-built `FileSpout` or write a spout from scratch. We shall go through both variations. 

### Custom Spout

There are three main functions that can be inherited and implemented in the Spout class: `setupDataSource`, `generateData`, `closeDataSource`. 

First, we specify a `filename` as a string pointing to `"/absolute/path/to/LOTR/file.csv"`. We then implement `setupDataSource` in order to extract the data and put it into a queue.

```scala
val filename = "/absolute/path/to/LOTR/file.csv"
val fileQueue = mutable.Queue[String]()

  override def setupDataSource(): Unit = {
    fileQueue++=
      scala.io.Source.fromFile(directory + "/" + file_name)
        .getLines
  }
```

Next, we implement `generateData` which sends each line of the file from the queue to the graph builder. The generate data function runs whenever the graph builder requests data. The crucial information that a line must contain is everything needed to build a node or edge or both.

**Note:** we call the `dataSourceComplete()` function here once the queue is empty to let the graph builders know not to pull any more data and so that Raphtory may fully synchronise, making the final timestamps available for analysis.  

```scala
 override def generateData(): Option[String] = {
    if(fileQueue isEmpty){
      dataSourceComplete()
      None
    }
    else
      Some(fileQueue.dequeue())
  }
```

Finally, we may be required to close the data source, such as in the instance of a Kafka stream or database connection. In this example, there is no need to develop the `closeDataSource` as we read in the file fully at the start, but know that it exists for cases where it's necessary.

### File Spout

Alternatively, you can use the `FileSpout` which is built into the Raphtory library and allows the reading of files line-by-line without any pre-processing. Here we must first import the FileSpout class and then we include the path of the file, in this case we are looking for a single file and thus specify the path and file seperately. 

```scala
import com.raphtory.spouts.FileSpout
...
val source  = new FileSpout("src/main/scala/com/raphtory/examples/lotr/data/","lotr.csv")
```

## Graph Builder

Now we will implement the graph builder class, which takes each ingested line of data and converts it into one or more graph updates. The function which does this (and in this case is the only part we need to define in this class) is `parseTuple`. Let's look at the code:

```scala
override def parseTuple(tuple: String) = {

    val fileLine = tuple.split(",").map(_.trim)
    val sourceNode = fileLine(0)
    val srcID = assignID(sourceNode)

    val targetNode = fileLine(1)
    val tarID = assignID(targetNode)

    val timeStamp = fileLine(2).toLong

    addVertex(timeStamp, srcID, Properties(ImmutableProperty("name",sourceNode)),Type("Character"))
    addVertex(timeStamp, tarID, Properties(ImmutableProperty("name",targetNode)),Type("Character"))
    addEdge(timeStamp,srcID,tarID, Type("Character Co-occurence"))
  }
}
```

First, we break up the line that has just been ingested by the spout into the relevant components; so, for example, the line `Gandalf,Elrond,33` becomes a tuple `(Gandalf, Elrond, 33)`. For each of the characters seen, we give them an ID of type `Long`. Finally, we send an update adding both of the vertices to the graph as well as the edge joining them, each with a timestamp of when that update occurred.

There are a few things worth pointing out here:

* We added a `name` property to each of the nodes. If we had reason to, we could have added any other property that might be appropriate. We set this as an `ImmutableProperty` in this case, as character names are treated as fixed, but this could be a mutable property if it were required to change later.

* We didn't check whether either vertices exist before sending a `addVertex` update. Another class deals with this so that we don't have to worry about that.

* The spout wasn't doing much heavy lifting here, just reading in the file line by line. The spout is used for pulling data from external sources (potentially streams) or extracting the useful parts of less structured data to send as a record to the graph builder.

To summarise, the spout takes an external source of data and turns it into a _stream of records_ and the graph builder converts each item from this stream of records into a _graph updates_.

## Raphtory Graph
Now that we have a way to ingest and parse the data we can create a graph. To do this we can first create a scala app. `Extends App` is a short hand for creating your runnable main class (if you come from a Java background) or can be viewed as a script if you are more comfortable with Python.

Inside of this we can create a spout and graphbuilder objects from the classes we have defined above and combine them into a RaphtoryGraph:


````scala
object Runner extends App{
 val source  = new LOTRSpout()
  val builder = new LOTRGraphBuilder()
  val rg = RaphtoryGraph[String](source,builder)
}
````
