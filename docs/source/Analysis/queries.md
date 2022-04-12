# Running Queries

To run your implemented algorithm or any of the algorithms included in the most recent Raphtory Release
([See here](com.raphtory.algorithms)), you must submit them to the graph. We use the [Lord of the Rings
graph](../Ingestion/sprouter.md) and the [degrees of separation algorithm](LOTR_six_degrees.md) examples to illustrate the query API.
You can either request a `pointQuery` to look at a single point in time or a `rangeQuery` over a subset of the history of the graph.
For more details, also see the [{s}`RaphtoryClient`](com.raphtory.client.RaphtoryClient) documentation.



# Running Queries

When running queries, the start point is a [{s}`TemporalGraph`](com.raphtory.algorithms.api.TemporalGraph),
which holds the information of a graph over a timeline. 
From this point, the overall process to get the things done is as follows.
First, you can filter the portion of the timeline you are interested in.
Then, you can create one or a collection of perspectives over selected timeline.
Thereafter, you can apply a sequence of graph operations that end up with a table and sequence
of table operations afterwards to get a writable result for your query.
Finally, you can write out your result using an output format.
This last step kicks off the computation inside Raphtory.

You can apply the graph operations and table operations directly over the graph and table objects,
but you can also use the algorithm API to create reusable algorithms.
This API allows defining a default way to tabularise graphs and algorithm chaining.
There are lots of algorithms ready to be used [here](com.raphtory.algorithms).
There is a quick example on how to use them in the next section.


## Quick example

You can start working with you graph objects right away.
In the most simple case, you can just apply a built-in algorithm over a graph and write out the results.
Doing this way, Raphtory runs the algorithm using all the information it has about the graph,
from the earliest to the latest updates.

```scala
val spout = ResourceSpout("path/to/your/input/file")
val builder = // provided by you regarding the format in your file
val graph = Raphtory.stream(spout, builder)

graph
  .execute(ConnectedComponents())
  .writeTo(FileOutputFormat("path/to/your/output/file"))
```

## Timeline manipulation

There are three operations available to filter the timeline.
You can filter the start with `from`, the end with `until` or `to`, or both at the same time with `slice`.
There are a couple of ways to express the time for these upper and lower bounds.
You can use just numbers, that can express arbitrary units
(from the number seconds since de epoch to the line number in a book).

In addition, you can provide strings expressing timestamps.
The default format is `"yyyy-MM-dd[ HH:mm:ss[.SSS]]"`. 
This means that you can provide just dates (`"2020-01-01"`),
or timestamps with up to seconds (`"2020-01-01 00:00:00"`) or up to milliseconds (`"2020-01-01 00:00:00.000"`).
Note that these three examples are equivalent as the trailing units are interpreted as zeros.

Indeed, you can mix both styles.
In such a case, Raphtory interprets numbers as milliseconds since the epoch to interoperate with timestamps.

```scala
graph
  .slice("2020-01-01", "2021-01-01")
  .execute(ConnectedComponents())
  .writeTo(FileOutputFormat("path/to/your/file"))
```

## Creating perspectives

We have only executed algorithms over single graphs so far.
However, the nice part about Raphtory is that you can run them over sequences of perspectives
to discover dynamic changes.
A quick example to do so below:

```scala
graph
  .depart("2020-01-01", "1 day") // departing from 2020-01-01 with steps of one day
  .window("1 day") // creates a window of one day in every step
  .execute(ConnectedComponents())
  .writeTo(FileOutputFormat("path/to/your/file"))
```
In this example, departing from 2020-01-01 with steps of one day, we create a window of one day each time.
Over all the perspectives of the graph enclosed by those windows,
we execute the algorithm and write the results to the file.
If we set up a spout from a streaming source, Raphtory creates a new window every day with the new information.

As noticed in the example, the process to create perspectives has two steps.
The first one is setting the points you are interested in.
You can just set one point using `at()` as well as a sequence of points with a given increment.
For the latter, you have available four different methods that allow you to set
the start of the sequence, the end of it, both, or none of them.
The methods are respectively `depart()`, `climb()`, `range()`, `walk()`.
The points are always aligned with the provided start or end.
In the case of a walk, as there are no points to align with,
the epoch or the point 0 regarding if timestamps or just numbers are being used.

The second step is to create perspectives from those points.
We have three options here.
We can just look to the `past` from every point, to the `future`, or set a `window`.
In the third case, we can align the windows using the start, the middle, or the end of it.
Refer to the API documentation for further details.



Coming back to our first example,
we can set up a walk along a prefiltered year of our data in steps of one day
creating a window of one day in each step as follows:

- maybe use window size different from increment, maybe weeks or months

```scala
graph
  .slice("2020-01-01", "2021-01-01")
  .walk("1 day")
  .window("1 day")
  .execute(ConnectedComponents())
  .writeTo(FileOutputFormat("path/to/your/file"))
```

As we don't specify any start or end for the sequence, the points are aligned with the epoch,
so we end up with a window for every day of the year 2020, as we could expect.




## Operating over the graph

Once we have defined the set ot perspectives we want to work with,
we can define a sequence of operations to apply over every of those perspectives.
The operations available are described in the documentation for the
[{s}`GraphOperations`](com.raphtory.algorithms.api.GraphOperations) trait.
In addition to using already defined graph algorithms,
you can also use graph operations directly over the graph object this way:

```scala
graph
  .slice("2020-01-01", "2021-01-01")
  .walk("1 day")
  .window("1 day")
  .filter(vertex => vertex.outDegree > 10)
  .step(vertex => vertex.messageOutNeighbours(vertex.name()))
  .select(vertex => Row(vertex.messageQueue))
  .writeTo(FileOutputFormat("path/to/your/file"))
```

Or a combination of both:

```scala
graph
  .slice("2020-01-01", "2021-01-01")
  .walk("1 day")
  .window("1 day")
  .filter(vertex => vertex.outDegree > 10)
  .execute(ConnectedComponents())
  .writeTo(FileOutputFormat("path/to/your/file"))
```











## Point Query

Point queries take the algorithm you wish to run and a timestamp specifying the time of interest. You may additionally
specify a List of windows with which Raphtory will generate a unique perspective for each. Within these perspectives
of the graph, entities which have been updated before the chosen timestamp, but as recently as the window size specified are included.  An example of these running a `DegreesSeparation` function on line 10000 can be seen below:

````scala
  graph.pointQuery(DegreesSeparation(name = "Gandalf"),FileOutputFormat("/tmp/sixDegPoint"), timestamp=32670)
  graph.pointQuery(DegreesSeparation(name = "Gandalf"),FileOutputFormat("/tmp/sixDegWindow"), timestamp=25000,windows=List(100,1000,10000)
````

Running this algorithm, returns the following data:

```
32670,Odo,2
32670,Samwise,1
32670,Elendil,2
32670,Valandil,2
32670,Angbor,2
32670,Arwen,2
32670,Treebeard,1
32670,Óin,3
32670,Butterbur,1
32670,Finduilas,2
32670,Celebrimbor,2
32670,Grimbeorn,2
32670,Lobelia,2
```

This data tells us that at a given time the time, person X and is N number of hops away.
For example at time 32670, Samwise was at minimum 1 hop away from Gandalf, whereas Lobelia was 2 hops away.

## Range Query

Range queries are similar to this, but take a start time, end time (inclusive) and increment with which it moves between these two points. An example of range queries over the full history (all pages) running `ConnectedComponents` can be seen below:

````scala
  graph.rangeQuery(ConnectedComponents(), FileOutputFormat("/tmp/rangeConComp"), start = 1,end = 32674,increment = 100)
  graph.rangeQuery(ConnectedComponents(), FileOutputFormat("/tmp/rangeConCompWindow"), start = 1,end = 32674,increment = 100,windows=List(100))
````

## Raphtory Client
Finally, if you have a running `RaphtoryGraph` and want to submit new queries to it, you can do this via the `RaphtoryClient`. This client is very simple to implement and can be run compiled, as the Service code above, or via the Scala REPL. An example client may be seen below:

```scala
import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.core.client.RaphtoryClient
import com.typesafe.config.{Config, ConfigFactory}

object ExampleClient extends App {
  val conf = ConfigFactory.load()
  val client = new RaphtoryClient(conf)
  client.pointQuery(ConnectedComponents(),FileOutputFormat("/tmp/pointConCompWin"), 10000,List(10000, 1000,100))
}
```
Here we can see that the client has only one argument, the Conf. This is an `application.conf` file which contains the settings required to connect to Raphtory.

The client then has the same `pointQuery` and `rangeQuery` functionality available to it as the RaphtoryGraph, and can be interacted with as such. Algorithms submitted this way will be sent across to the QueryManager and the results saved by your given output format.



## What now?
To summarise, Raphtory's analytical engine provides a way of expressing a large variety of graph algorithms,
implemented by vertex computations. Unlike more general graph analytics libraries, it has functionalities
for expressing temporal queries in a simple way.

Next, you can take a look at the [detailed overview of the algorithm API](analysis-explained.md) to learn how to implement
your own graph algorithms or take a look at the built-in [generic](com.raphtory.algorithms.generic) and 
[temporal](com.raphtory.algorithms.temporal) algorithms.