# Running Queries

To run your implemented algorithm or any of the algorithms included in the most recent Raphtory Release
([See here](com.raphtory.algorithms)), you must submit them to the graph. We use the [Lord of the Rings
graph](../Ingestion/sprouter.md) and the [degrees of separation algorithm](LOTR_six_degrees.md) examples to illustrate the query API.

When running queries, the start point is a [`TemporalGraph`](com.raphtory.algorithms.api.TemporalGraph),
which holds the information of a graph over a timeline. 
From this point, the overall process to get the things done is as follows.
First, you can filter the portion of the timeline you are interested in.
Then, you can create one or a collection of perspectives over selected timeline.
Thereafter, you can apply a sequence of graph operations that end up with a table and sequence
of table operations afterwards to get a writable result for your query.
Finally, you can write out your result using an output format.
This last step kicks off the computation inside Raphtory.

## Quick start

Instead of going through this whole process,
we can just start executing algorithms from the same graph object we saw in the last section:

```scala
graph
  .execute(DegreesSeparation())
  .writeTo(output)
```

In this case, Raphtory runs the algorithm using all the information it has about the graph,
from the earliest to the latest updates.
If we are working with a streaming graph,
this means that we run the algorithm over the most updated version of our data.

## Timeline filtering

However, maybe we are just interested in a portion of all the data.
Let's say that you want to see the relationships between characters
taking into account only before line 1000 or between lines 4000 and 5000.
You can get new versions of the graph object for both cases:

```scala
val first1000lines = graph.until(1000)
val linesFrom4000To5000 = graph.slice(4000, 5000)
```

Here, `first1000lines` holds all the lines before 1000
and `linesFrom4000To5000` holds all the lines between 4000 (included) and 5000 (excluded).

In addition, you have access to two more methods: `from` and `to`.
The table below provides a quick summary:

| Call                | Activity kept                          |
|---------------------|----------------------------------------|
| `from(start)`       | everything after `start` (inclusive)   |
| `until(end)`        | everything before `end` (exclusive)    |
| `to(end)`           | everything before `end` (inclusive)    |
| `slice(start, end)` | equivalent to `from(start).until(end)` |

If you are working with real times, then you can also  provide strings expressing timestamps.
The default format is `"yyyy-MM-dd[ HH:mm:ss[.SSS]]"`. 
This means that you can provide just dates (`"2020-01-01"`),
or timestamps with up to seconds (`"2020-01-01 00:00:00"`) or up to milliseconds (`"2020-01-01 00:00:00.000"`).
Note that these three examples are equivalent as the trailing units are interpreted as zeros.

For instance, let's say that you are only interested in the activity of your graph that happened within the year 2020.
In order to apply an algorithm only over the activity of the graph within the year 2020, you can do this:

```scala
graph
  .slice("2020-01-01", "2021-01-01")
  .execute(ConnectedComponents())
  .writeTo(output)
```

As a third option, you can mix both styles.
In such a case, Raphtory interprets numbers as milliseconds since the epoch to interoperate with timestamps.

## Creating perspectives

We have only executed algorithms over monolithic views of graphs so far.
However, the nice part about Raphtory is that you can run them over sequences of perspectives
to discover dynamic changes.
A quick example to do so below:

```scala
graph
  .depart("2020-01-01", "1 day") // departing from Jan 1, 2020 with steps of one day
  .window("1 day") // creates a window of one day in every step
  .execute(ConnectedComponents())
  .writeTo(output)
```
In this example, departing from Jan 1st 2020 with steps of one day, we create a window of one day each time.
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
the epoch or the point 0 is minded regarding if timestamps or just numbers are being used.

The second step is to create perspectives from those points.
We have three options here.
We can just look to the `past` from every point, to the `future`, or set a `window`.
In the third case, we can align the windows using the start, the middle, or the end of it.
Refer to the [DottedGraph](com.raphtory.algorithms.api.DottedGraph) documentation for further details.

Coming back to our first example,
we can set up a walk along a prefiltered year of our data in steps of one day
creating a window of one week in each step as follows:

- maybe use window size different from increment, maybe weeks or months

```scala
graph
  .slice("2020-01-01", "2021-01-01")
  .walk("1 day")
  .window("1 week")
  .execute(ConnectedComponents())
  .writeTo(output)
```

As we don't specify any start or end for the sequence, the points are aligned with the epoch,
so we end up with a sequence of windows including from Jan 1 to Jan 7, from Jan 2 to Jan 7, etc.
The last window of the sequence is that going from Dec 31, 2020 to Jan 6, 2021.
The next window to this, from Jan 1 to Jan 7 (2021), and the following windows are omitted,
as there is no available data for them after the initial filtering.

## Operating over the graph

Once we have defined the set ot perspectives we want to work with,
we can define a sequence of operations to apply over every of those perspectives.
The operations available are described in the documentation for the
[`GraphOperations`](com.raphtory.algorithms.api.GraphOperations) trait.
In addition to using already defined graph algorithms as we have done so far,
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

This possibility is especially useful when you want to
preprocess the graph before applying an already defined algorithm.
For instance, you can filter a set of nodes that fulfill some condition.

## Looking at the output

Coming back to our Lord of the Rings example,
we can analyse the output produced by the query inside `FileOutputRunner.scala`:

```scala
graph
  .at(32674)
  .past()
  .execute(DegreesSeparation())
  .writeTo(output)
```
As we can understand now, what we are doing is creating a perspective at line 32674 looking at the past,
and therefore including from line 1.
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

## What now?
To summarise, Raphtory's analytical engine provides a way of expressing a large variety of graph algorithms,
implemented by vertex computations. Unlike more general graph analytics libraries, it has functionalities
for expressing temporal queries in a simple way.

Next, you can take a look at the [detailed overview of the algorithm API](analysis-explained.md) to learn how to implement
your own graph algorithms or take a look at the built-in [generic](com.raphtory.algorithms.generic) and 
[temporal](com.raphtory.algorithms.temporal) algorithms.