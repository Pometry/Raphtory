# Running queries across time

To run your implemented algorithm or any of the algorithms included in Raphtory (both [Generic](com.raphtory.algorithms.generic) and [Temporal](com.raphtory.algorithms.temporal)), you must submit them to the graph. We can again use the [Lord of the Rings
graph](../Ingestion/sprouter.md) and the [degrees of separation algorithm](LOTR_six_degrees.md) to illustrate the query API.

When running queries, our starting point is always the {scaladoc}`com.raphtory.api.analysis.graphview.TemporalGraph` 
created from a call to `Raphtory.load()` or `Raphtory.stream()`. This contains the full history of your data over its lifetime. 
From this point, the overall process to get things done is as follows: 

* First, you can filter the timeline to the segment you are interested in. 

* Secondly, you can create a collection of perspectives over the selected timeline.

* Thirdly, you can apply a sequence of graph operations (such as `step` and `iterate`) that end with a `select()` 
  (returning a {scaladoc}`com.raphtory.api.analysis.table.Table`) and a sequence of table operations to get a writable 
  result for your query.

* Finally, you can write out your result using a {scaladoc}`com.raphtory.api.output.sink.Sink`. This last step kicks off the computation inside Raphtory.

A conceptual example of the stages for creating perspectives from a temporal graph is depicted below.

<p align="center">
	<img src="../_static/query-stages.png" width="80%" alt="Stages of queries"/>
</p>

## Quick start

Instead of going through this whole process, we can just start executing algorithms from the same graph object we saw in the last section:

```{literalinclude} ../../../doc-tests/src/test/scala/Queries.scala
:language: scala
:start-after: "[everything]"
:end-before: "[everything]"
```

In this case, Raphtory runs the algorithm using all the information it has about the graph, from the earliest to the 
latest updates. If we are working with a streaming graph, this means that we run the algorithm over the most recently 
ingested version of our data.

## Timeline filtering

However, maybe we are just interested in a portion of the data. Let's say that you want to see the relationships 
between characters before sentence 1000 or between sentences 4000 and 5000. We need different versions 
({scaladoc}`com.raphtory.api.analysis.graphview.GraphPerspective`) of the graph for both cases:

```{literalinclude} ../../../doc-tests/src/test/scala/Queries.scala
:language: scala
:start-after: "[sentence-filtering]"
:end-before: "[sentence-filtering]"
```

Here, `first1000sentences` holds all the interactions before sentence 1000
and `sentencesFrom4000To5000` holds all the interactions between sentence 4000 (included) and 5000 (excluded).

In addition, you have access to two more methods: `from` and `to`.
The table below provides a quick summary:

| Function            | Activity kept                          |
|---------------------|----------------------------------------|
| `from(start)`       | everything after `start` (inclusive)   |
| `until(end)`        | everything before `end` (exclusive)    |
| `to(end)`           | everything before `end` (inclusive)    |
| `slice(start, end)` | equivalent to `from(start).until(end)` |

If you are working with real times, then you can also provide strings expressing timestamps. The default format is `"yyyy-MM-dd[ HH:mm:ss[.SSS]]"`. 
This means that you can provide just dates (`"2020-01-01"`), or timestamps with up to seconds (`"2020-01-01 00:00:00"`) or up to milliseconds (`"2020-01-01 00:00:00.000"`). **Note:** These examples are interpreted as having all trailing units as zeros.

For instance, let's say that you are only interested in the activity of your graph within the year 2020. In order to apply an algorithm over only that interval, we can do the following:

```{literalinclude} ../../../doc-tests/src/test/scala/Queries.scala
:language: scala
:start-after: "[year-slice]"
:end-before: "[year-slice]"
```

As a third option, you can mix both styles. In such a case, Raphtory interprets numbers as __milliseconds since the linux epoch__ to interoperate with timestamps.

## Creating perspectives

We have only executed algorithms over monolithic views of graphs so far. However, the nice part about Raphtory is that you can run them over sequences of perspectives to discover dynamic changes.
A quick example to do so is:

```{literalinclude} ../../../doc-tests/src/test/scala/Queries.scala
:language: scala
:start-after: "[depart-window]"
:end-before: "[depart-window]"
```

In this example, starting from January 1 2020 we move forward one day at a time, looking forward over the next day of data. At each of these stopping points (`perspectives`) we execute the algorithm and write the results to the file. If we set up a spout from a streaming source which continues to ingest data, Raphtory will continue to create a new `windowed perspective` every day as the new information arrives.

As can be seen in the example, the process to create perspectives has two steps. The first of these is setting the times you are interested in, which can be a singular point (using `at()`) or, alternatively, a sequence of points with a given increment. For sequences, four different methods are available:

| Function            | Effect                                            |
|---------------------|---------------------------------------------------|
| `depart(time,increment)`   | Set the start time and increment           |
| `climb(time,increment)`    | Set the end time and increment             |
| `range(start,end,increment`| Set the start time, end time and increment |
| `walk(increment)`          | Set only the increment                     |

```{note} 
If no start and end time are provided, Raphtory will default to the minimum and maximum times in the data (or min and max of the time range if a `slice()` etc. has been applied).
```

The second step is to specify which direction we are looking in at each time point, for which we have three options. We can look to the `past`, to the `future`, or set a `window`. In the third case, we can align the time point as: 

* The `Start` of the window - looking into the future the set amount of time. This is the default.
* The `End` of the window - looking into the past the set amount of time. 
* The `Middle` of the window - including data from half the time in both the past and future, providing a smoothing effect.


```{note} 
 You can refer to the {scaladoc}`com.raphtory.api.analysis.graphview.DottedGraph` documentation for further details.
```

Coming back to our first example, we can execute a `walk` along a year of data with increments of one day, and a window of one week into the future as follows:

```{literalinclude} ../../../doc-tests/src/test/scala/Queries.scala
:language: scala
:start-after: "[walk-window]"
:end-before: "[walk-window]"
```

The `walk` function doesn't take a start or end time as it explores all available perspectives (given the other filters applied). For the above instance this generates a sequence of perspectives where the first contains data between `Dec 26, 2019` to `Jan 1, 2020`, the second one from `Dec 27, 2019` to `Jan 2, 2020`, etc. The last perspective of the sequence is then going to be from `Dec 31, 2020` to `Jan 6, 2021`. 

```{note} 
The reason dates outside of the `.slice("2020-01-01", "2021-01-01")` appear in this sequence is because Raphtory includes `partial windows` i.e. where only part of the perspectives time range is inside of the slice. For instance, the perspective for `Dec 27, 2019` to `Jan 2, 2020` has a small amount of data inside of the slice which can be analysed. An example of these partial windows can be seen in the bottom left of the diagram at the top of the page.  
```

## Operating over the graph

Once we have defined the set of perspectives we want to work with, we can define a sequence of operations to apply 
to them all. The operations available are described in the documentation for the 
{scaladoc}`com.raphtory.api.analysis.graphview.GraphPerspective` trait. In addition to using already defined graph algorithms 
(as we have done so far), you can also apply operations directly to the graph object, for instance:

```{literalinclude} ../../../doc-tests/src/test/scala/Queries.scala
:language: scala
:start-after: "[direct-style]"
:end-before: "[direct-style]"
```

Or a combination of both:

```{literalinclude} ../../../doc-tests/src/test/scala/Queries.scala
:language: scala
:start-after: "[mixed-style]"
:end-before: "[mixed-style]"
```

This is especially useful when you want to preprocess the graph before applying an already defined algorithm. For instance, above we only keep nodes with an out degree greater than 10.

```{note} 
When filtering vertices, if a vertex is to be removed so will ALL of its edges. This means they will no longer be available to the vertex attached on the other side. 
```

## Looking at the output

Coming back to our Lord of the Rings example, we can analyse the output produced by the query inside `TutorialRunner.scala`:

```scala
  val queryHandler = graph
    .at(32674)
    .past()
    .execute(DegreesSeparation())
    .writeTo(output)
```
As we can now understand, what we are doing here is creating a perspective at sentence 32674, looking into the past and, therefore, including everything from sentence 1. Running this algorithm returns the following data:

```
32674,Hirgon,1
32674,Hador,3
32674,Horn,2
32674,Galadriel,1
32674,Isildur,1
32674,Mablung,2
32674,Gram,2
32674,Thingol,2
32674,Celebrían,3
32674,Gamling,2
32674,Déagol,2
32674,Findegil,2
32674,Brand,-1
32674,Baldor,2
32674,Helm,1
32674,Thengel,1
32674,Gil-galad,2
...
```

This data tells us that at a given time, person X and Gandalf are N number of hops away. In this instance, at time 32674, Isildur was at minimum 1 hop away from Gandalf, whereas Baldor was 2 hops away.

## What now?
To summarise, Raphtory's analytical engine provides a way of expressing a large variety of graph algorithms, implemented by vertex computations and, unlike other graph tools, has functionalities for expressing temporal queries in an intuitive manner.

Next, you can take a look at the [detailed overview of the algorithm API](analysis-explained.md) to learn how to implement
your own graph algorithms or take a look at the built-in [generic](com.raphtory.algorithms.generic) and 
[temporal](com.raphtory.algorithms.temporal) algorithms.