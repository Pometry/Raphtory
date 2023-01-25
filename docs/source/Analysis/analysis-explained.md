# A deeper dive into Raphtory analysis

Raphtory's analysis engine works by *vertex centric computation*. Each vertex has access to local information about the graph (just its immediate vicinity). To complement this, vertices can communicate with their neighbours (other vertices that are directly connected to it). Many graph algorithms which operate on a per-vertex level can be expressed in this way. The benefit of this is that graphs can be distributed over multiple cores/machines, each containing a proportion of the vertices, and these vertex computations can be executed in a parallel manner.

Each vertex (or node) knows:

* Its own update history, property set and property history e.g. values and update times.
* The history/properties of the edges attached to it - both incoming and outgoing as Raphtory has a directed graph model. E.g. in Twitter where the other person has to follow you back, as opposed to Facebook friends where two friends are connected by default.
* Its own algorithmic state - this is a map of temporary values that can be set and modified during the computation steps.

<p align="center">
  <img src="../_static/vertex_time2.png" style="width: 20vw;" alt="vertex time view"/>
<figcaption>The information available to vertex V1 - this includes the time it was created, the IDs of its neighbours and the times at which its edges are established.</figcaption>
</p>

The next sections will explore how algorithms can be written using these vertex communications.

## The GraphAlgorithm API

The core of the Raphtory algorithm API is the {scaladoc}`com.raphtory.api.analysis.algorithm`
package which contains base-classes for different types of algorithms that custom algorithms should extend.
The different algorithm base classes support mapping between different types of views of the underlying graph. 
Currently Raphtory supports an aggregate {scaladoc}`com.raphtory.api.analysis.graphview.ReducedGraphPerspective` view 
where algorithm steps act on vertices (see {scaladoc}`com.raphtory.api.analysis.visitor.Vertex`) and a multilayer 
{scaladoc}`com.raphtory.api.analysis.graphview.MultilayerGraphPerspective` view where algorithm steps act on 
exploded vertices (see {scaladoc}`com.raphtory.api.analysis.visitor.ExplodedVertex`) which represent a vertex at a 
given point in time. 

In general, an algorithm has two stages: graph processing and tabularising results. Graph processing is defined
using the {s}`apply()` method whereas tabularising results is handled by the
{s}`tabularise()` method. Graph operations are defined by the
{scaladoc}`com.raphtory.api.analysis.graphview.GraphPerspective`
class and tabular data by the {scaladoc}`com.raphtory.api.analysis.table.Table` class.
Rows in a {scaladoc}`com.raphtory.api.analysis.table.Table` are manipulated using the
{scaladoc}`com.raphtory.api.analysis.table.Row` class. 

The support for different graph views is still experimental. Algorithms that are view-agnostic (in particular, 
all algorithms from Raphtory-akka (The prior deprecated version)) should extend the 
{scaladoc}`com.raphtory.api.analysis.algorithm.Generic` algorithm base-class.

The algorithm API package also contains
{scaladoc}`com.raphtory.api.analysis.algorithm.Identity` which is an algorithm that leaves the graph unchanged and does
not write out any results. This is mainly useful as a default argument for algorithms that can optionally
run another graph algorithm, e.g., as an optional pre- or post-processing step. An example of such an algorithm
is community-based outlier detection [{s}`CBOD`](com.raphtory.algorithms.generic.CBOD) which can optionally
run a community detection algorithm to label the vertices.

### Graph processing

The core of most algorithms (though not all, see zero-step algorithms below) is the graph processing stage.
The graph processing is implemented by overriding the {s}`apply()` method, 
(e.g., for a {scaladoc}`com.raphtory.api.analysis.algorithm.Generic` algorithm):

```scala
override def apply(graph: GraphPerspective): graph.Graph = {
```
The {s}`apply()` method takes a {scaladoc}`com.raphtory.api.analysis.graphview.GraphPerspective` as input,
manipulates the state of vertices, and then returns the
graph view, either for further processing by
other algorithms or for collecting and writing out results.
A {scaladoc}`com.raphtory.api.analysis.graphview.GraphPerspective` has two key methods which are used during
graph processing, {s}`step()` and {s}`iterate()`.

#### step()
{s}`step()` takes in a function to be applied to each vertex in the graph, and permits each vertex to mutate
its state and send messages to some or all of its neighbours. Vertices are represented in Raphtory by the 
{scaladoc}`com.raphtory.api.analysis.visitor.Vertex` class, which exposes methods for storing computational state,
messaging other vertices, accessing edges (represented by the {scaladoc}`com.raphtory.api.analysis.visitor.Edge` class),
and accessing properties set when the graph was constructed. The {s}`step()` method is often used as the setup for
an algorithm, getting each vertex ready with a default state or finding the subset of vertices that are required to
send the first messages. For example:

```scala
graph
  .step({
    vertex =>
      vertex.setState("cclabel", vertex.ID)
      vertex.messageAllNeighbours(vertex.ID)
  })
```
This is a snippet from the Raphtory
[Connected Components implementation](com.raphtory.algorithms.generic.ConnectedComponents). Here, each vertex sets
its `cclabel` to its own ID and then sends this label to all of its neighbours.

#### iterate()
{s}`iterate()` does the same thing as {s}`step()`, but is run repeatedly until some criterion is met or a
maximum number of iterations is reached. Vertex state is often used to record progress during iterations and to
decide if an algorithm has converged. The convergence criterion is established by vertices *voting to halt* unanimously
using the {scaladoc}`com.raphtory.api.analysis.visitor.Vertex.voteToHalt()` method.  
All of this can be seen in the example below:

```scala
        .iterate({
            vertex =>
                val label = vertex.messageQueue[Long].min
                if (label < vertex.getState[Long]("cclabel")) {
                    vertex.setState("cclabel", label)
                    vertex messageAllNeighbours label
                }
                else
                    vertex.voteToHalt()
        }, iterations = 100, executeMessagedOnly = true)
}
```

In this instance, the vertices check the messages they have received from neighbours and set their `cclabel` to be
the minimum number received. This new label is then sent to their neighbours, allowing it to propagate to the
neighbours who sent the other labels in the next step. If no new label is found
(as their own label is already lower) a vertex may call {s}`voteToHalt()`. This means that they believe they have
found their final value and therefore the algorithm may converge early. No new messages are sent in this instance.

Due to the nature of this algorithm and those like it, {s}`iterate()` has an additional flag of
`executeMessagedOnly`, which when set means that only vertices which have received new messages will execute the function.
This can drastically increase the efficiency of algorithms on large graphs, where often only a few vertices
may need to execute at any one step (especially when looking at algorithms like random walks or paths).
For connected components, as a vertex won't change its label unless a lower label is received from a neighbour,
it can be set here.

### Global state

Many algorithms require computing some global properties, e.g., normalisation constants. This is supported in
Raphtory by using accumulators. The function defining the algorithmic step for both the {s}`step()` and {s}`iterate()`
method can optionally take a second {scaladoc}`com.raphtory.api.analysis.graphstate.GraphState` argument. Before we can
use the global state, we first have to define some accumulators using the
{scaladoc}`com.raphtory.api.analysis.graphview.GraphPerspective.setGlobalState()` method.
The {scaladoc}`com.raphtory.api.analysis.graphstate.Accumulator` class has a {s}`+=` operator to add new values to the
accumulator and a {s}`value` attribute for accessing the last computed value.

The example below illustrates how to compute the maximum degree in the graph using an accumulator:

```scala
graph
  .setGlobalState({
    graphState =>
      graphState.newMax[Int]("maxDegree")
  })
  .step({ 
    (vertex, graphState) =>
      graphState("maxDegree") += vertex.degree
  })
```



### Tabularising results

Once graph processing is complete, the algorithm proceeds to collect vertex state into tabular form.
This stage of the algorithm is implemented by overriding the {s}`tabularise()` method, i.e.

```scala
override def tabularise(graph: GraphPerspective): Table = {
```

#### select() and explode()

Typically, one calls {s}`select()` on the input graph as the first step of {s}`tabularise()` (though one can do further
graph post-processing at this stage).
`select()` maps a vertex to a {scaladoc}`com.raphtory.api.analysis.table.Row` object by keys and results for that vertex are obtained.

As an alternative, one can also use the {s}`select()`, following with an {s}`explode()` method. The {s}`explode()` method takes one key from your select function that you would like to return a list of rows rather than a single row.
This can be used to return multiple rows per vertex (e.g. for edge-level outputs
[{s}`EdgeList`](com.raphtory.algorithms.generic.EdgeList)) or as a way of filtering results by returning an empty list.
Like the {s}`step()` and {s}`iterate()` methods.
If you would like to tabularise all states, you can set the keys of your states in the {s}`globalSelect()` method, which maps the global
{scaladoc}`com.raphtory.api.analysis.graphstate.GraphState` to
a single {scaladoc}`com.raphtory.api.analysis.table.Row`. For example, we could use this to return the maximum degree
computed above:

```scala
graph
  .globalSelect("maxDegree"))
```

In the connected components instance, we are interested in extracting the ID of the vertex and the final component ID
that it saved in its state:

```scala
graph
  .step(vertex => vertex.set_state("id",vertex.ID())).step("cclabel",vertex.getState[Long]("cclabel"))
  .select("id","cclabel")
```
Once we have the data in Row form we may perform a different set of transformations.

#### filter()

The filter function can only be run after the vertex data has been converted to
{scaladoc}`com.raphtory.api.analysis.table.Table` format by the {s}`select()` call.
For example:

```scala
table
  .filter(row => row.get(1) == true)
```
This can be important if you only want to return elements that have received a certain label.
For example, if we are looking for nodes reachable from a given entity in the graph, we can store a flag in their state
and then filter on this once in Row form. Alternatively, we could have implemented this using {s}`explode()`.

#### explode()

`explode()` can be used to increase the number of rows, or prevent the output from producing any arrays.
For example, if the select function returned a list within the row, we can use the explode to turn this list
into individual items:

```scala
table
  .explode("list")
```

### Writing out results

Finally, once you are happy with the format of your data you can output it to disk.
This is done by using a {scaladoc}`com.raphtory.api.output.sink.Sink` which is given to the
query when it is executed. Several inbuilt output formats are available within Raphtory, but it is also very 
simple to implement your own if you have a specific destination in mind.

As an example from our prior code snippets the {scaladoc}`com.raphtory.sinks.FileSink` saves the results of each 
partition as separate
files in a directory. This directory is the only argument required when creating the
{scaladoc}`com.raphtory.sinks.FileSink` object and passing it to the query:

```scala
val sink = FileSink("/tmp")
graph
  .execute(ConnectedComponents())
  .writeTo(sink)
```

Similarly the {scaladoc}`com.raphtory.pulsar.sink.PulsarSink` can be used to write results directly to
[Pulsar](https://pulsar.apache.org) topics. The user gives the Topic as an argument when creating the
{scaladoc}`com.raphtory.pulsar.sink.PulsarSink` object:

```scala
val sink = PulsarSink("components")
graph
  .execute(ConnectedComponents())
  .writeTo(sink)
```

## Types of Algorithms

### Zero-step algorithms
Zero-step algorithms refer to all algorithms which require no vertex messaging step, and can be expressed just using a
{s}`select()` operation. This means that the algorithm only requires knowledge of each vertex and the edges
connected to it. This might be familiar as `local measures` in the network science literature. Some algorithms that fit
into this category are:
* Vertex degree
* Vertex/edge property extraction
* *Some* temporal motifs centred on a vertex, e.g. 3-node 2-edge temporal motifs.

In principle, one can implement such an algorithm by only overriding the {s}`tabularise()`,
leaving the default {s}`apply()` method which simply returns the input graph unchanged.

To see an example of this, here is a snippet extracting vertex degrees:

```{code-block} scala
---
lineno-start: 1
---
  override def tabularise(graph: GraphPerspective): Table = {
  graph
    .step({
      vertex =>
        val inDegree = vertex.getInNeighbours().size
        val outDegree = vertex.getOutNeighbours().size
        val totalDegree = vertex.getAllNeighbours().size
        vertex.setState("name", vertex.name())
        vertex.setState("inDegree", inDegree)
        vertex.setState("outDegree", outDegree)
        vertex.setState("totalDegree", totalDegree) 
    }).select("name", "inDegree", "outDegree", "totalDegree")
}
```
In here, the vertex's in/out/total degree is extracted in a straightforward way, with line 8 mapping these to a
row for outputting. However, this means that the algorithm cannot usefully participate in a more complicated
processing pipeline as downstream algorithms do not have access to the computed results. For something as simple as
outputting node degrees, which other algorithms already trivially have access to, this may make sense.
Generally, however, it is better practise to implement such algorithms using a single {s}`step()` during graph
processing which does not send any messages and sets the computed values as state on the vertices.
For the degree algorithm above, we would instead have (this is how the built-in
[{s}`Degree`](com.raphtory.algorithms.generic.centrality.Degree) works):

```scala
override def apply(graph: GraphPerspective): graph.Graph = {
  graph.step({
    vertex => 
      vertex.setState("inDegree", vertex.getInNeighbours().size)
      vertex.setState("outDegree", vertex.getOutNeighbours().size)
      vertex.setState("totalDegree", vertex.getAllNeighbours().size)
  })
}

override def tabularise(graph: GraphPerspective): Table = {
  graph.select("name", "inDegree", "outDegree", "totalDegree")
}
```
Implemented in this way, the algorithm can participate usefully in algorithm chaining discussed below.

### One-step algorithms
One-step algorithms are those which require precisely one messaging step, and can be expressed using
two calls to {s}`step()` to send out the messages and collate the results. This maps to measures which
require knowledge of a vertex's neighbours and the connections between them. Some examples of these include:

* Local triangle count
* Local clustering coefficient
* Average neighbour degree

For an example of this, let's look at a snippet of the
[{s}`LocalTriangleCount`](com.raphtory.algorithms.generic.motif.TriangleCount) algorithm:

```scala
override def apply(graph: GraphPerspective): graph.Graph = {
  graph
    .step({
      vertex =>
        vertex.setState("triangles",0)
        val neighbours = vertex.getAllNeighbours().toSet
        vertex.messageAllNeighbours(neighbours)
    })
    .step({
      vertex =>
        val neighbours = vertex.getAllNeighbours().toSet
        val queue = vertex.messageQueue[Set[Long]]
        var tri = 0
        queue.foreach(
          nbs =>
            tri+=nbs.intersect(neighbours).size
        )
        vertex.setState("triangles",tri/2)
    })
}
    
override def tabularise(graph: GraphPerspective): Table = {
  graph
    .step({ vertex => 
      vertex.setState("name",vertex.name())
    })
    .select("name","triangles")
}
```

The first {s}`step()` function tells each vertex to send a list of its neighbours to all neighbours.
Then the second {s}`step()` function tells each vertex to compute the intersection of the received sets with its own
set of neighbours. The sum of these intersections is twice the number of triangles for that vertex,
so this number is halved.

### Iterative algorithms

Finally, iterative algorithms cover those which require an unknown number of messaging steps,
which are executed using a mixture of {s}`step()` and {s}`iterate()`. These algorithms correspond to
measures that take into account the large scale structure of the graph, including:

* Connected components
* Single source shortest path
* PageRank, eigenvector and hub/authority centrality
* Community detection (e.g., using the label propagation algorithm)
* Watts' linear threshold model
* Diffusion models (taint tracking, SIS/SIR)

An example of this is the [{s}`ConnectedComponents`](com.raphtory.algorithms.generic.ConnectedComponents)
algorithm discussed previously.

### Tabularisers
It is possible to write algorithms that only extract vertex state and output it to table format.
These are particularly useful as the final step in an algorithm chain where one may want to also extract
intermediate results. In this case, there is no need to override the {s}`apply()` method as the default
implementation simply returns the input graph unchanged. A tabulariser can simply override the {s}`tabularise()`
method as needed.

## Chaining algorithms
It is often useful to compose/chain different algorithms together. This is implemented in Raphtory using the
`->` operator of the algorithm classes.

As an example, consider the [{s}`CBOD`](com.raphtory.algorithms.generic.CBOD) algorithm which detects outliers
based on community labels for vertices.

In some cases, community labels may already be included in the input data.
However, most of the time one would need to run a community detection algorithm (e.g.,
[{s}`LPA`](com.raphtory.algorithms.generic.community.LPA)) first to get the labels.
One way to express this in Raphtory is to use chaining, i.e,
```scala
import com.raphtory.algorithms.generic.community.LPA
import com.raphtory.algorithms.generic.CBOD
val lpa_cbod = LPA() -> CBOD(label="lpalabel")
```
This results in a new algorithm with an {s}`apply()` method that simply calls the {s}`apply()` method
of the input algorithms in sequence, i.e.,

 ```scala
 lpa_cbod.apply(graph)
 ```
is equivalent to calling
 ```scala
CBOD(label="lpalabel").apply(LPA().apply(graph))
```

The {s}`tabularise` method of a chained algorithm calls the corresponding methods of the last algorithm in the chain, i.e.,
```scala
lpa-cbod.tabularise(graph) 
```
is equivalent to
 ```scala
CBOD(label="lpalabel").tabularise(graph)
```

In this example, we first use [{s}`LPA`](com.raphtory.algorithms.generic.community.LPA) to compute community labels
and store them in a vertex state with key {s}`"lpalabel"`.
[{s}`CBOD`](com.raphtory.algorithms.generic.CBOD) then uses the labels to identify outliers and writes out the results.

In the case of [{s}`CBOD`](com.raphtory.algorithms.generic.CBOD), one can also supply the community detection
algorithm as an optional argument, i.e., {s}`CBOD(label="lpalabel", labeler=LPA())`, where by default
{s}`labeler=Identity` which does nothing. This is an example of the intended use of the
{scaladoc}`com.raphtory.api.analysis.algorithm.Identity` algorithm.

