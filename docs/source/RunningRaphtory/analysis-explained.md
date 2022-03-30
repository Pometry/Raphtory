# An introduction to algorithms in Raphtory

Raphtory's analysis engine works by *vertex centric computation*. Each vertex has access to local information about the graph (just its immediate vicinity). To complement this, vertices can communicate with their neighbours (other vertices that are directly connected to it). Many graph algorithms which operate on per-vertex level can be expressed in this way. The benefit of this is that graphs can be distributed over multiple cores/machines, each containing a proportion of the vertices, and these vertex computations can be executed in a parallel manner.

Each vertex knows:

* It's own update history, property set and property history e.g. values and update times;
* The history/properties of the edges attached to it - both incoming and outgoing as Raphtory has a directed graph model - think twitter following, where the other person has to follow you back, instead of facebook friends.
* Its own algorithmic state - this is a map of temporary values that can be set and modified during the computation steps.

<p align="center">
  <img src="../_static/vertex_time2.png" style="width: 20vw;" alt="vertex time view"/>
<figcaption>The information available to node V1 which includes the time it was created, the IDs of its neighbours and the times at which its edges are established.</figcaption>
</p>


The next sections will explore how algorithms can be written using these vertex communications.

## step(), iterate(), select()
Algorithms in Raphtory take a `RaphtoryGraph` as input, and return a `Row` for each vertex of that graph containing the result of the executed algorithm. To this end, there are three functions which are executed sequentially on the graph to get to these vertex results. 

### step()
`step()` takes in a function to be applied to each vertex in the graph, and permits each vertex to mutate its state and send messages to some or all of its neighbours. This is often used as the setup for an algorithm, getting each vertex ready with a default state or finding the subset of nodes which are required to send the first messages. For example:

```scala
graph
  .step({
    vertex =>
      vertex.setState("cclabel", vertex.ID)
      vertex.messageAllNeighbours(vertex.ID)
  })
```
This is a snippet from the Raphtory connected components implementation. Here, each node sets its `cclabel` to its own ID and then sends this ID to all of its neighbours.

### iterate()
`iterate()` does the same thing as `step`, but is run repeatedly until some criterion is met or a maximum number of iterations is reached. Vertex state is often used to record progress during iterations and to decide if an algorithm has converged.  The convergence criterion is established by vertices *voting to halt* unanimously.  All of this can be seen in the example below:


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
```


In this instance, the vertices check the messages they have received from neighbours and set their `cclabel` to be the minimum number received. This new label is then sent to their neighbours, allowing it to propagate and the neighbours who sent other labels to set it themselves in the next step. If no new label is found (as their own label is already lower) a vertex may call `voteToHalt`. This means that they believe they have found their final value and therefore the algorithm may converge early. No new messages are sent in this instance.

Due to the nature of this algorithm and those like it, `iterate` has an additional flag of `executeMessagedOnly`, which when set means that only vertices which have received new messages will execute the function. This can drastically increase the effeciency of algorithms on large graphs, where often only a few vertices recieve new updates and may need to execute at any one step (especially when looking at algorithms like random walks or paths). For connected components, as a vertex won't change its label unless a lower label is received from a neighbour, it is set to `true` here.  

### select()

`select()` maps a vertex to a `Row` object containing the results for that vertex. For example:

```scala
.select(vertex => Row(vertex.ID(),vertex.getState[Long]("cclabel")))
```
In the connected components instance, we are interested in extracting the ID of the vertex and the final component ID that it saved in its state.

## filter(), explode() and writeTo()

Once we have the data in Row form we may perform a different set of transformations:

### filter()

`filter()` is used in order to reduce the amount of data being saved. 
The filter function can only be run on the output of the select. For example:

```scala
.filter(r=> r.get(1) == true)
```
This can be important if you only want to return elements that have received a certain label. Such as if we are looking for nodes reachable from a given entity in the graph, we can store a flag in their state and then filter on this once in Row form.
### explode()

`explode()` can be used to increase the number of rows, or prevent the output from producing any arrays. For example, if the select function returned a list within the row, we can use the explode to turn this list into individual items.  

```scala
.explode( row => row.get(2).asInstanceOf[List[(Long, String)]].map(
          expl => Row( row(0), expl._1, expl._2)
      )
)
```
### writeTo()
Finally, once you are happy with the format of your data you can output this via `.writeTo(path)` where the path is anywhere on your local machine. Inside this directory will appear a folder containing the ouput from each of the partitions within your running Raphtory instance. 

## Types of Algorithm

### Zero-step algorithms
Zero-step algorithms refer to all algorithms which require no vertex messaging step, and can be expressed just using a `select()` operation. This means that the algorithm only requires knowledge of each vertex and the edges connected to it, and might be known as local measures in the network science literature. Some algorithms that fit into this category are:
* Vertex degree
* Vertex/edge property extraction
* *Some* temporal motifs centred on a vertex, e.g. 3-node 2-edge temporal motifs.

To see an example of this, here is a snippet from the `Degree` algorithm.

```scala
  override def algorithm(graph: GraphPerspective): Unit = {
    graph.select({
      vertex =>
      val inDegree = vertex.getInNeighbours().size
      val outDegree = vertex.getOutNeighbours().size
      val totalDegree = vertex.getAllNeighbours().size
    Row(vertex.getPropertyOrElse("name", vertex.ID()), inDegree, outDegree, totalDegree)
    })
```
In here, the vertex's in/out/total degree is extracted in a straightforward way, with line 7 mapping these to a row for outputting.

### One-step algorithms
One-step algorithms are those which require precisely one messaging step, and can be expressed using `step()`, followed by `select()`. This maps to measures which require knowledge of a vertex's neighbours and the connections between them. Some examples of these include:

* Local triangle count
* Local clustering coefficient
* Average neighbour degree

For an example of this, let's look at a snippet of the `TriangleCount` algorithm.

```scala
  override def algorithm(graph: GraphPerspective): Unit = {
    graph.step({
      vertex =>
        vertex.setState("triangles",0)
        val neighbours = vertex.getAllNeighbours().toSet
        neighbours.foreach({
          nb =>
            vertex.messageNeighbour(nb, neighbours)
        })
    })
      .select({
        vertex =>
          val neighbours = vertex.getAllNeighbours().toSet
          val queue = vertex.messageQueue[Set[Long]]
          var tri = 0
          queue.foreach(
            nbs =>
              tri+=nbs.intersect(neighbours).size
          )
          vertex.setState("triangles",tri/2)
          Row(vertex.getPropertyOrElse("name", vertex.ID()), vertex.getState[Int]("triangles"))
      }))
```

The `step()` function tells each vertex to send a list of its neighbours to all neighbours. Then the `select()` function tells each vertex to compute the intersection of the received sets with its own set of neighbours. The sum of these intersections is twice the number of triangles for that vertex, so this number is halved.

### Iterative algorithms

Finally, iterative algorithms cover those which require an unknown number of messaging steps, which are executed using a mixture of `step()` and `iterate()`, finally followed by `select()`. These algorithms correspond to measures that take into account the large scale structure of the graph, including:

* Connected components
* Single source shortest path
* PageRank, eigenvector and hub/authority centrality
* Community detection (using the label propagation algorithm)
* Watts' linear threshold model
* Diffusion models (taint tracking, SIS/SIR)
 
 An example of this is the `ConnectedComponents` algorithm discussed previously, chunk by chunk:
 
 ``` scala
  override def algorithm(graph: GraphPerspective): Unit = {
    graph
      .step({
        vertex =>
          vertex.setState("cclabel", vertex.ID)
          vertex.messageAllNeighbours(vertex.ID)
      })
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
      .select(vertex => Row(vertex.ID(),vertex.getState[Long]("cclabel")))
 ```

First, in `step()` each vertex is initialised with a connected components ID, set to be its own vertex ID; this ID is propagated to its neighbours. Then, in `iterate()`, at each step, the vertex takes on an ID that is either its current ID or the smallest ID that was propagated to it, whichever is smaller. When an iteration step is reached where none of the vertices change their label, then the computation is finished.

## What now? 
To summarise, Raphtory's analytical engine provides a way of expressing a large variety of graph algorithms, implemented by vertex computations. Unlike more general graph analytics libraries, it has functionalities for expressing temporal queries in a simple way.

Next, you can take a look at following our next guide which implements a Degree of Separation algorithm.