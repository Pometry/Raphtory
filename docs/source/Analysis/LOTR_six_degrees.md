

# Your first algorithm - Six Degrees of Gandalf

In the [previous entry](../Ingestion/sprouter.md), you learnt about Spouts and how to write your own Graph Builder to ingest data. Here we're going to go over how to write a new algorithm for the Lord of the Rings dataset which will calculate the size of the _six degrees of separation_ network for a character; in this case,`Gandalf`. 
Six degrees of separation is "the idea that all people on average are six, or fewer, social connections away from each other." ([wiki here in case you want to know more](https://en.wikipedia.org/wiki/Six_degrees_of_separation)).

## Algorithm

The class we are creating extends {scaladoc}`com.raphtory.api.analysis.algorithm.Generic`:

```scala
import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.Table

class DegreesSeparation(name: String = "Gandalf") extends Generic {
```

```{note}
For those not familiar with Scala, the name argument given to the class has a default value of `Gandalf`. 
This means if the user does not explicitly give a name when they create an instance of the algorithm, this value will 
be used. 
```

To actually implement the algorithm we need to override the `apply` method within which we gain access to a 
{scaladoc}`com.raphtory.api.analysis.graphview.GraphPerspective`. This has all of the functional building blocks 
which allow us to specify what a vertex should be doing at each stage from initialisation through to output. 
All of these functions are explored in-depth in the [next section of the tutorial](analysis-explained.md).

```scala
override def apply(graph: GraphPerspective): graph.Graph = {
```

```{note}
The return type {s}`graph.Graph` for a {scaladoc}`com.raphtory.api.analysis.algorithm.Generic` algorithm
means that we return the same type of view as the input {s}`graph`. For other types of algorithms that change the 
view see the {scaladoc}`com.raphtory.api.analysis.algorithm` package. 
```


### Step
In the `DegreeSeparation` algorithm we first create a property to store the state of _separation_ and initialize it in {s}`step()`. 
Here we are finding the node which is the starting point, in this case we are looking for Gandalf. 
Once found we set his state to `0` and then message all of his neighbours. If a node is not Gandalf, 
their state is initialised to `-1`, which will be used later to work out nodes which are unreachable from Gandalf. 

```scala
graph
  .step({
    vertex =>
      if (vertex.name() == name) 
      {
        vertex.messageAllNeighbours(0)
        vertex.setState("SEPARATION", 0)
      } else 
      {
        vertex.setState("SEPARATION", -1)
      }
  })

```

### The bulk
Next we implemented the bulk of the algorithm inside of the {s}`iterate()` function. In here, message queues for all 
nodes are checked and their separation status is updated if it has not been set previously. Nodes that are a single hop 
from Gandalf will have received a message of `0`, this is thus incremented to `1` and becomes their separation state. 
These nodes then message all their neighbours with their new state, `1`, and the cycle repeats. Nodes only update 
their state if this has not been changed before (as we want the lowest number of hops). 

This function is only executed on vertices that have been sent a message ({s}`executeMessagedOnly = true`) and runs up to 
6 times ({s}`iterations = 6`).

```scala
    .iterate(
      {
        vertex =>
          val sep_state = vertex.messageQueue[Int].max + 1
          val current_sep = vertex.getStateOrElse[Int]("SEPARATION", -1)
          if (current_sep == -1 & sep_state > current_sep) {
            vertex.setState("SEPARATION", sep_state)
            vertex.messageAllNeighbours(sep_state)
          }
      }, iterations = 6, executeMessagedOnly = true)
```

### The Return of The King
Now that the algorithm has converged, we need to get the results back and process them if necessary. 
For this we override the `tabularise()` function where we convert from a 
{scaladoc}`com.raphtory.api.analysis.graphview.GraphPerspective` into a 
{scaladoc}`com.raphtory.api.analysis.table.Table` -- or more practically from `Vertices` to `Rows` which may 
be output back to csv.

The following goes through all vertices and extracts the node name and the final label value: 
```scala
override def tabularise(graph: GraphPerspective): Table = {
  graph.select(vertex => Row(vertex.getPropertyOrElse("name", "unknown"), vertex.getStateOrElse[Int]("SEPARATION", -1)))
}
```

We could add a filter {s}`.filter(row => row.getInt(1) > -1)` to ignore any nodes that weren't reached by the messaging. 
This would exclude nodes that are not at all connected to Gandalf or whose shortest path to Gandalf is longer than 6 hops.

Finally, at the end of the file we create an `Object` for the algorithm, so we can pass this algorithm to the 
Raphtory executor without having to call the `new` keyword. 

```scala
object DegreesSeparation{
  def apply(name: String = "Gandalf") = new DegreesSeparation(name)
}
```

## What now?

In the [next part](queries.md) of the tutorial, we take a look at actually running your algorithms using the query API. 
Alternatively, you can learn more about the algorithm API [here](analysis-explained.md).

