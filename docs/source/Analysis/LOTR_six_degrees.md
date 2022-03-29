

# Six Degrees of Gandalf

In the [previous entry](../Ingestion/sprouter.md), you learnt how to write your own spout and builder to ingest the data 
and how our analysis API works. Here, we're going to go over how to write a new algorithm for a Lord of the Rings 
dataset that will get the size of the _six degrees of separation_ network for a character; in this case,`Gandalf`. 
Six degrees of separation is "the idea that all people on average are six, or fewer, social connections away from 
each other." ([wiki here in case you want to know more](https://en.wikipedia.org/wiki/Six_degrees_of_separation)).

This example can be found in this [repository](https://github.com/Raphtory/Examples/raphtory-example-lotr/src/main/resources). 

## Algorithm

The class we are creating extends [{s}`GraphAlgorithm`](com.raphtory.algorithms.api.GraphAlgorithm) with the type of 
the data the algorithm is to return, in this case:

```scala
import com.raphtory.algorithms.api.{GraphAlgorithm, GraphPerspective, Row, Table}

class DegreesSeparation(name: String = "Gandalf") extends GraphAlgorithm {
```

**Note:** For those not familiar with Scala, the name argument given to the class has a default value of `Gandalf`. 
This means if the user does not explicitly give a name when they create an instance of the algorithm, this value will 
be used. A similar premise is used for the output path.

First we need to override the `apply` method to implement the core algorithm:
```scala
    override def apply(graph: GraphPerspective): GraphPerspective = {
```

### Step
First, we need to create a property to store the state of _separation_ and initialize it in {s}`step()`. 
Here we are finding the node which is the starting point, in this case we are looking for Gandalf. 
Once found we set his state to `0` and then message all of its neighbours. If a node is not Gandalf, 
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
As mentioned before, the {s}`iterate()` module implements the bulk of the algorithm. In here, message queues for all 
nodes are checked, their separation status is updated if it has not been set previously. Nodes that are a single hop 
from Gandalf will have received a message of `0`, this is thus incremented to `1` and this becomes their separation state. 
These nodes then message all their neighbours with their new state, `1`, and the cycle repeats. Nodes only update 
their state if they have not been changed before. 

This process only runs through vertices that have been sent a message ({s}`executeMessagedOnly = true`) and runs up to 
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
The following goes through all nodes and extracts the final label value aquired. 
```scala
override def tabularise(graph: GraphPerspective): Table = {
  graph.select(vertex => Row(vertex.getPropertyOrElse("name", "unknown"), vertex.getStateOrElse[Int]("SEPARATION", -1)))
}
```

We could add a filter `.filter(row=> row.getInt(1) > -1)` to ignore any nodes that have not had their state change. 
This would exclude nodes that are not at all connected to Gandalf or whose shortest path to Gandalf is longer than 6 hops.

Finally, at the end of the file we create this as an object, so we can pass this algorithm to the Raphtory executor. 

```scala
object DegreesSeparation{
  def apply(name: String = "Gandalf") = new DegreesSeparation(name)
}
```

## What now?

In the [next part](queries.md) of the tutorial, we take a look at actually running your algorithms using the query API. 
Alternatively, you can learn more about the algorithm API [here](analysis-explained.md).

