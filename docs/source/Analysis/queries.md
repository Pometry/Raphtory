# Running Queries

To run your implemented algorithm or any of the algorithms included in the most recent Raphtory Release
([See here](com.raphtory.algorithms)), you must submit them to the graph. We use the [Lord of the Rings
graph](../Ingestion/sprouter.md) and the [degrees of separation algorithm](LOTR_six_degrees.md) examples to illustrate the query API.
You can either request a `pointQuery` to look at a single point in time or a `rangeQuery` over a subset of the history of the graph.
For more details, also see the [{s}`RaphtoryClient`](com.raphtory.client.RaphtoryClient) documentation.


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
your own graph algorithms or take a look at the build-in [generic](com.raphtory.algorithms.generic) and 
[temporal](com.raphtory.algorithms.temporal) algorithms.