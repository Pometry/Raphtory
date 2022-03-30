# Six Degrees of Gandalf

In the previous entry, you learnt how to write your own spout and builder to ingest the data and how our analysis API works. Here, we're going to go over how to write an new algorithm for a Lord of the Rings dataset that will get the size of the _six degrees of separation_ network for a character; in this case,`Gandalf`. Six degrees of separation is "the idea that all people on average are six, or fewer, social connections away from each other." ([wiki here in case you want to know more](https://en.wikipedia.org/wiki/Six_degrees_of_separation)).

The example data can be found in the path `src/main/scala/examples/lotr/data` in the example directory cloned in the [installation guide](https://raphtory.github.io/documentation/install). The class we are creating extends `GraphAlgorithm`

```scala
class DegreesSeperation(name: String = "Gandalf", fileOutput:String="/tmp/DegreesSeperation") 
  extends GraphAlgorithm 
```
**Note:** For those not familar with scala, the name argument given to the class has a default value of `Gandalf`. This means if the user does not explicitly give a name when they create an instance of the algorithm, this value will be used. A similar premise is used for the output path.

### Step
First, we need to create a property to store the state of _separation_ and initialize it in `step`. Here we are finding the node which is the starting point, in this case we are looking for Gandalf. Once found we set his state to `0` and then message all of its neighbours. If a node is not Gandalf, their state is initialised to -1, which will be used later to work out nodes which are unreachable from Gandalf. 

```scala
    graph
      .step({
        vertex =>
          if (vertex.getPropertyOrElse("name", "") == name) 
          {
            vertex.messageAllNeighbours(0)
            vertex.setState(SEPARATION, 0)
          } else 
          {
            vertex.setState(SEPARATION, -1)
          }
      })
```
### The bulk
As mentioned before, the `iterate` module implements the bulk of the algorithm. In here, message queues for all nodes are checked, their separation status is updated if it has not been set previously. Nodes that are a single hop from Gandalf will have received a message of `0`, this is thus incremented to `1` and this becomes their separation state. These nodes then message all their neighbours with their new state, `1`, and the cycle repeats. Nodes only update their state if they have not been changed before. 

This process only runs through vertices that have been sent a message (`executeMessagedOnly = true`) and runs up to 6 times `iterations = 6`.

```scala
  .iterate(
    {
    vertex =>
      val sep_state = vertex.messageQueue[Int].max + 1
      val current_sep = vertex.getStateOrElse[Int](SEPARATION, -1)
      if (current_sep == -1) {
        vertex.setState(SEPARATION, sep_state)
        vertex.messageAllNeighbours(sep_state)
      }
    }, iterations = 6, executeMessagedOnly = true)
```

### The Return of The King
Now that the algorithm has converged, we need to get the results back and process them if necessary. The following goes through all nodes and extracts the final label value aquired. 

```scala
  .select(vertex => Row(vertex.getPropertyOrElse("name", "unknown"), vertex.getStateOrElse[Int](SEPARATION, -1)))
```

We could add a filter `.filter(row=> row.getInt(1) > -1)` to ignore any nodes that have not had their state change. This would exclude nodes that are not at all connected to Gandalf. 


## Running Analysis
To run your implemented algorithm or any of the algorithms included in the most recent Raphtory Release ([See here](https://github.com/Raphtory/Raphtory/tree/master/mainproject/src/main/scala/com/raphtory/algorithms)), you must submit them to the graph. You can either request a `pointQuery` to look at a single point in time or a `rangeQuery` over a subset of the history of the graph.

Some example point queries are found within the `Runner` App we created in the previous tutorial:


### Point Query

Point queries take the algorithm you wish to run and a timestamp specifying the time of interest. You may additionally specify a List of windows with which Raphtory will generate a unique perspective for each. Within these perspectives of the graph, entities which have been updated before the chosen timestamp, but as recently as the window size specified are included.  An example of these running a `DegreesSeperation` function on line 10000 can be seen below:

````scala
  rg.pointQuery(DegreesSeperation(name = "Gandalf"),timestamp=32670)
  rg.pointQuery(DegreesSeperation(name = "Gandalf"),timestamp=25000,windows=List(100,1000,10000)
````

Running this algorithm, returns the following data:

```json
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

### Range Query

Range queries are similar to this, but take a start time, end time (inclusive) and increment with which it moves between these two points. An example of range queries over the full history (all pages) running `ConnectedComponents` can be seen below:

````scala
  rg.rangeQuery(ConnectedComponents(), start = 1,end = 32674,increment = 100)
  rg.rangeQuery(ConnectedComponents(), start = 1,end = 32674,increment = 100,windows=List(100))
    
````
### What Next?
Now that we have gone through how to build a graph and how to perform analytics on it the final step is how to properly deploy it in a distributed environment! 

<sub>If you have any questions, come join the Raphtory Slack and get involved with the discussion. </sub>

