

A `foreach` function is called which basically reads each line of the CSV file - this is essentially what the Spout does in Raphtory. For each file line, we split the line into its respective variables: source, target and timestamp. These variables are then added as nodes and edges on the graph object we created at the start - this is essentially what the Graph Builder does in Raphtory. 


## Spout 

Instead of iterating over the file with a for loop, Raphtory has a Spout function that can be used to read lines of data, this enables you to feed graphs from many different data sources into Raphtory, including real time streaming. For this example, we will make use of the `FileSpout`. This takes a file on your machine and pushes it into our Sources to parse. We automatically download the lotr.csv file from the [Raphtory data repository](https://github.com/Raphtory/Data) (hence why it run in the last tutorial), so you shouldn't have to set anything if using it. If you want to swap this file out for your own data, simply remove this download and change the `FileSpout` path to point to where your files are.

```scala 
val spout  = FileSpout("YOUR_FILE_HERE")
```

## Source

Raphtory includes different types of Source's to reduce the need of writing a graph builder. You also have the option of creating your own graph builder (this is explained in the next section). If your data is in CSV format, non-nested JSON format or NetworkX JSON format, it is likely you will not need to write your own graph builder. Simply wrap your spout in the Source object you would like to use: `CSVEdgeListSource`, `JSONEdgeListSource` or `JSONSource` (NetworkX JSON). The Source takes each ingested line of data and converts it into one or more graph updates.

```scala
val source = CSVEdgeListSource(spout)
```



First, we take the String output from the Spout. We then break up the line into the relevant components, splitting on commas (as it was a csv file ingested) via `.split(",")`. This means, for example, the line `Gandalf,Elrond,33` becomes a tuple `(Gandalf, Elrond, 33)` where each is accessible. For each of the characters seen, we give them a node ID of type `Long` via the `assignID` function - if your data already has numerical node id's you can skip this step. Finally, we send an update adding both of the vertices to the graph as well as the edge joining them, each with a timestamp of when the update occurred.

There are a few things worth pointing out here:

* We added a `name` property to each of the nodes. If we had reason to, we could have added any other property that might be appropriate. 
  We set this as an `ImmutableProperty` in this case, as character names are treated as fixed, but this could be a mutable
  property if it were required to change later. The different types of properties that can be added into a Raphtory
  graph can be found here: {scaladoc}`com.raphtory.api.input`.

* We didn't check whether either vertices exist before sending an `addVertex` update. Another component deals with this so we don't have to worry about that.


## What next?

In the next parts of this tutorial, we will take a look at [implementing your own graph algorithms](../Analysis/LOTR_six_degrees.md) 
and [running algorithms](../Analysis/queries.md) using the query interface. Alternatively, you can take a look at the 
[detailed overview of the Analysis API](../Analysis/analysis-explained.md).



