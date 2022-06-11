# Running Java/Scala algorithms from Python

## Pre-requisites

Please ensure you have followed the Python Client Setup guide and LOTR demo running before continuing.

In the previous examples we used the Python Raphtory client to read data from algorithms
that have been run via the scala interface. However, we can also run these scala algorithms 
directly from Python using [Py4j](https://www.py4j.org/). 

Note that Py4j only supports Java. Since Raphtory is written in Scala, the Py4j interface will 
make use of the java bindings. Therefore, we must ensure that our parameters and functions called 
match exactly with the java variant, otherwise Py4j will not know which method to run. 

In a python interactive terminal you can run the py4j help function on the java objects to identify 
their parameters. 

## Tutorial

### Setup - Create Client

First we create a client and pass in the deployment id.

The code will try to connect to a local raphtory instance with the matching deployment id. 

Note: Prior to this you should have the LOTR example running.

Replace this value below `raphtory_deployment_id=<YOUR_DEPLOYMENT__ID>`
e.g. `raphtoryclient.client(raphtory_deployment_id="raphtory_12783638")`

```python
import raphtoryclient
raphtory = raphtoryclient.client(raphtory_deployment_id="<YOUR_DEPLOYMENT__ID>")
```

    Connecting to RaphtoryClient...
    Connected.
    Setting up Java gateway...
    Java gateway connected.
    Creating Raphtory java object...
    Created Raphtory java object.
    

### Setting up an algorithm

To run an algorithm we must first import the class that this algorithm belongs to. 

For example to run the ConnectedComponents algorithm that exports to a CSV file (using the `FileSink`)
we must do the following

```python 
raphtory.java_import("com.raphtory.algorithms.generic.ConnectedComponents")
connectedComponentsAlgorithm = raphtory.java().ConnectedComponents

raphtory.java_import("com.raphtory.sinks.FileSink")
raphtory.java_import("com.raphtory.formats.CsvFormat")
fileSink = raphtory.java().FileSink
CsvFormat = raphtory.java().CsvFormat
output = raphtory.java().FileSink.apply("/tmp/pythonCC", CsvFormat())

```

### Running an algorithm 

To run the algorithm we can invoke one the temporal queries.
For example, lets say we wanted to run the connected components at a specific
point in time and include the entire graph up to and including this point. 
We can do the following. This will run the query on the Raptory Java/Scala instance,
you can check the progress of the query there. When complete it will save the output 
to `/tmp/pythonCC`

```python
queryHandler = raphtory.graph\
    .at(32674)\
    .past()\
    .execute(connectedComponentsAlgorithm())\
    .writeTo(output)
```

#### Warning: Give exact arguments when calling algorithms 

In order for a scala algorithm to run via python, you must call the algorithm
with its exact arguments, otherwise java will not be able to find it.

For example, the Connected Components algorithm has an object, with an empty `apply` function.
This is used to call the algorithm from python. This is a side effect of using py4j.