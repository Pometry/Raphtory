# Lord of the Rings Character Interactions

## Overview
This example is a dataset that tells us when two characters have some type of interaction in the Lord of the Rings trilogy books. It's a great dataset to test different algorithms or even your own written algorithms.

## Project Overview

* FileOutputRunner builds a graph and uses the algorithm `DegreesSeparation.scala` which is explained [here](../Analysis/LOTR_six_degrees.md). 

The data is a `csv` file (comma-separated values) and is pulled from our Github data repository. 
Each line contains two characters that appeared in the same sentence in the 
book, along with which sentence they appeared as indicated by a number. 
In the example, the first line of the file is `Gandalf,Elrond,33` which tells
us that Gandalf and Elrond appears together in sentence 33.

Refer back to [Building a graph from your data](../Ingestion/sprouter.md) for a more in-depth explanation on how these parts work to build your graph.

## IntelliJ setup guide

As of February 2022, this is a guide to run this within IntelliJ.

1. From https://adoptopenjdk.net/index.html download OpenJDK 11 (LTS) with the HotSpot VM.
2. Enable this as the project SDK under File > Project Structure > Project Settings > Project > Project SDK.
3. Create a new configuration as an `Application` , select Java 11 as the build, and `com.raphtory.examples.lotr.TutorialRunner` as the class, add the Environment Variables too.

## Running this example

1. This example project is up on Github: [raphtory-example-lotr](https://github.com/Raphtory/Raphtory/tree/master/examples/raphtory-example-lotr). If you have downloaded the Examples folder from the installation guide previously, then the Lotr example will already be set up. If not, please return [there](../Install/installdependencies.md) and complete this step first. 
2. In the Examples folder, open up the directory `raphtory-example-lotr` to get this example running.
3. Install all the python libraries necessary for visualising your data via the [Jupyter Notebook Tutorial](../PythonClient/tutorial_py_raphtory.md). Once you have Jupyter Notebook up and running on your local machine, you can open up the Jupyter Notebook specific for this project, with all the commands needed to output your graph. This can be found by following the path `python/LOTR_demo.ipynb`.
4. You are now ready to run this example. You can either run this example via Intellij by running the class `com.raphtory.examples.lotr.TutorialRunner` or [via sbt](../Install/installdependencies.md#running-raphtory-via-sbt).
5. Once your job has finished, you are ready to go onto Jupyter Notebook and run your analyses/output.

## Output

Output in terminal when running `TutorialRunner.scala`
//TODO insert terminal output

Terminal output to show job has finished and is ready for visualising the data in Jupyter Notebook
//TODO insert terminal output

The results will be saved to 

EdgeList sample dataframe
```python

```

PageRank sample dataframe
```python

```

### EdgeList
```python
30000,Hirgon,Denethor
30000,Hirgon,Gandalf
30000,Horn,Harding
30000,Galadriel,Elrond
30000,Galadriel,Faramir
...
```

This says that at time 30,000 Hirgon was connected to Denethor. 
Hirgon was connected to Gandalf etc. 

### PageRank
```python
20000,10000,Balin,0.15000000000000002
20000,10000,Orophin,0.15000000000000002
20000,10000,Arwen,0.15000000000000002
20000,10000,Isildur,0.15000000000000002
20000,10000,Samwise,0.15000000000000002
...
```

This data tells us that at time 20,000, window 10,000 that Balin had a rank of 0.15. 
etc. 

Graph visualisation output of Lord of the Ring characters and their Pagerank
<p>
 <img src="../_static/lotr_pagerank.png" width="700px" style="padding: 15px" alt="Graph Output of LOTR Pagerank"/>
</p>