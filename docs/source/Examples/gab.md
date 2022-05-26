# Gab Social Network Analysis

## Overview

In recent work, Raphtory has been used to study the evolution of a fairly new social network Gab used largely by the alt-right. For a deep delve into this topic, take a look at our paper ["Moving with the times: Investigating the alt right network Gab using temporal interaction graphs"](https://www.researchgate.net/publication/344294385_Moving_with_the_Times_Investigating_the_Alt-Right_Network_Gab_with_Temporal_Interaction_Graphs)

## Project Overview

This [example](https://github.com/Raphtory/Raphtory/tree/master/examples/raphtory-example-gab) demonstrates an analysis used to study the evolution of the Gab Social network. The data is a `csv` file (comma-separated values) located in the `data` folder. Each row of data consists of `Time of post`, `post ID`, `user ID`, `topic ID`, `parent post ID` and `parent user ID`.
`post ID`: Identifier for the post that was made
`user ID`: User that authored the post
`topic ID`: Identifier for topic of post
`parent post ID`: Identifier for post that it is replying to
`parent user ID`: User that authored the post being replied to

If the post is an original post (not a reply), then both the parent post ID and parent user ID are set to -1 and not included in the graph. 

`GabUserGraphBuilder.scala` focuses on user to users. The source node is created from user ID and the target node is created from parent user ID, looking at who is reply to who in the network.
`GabPostGraphBuilder.scala` focuses on post to post. The source node is created from post ID and the target node is created from parent post ID, looking at the links between posts and replies.
`GabRawGraphBuilder.scala` and the files in the directory `rawgraphmodel` are used to read the raw JSON data obtained by crawling the Gab Rest API.

In `Runner.scala`, we have used `GabUserGraphBuilder.scala` with algorithms: `EdgeList` and `ConnectedComponents`. This can obviously be swapped out for the graph builder and algorithms of your choice. 

We have also included python scripts in directorysrc/main/python to output the dataframes straight into Jupyter notebook. The example python scripts uses PyMotif and outputs all the connections in the Gab network onto a graph.

## IntelliJ setup guide

As of February 2022, this is a guide to run this within IntelliJ.

1. From https://adoptopenjdk.net/index.html download OpenJDK 11 (LTS) with the HotSpot VM.
2. Enable this as the project SDK under File > Project Structure > Project Settings > Project > Project SDK.
3. Create a new configuration as an `Application` , select Java 11 as the build, and `com.raphtory.examples.gab.Runner` as the class, add the Environment Variables too.

## Running this example

1. This example project is up on Github: [raphtory-example-gab](https://github.com/Raphtory/Raphtory/tree/master/examples/raphtory-example-gab). If you have downloaded the Examples folder from the installation guide previously, then the Gab example will already be set up. If not, please return [there](../Install/installdependencies.md) and complete this step first. 
2. In the Examples folder, open up the directory `raphtory-example-gab` to get this example running.
3. Install all the python libraries necessary for visualising your data via the [Jupyter Notebook Tutorial](../PythonClient/tutorial_py_raphtory.md). Once you have Jupyter Notebook up and running on your local machine, you can open up the Jupyter Notebook specific for this project, with all the commands needed to output your graph. This can be found by following the path `src/main/python/GabJupyterNotebook.ipynb`.
4. You are now ready to run this example. You can either run this example via Intellij by running the class `Runner.scala` or [via sbt](../Install/installdependencies.md#running-raphtory-via-sbt).
5. Once your job has finished, you are ready to go onto Jupyter Notebook and run your analyses/output.

## Output

ConnectedComponents Sample Data
```python
pulsar_timestamp    window          vertex_id   component_size
1470884317000	    31536000000	    1	        1
1470884317000	    31536000000	    81	        1
1470884317000	    31536000000	    241	        1
1470884317000	    31536000000	    231	        1
1470884317000	    31536000000	    31	        1
...	...	...	...	...
1476068317000	    3600000	    28667	17950
1476068317000	    3600000	    2557	51
1476068317000	    3600000	    7165	51
1476068317000	    3600000	    14717	51
1476068317000	    3600000	    8703	51
```

Take a look at our paper ["Moving with the times: Investigating the alt right network Gab using temporal interaction graphs"](https://www.researchgate.net/publication/344294385_Moving_with_the_Times_Investigating_the_Alt-Right_Network_Gab_with_Temporal_Interaction_Graphs) for my example graphs and examples of the Gab social network.