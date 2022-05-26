# Detecting bot activity on Twitter

## Overview

Raphtory implements a chain of algorithms to analyse a Twitter dataset from [SNAP](https://snap.stanford.edu/data/higgs-twitter.html), collected during and after the announcement of the discovery of a new particle with the features of the elusive Higgs boson on 4th July 2012. In our example, we have taken the retweet network data. We wanted to see whether Raphtory could detect bot retweet activity in the Higgs dataset.

<p>
 <img src="../_static/higgs-boson.jpeg" width="400px" style="padding: 15px" alt="Higgs Boson"/>
</p>

We used three algorithms in this example: PageRank, MemberRank and TemporalMemberRank.

[PageRank](https://en.wikipedia.org/wiki/PageRank) is an algorithm used to rank web pages during a Google search. PageRank algorithm ranks nodes depending on their connections to determine how important the node is. This assumes a node is more important if it receives more connections from others. Each vertex begins with an initial state. If it has any neighbours, it sends them a message which is the initial label / the number of neighbours. Each vertex, checks its messages and computes a new label based on: the total value of messages received and the damping factor. This new value is propagated to all outgoing neighbours. A vertex will stop propagating messages if its value becomes stagnant (i.e. has a change of less than 0.00001). This process is repeated for a number of iterate step times. Most algorithms should converge after approx. 20 iterations.

MemberRank is an algorithm that takes the PageRank score from the vertex neighbours and the original ranking of the vertex from the raw dataset and multiplies the two scores together to form the 'MemberRank score'. A vertex receiving a high MemberRank score means that other people with high PageRank scores have ranked them highly. If the Twitter user is influential, MemberRank will bump their score higher. If the Twitter user is non-influential (potentially a bot) the MemberRank score will be low. This should dampen the effect of bots further.

TemporalMemberRank is an algorithm that filters users with big differences in their raw dataset scores and MemberRank scores (potential bots) and checks their in-edge creations over time. Bot activity usually occurs in a very short timeframe (spamming) or at regular intervals. This algorithm will be able to give more evidence on whether a particular user is a bot or not. 

## Project Overview

This example builds a temporal graph from the [Higgs Twitter Dataset](https://snap.stanford.edu/data/higgs-twitter.html) and runs a query to tell us potential bot retweet activity.

The data is a `csv` file (comma-separated values) and is located in the `resources` folder. Each line contains user A and user B, where user B is being retweeted by user A. The last value in the line is the time of the retweet in Unix epoch time.

Also, in the examples folder you will find `TwitterGraphBuilder.scala` and `Runner.scala`.

* `TwitterGraphBuilder.scala` builds the graph
* `Runner.scala` runs the application including the analysis

There are three algorithm files to run the analysis: `PageRank.scala`, `MemberRank.scala` and `TemporalMemberRank.scala`.
* `PageRank.scala`  based on the Google Pagerank algorithm, ranks nodes (Twitter Users) depending on their connections to determine how important the node is. 
* `MemberRank.scala` takes the Page Rank score and ranks users further by taking into account the number of retweets they received.
* `TemporalMemberRank.scala` filters nodes with big differences in the number of retweets they received on Twitter and the final rank given to them by MemberRank, outputting the suspected retweet bot IDs, along with the user ID they retweeted and the time of the retweet. The first element in each line is the Pulsar Output Timestamp, this can be dropped or ignored as it is irrelevant to the bot activity results.

In `Runner.scala` we have chained the three algorithms together, but you are able to alter the algorithms you would like to run to get your desired results. e.g. `PageRank() -> MemberRank()`

We have also included python scripts in directory`src/main/python` to output the dataframes straight into Jupyter notebook.


## IntelliJ setup guide

As of 9th February 2022. This is a guide to run this within IntelliJ.

1. From https://adoptopenjdk.net/index.html download OpenJDK 11 (LTS) with the HotSpot VM.
2. Enable this as the project SDK under File > Project Structure > Project Settings > Project > Project SDK.
3. Create a new configuration as an `Application` , select Java 11 as the build, and `com.raphtory.examples.twitter.Runner` as the class, add the Environment Variables too.

## Running this example

1. This example project is up on Github: [raphtory-example-twitter](https://github.com/Raphtory/Raphtory/tree/master/examples/raphtory-example-twitter). If you have downloaded the Examples folder from the installation guide previously, then the Twitter example will already be set up. If not, please return [there](../Install/installdependencies.md) and complete this step first. 
2. In the Examples folder, open up the directory `raphtory-example-twitter` to get this example running.
3. Install all the python libraries necessary for visualising your data via the [Jupyter Notebook Tutorial](../PythonClient/tutorial_py_raphtory.md). Once you have Jupyter Notebook up and running on your local machine, you can open up the Jupyter Notebook specific for this project, with all the commands needed to output your graph. This can be found by following the path `src/main/python/TwitterJupyterNotebook.ipynb`.
4. You are now ready to run this example. The data file is already in the `resources` folder. You can either run this example via Intellij by running the class `Runner.scala` or [via sbt](../Install/installdependencies.md#running-raphtory-via-sbt).
5. Once your job has finished (this may take a while and seem like it is stuck, but since this example chains together 3 algorithms, it takes a while to get through all the data - around 4-5 minutes as shown below.), you can go onto Jupyter notebook to run your analyses and output.

## Output

When you start `Runner.scala` you should see logs in your terminal like this:
```bash
15:59:37.753 [main] INFO  com.raphtory.core.config.ComponentFactory - Creating '2' Partition Managers.
15:59:40.884 [main] INFO  com.raphtory.core.config.ComponentFactory - Creating new Query Manager.
15:59:41.257 [main] INFO  com.raphtory.core.config.ComponentFactory - Creating new Spout 'raphtory_data_raw_1844626742'.
15:59:41.257 [main] INFO  com.raphtory.core.config.ComponentFactory - Creating '2' Graph Builders.
15:59:42.397 [main] INFO  com.raphtory.core.client.RaphtoryGraph - Created Graph object with deployment ID 'raphtory_1844626742'.
15:59:42.397 [main] INFO  com.raphtory.core.client.RaphtoryGraph - Created Graph Spout topic with name 'raphtory_data_raw_1844626742'.
16:00:02.677 [main] INFO  com.raphtory.core.config.ComponentFactory - Creating new Query Progress Tracker for deployment 'raphtory_1844626742' and job 'Chain_1646064002404' at topic 'raphtory_1844626742_Chain_1646064002404'.
16:00:02.678 [main] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Starting query progress tracker.
16:00:02.775 [pulsar-external-listener-9-1] INFO  com.raphtory.core.components.querymanager.QueryManager - Range Query 'Chain_1646064002404' received, your job ID is 'Chain_1646064002404'.
16:00:10.949 [pulsar-external-listener-9-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job 'Chain_1646064002404': Perspective '1341101181' finished in 8271 ms.
16:00:10.949 [pulsar-external-listener-9-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job Chain_1646064002404: Running query, processed 1 perspectives.
16:04:42.174 [pulsar-external-listener-9-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job 'Chain_1646064002404': Perspective '1341705593' finished in 271225 ms.
16:04:42.174 [pulsar-external-listener-9-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job Chain_1646064002404: Running query, processed 2 perspectives.
16:04:42.331 [pulsar-external-listener-9-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job Chain_1646064002404: Query completed with 2 perspectives and finished in 279653 ms.
```
This indicates that the job has finished and your data should be ready to be analysed in Jupyter Notebook.
```bash
Job Chain_1646064002404: Query completed with 2 perspectives and finished in 279653 ms.
```

Output for Vertex 4 (potential bot): 

```python
sys_time       src      dst	       time
1341705593	4	117251	2012-07-04 05:20:05
1341705593	4	9631	2012-07-04 05:26:56
1341705593	4	270457	2012-07-04 05:28:27
1341705593	4	21541	2012-07-04 05:28:59
1341705593	4	357839	2012-07-04 05:29:13
1341705593	4	497	2012-07-04 05:29:26
1341705593	4	6765	2012-07-04 05:30:01
1341705593	4	180606	2012-07-04 05:30:04
1341705593	4	73922	2012-07-04 05:30:35
1341705593	4	92987	2012-07-04 05:32:06
1341705593	4	244918	2012-07-04 05:32:50
1341705593	4	85583	2012-07-04 05:34:41
1341705593	4	117251	2012-07-04 05:40:06
1341705593	4	114744	2012-07-04 05:41:36
1341705593	4	281202	2012-07-04 05:42:05
1341705593	4	52232	2012-07-04 05:50:21
1341705593	4	85084	2012-07-04 05:50:40
1341705593	4	5552	2012-07-04 05:51:10
1341705593	4	6765	2012-07-04 05:51:43
1341705593	4	497	2012-07-04 05:52:11
1341705593	4	156739	2012-07-04 05:52:12
1341705593	4	18205	2012-07-04 05:52:27
1341705593	4	39065	2012-07-04 05:52:55
1341705593	4	89050	2012-07-04 05:53:06
1341705593	4	2564	2012-07-04 05:54:02
1341705593	4	315004	2012-07-04 05:54:29
1341705593	4	63669	2012-07-04 05:54:34
1341705593	4	127424	2012-07-04 05:56:57
1341705593	4	9662	2012-07-04 06:00:08
1341705593	4	62056	2012-07-04 06:02:06
1341705593	4	424319	2012-07-04 06:03:03
1341705593	4	103978	2012-07-04 06:03:54
1341705593	4	103978	2012-07-04 06:03:58
1341705593	4	34620	2012-07-04 06:04:47
1341705593	4	424047	2012-07-04 06:09:55
1341705593	4	1162	2012-07-04 06:13:05
1341705593	4	264150	2012-07-04 06:15:09
1341705593	4	134193	2012-07-04 06:15:33
1341705593	4	55836	2012-07-04 06:16:36
```
We singled out vertex 4 as an example user that may show bot activity since after the final step of the chaining: `TemporalMemberRank.scala`, the score that vertex 4 originally had in the raw dataset significantly decreased after running PageRank and MemberRank. The output shows retweet activity happening within minutes and seconds of each other, this could be an indication of suspicious activity and further analysis on this data can be performed.

Feel free to play around with the example code and Jupyter notebook analysis. For example, in `TemporalMemberRank.scala`, you can change line 32:
`val difference: Boolean = (positiveRaw > (positiveNew * 4))`
`4` is the multiplication factor to see if the raw scores and new scores are significantly different. This can be increased or decreased depending on your desired analysis. 
