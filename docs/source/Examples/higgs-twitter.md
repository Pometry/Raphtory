# Detecting bot activity on Twitter 🤖

## Overview

This is an example to show how Raphtory can run a chain of algorithms to analyse a Twitter dataset from [SNAP](https://snap.stanford.edu/data/higgs-twitter.html), 
collected during and after the announcement of the discovery of a new particle with the features of the elusive Higgs boson on 4th July 2012. 

A Twitter bot typically uses the Twitter API to interact and engage with Twitter users. They can autonomously tweet, retweet, like, follow, unfollow, or DM other accounts.

In our example, we took the retweet network dataset from SNAP to see whether Raphtory could detect bot activity during and after the announcement of the elusive Higgs boson.

<p>
 <img src="../_static/higgstwittergraph.png" width="1000px" style="padding: 15px" alt="Retweet Network Graph"/>
</p>

## Pre-requisites

Follow our Installation guide: [Scala](../Install/installdependencies.md) or [Python](../PythonClient/setup.md).

## Data

The data is a `csv` file (comma-separated values) and can be found in [Raphtory's data repo](https://github.com/Raphtory/Data/blob/main/higgs-retweet-activity.csv). 
Each line contains user A and user B, where user B is being retweeted by user A. The last value in the line is the time of the retweet in Unix epoch time.

# Running and writing custom algorithms in Raphtory

We used the PageRank algorithm, which is already readily available in Raphtory. We wrote two more algorithms which we've named: MemberRank and TemporalMemberRank, to further our investigations.

[PageRank](https://en.wikipedia.org/wiki/PageRank) is an algorithm used to rank web pages during a Google search. 
The PageRank algorithm ranks nodes depending on their connections to determine how important the node is. 
This assumes a node is more important if it receives more connections from others. Each vertex begins with an initial state. If it has any neighbours, it sends them a message which is the initial label / the number of neighbours. 
Each vertex, checks its messages and computes a new label based on: the total value of messages received and the damping factor. 
This new value is propagated to all outgoing neighbours. A vertex will stop propagating messages if its value becomes stagnant (i.e. has a change of less than 0.00001). 
This process is repeated for a number of iterate step times. Most algorithms should converge after approx. 20 iterations.

MemberRank is an algorithm that takes the PageRank score from the vertex neighbours and the original ranking of the vertex from the raw dataset and multiplies the two scores together to form the 'MemberRank score'. A vertex receiving a high MemberRank score means that other people with high PageRank scores have ranked them highly. If the Twitter user is influential, MemberRank will bump their score higher. If the Twitter user is non-influential (potentially a bot) the MemberRank score will be low. This should dampen the effect of bots further.

TemporalMemberRank is an algorithm that filters users with big differences in their raw dataset scores and MemberRank scores (potential bots) and checks their in-edge creations over time. Bot activity usually occurs in a very short timeframe (spamming) or at regular intervals. This algorithm will be able to give more evidence on whether a particular user is a bot or not. 


````{tabs}

```{tab} Python

```
```{tab} Scala

In the Twitter examples folder you will find `HiggsRunner.scala`.

* `HiggsRunner.scala` runs the data in Raphtory, performing all the algorithms and analysis.

You will find our two custom algorithms in the analysis folder: `MemberRank.scala` and `TemporalMemberRank.scala`. `PageRank.scala` is pulled from the main Raphtory algorithm folder.

## What the algorithms do to our data:
* `PageRank.scala`  based on the Google Pagerank algorithm, ranks nodes (Twitter Users) depending on their connections to determine how important the node is. 
* `MemberRank.scala` takes the Page Rank score and ranks users further by taking into account the number of retweets they received.
* `TemporalMemberRank.scala` filters nodes with big differences in the number of retweets they received on Twitter and the final rank given to them by MemberRank, outputting the suspected retweet bot IDs, along with the user ID they retweeted and the time of the retweet. The first element in each line is the Pulsar Output Timestamp, this can be dropped or ignored as it is irrelevant to the bot activity results.

In Raphtory, you can run multiple algorithms by chaining them together with an arrow: e.g. `PageRank() -> MemberRank()`.
You can also write your own algorithms to output your desired analysis/results.

We have also included python scripts in the directory `src/main/python` to output the results from Raphtory into a Jupyter notebook.

## Output

When you start `Runner.scala` you should see logs in your terminal like this:


Output for Vertex 4 (potential bot): 

We singled out vertex 4 as an example user that may show bot activity since after the final step of the chaining: `TemporalMemberRank.scala`, the score that vertex 4 originally had in the raw dataset significantly decreased after running PageRank and MemberRank. The output shows retweet activity happening within minutes and seconds of each other, this could be an indication of suspicious activity and further analysis on this data can be performed.

Feel free to play around with the example code and Jupyter notebook analysis. For example, in `TemporalMemberRank.scala`, you can change line 32:
`val difference: Boolean = (positiveRaw > (positiveNew * 4))`
`4` is the multiplication factor to see if the raw scores and new scores are significantly different. This can be increased or decreased depending on your desired analysis. 

```
````
