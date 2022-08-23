package com.raphtory.examples.twitter.followernetwork.graphbuilders

import com.raphtory.api.input.Graph
import com.raphtory.api.input.GraphBuilder
import com.raphtory.api.input.ImmutableProperty
import com.raphtory.api.input.Properties
import com.raphtory.api.input.Type

/*
 * The Twitter dataset consists of 'circles' (or 'lists') from Twitter crawled from public sources.
 * The dataset includes node features (profiles), circles, and ego networks. Data is also available from Facebook
 * and Google+.
 * Dataset statistics
 * Nodes	81306
 * Edges	1768149
 * Nodes in largest WCC	81306 (1.000)
 * Edges in largest WCC	1768149 (1.000)
 * Nodes in largest SCC	68413 (0.841)
 * Edges in largest SCC	1685163 (0.953)
 * Average clustering coefficient	0.5653
 * Number of triangles	13082506
 * Fraction of closed triangles	0.06415
 * Diameter (longest shortest path)	7
 * 90-percentile effective diameter	4.5
 *
 * Reference: https://snap.stanford.edu/data/ego-Twitter.html
 *
 * */

class TwitterCirclesGraphBuilder extends GraphBuilder[String] {

  override def parse(graph: Graph, tuple: String): Unit = {
    val fileLine   = tuple.split(" ").map(_.trim)
    val sourceNode = fileLine(0)
    val srcID      = sourceNode.toLong
    val targetNode = fileLine(1)
    val tarID      = targetNode.toLong
    val timeStamp  = fileLine(2).toLong

    graph.addVertex(timeStamp, srcID, Properties(ImmutableProperty("name", sourceNode)), Type("User"))
    graph.addVertex(timeStamp, tarID, Properties(ImmutableProperty("name", targetNode)), Type("User"))
    graph.addEdge(timeStamp, srcID, tarID, Type("Follow"))
  }

}
