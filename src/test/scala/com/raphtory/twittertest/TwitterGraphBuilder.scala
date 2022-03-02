package com.raphtory.twittertest

import com.raphtory.core.components.graphbuilder.GraphBuilder
import com.raphtory.core.components.graphbuilder.ImmutableProperty
import com.raphtory.core.components.graphbuilder.Properties
import com.raphtory.core.components.graphbuilder.Type

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
class TwitterGraphBuilder() extends GraphBuilder[String] {

  override def parseTuple(tuple: String): Unit = {
    val fileLine   = tuple.split(" ").map(_.trim)
    val sourceNode = fileLine(0)
    val srcID      = sourceNode.toLong
    val targetNode = fileLine(1)
    val tarID      = targetNode.toLong
    val timeStamp  = 1L

    addVertex(
            timeStamp,
            srcID,
            Properties(ImmutableProperty("name", sourceNode)),
            Type("Character")
    )
    addVertex(
            timeStamp,
            tarID,
            Properties(ImmutableProperty("name", targetNode)),
            Type("Character")
    )
    addEdge(timeStamp, srcID, tarID, Type("Character Co-occurence"))
  }
}
