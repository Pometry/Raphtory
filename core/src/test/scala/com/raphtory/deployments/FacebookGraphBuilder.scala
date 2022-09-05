package com.raphtory.deployments

import com.raphtory.api.input._
import com.raphtory.internals.graph.GraphBuilder

/*
 *
 * The facebook dataset consists of 'circles' (or 'friends lists') from Facebook. Facebook data was collected from survey
 * participants using this Facebook app. The dataset includes node features (profiles), circles, and ego networks.
 * Facebook data has been anonymized by replacing the Facebook-internal ids for each user with a new value.
 * Also, while feature vectors from this dataset have been provided, the interpretation of those features
 * has been obscured. For instance, where the original dataset may have contained a feature "political=Democratic Party"
 * the new data would simply contain "political=anonymized feature 1". Thus, using the anonymized data it is possible
 * to determine whether two users have the same political affiliations, but not what their individual political
 *  affiliations represent.
 *
 * Dataset statistics
 * Nodes	4039
 * Edges	88234
 * Nodes in largest WCC	4039 (1.000)
 * Edges in largest WCC	88234 (1.000)
 * Nodes in largest SCC	4039 (1.000)
 * Edges in largest SCC	88234 (1.000)
 * Average clustering coefficient	0.6055
 * Number of triangles	1612010
 * Fraction of closed triangles	0.2647
 * Diameter (longest shortest path)	8
 * 90-percentile effective diameter	4.7

Note that these statistics were compiled by combining the ego-networks, including the ego nodes themselves
 *  (along with an edge to each of their friends).
 *
 * Reference: https://snap.stanford.edu/data/ego-Facebook.html
 *
 * */
class FacebookGraphBuilder() extends GraphBuilder[String] {

  override def parse(graph: Graph, tuple: String): Unit = {

    val fileLine   = tuple.split(" ").map(_.trim)
    val sourceNode = fileLine(0)
    val srcID      = sourceNode.toLong

    val targetNode = fileLine(1)
    val tarID      = targetNode.toLong

    val timeStamp = fileLine(2).toLong

    graph.addVertex(
            timeStamp,
            srcID,
            Properties(ImmutableProperty("name", sourceNode)),
            Type("Character")
    )
    graph.addVertex(
            timeStamp,
            tarID,
            Properties(ImmutableProperty("name", targetNode)),
            Type("Character")
    )
    graph.addEdge(timeStamp, srcID, tarID, Type("Character Co-occurence"))
  }
}
