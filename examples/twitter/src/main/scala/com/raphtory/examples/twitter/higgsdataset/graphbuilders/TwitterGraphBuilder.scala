package com.raphtory.examples.twitter.higgsdataset.graphbuilders

import com.raphtory.api.input.{GraphBuilder, ImmutableProperty, Properties, Type}

class TwitterGraphBuilder() extends GraphBuilder[String] {

  override def parseTuple(tuple: String): Unit = {
    val fileLine   = tuple.split(",").map(_.trim)
    val sourceNode = fileLine(0).trim
    val srcID      = sourceNode.toLong
    val targetNode = fileLine(1).trim
    val tarID      = targetNode.toLong
    val timeStamp  = fileLine(2).toLong

    addVertex(timeStamp, srcID, Properties(ImmutableProperty("name", sourceNode)), Type("User"))
    addVertex(timeStamp, tarID, Properties(ImmutableProperty("name", targetNode)), Type("User"))
    //Edge shows srcID retweets tarID's tweet
    addEdge(timeStamp, srcID, tarID, Type("Retweet"))
  }
}
