package com.raphtory.twitter.builder

import com.raphtory.api.input.Graph
import com.raphtory.api.input.GraphBuilder
import com.raphtory.api.input.ImmutableProperty
import com.raphtory.api.input.Properties
import com.raphtory.api.input.Type
import com.raphtory.twitter.spout.LiveTwitterSpout
import com.raphtory.twitter.spout.LiveTwitterSpoutInstance
import io.github.redouane59.twitter.dto.tweet.Tweet

import java.time.ZoneOffset
import scala.util.matching.Regex

/*
 * The LiveTwitterGraphBuilder sets source node as the user ID that made the retweet,
 * the target node is the user ID who is being retweeted, therefore the edge represents
 * the retweet relationship.
 */

class LiveTwitterRetweetGraphBuilder() extends GraphBuilder[Tweet] {

  override def parse(graph: Graph, tweet: Tweet): Unit = {
    val sourceNode    = tweet.getAuthorId
    val srcID         = sourceNode.toLong
    val timeStamp     = tweet.getCreatedAt.toEpochSecond(ZoneOffset.UTC)
    val pattern       = new Regex("(@[\\w-]+)")
    val twitterSpout  = LiveTwitterSpout().buildSpout().asInstanceOf[LiveTwitterSpoutInstance]
    val twitterClient = twitterSpout.spout.twitterClient

    //finding retweeted username in tweet by matching regex pattern, extracting the ID of that user by dropping the @ sign (substring method)
    val retweetedUserId = for {
      username <- pattern.findFirstIn(tweet.getText)
    } yield twitterClient.getUserFromUserName(username.substring(1)).getId
    val targetNode      = retweetedUserId.getOrElse(sourceNode)
    val tarID           = targetNode.toLong
    graph.addVertex(
            timeStamp,
            srcID,
            Properties(ImmutableProperty("user id", sourceNode)),
            Type("User ID")
    )
    graph.addVertex(
            timeStamp,
            tarID,
            Properties(ImmutableProperty("user id", targetNode)),
            Type("Retweeted User ID")
    )
    graph.addEdge(timeStamp, srcID, tarID, Type("Retweet Relationship"))
  }
}

class LiveTwitterUserGraphBuilder() extends GraphBuilder[Tweet] {

  override def parse(graph: Graph, tweet: Tweet): Unit = {
    val sourceNode = tweet.getAuthorId
    val srcID      = sourceNode.toLong
    val targetNode = tweet.getId
    val tarID      = targetNode.toLong
    val timeStamp  = tweet.getCreatedAt.toEpochSecond(ZoneOffset.UTC)

    graph.addVertex(
            timeStamp,
            srcID,
            Properties(ImmutableProperty("user id", sourceNode)),
            Type("User ID")
    )
    graph.addVertex(
            timeStamp,
            tarID,
            Properties(ImmutableProperty("tweet id", targetNode)),
            Type("Tweet ID")
    )
    graph.addEdge(timeStamp, srcID, tarID, Type("User to Tweet Relationship"))
  }
}
