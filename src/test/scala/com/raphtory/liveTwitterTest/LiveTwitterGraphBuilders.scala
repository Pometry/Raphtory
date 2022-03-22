package com.raphtory.liveTwitterTest

import com.raphtory.core.components.graphbuilder.GraphBuilder
import com.raphtory.core.components.graphbuilder.Properties._
import com.raphtory.spouts.LiveTwitterSpout
import io.github.redouane59.twitter.dto.tweet.Tweet

import java.time.ZoneOffset
import scala.util.matching.Regex

/*
 * The LiveTwitterGraphBuilder sets source node as the user ID that made the retweet,
 * the target node is the user ID who is being retweeted, therefore the edge represents
 * the retweet relationship.
 */

class LiveTwitterRetweetGraphBuilder() extends GraphBuilder[Tweet] {

  override def parseTuple(tweet: Tweet): Unit = {
    val sourceNode    = tweet.getAuthorId
    val srcID         = sourceNode.toLong
    val timeStamp     = tweet.getCreatedAt.toEpochSecond(ZoneOffset.UTC)
    val pattern       = new Regex("(@[\\w-]+)")
    val twitterSpout  = new LiveTwitterSpout()
    val twitterClient = twitterSpout.spout.twitterClient

    //finding retweeted username in tweet by matching regex pattern, extracting the ID of that user by dropping the @ sign (substring method)
    val retweetedUserId = for {
      username <- pattern.findFirstIn(tweet.getText)
    } yield twitterClient.getUserFromUserName(username.substring(1)).getId
    val targetNode      = retweetedUserId.getOrElse(sourceNode)
    val tarID           = targetNode.toLong
    println(tweet.getText)
    addVertex(
            timeStamp,
            srcID,
            Properties(ImmutableProperty("user id", sourceNode)),
            Type("User ID")
    )
    addVertex(
            timeStamp,
            tarID,
            Properties(ImmutableProperty("user id", targetNode)),
            Type("Retweeted User ID")
    )
    addEdge(timeStamp, srcID, tarID, Type("Retweet Relationship"))
  }
}

class LiveTwitterUserGraphBuilder() extends GraphBuilder[Tweet] {

  override def parseTuple(tweet: Tweet): Unit = {
    val sourceNode = tweet.getAuthorId
    val srcID      = sourceNode.toLong
    val targetNode = tweet.getId
    val tarID      = targetNode.toLong
    val timeStamp  = tweet.getCreatedAt.toEpochSecond(ZoneOffset.UTC)
    println(tweet.getText)
    addVertex(
            timeStamp,
            srcID,
            Properties(ImmutableProperty("user id", sourceNode)),
            Type("User ID")
    )
    addVertex(
            timeStamp,
            tarID,
            Properties(ImmutableProperty("tweet id", targetNode)),
            Type("Tweet ID")
    )
    addEdge(timeStamp, srcID, tarID, Type("User to Tweet Relationship"))
  }
}
