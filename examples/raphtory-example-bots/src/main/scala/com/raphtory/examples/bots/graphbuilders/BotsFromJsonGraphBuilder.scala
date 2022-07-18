package com.raphtory.examples.bots.graphbuilders

import com.raphtory.api.input.{GraphBuilder, ImmutableProperty, IntegerProperty, Properties, StringProperty, Type}
import com.raphtory.examples.bots.rawgraphmodel.Tweet
import com.raphtory.examples.bots.rawgraphmodel.TweetJsonProtocol.TweetJsonFormat
import spray.json._

class BotsFromJsonGraphBuilder extends GraphBuilder[String] {

  override def parseTuple(tuple: String) = {
      val command = tuple
      val tweet = command.parseJson.convertTo[Tweet]
      sendPostToPartitions(tweet)

    def sendPostToPartitions(
                              tweet: Tweet
                            ): Unit = {
      val userID  = tweet.author_id.get
      val tweetID = tweet.id.get
      //val timestamp = OffsetDateTime.parse(tweet.created_at.get).toEpochSecond
      val timestamp = tweet.created_at.get
      //val user_created = 1426734597 //TODO add actual user created
      addVertex(timestamp, userID, Properties(StringProperty("Bot Label", tweet.label.get)), Type("User"))
      tweet.in_reply_to_user_id match {
        case Some(replyID) =>
          if (replyID != 0L) {
            addVertex(timestamp, replyID, Type("User"))
            addEdge(timestamp, userID, replyID,Properties(ImmutableProperty("id", tweet.id.get.toString)), Type("Retweeted"),
            )
          }
        case None =>
      }

    }
  }
}