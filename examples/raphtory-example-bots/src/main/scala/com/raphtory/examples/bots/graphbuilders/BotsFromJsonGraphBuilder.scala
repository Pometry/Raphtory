package com.raphtory.examples.bots.graphbuilders

import com.raphtory.api.input.{GraphBuilder, Properties, StringProperty, Type}
import com.raphtory.examples.bots.rawgraphmodel.Tweet
import com.raphtory.examples.bots.rawgraphmodel.TweetJsonProtocol.TweetJsonFormat
import spray.json._

class BotsFromJsonGraphBuilder extends GraphBuilder[String] {

  private val nullStr = "null"

  override def parseTuple(tuple: String) = {

      //val command = tuple.substring(0, tuple.length()-1)
      val command = tuple
      val tweet = command.parseJson.convertTo[Tweet]
      sendPostToPartitions(tweet)

    //val parsedOBJ: Command = command.parseJson.convertTo[Command]
    //val manager = getManager(parsedOBJ.value.srcId, getManagerCount)
    //mediator ! DistributedPubSubMediator.Send(manager, parsedOBJ.value, false)

    def sendPostToPartitions(
                              tweet: Tweet
                            ): Unit = {
      val srcID  = tweet.author_id.get
      //val timestamp = OffsetDateTime.parse(tweet.created_at.get).toEpochSecond
      val timestamp = tweet.created_at.get/1000
      addVertex(timestamp, srcID, Properties(), Type("Tweet"))
      tweet.in_reply_to_user_id match {
        case Some(tarID) =>
          if (tarID != 0L) {
            addVertex(timestamp, tarID, vertexType = Type("Tweet"))
            addEdge(timestamp, srcID, tarID, edgeType = Type("Retweet")
            )
          }
        case None =>
      }

    }
  }
}