package graphbuilders

import com.raphtory.api.input.{GraphBuilder, Properties, StringProperty, Type}
import rawgraphmodel.TweetJsonProtocol.TweetJsonFormat
import rawgraphmodel.Tweet

import java.time.OffsetDateTime
import spray.json._

class BotsFromJsonGraphBuilder extends GraphBuilder[String] {


  private val nullStr = "null"

  override def parseTuple(tuple: String) = {
    try {
      val command = tuple
      val tweet    = command.parseJson.convertTo[Tweet]
      sendPostToPartitions(tweet)
    }
    catch {
      case e: Exception => println("Could not parse post")
    }

    //val parsedOBJ: Command = command.parseJson.convertTo[Command]
    //val manager = getManager(parsedOBJ.value.srcId, getManagerCount)
    //mediator ! DistributedPubSubMediator.Send(manager, parsedOBJ.value, false)

    def sendPostToPartitions(
                              tweet: Tweet,
                            ): Unit = {
      val srcID  = tweet.author_id.get
      //val timestamp = OffsetDateTime.parse(tweet.created_at.get).toEpochSecond
      val timestamp = tweet.created_at.get/1000
      addVertex(timestamp,
        srcID,
        Properties(
          StringProperty("retweet_count",
            tweet.public_metrics.get.retweet_count match {
              case Some(r) => r.toString
              case None    => nullStr
            }
          ),
          StringProperty("like_count",
            tweet.public_metrics.get.like_count match {
              case Some(l) => l.toString
              case None            => nullStr
            }
          )
        ),
        Type("Tweet"))
      tweet.in_reply_to_user_id match {
        case Some(tarID) =>       if (tarID != 0L) {
          addVertex(timestamp, tarID, vertexType = Type("Tweet"))
          addEdge(timestamp, srcID, tarID, edgeType = Type("Retweet")
          )
        }
        case None =>
      }

    }
  }
}