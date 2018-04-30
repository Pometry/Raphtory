package com.raphtory.Actors.RaphtoryActors.DataSource

import java.time.OffsetDateTime

import com.raphtory.caseclass.{EdgeAddWithProperties, VertexAddWithProperties}
import com.redis.RedisClient
import spray.json._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps
import com.raphtory.RawGraphsModels.GabPost
import com.raphtory.utils.{CommandEnum, GabEntityType}

final class gabSpout extends UpdaterTrait {
  private val redis    = new RedisClient("localhost", 6379) // TODO
  private val redisKey = "posts"

  import com.raphtory.RawGraphsModels.GabJsonProtocol._

  override def preStart() {
    super.preStart()
    context.system.scheduler.schedule(Duration(1, MINUTES), Duration(2, MILLISECONDS), self, "parsePost")
  }

  override def running() : Unit = if (isSafe) {
    val post = getNextPost()
    val timestamp = OffsetDateTime.parse(post.created_at).toEpochSecond
    val user      = post.user
    val topic     = post.topic

    val topicUUID = Math.pow(2,16).toInt + topic.id.hashCode
    val userUUID  = -user.id
    val postUUID  = post.id.toInt

    sendCommand(CommandEnum.vertexAdd, VertexAddWithProperties(timestamp, postUUID, Map(
      "user"         -> user.id.toString,
      "category"     -> post.category.toString,
      "likeCount"    -> post.like_count.toString,
      "score"        -> post.score.toString,
      "topic"        -> topic.id,
      "type"         -> GabEntityType.post.toString
    )))

    sendCommand(CommandEnum.vertexAdd, VertexAddWithProperties(timestamp, userUUID, Map(
      "username" -> user.username,
      "type"     -> GabEntityType.user.toString
    )))

    sendCommand(CommandEnum.vertexAdd, VertexAddWithProperties(timestamp, topicUUID, Map(
      "created_at" -> topic.created_at,
      "category"   -> topic.category.toString,
      "title"      -> topic.title,
      "type"       -> GabEntityType.topic.toString,
      "id"         -> topic.id
      )
    ))

    sendCommand(CommandEnum.edgeAdd,
      EdgeAddWithProperties(timestamp, postUUID, topicUUID, Map()))

    sendCommand(CommandEnum.edgeAdd,
      EdgeAddWithProperties(timestamp, userUUID, postUUID, Map()))
  }

  override protected def processChildMessages(rcvdMessage: Any) : Unit = {
    rcvdMessage match {
      case "parsePost" => running()
    }
  }

  private def getNextPost() : GabPost = {
    redis.lpop(redisKey).get.parseJson.convertTo[GabPost]
  }
  /*      sendCommand(s"""" {"EdgeAdd":{ "messageID":$time ,  "srcID":${txid.hashCode} ,  "dstID":${address.hashCode} , "properties":{"n": $n, "value":$value}}}"""") //creates edge between the transaction and the wallet
      sendCommand(s"""" {"VertexAdd":{ "messageID":$time ,  "srcID":${txid.hashCode} , "properties":{"type":"transaction", "time":$time, "id:$txid, "total": $total,"blockhash":$blockID}}}"""")
  */
}
