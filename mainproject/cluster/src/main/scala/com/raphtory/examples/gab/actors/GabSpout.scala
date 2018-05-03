package com.raphtory.examples.gab.actors

import java.time.OffsetDateTime

import akka.actor.Cancellable
import com.raphtory.core.actors.datasource.UpdaterTrait
import com.raphtory.core.model.communication.{EdgeAddWithProperties, VertexAddWithProperties}
import com.raphtory.core.utils.{CommandEnum, GabEntityType}
import com.raphtory.examples.gab.rawgraphmodel.GabPost
import com.redis.RedisClient
import spray.json._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps

final class GabSpout extends UpdaterTrait {
  import com.raphtory.examples.gab.rawgraphmodel.GabJsonProtocol._
  private val redis    = new RedisClient("localhost", 6379)
  private val redisKey = "gab-posts"
  private var sched : Cancellable = null
  private val nullStr = "null"

  override def preStart() {
    super.preStart()
    sched = context.system.scheduler.schedule(Duration(1, MINUTES), Duration(2, MILLISECONDS), self, "parsePost")
  }

  override def running() : Unit = if (isSafe) {
    getNextPost() match {
      case None => return
      case Some(p) => sendPostToPartitions(p)
    }

  }

  def sendPostToPartitions(post : GabPost, recursiveCall : Boolean = false) : Unit = {

    val postUUID  = post.id.get.toInt
    val timestamp = OffsetDateTime.parse(post.created_at.get).toEpochSecond

    sendCommand(CommandEnum.vertexAdd, VertexAddWithProperties(timestamp, postUUID, Map(
      "user"         -> {post.user match {
                            case Some(u) => u.id.toString
                            case None => nullStr
                        }},
      "likeCount"    -> post.like_count.toString,
      "score"        -> post.score.toString,
      "topic"        -> {post.topic match {
                          case Some(topic) => topic.id
                          case None => nullStr
                        }},
      "type"         -> GabEntityType.post.toString
    )))
    post.user match {
      case Some(user) => {
        val userUUID  = -user.id
        sendCommand(CommandEnum.vertexAdd, VertexAddWithProperties(timestamp, userUUID, Map(
          "username" -> user.username,
          "type"     -> GabEntityType.user.toString
        )))
        sendCommand(CommandEnum.edgeAdd,
          EdgeAddWithProperties(timestamp, userUUID, postUUID, Map()))
      }
      case None =>
    }

    post.topic match {
      case Some(topic) => {
        val topicUUID = -Math.pow(2, 16).toInt - topic.id.hashCode
        sendCommand(CommandEnum.vertexAdd, VertexAddWithProperties(timestamp, topicUUID, Map(
          "created_at" -> topic.created_at,
          "category"   -> topic.category.toString,
          "title"      -> topic.title.get,
          "type"       -> GabEntityType.topic.toString,
          "id"         -> topic.id
          )
        ))

        sendCommand(CommandEnum.edgeAdd,
          EdgeAddWithProperties(timestamp, postUUID, topicUUID, Map()))
      }
      case None =>
    }

    post.parent match {
      case Some(p) => {
        if (!recursiveCall) { // Allow only one recursion per post
          println("Found parent post: Recursion!")
          sendPostToPartitions(p, true)
        }
      }
      case None =>
    }
  }

  override protected def processChildMessages(rcvdMessage: Any) : Unit = {
    rcvdMessage match {
      case "parsePost" => running()
    }
  }

  private def getNextPost() : Option[GabPost] = {

    redis.lpop(redisKey) match {
      case Some(i) => {
        val x = redis.get(i).get
        Some(x.drop(2).dropRight(1).replaceAll("""\\"""","").replaceAll("""\\""", "").parseJson.convertTo[GabPost])
      }
      case None => {
        println("Stream end")
        sched.cancel()
        None
      }
    }
  }
}
