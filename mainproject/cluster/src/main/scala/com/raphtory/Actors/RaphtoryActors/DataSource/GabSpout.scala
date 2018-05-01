package com.raphtory.Actors.RaphtoryActors.DataSource

import java.time.OffsetDateTime

import akka.actor.Cancellable
import com.raphtory.caseclass.{EdgeAddWithProperties, RaphCaseClass, VertexAddWithProperties}
import com.redis.RedisClient
import spray.json._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps
import com.raphtory.RawGraphsModels.GabPost
import com.raphtory.utils.{CommandEnum, GabEntityType}

final class GabSpout extends UpdaterTrait {
  private val redis    = new RedisClient("localhost", 6379)
  private val redisKey = "gab-posts"
  private var sched : Cancellable = null
  private val nullStr = "null"
  import com.raphtory.RawGraphsModels.GabJsonProtocol._

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

  def sendPostToPartitions(post : GabPost) : Unit = {

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
        println("Found parent post: Recursion!")
        sendPostToPartitions(p)
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
