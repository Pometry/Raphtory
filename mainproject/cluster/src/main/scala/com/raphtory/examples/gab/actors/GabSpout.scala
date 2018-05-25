package com.raphtory.examples.gab.actors

import java.time.OffsetDateTime

import akka.actor.Cancellable
import com.raphtory.core.actors.datasource.UpdaterTrait
import com.raphtory.core.model.communication.{EdgeAddWithProperties, VertexAddWithProperties, VertexAdd, EdgeAdd}
import com.raphtory.core.utils.{CommandEnum, GabEntityType}
import com.raphtory.examples.gab.rawgraphmodel.GabPost
import com.redis.{RedisClient, RedisConnectionException}
import spray.json._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps

final class GabSpout extends UpdaterTrait {
  import com.raphtory.examples.gab.rawgraphmodel.GabJsonProtocol._
  import com.raphtory.core.model.communication.RaphtoryJsonProtocol._
  private val redis    = new RedisClient("moe", 6379)
  private val redisKey = "gab-posts"
  private var sched : Cancellable = null
  private val nullStr = "null"

  override def preStart() {
    super.preStart()
    sched = context.system.scheduler.schedule(Duration(1, MINUTES), Duration(10, MILLISECONDS), self, "parsePost")
    //sched = context.system.scheduler.scheduleOnce(Duration(30, SECONDS), self, "parsePost")
  }

  override def running() : Unit = if (isSafe) {
    /*sendCommand(CommandEnum.vertexAdd, VertexAddWithProperties(System.currentTimeMillis(), 0, Map()))
    sendCommand(CommandEnum.vertexAdd, VertexAddWithProperties(System.currentTimeMillis(), 1, Map()))
    sendCommand(CommandEnum.vertexAdd, VertexAddWithProperties(System.currentTimeMillis(), 2, Map()))
    sendCommand(CommandEnum.vertexAdd, VertexAddWithProperties(System.currentTimeMillis(), 3, Map()))
    sendCommand(CommandEnum.vertexAdd, VertexAddWithProperties(System.currentTimeMillis(), 4, Map()))
    sendCommand(CommandEnum.edgeAdd, EdgeAddWithProperties(System.currentTimeMillis(), 0, 1, Map()))
    sendCommand(CommandEnum.edgeAdd, EdgeAddWithProperties(System.currentTimeMillis(), 0, 4, Map()))
    sendCommand(CommandEnum.edgeAdd, EdgeAddWithProperties(System.currentTimeMillis(), 0, 3, Map()))
    sendCommand(CommandEnum.edgeAdd, EdgeAddWithProperties(System.currentTimeMillis(), 4, 3, Map()))
    sendCommand(CommandEnum.edgeAdd, EdgeAddWithProperties(System.currentTimeMillis(), 2, 4, Map()))
    sendCommand(CommandEnum.edgeAdd, EdgeAddWithProperties(System.currentTimeMillis(), 1, 4, Map()))
    sendCommand(CommandEnum.edgeAdd, EdgeAddWithProperties(System.currentTimeMillis(), 2, 0, Map()))
    sendCommand(CommandEnum.edgeAdd, EdgeAddWithProperties(System.currentTimeMillis(), 2, 1, Map()))*/
    getNextPost() match {
      case None =>
      case Some(p) => sendPostToPartitions(p)
    }
    //sched.cancel()
    //sched = context.system.scheduler.scheduleOnce(Duration(10, MILLISECONDS), self, "parsePost")
  }

  def sendPostToPartitions(post : GabPost, recursiveCall : Boolean = false, parent : Int = 0) : Unit = {

    val postUUID  = post.id.get.toInt
    val timestamp = OffsetDateTime.parse(post.created_at.get).toEpochSecond

    /*sendCommand(CommandEnum.vertexAdd, VertexAddWithProperties(timestamp, postUUID, Map(
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
      "type"         -> GabEntityType.post.toString,
    )))*/
    //sendCommand(CommandEnum.vertexAdd, VertexAdd(timestamp, postUUID))
    sendCommand2(
        s"""{"VertexAdd":${VertexAdd(timestamp, postUUID).toJson.toString()}}"""
    )
    post.user match {
      case Some(user) => {
        val userUUID  = Math.pow(2,24).toInt + user.id
        if (userUUID < 0 || userUUID > Int.MaxValue)
          println(s"UserID is $userUUID")
        /*sendCommand(CommandEnum.vertexAdd, VertexAddWithProperties(timestamp, userUUID, Map(
          "username" -> user.username,
          "type"     -> GabEntityType.user.toString
        )))*/
        /*sendCommand(CommandEnum.vertexAdd, VertexAdd(timestamp, userUUID))
        sendCommand(CommandEnum.edgeAdd, EdgeAdd(timestamp, userUUID, postUUID))*/
        sendCommand2(
          s"""{"VertexAdd":${VertexAdd(timestamp, userUUID).toJson.toString()}}"""
        )
        //sendCommand2(
        //  s"""{"EdgeAdd":${EdgeAdd(timestamp, userUUID, postUUID).toJson.toString()}}"""
        //)
        /*sendCommand(CommandEnum.edgeAdd,
          EdgeAddWithProperties(timestamp, userUUID, postUUID, Map()))*/
      }
      case None =>
    }

    /*post.topic match {
      case Some(topic) => {
        val topicUUID : Int = (Math.pow(2, 10) + topic.id.hashCode()).toInt
        println(topicUUID)
        println(sendCommand(CommandEnum.vertexAdd, VertexAddWithProperties(timestamp, topicUUID, Map(
          "created_at" -> topic.created_at,
          "category"   -> topic.category.toString,
          "title"      -> topic.title.getOrElse("null"),
          "type"       -> GabEntityType.topic.toString,
          "id"         -> topic.id
          )
        )))

        //sendCommand(CommandEnum.edgeAdd,
        //  EdgeAddWithProperties(timestamp, postUUID, topicUUID, Map()))
      }
      case None =>
    }*/


    // Edge from child to parent post
    if (recursiveCall && parent > 0) {
      /*sendCommand(CommandEnum.edgeAdd,
        EdgeAddWithProperties(timestamp, postUUID, parent, Map()))*/
      //sendCommand(CommandEnum.edgeAdd, EdgeAdd(timestamp, postUUID, parent))
      sendCommand2(
        s"""{"EdgeAdd":${EdgeAdd(timestamp, postUUID, parent).toJson.toString()}}"""
      )
    }
    post.parent match {
      case Some(p) => {
        if (!recursiveCall) { // Allow only one recursion per post
          //println("Found parent post: Recursion!")
          sendPostToPartitions(p, true, postUUID)
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
        val y = x.drop(2).dropRight(1)
        try {
          Some(y.replaceAll("""\\"""", "").replaceAll("""\\""", "").parseJson.convertTo[GabPost])
        } catch {
          case e =>
            println(e.toString)
            None
        }
      }
      case None => {
        println("Stream end")
        sched.cancel()
        None
      }
    }
  }
}
