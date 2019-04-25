package com.raphtory.examples.gab.actors

import java.time.OffsetDateTime

import akka.cluster.pubsub.DistributedPubSubMediator
import com.raphtory.core.actors.router.TraditionalRouter.Helpers.RouterSlave
import com.raphtory.core.actors.router.TraditionalRouter.RaphtoryRouter
import com.raphtory.core.model.communication.{EdgeAdd, EdgeAddWithProperties, VertexAdd, VertexAddWithProperties}
import com.raphtory.core.utils.CommandEnum
import com.raphtory.examples.gab.rawgraphmodel.GabPost
import spray.json._
import com.raphtory.core.model.communication.RaphtoryJsonProtocol._

/**
  * The Graph Manager is the top level actor in this system (under the stream)
  * which tracks all the graph partitions - passing commands processed by the 'command processor' actors
  * to the correct partition
  */

/**
  * The Command Processor takes string message from Kafka and translates them into
  * the correct case Class which can then be passed to the graph manager
  * which will then pass it to the graph partition dealing with the associated vertex
  */

final class RaphtoryGabRouter(val routerId:Int, val initialManagerCount:Int) extends RouterSlave {
  import com.raphtory.examples.gab.rawgraphmodel.GabJsonProtocol._
  import com.raphtory.core.model.communication.RaphtoryJsonProtocol._
  private val nullStr = "null"
  override def parseRecord(record:Any) : Unit= {
    try{
      val command = record.asInstanceOf[String]
      val post = command.parseJson.convertTo[GabPost]
      sendPostToPartitions(post)
    }
    catch {
      case e:Exception => println("Could not parse post")
    }

    //val parsedOBJ: Command = command.parseJson.convertTo[Command]
    //val manager = getManager(parsedOBJ.value.srcId, getManagerCount)
    //mediator ! DistributedPubSubMediator.Send(manager, parsedOBJ.value, false)
  }


  def sendPostToPartitions(post : GabPost, recursiveCall : Boolean = false, parent : Int = 0) : Unit = {
    val postUUID  = post.id.get.toInt
    val timestamp = OffsetDateTime.parse(post.created_at.get).toEpochSecond
    toPartitionManager(VertexAddWithProperties(routerId,timestamp, postUUID, Map(
      "user"         -> {post.user match {
        case Some(u) => u.id.toString
        case None => nullStr
      }},
      "likeCount"    -> {post.like_count match {
        case Some(likeCount) => likeCount.toString
        case None => nullStr
      }},
      "score"        -> {post.score match {
        case Some(score) => score.toString
        case None => nullStr
      }},
      "topic"        -> {post.topic match {
        case Some(topic) => topic.id
        case None => nullStr
      }},
      "type"         -> "post"
    )))

    post.user match {
      case Some(user) =>{
        val userUUID:Int = "user".hashCode() + user.id //TODO improve in case of clashes
        toPartitionManager(VertexAddWithProperties(routerId,timestamp,userUUID,Map(
          "type" -> "user",
          "id" ->user.id.toString,
          "name" -> user.name,
          "username" -> user.username,
          "verified" -> user.verified.toString
        )))

        toPartitionManager(EdgeAddWithProperties(routerId,timestamp,userUUID,postUUID,Map("type"->"userToPost")))
        toPartitionManager(EdgeAddWithProperties(routerId,timestamp,postUUID,userUUID,Map("type"->"postToUser")))
      }
      case None =>
    }

    post.topic match {
      case Some(topic) => {
        val topicUUID : Int = Math.pow(2,24).toInt+ (topic.id.hashCode())
        toPartitionManager(VertexAddWithProperties(routerId,timestamp, topicUUID, Map(
          "created_at" -> topic.created_at,
          "category"   -> topic.category.toString,
          "title"      -> topic.title.getOrElse("null"),
          "type"       -> "topic",
          "id"         -> topic.id
        )
        ))

        toPartitionManager(EdgeAddWithProperties(routerId,timestamp, postUUID, topicUUID,Map("type"->"postToTopic")))
      }
      case None =>
    }


    // Edge from child to parent post
    if (recursiveCall && parent != 0) {
      toPartitionManager(EdgeAddWithProperties(routerId,timestamp, postUUID, parent,Map("type"->"childToParent")))
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

}
