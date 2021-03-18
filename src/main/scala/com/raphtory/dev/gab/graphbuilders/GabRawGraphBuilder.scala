package com.raphtory.dev.gab.graphbuilders

import java.time.OffsetDateTime

import com.raphtory.core.actors.Router.GraphBuilder
import com.raphtory.core.model.communication.{ Properties, StringProperty}
import com.raphtory.examples.gab.rawgraphmodel.GabPost
import spray.json._

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
final class GabRawGraphBuilder extends GraphBuilder[String] {

  import com.raphtory.examples.gab.rawgraphmodel.GabJsonProtocol._

  private val nullStr = "null"

  override def parseTuple(tuple: String) = {
    try {
      val command = tuple
      val post = command.parseJson.convertTo[GabPost]
      sendPostToPartitions(post)
    } catch {
      case e: Exception => println("Could not parse post")
    }

    //val parsedOBJ: Command = command.parseJson.convertTo[Command]
    //val manager = getManager(parsedOBJ.value.srcId, getManagerCount)
    //mediator ! DistributedPubSubMediator.Send(manager, parsedOBJ.value, false)

    def sendPostToPartitions(post: GabPost, recursiveCall: Boolean = false, parent: Int = 0): Unit = {
      val postUUID = post.id.get.toInt
      val timestamp = OffsetDateTime.parse(post.created_at.get).toEpochSecond
       addVertex(
          timestamp,
          postUUID,
          Properties(
            StringProperty("user", post.user match {
              case Some(u) => u.id.toString
              case None => nullStr
            }),
            StringProperty("likeCount", post.like_count match {
              case Some(likeCount) => likeCount.toString
              case None => nullStr
            }),
            StringProperty("score", post.score match {
              case Some(score) => score.toString
              case None => nullStr
            }),
            StringProperty("topic", post.topic match {
              case Some(topic) => topic.id
              case None => nullStr
            }),
            StringProperty("type", "post")
          )
      )

      post.user match {
        case Some(user) =>
          val userUUID: Int = "user".hashCode() + user.id //TODO improve in case of clashes
          addVertex(
              timestamp,
              userUUID,
              Properties(
                StringProperty("type", "user"),
                StringProperty("id", user.id.toString),
                StringProperty("name", user.name),
                StringProperty("username", user.username),
                StringProperty("verified", user.verified.toString)
              )
          )


            addEdge(timestamp, userUUID, postUUID, Properties((StringProperty("type", "userToPost"))))

            addEdge(timestamp, postUUID, userUUID, Properties(StringProperty("type", "postToUser")))

        case None =>
      }

      post.topic match {
        case Some(topic) =>
          val topicUUID: Int = Math.pow(2, 24).toInt + (topic.id.hashCode())
          addVertex(
              timestamp,
              topicUUID,
              Properties(
                StringProperty("created_at", topic.created_at),
                StringProperty("category", topic.category.toString),
                StringProperty("title", topic.title.getOrElse("null")),
                StringProperty("type", "topic"),
                StringProperty("id", topic.id)
              )
          )

            addEdge(timestamp, postUUID, topicUUID, Properties(StringProperty("type", "postToTopic")))
        case None =>
      }

      // Edge from child to parent post
      if (recursiveCall && parent != 0)
          addEdge(timestamp, postUUID, parent, Properties(StringProperty("type", "childToParent")))
      post.parent match {
        case Some(p) =>
          if (!recursiveCall) // Allow only one recursion per post
          //println("Found parent post: Recursion!")
            sendPostToPartitions(p, true, postUUID)
        case None =>
      }

    }
  }
}

