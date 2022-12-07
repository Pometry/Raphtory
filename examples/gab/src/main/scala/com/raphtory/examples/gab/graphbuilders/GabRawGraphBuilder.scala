package com.raphtory.examples.gab.graphbuilders

import com.raphtory.api.input.Graph
import com.raphtory.api.input.Properties
import com.raphtory.api.input.MutableString

import java.time.OffsetDateTime
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
object GabRawGraphBuilder {

  import com.raphtory.examples.gab.rawgraphmodel.GabJsonProtocol._

  private val nullStr = "null"

  def parser(graph: Graph, tuple: String) = {
    try {
      val command = tuple
      val post    = command.parseJson.convertTo[GabPost]
      sendPostToPartitions(graph, post)
    }
    catch {
      case e: Exception => println("Could not parse post")
    }

    //val parsedOBJ: Command = command.parseJson.convertTo[Command]
    //val manager = getManager(parsedOBJ.value.srcId, getManagerCount)
    //mediator ! DistributedPubSubMediator.Send(manager, parsedOBJ.value, false)

    def sendPostToPartitions(
        graph: Graph,
        post: GabPost,
        recursiveCall: Boolean = false,
        parent: Int = 0
    ): Unit = {
      val postUUID  = post.id.get.toInt
      val timestamp = OffsetDateTime.parse(post.created_at.get).toEpochSecond
      graph.addVertex(
              timestamp,
              postUUID,
              Properties(
                      MutableString(
                              "user",
                              post.user match {
                                case Some(u) => u.id.toString
                                case None    => nullStr
                              }
                      ),
                      MutableString(
                              "likeCount",
                              post.like_count match {
                                case Some(likeCount) => likeCount.toString
                                case None            => nullStr
                              }
                      ),
                      MutableString(
                              "score",
                              post.score match {
                                case Some(score) => score.toString
                                case None        => nullStr
                              }
                      ),
                      MutableString(
                              "topic",
                              post.topic match {
                                case Some(topic) => topic.id
                                case None        => nullStr
                              }
                      ),
                      MutableString("type", "post")
              )
      )

      post.user match {
        case Some(user) =>
          val userUUID: Int = "user".hashCode() + user.id //TODO improve in case of clashes
          graph.addVertex(
                  timestamp,
                  userUUID,
                  Properties(
                          MutableString("type", "user"),
                          MutableString("id", user.id.toString),
                          MutableString("name", user.name),
                          MutableString("username", user.username),
                          MutableString("verified", user.verified.toString)
                  )
          )

          graph.addEdge(timestamp, userUUID, postUUID, Properties((MutableString("type", "userToPost"))))
          graph.addEdge(timestamp, postUUID, userUUID, Properties(MutableString("type", "postToUser")))

        case None       =>
      }

      post.topic match {
        case Some(topic) =>
          val topicUUID: Int = Math.pow(2, 24).toInt + (topic.id.hashCode())
          graph.addVertex(
                  timestamp,
                  topicUUID,
                  Properties(
                          MutableString("created_at", topic.created_at),
                          MutableString("category", topic.category.toString),
                          MutableString("title", topic.title.getOrElse("null")),
                          MutableString("type", "topic"),
                          MutableString("id", topic.id)
                  )
          )

          graph.addEdge(timestamp, postUUID, topicUUID, Properties(MutableString("type", "postToTopic")))
        case None        =>
      }

      // Edge from child to parent post
      if (recursiveCall && parent != 0)
        graph.addEdge(timestamp, postUUID, parent, Properties(MutableString("type", "childToParent")))
      post.parent match {
        case Some(p) =>
          if (!recursiveCall) // Allow only one recursion per post
            //println("Found parent post: Recursion!")
            sendPostToPartitions(graph, p, true, postUUID)
        case None    =>
      }
    }
  }
}
