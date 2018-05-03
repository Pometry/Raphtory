package com.raphtory.tests
import java.time.OffsetDateTime

import spray.json._
import com.raphtory.examples.gab.rawgraphmodel.GabPost

import scala.util.parsing.json.JSONObject

object GabJsonTest extends App {
  import com.raphtory.examples.gab.rawgraphmodel.GabJsonProtocol._
  import com.raphtory.core.model.communication.RaphtoryJsonProtocol._

  val myString = """{"id":250349,"created_at":"2016-09-09T15:38:29+00:00","revised_at":null,"edited":false,"body":"Meanwhile, on twatter...\\n\\nhttps:\\/\\/pbs.twimg.com\\/media\\/ClXHoZWWAAMS-8r.jpg","body_html":null,"body_html_summary":null,"body_html_summary_truncated":false,"only_emoji":false,"liked":false,"disliked":false,"bookmarked":false,"repost":false,"reported":false,"score":1,"like_count":0,"dislike_count":0,"reply_count":0,"repost_count":0,"is_quote":false,"is_reply":false,"is_replies_disabled":false,"embed":{"html":"<a href=\https:\\/\\/pbs.twimg.com\\/media\\/ClXHoZWWAAMS-8r.jpg\ target=\_blank\ class=\post__embed__body post__embed__body--photo\><div class=\post__embed__body__image\ style=\background-image: url(\'https:\\/\\/ipr2.gab.ai\\/52232a4964df5f85a4f6a0ec6caf6688f03b1837\\/68747470733a2f2f7062732e7477696d672e636f6d2f6d656469612f436c58486f5a575741414d532d38722e6a7067\\/\')\><\\/div><\\/a>","iframe":false},"attachment":{"type":"url","value":{"image":"https:\\/\\/ipr2.gab.ai\\/52232a4964df5f85a4f6a0ec6caf6688f03b1837\\/68747470733a2f2f7062732e7477696d672e636f6d2f6d656469612f436c58486f5a575741414d532d38722e6a7067\\/","title":null,"description":null,"url":"https:\\/\\/pbs.twimg.com\\/media\\/ClXHoZWWAAMS-8r.jpg","source":"pbs.twimg.com"}},"category":null,"category_details":null,"language":null,"nsfw":false,"is_premium":false,"is_locked":false,"user":{"id":5585,"name":"Bluto","username":"Darth_Bluto","picture_url":"https:\\/\\/files.gab.ai\\/user\\/5aadea5f52d4e.png","verified":false,"is_donor":false,"is_investor":false,"is_pro":false,"is_private":false,"is_premium":false},"replies":{"data":[]}}"""

  val myJsonAst = myString.replaceAll("""\\""", "").parseJson

  println("JsonAst prettyPrint")
  myJsonAst.prettyPrint

  val post = myJsonAst.convertTo[GabPost]
  println("The GabPost Object")
  println(post)
  println(post.created_at)
  val timestamp = OffsetDateTime.parse(post.created_at.get).toEpochSecond

  val user      = post.user
  val topic     = post.topic
  /*println(Command(CommandEnum.vertexAdd, VertexAddWithProperties(timestamp, post.id.toInt, Map(
    "user"         -> user.id.toString,
    "category"     -> post.category.toString,
    "likeCount"    -> post.like_count.toString,
    "score"        -> post.score.toString,
    "topic"        -> topic.id,
    "type"         -> GabEntityType.post.toString
  ))).toJson.toString())

  sendCommand(CommandEnum.vertexAdd, VertexAddWithProperties(timestamp, -user.id, Map(
    "username" -> user.username,
    "type"     -> GabEntityType.user.toString
  )))

  sendCommand(CommandEnum.vertexAdd, VertexAddWithProperties(timestamp, Math.pow(2,16).toInt+topic.id.hashCode, Map(
    "created_at" -> topic.created_at,
    "category"   -> topic.category.toString,
    "title"      -> topic.title,
    "type"       -> GabEntityType.topic.toString,
    "id"         -> topic.id
    )
  ))

  println(Command(CommandEnum.edgeAdd, EdgeAddWithProperties(12,12,12,Map())).toJson.toString())

  def sendCommand[T <: RaphCaseClass](command: CommandEnum.Value, value: T) = {
    println(Command(command, value).toJson.toString)
  }*/
}
