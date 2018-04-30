package com.raphtory.tests
import java.time.OffsetDateTime

import spray.json._
import DefaultJsonProtocol._
import com.raphtory.RawGraphsModels.GabPost
import com.raphtory.RawGraphsModels._
import com.raphtory.caseclass.{Command, EdgeAddWithProperties, RaphCaseClass, VertexAddWithProperties}
import com.raphtory.utils.{CommandEnum, GabEntityType}

import scala.util.parsing.json.JSONObject

object GabJsonTest extends App {
  import com.raphtory.RawGraphsModels.GabJsonProtocol._
  import com.raphtory.caseclass.RaphtoryJsonProtocol._

  val myString = """{"id":24470573,"created_at":"2018-04-25T04:04:18+00:00","revised_at":null,"edited":false,"body":"NBA commissioner Adam Silver in the middle.","body_html":"<p>NBA commissioner Adam Silver in the middle.<\/p>\n\n\n","body_html_summary":"<p>NBA commissioner Adam Silver in the middle.<\/p>\n\n\n","body_html_summary_truncated":false,"only_emoji":false,"liked":false,"disliked":false,"bookmarked":false,"repost":false,"reported":false,"score":18,"like_count":18,"dislike_count":0,"reply_count":3,"repost_count":7,"is_quote":false,"is_reply":false,"is_replies_disabled":false,"embed":{"html":null,"iframe":null},"attachment":{"type":"media","value":[{"id":"e2a7ede4-33af-4c18-b2a9-c8c2497ae289","url_thumbnail":"https:\/\/gabfiles.blob.core.windows.net\/image\/5adffaf011a82.jpeg","url_full":"https:\/\/gabfiles.blob.core.windows.net\/image\/5adffaf011ad5.jpeg","width":900,"height":600}]},"category":5,"category_details":{"title":"Sports","slug":"sports","value":5,"emoji":"\u26bd"},"language":"en","nsfw":false,"is_premium":false,"is_locked":false,"user":{"id":69280,"name":"America1stMotif","username":"spirit76","picture_url":"https:\/\/files.gab.ai\/user\/5833ef1a13ca2.jpg","verified":false,"is_donor":false,"is_investor":false,"is_pro":false,"is_private":false,"is_premium":false},"topic":{"id":"72e45c1b-8a40-4ada-a2a3-c4094779d6db","created_at":"2018-04-25T02:33:42+00:00","is_featured":true,"title":"NBA Playoffs","category":5},"replies":{"data":[]}}"""

  val myJsonAst = myString.parseJson

  println("JsonAst prettyPrint")
  myJsonAst.prettyPrint

  val post = myJsonAst.convertTo[GabPost]
  println("The GabPost Object")
  println(post)
  println(post.created_at)
  val timestamp = OffsetDateTime.parse(post.created_at).toEpochSecond

  val user      = post.user
  val topic     = post.topic
  println(Command(CommandEnum.vertexAdd, VertexAddWithProperties(timestamp, post.id.toInt, Map(
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
  }
}
