package com.raphtory.RawGraphsModels

import spray.json._

import scala.util.parsing.json.JSONObject

object GabJsonProtocol extends DefaultJsonProtocol {

  implicit val gabEmbedFormat            = jsonFormat2(GabEmbed)
  implicit val gabMediaFormat            = jsonFormat5(GabMedia)
  implicit val gabAttachmentFormat       = jsonFormat2(GabAttachment)
  implicit val gabCategoryDetailsFormat  = jsonFormat4(GabCategoryDetails)
  implicit val gabCategotyUserFormat     = jsonFormat10(GabUser)
  implicit val gabTopicFormat            = jsonFormat5(GabTopic)
  implicit val gabRepliesFormat          = jsonFormat1(GabReplies)

  implicit object GabPostJsonFormat extends RootJsonFormat[GabPost] {
    // TODO Writer method

    def getRawField(field :String)(implicit jsObj : JsObject) : JsValue = {
      jsObj.getFields(field).head
    }

    def getField(field : String)(implicit jsObj : JsObject) : String = {
      getRawField(field).toString
    }



    def write(p : GabPost) = JsString("TODO")

    def read(value: JsValue) = {
      implicit val jsObj = value.asJsObject

      new GabPost(
        getField("id").toLong,
        getField("created_at"),
        getField("revised_at"),
        getField("edited").toBoolean,
        getField("body"),
        getField("body_html"),
        getField("body_html_summary"),
        getField("body_html_summary_truncated").toBoolean,
        getField("only_emoji").toBoolean,
        getField("liked").toBoolean,
        getField("disliked").toBoolean,
        getField("bookmarked").toBoolean,
        getField("repost").toBoolean,
        getField("score").toInt,
        getField("like_count").toInt,
        getField("dislike_count").toInt,
        getField("reply_count").toInt,
        getField("repost_count").toInt,
        getField("is_quote").toBoolean,
        getField("is_reply").toBoolean,
        getField("is_replies_disabled").toBoolean,
        //getRawField("embed").convertTo[GabEmbed],
        //getRawField("attachment").convertTo[GabAttachment],
        getField("category").toInt,
        getRawField("category_details").convertTo[GabCategoryDetails],
        getField("language"),
        getField("nsfw").toBoolean,
        getField("is_premium").toBoolean,
        getField("is_locked").toBoolean,
        getRawField("user").convertTo[GabUser],
        getRawField("topic").convertTo[GabTopic]
      )
    }
  }

}
