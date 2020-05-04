package com.raphtory.examples.gab.rawgraphmodel

import spray.json._

object GabJsonProtocol extends DefaultJsonProtocol {

  implicit val gabEmbedFormat           = jsonFormat2(GabEmbed)
  implicit val gabMediaFormat           = jsonFormat5(GabMedia)
  implicit val gabAttachmentFormat      = jsonFormat2(GabAttachment)
  implicit val gabCategoryDetailsFormat = jsonFormat4(GabCategoryDetails)
  implicit val gabCategotyUserFormat    = jsonFormat10(GabUser)
  implicit val gabTopicFormat           = jsonFormat5(GabTopic)
  implicit val gabRepliesFormat         = jsonFormat1(GabReplies)

  implicit object GabPostJsonFormat extends RootJsonFormat[GabPost] {
    // TODO Writer method

    def getRawField(field: String)(implicit jsObj: JsObject): Option[JsValue] =
      jsObj.getFields(field).headOption

    def getField(field: String)(implicit jsObj: JsObject): Option[String] =
      getRawField(field) match {
        case Some(s) => Some(s.toString())
        case None    => None
      }

    def getBoolean(field: String)(implicit jsObj: JsObject): Option[Boolean] =
      getField(field) match {
        case Some(s) => Some(s.toBoolean)
        case None    => None
      }

    def getInt(field: String)(implicit jsObj: JsObject): Option[Int] =
      getField(field) match {
        case Some(s) => Some(s.toInt)
        case None    => None
      }

    def getLong(field: String)(implicit jsObj: JsObject): Option[Long] =
      getField(field) match {
        case Some(s) => Some(s.toLong)
        case None    => None
      }

    def write(p: GabPost) = JsString("TODO")

    def read(value: JsValue) = {
      implicit val jsObj = value.asJsObject

      new GabPost(
              getLong("id"),
              getField("created_at") match {
                case Some(s) => Some(s.replaceAll("\"", ""))
                case None    => None
              },
              getField("revised_at") match {
                case Some(s) => Some(s.replaceAll("\"", ""))
                case None    => None
              },
              getBoolean("edited"),
              /*getField("body"),
        getField("body_html"),
        getField("body_html_summary"),
        getField("body_html_summary_truncated").toBoolean,*/
              getBoolean("only_emoji"),
              getBoolean("liked"),
              getBoolean("disliked"),
              getBoolean("bookmarked"),
              getBoolean("repost"),
              getInt("score"),
              getInt("like_count"),
              getInt("dislike_count"),
              getInt("reply_count"),
              getInt("repost_count"),
              getBoolean("is_quote"),
              getBoolean("is_reply"),
              getBoolean("is_replies_disabled"),
              //getRawField("embed").convertTo[GabEmbed],
              //getRawField("attachment").convertTo[GabAttachment],
              //getInt("category"),
              /*tRawField("category_details") match {
          case Some(c) => Some(c.convertTo[GabCategoryDetails])
          case None    => None
        },*/
              getField("language"),
              getBoolean("nsfw"),
              getBoolean("is_premium"),
              getBoolean("is_locked"),
              getRawField("user") match {
                case Some(u) => Some(u.convertTo[GabUser])
                case None    => None
              },
              getRawField("topic") match {
                case Some(t) => Some(t.convertTo[GabTopic])
                case None    => None
              },
              getRawField("parent") match {
                case Some(p) => Some(p.convertTo[GabPost])
                case None    => None
              }
      )
    }
  }

}
