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
    def write(p : GabPost) = JsString("TODO")

    def read(value: JsValue) = {
      value.asJsObject.getFields("id", "created_at", "revised_at", "edited", "body", "body_html", "body_html_summary",
        "body_html_summary_truncated", "only_emoji", "score",
        "like_count", "dislike_count", "reply_count", "repost_count", "is_quote", "is_reply", "is_replies_disabled",
        "embed", "attachment", "category", "category_details", "language", "nsfw", "is_premium", "is_locked", "user",
        "topic", "replies") match {
        case Seq(JsNumber(id), JsString(createdAt), JsString(revisedAt), JsBoolean(edit), JsString(body),
        JsString(bodyHtml), JsBoolean(onlyEmoji),
        JsNumber(score),
        JsNumber(likeCount), JsNumber(dislikeCount), JsNumber(repliesCount), JsNumber(repostCount), JsBoolean(isQuote),
        JsBoolean(isReply), JsBoolean(isRepliesDisabled), embed, attachment, JsNumber(category),
        categoryDetails,
        user, topic) => {
          embed
          new GabPost(
            id.toLong, createdAt, revisedAt, edit, body,
            bodyHtml, onlyEmoji,
            score.toInt,
            likeCount.toInt, dislikeCount.toInt, repliesCount.toInt, repostCount.toInt, isQuote,
            isReply, isRepliesDisabled, embed.convertTo[GabEmbed], attachment.convertTo[GabAttachment], category.toInt,
            categoryDetails.convertTo[GabCategoryDetails],
            user.convertTo[GabUser], topic.convertTo[GabTopic])
        }
      }
    }
  }

}
