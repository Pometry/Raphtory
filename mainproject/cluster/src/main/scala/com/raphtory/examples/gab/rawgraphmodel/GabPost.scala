package com.raphtory.examples.gab.rawgraphmodel

case class GabPost(
    id: Option[Long],
    created_at: Option[String],
    revised_at: Option[String],
    edit: Option[Boolean],
    /*body : Option[String],
  body_html : Option[String],
  body_html_summary : Option[String],
  body_html_summary_trucated : Option[Boolean],*/
    only_emojy: Option[Boolean],
    liked: Option[Boolean],
    disliked: Option[Boolean],
    bookmarked: Option[Boolean],
    repost: Option[Boolean],
    score: Option[Int],
    like_count: Option[Int],
    dislike_count: Option[Int],
    reply_count: Option[Int],
    repost_count: Option[Int],
    is_quote: Option[Boolean],
    is_reply: Option[Boolean],
    is_replies_disabled: Option[Boolean],
    //embed : Option[GabEmbed],
    //attachment : Option[GabAttachment],
    //category : Option[Int],
    //categoryDetails: Option[GabCategoryDetails],
    language: Option[String],
    nsfw: Option[Boolean],
    is_premium: Option[Boolean],
    is_locked: Option[Boolean],
    user: Option[GabUser],
    topic: Option[GabTopic],
    parent: Option[GabPost]
)

case class GabEmbed(
    html: Option[String],
    iframe: Option[String]
)

case class GabAttachment(
    type_ : String,
    value: Seq[GabMedia]
)

case class GabMedia(
    id: String,
    url_thumbnail: String,
    url_full: String,
    width: Int,
    height: Int
)
case class GabCategoryDetails(
    title: String,
    slug: String,
    value: Int,
    emoji: String
)

case class GabUser(
    id: Int,
    name: String,
    username: String,
    picture_url: String,
    verified: Boolean,
    is_donor: Boolean,
    is_investor: Boolean,
    is_pro: Boolean,
    is_private: Boolean,
    is_premium: Boolean
)

case class GabTopic(
    id: String,
    created_at: String,
    is_featured: Option[Boolean],
    title: Option[String],
    category: Option[Long]
)

case class GabReplies(
    data: String
)
