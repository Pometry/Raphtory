package com.raphtory.RawGraphsModels

case class GabPost (
  id : Long,
  created_at : String,
  revised_at : String,
  edit : Boolean,
  body : String,
  body_html : String,
  only_emojy : Boolean,
  score : Int,
  like_count : Int,
  dislike_count : Int,
  reply_count : Int,
  repost_count : Int,
  is_quote : Boolean,
  is_reply : Boolean,
  is_replies_disabled : Boolean,
  embed : GabEmbed,
  attachment : GabAttachment,
  category : Int,
  categoryDetails: GabCategoryDetails,
  user : GabUser,
  topic : GabTopic,
)

case class GabEmbed(
  html : String,
  iframe : String
)

case class GabAttachment(
  type_ : String,
  value : Seq[GabMedia]
)

case class GabMedia(
  id : String,
  url_thumbnail : String,
  url_full : String,
  width : Int,
  height : Int
  )
case class GabCategoryDetails(
  title : String,
  slug : String,
  value : Int,
  emojy : String
)

case class GabUser(
  id : Int,
  name : String,
  user_name : String,
  picture_url : String,
  verified : Boolean,
  is_donor : Boolean,
  is_investor : Boolean,
  is_pro : Boolean,
  is_private : Boolean,
  is_premium : Boolean
)

case class GabTopic (
  id : String,
  created_at : String,
  is_featured : Boolean,
  title : String,
  category : String
)

case class GabReplies (
  data : String
)
