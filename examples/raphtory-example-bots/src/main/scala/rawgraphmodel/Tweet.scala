package rawgraphmodel

case class Tweet (
                   author_id: Long,
                   conversation_id: Long,
                   created_at: Option[String],
                   id: Long,
                   in_reply_to_user_id: Option[Long],
                   lang: String,
                   public_metrics: Public_Metrics,
                   referenced_tweets: Option[Seq[Referenced_Tweet]],
                   source: String,
                   text: String
                     )

case class Public_Metrics (
                         retweet_count: Option[Int],
                         reply_count: Option[Int],
                         like_count: Option[Int],
                         quote_count: Option[Int]
                          )

case class Referenced_Tweet (
                             reference_type: String,
                             id: Long
                             )

