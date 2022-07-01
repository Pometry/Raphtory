package rawgraphmodel

case class Tweet (
                   author_id: Option[Long],
                   conversation_id: Option[Long],
                   created_at: Option[Long],
                   id: Option[Long],
                   in_reply_to_user_id: Option[Long],
                   lang: Option[String],
                   public_metrics: Option[Public_Metrics],
                   //                   referenced_tweets: Option[Seq[Referenced_Tweet]],
                   source: Option[String],
                   text: Option[String]
                 )

case class Public_Metrics (
                            retweet_count: Option[Int],
                            reply_count: Option[Int],
                            like_count: Option[Int],
                            quote_count: Option[Int]
                          )

case class Referenced_Tweet (
                              reference_type: Option[String],
                              id: Option[Long]
                            )



