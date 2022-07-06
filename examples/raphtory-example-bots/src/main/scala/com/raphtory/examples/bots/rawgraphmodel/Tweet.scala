package com.raphtory.examples.bots.rawgraphmodel

case class Tweet (
                   author_id: Option[Long],
                   conversation_id: Option[Long],
                   created_at: Option[Long],
                   id: Option[String],
                   in_reply_to_user_id: Option[Long],
                   lang: Option[String],
                   public_metrics: Option[Public_Metrics],
                   referenced_tweets: Option[String],
                   source: Option[String],
                   text: Option[String],
                   //                   parent: Option[Tweet]
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




