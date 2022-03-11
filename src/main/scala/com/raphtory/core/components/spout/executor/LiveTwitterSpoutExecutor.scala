package com.raphtory.core.components.spout.executor

import com.raphtory.core.components.spout.SpoutExecutor
import com.raphtory.core.components.spout.executor.Util.filterRules
import com.raphtory.core.components.spout.executor.Util.hashtag
import com.raphtory.core.components.spout.executor.Util.twitterClient
import com.raphtory.core.components.spout.executor.Util.twitterEventListener
import com.raphtory.core.config.PulsarController
import com.raphtory.core.deploy.Raphtory
import com.raphtory.core.util.FileUtils.logger
import com.raphtory.serialisers.PulsarKryoSerialiser
import com.typesafe.config.Config
import io.github.redouane59.twitter.dto.tweet.Tweet
import io.github.redouane59.twitter.dto.tweet.TweetType
import io.github.redouane59.twitter.IAPIEventListener
import io.github.redouane59.twitter.TwitterClient
import io.github.redouane59.twitter.dto.rules.FilteredStreamRulePredicate
import io.github.redouane59.twitter.signature.TwitterCredentials
import monix.execution.Scheduler

class LiveTwitterSpoutExecutor(
    conf: Config,
    pulsarController: PulsarController,
    scheduler: Scheduler
) extends SpoutExecutor[Tweet](
                conf: Config,
                pulsarController: PulsarController,
                scheduler: Scheduler
        ) {

  def streamTweets() =
    //Filter Twitter stream if hashtag field in application.conf is not empty
    if (hashtag.nonEmpty) {
      filterRules()
      twitterClient.startFilteredStream(twitterEventListener(conf))
    }
    else
      twitterClient.startSampledStream(twitterEventListener(conf))

  override def run(): Unit = {
    logger.info("Starting Twitter Stream")
    streamTweets()
  }

  override def stop(): Unit = {
    logger.info("Stopping Twitter Stream")
    twitterClient.stopFilteredStream(streamTweets())
  }
}

object Util {
  val raphtoryConfig: Config = Raphtory.getDefaultConfig()
  val hashtag: String        = raphtoryConfig.getString("raphtory.spout.twitter.local.hashtag")
  val tag: String            = raphtoryConfig.getString("raphtory.spout.twitter.local.tag")

  val enableRetweetGraphBuilder: Boolean =
    raphtoryConfig.getBoolean("raphtory.spout.twitter.local.enableRetweetFilter")

  val getTweetLanguage: String =
    raphtoryConfig.getString("raphtory.spout.twitter.local.setLanguage")

  val twitterClient =
    try new TwitterClient(
            TwitterCredentials
              .builder()
              .accessToken(raphtoryConfig.getString("raphtory.spout.twitter.local.accessToken"))
              .accessTokenSecret(
                      raphtoryConfig.getString("raphtory.spout.twitter.local.accessTokenSecret")
              )
              .apiKey(raphtoryConfig.getString("raphtory.spout.twitter.local.apiKey"))
              .apiSecretKey(raphtoryConfig.getString("raphtory.spout.twitter.local.apiSecretKey"))
              .build()
    )
    catch {
      case e: Exception =>
        logger.error(
                s"Cannot connect to Twitter API, check your credentials, stopping application: $e"
        )
        System.exit(1)
        throw new RuntimeException(
                s"Cannot connect to Twitter API, check your credentials: $e"
        )
    }

  def filterRules() = {
    //retrieve filtered stream rules
    val rules = twitterClient.retrieveFilteredStreamRules()
    //Clear hashtag rules
    if (rules != null)
      rules.forEach { existingStreamRules =>
        twitterClient.deleteFilteredStreamRuleId(existingStreamRules.getId)
      }
    twitterClient.addFilteredStreamRule(FilteredStreamRulePredicate.withHashtag(hashtag), tag)
  }

  def twitterEventListener(conf: Config): IAPIEventListener = {
    val pulsarController           = new PulsarController(conf)
    val producer                   = pulsarController.toBuildersProducer()
    val kryo: PulsarKryoSerialiser = PulsarKryoSerialiser()

    val tweetType =
      if (enableRetweetGraphBuilder)
        TweetType.RETWEETED
      else
        TweetType.DEFAULT

    new IAPIEventListener {

      override def onStreamError(httpCode: Int, error: String): Unit =
        logger.error(s"Error: $error, Http Code: $httpCode")

      override def onTweetStreamed(tweet: Tweet): Unit = {
        val tweetLanguage =
          if (getTweetLanguage.nonEmpty)
            tweet.getLang == getTweetLanguage
          else tweet.getLang == tweet.getLang
        try
        //serialise tweet (Java Object) into Array bytes if tweet is in English and was retweet
        if (tweetLanguage && tweet.getTweetType.equals(tweetType))
          producer.sendAsync(kryo.serialise(tweet))
        catch {
          case e: Exception =>
            e.printStackTrace()
        }
      }

      override def onUnknownDataStreamed(json: String): Unit =
        logger.warn(s"Unknown: $json")

      override def onStreamEnded(e: Exception): Unit =
        logger.warn(s"Ended: ${e.getMessage}")
    }
  }

}
