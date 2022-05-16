package com.raphtory.twitter

import com.raphtory.components.spout.Spout
import com.raphtory.deployment.Raphtory
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import io.github.redouane59.twitter.IAPIEventListener
import io.github.redouane59.twitter.TwitterClient
import io.github.redouane59.twitter.dto.rules.FilteredStreamRulePredicate
import io.github.redouane59.twitter.dto.tweet.Tweet
import io.github.redouane59.twitter.dto.tweet.TweetType
import io.github.redouane59.twitter.signature.TwitterCredentials
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentLinkedQueue

/** Reads in data from the Twitter API
 *
 * The Live Twitter Spout uses Twitter credentials
 * that can be entered into application.conf, and this builds a Twitter Client
 * to be used as an entry point for the Twitter data. You will need to have a
 * Twitter Developer account and enter your API Key, API secret key, access token,
 * and secret access token, which can all be found in the Twitter Developer Portal.
 *
 * This Twitter Event Listener detects the tweets coming through and adds this
 * to tweetQueue (a concurrent linked queue which is thread-safe).
 * Whilst the tweets are being added, it filters for language (which can be set in application.conf)
 * and retweets only if the retweet filter is on.
 *
 * A filter for hashtag can also be added in application.conf as well.
 */
class LiveTwitterSpout() extends Spout[Tweet] {

  val tweetQueue = new ConcurrentLinkedQueue[Tweet]()
  val spout      = new LiveTwitterAddSpout(tweetQueue)

  override def close(): Unit = System.exit(1)

  override def spoutReschedules(): Boolean = true

  override def hasNext(): Boolean =
    !tweetQueue.isEmpty //always true as we are waiting for new tweets

  override def next(): Tweet = tweetQueue.poll()
}

class LiveTwitterAddSpout(tweetQueue: ConcurrentLinkedQueue[Tweet]) {
  val logger: Logger         = Logger(LoggerFactory.getLogger(this.getClass))
  val raphtoryConfig: Config = Raphtory.getDefaultConfig()
  val hashtag: String        = raphtoryConfig.getString("raphtory.spout.twitter.local.hashtag")
  val tag: String            = raphtoryConfig.getString("raphtory.spout.twitter.local.tag")
println(hashtag)
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

  def filterRules() =
    if (hashtag.nonEmpty) {
      //retrieve filtered stream rules
      val rules = twitterClient.retrieveFilteredStreamRules()
      //Clear hashtag rules
      if (rules != null)
        rules.forEach { existingStreamRules =>
          twitterClient.deleteFilteredStreamRuleId(existingStreamRules.getId)
        }
      twitterClient.addFilteredStreamRule(FilteredStreamRulePredicate.withHashtag(hashtag), tag)
    }

  def twitterEventListener(): IAPIEventListener =
    new IAPIEventListener {

      override def onStreamError(httpCode: Int, error: String): Unit =
        logger.error(s"Error: $error, Http Code: $httpCode")

      override def onTweetStreamed(tweet: Tweet): Unit = {

        def postIfThisLanguage: Boolean =
          getTweetLanguage.nonEmpty match {
            case true => tweet.getLang == getTweetLanguage
            case _    => tweet.getLang == tweet.getLang
          }

        def getRetweet: Boolean =
          enableRetweetGraphBuilder match {
            case true => tweet.getTweetType.equals(TweetType.RETWEETED)
            case _    => tweet.getTweetType.equals(TweetType.DEFAULT)
          }

        try if (postIfThisLanguage && getRetweet) {
          filterRules()
          tweetQueue.add(tweet)
        }
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

