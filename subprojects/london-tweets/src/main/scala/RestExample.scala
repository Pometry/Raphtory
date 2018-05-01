import com.danielasfregola.twitter4s.TwitterRestClient
import scala.concurrent.ExecutionContext.Implicits.global


object RestExample extends App {

  // Make sure to define the following env variables:
  // TWITTER_CONSUMER_TOKEN_KEY and TWITTER_CONSUMER_TOKEN_SECRET
  // TWITTER_ACCESS_TOKEN_KEY and TWITTER_ACCESS_TOKEN_SECRET
  val restClient = TwitterRestClient()

  restClient.homeTimeline().map { ratedData =>
    val tweets = ratedData.data
    tweets.foreach(tweet => println(tweet.text))
  }

}
