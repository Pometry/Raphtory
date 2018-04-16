import com.danielasfregola.twitter4s.{TwitterRestClient, TwitterStreamingClient}
import com.danielasfregola.twitter4s.entities.{AccessToken, ConsumerToken, Tweet}


object StreamingExample extends App {

  val consumerToken = ConsumerToken(key = args(0), secret = args(1))
  val accessToken = AccessToken(key = args(2), secret = args(3))

  val restClient = TwitterRestClient(consumerToken, accessToken)
  val streamingClient = TwitterStreamingClient(consumerToken, accessToken)
  val trackedWords = Seq("#scala", "#functionalprogramming")

  streamingClient.filterStatuses(tracks = trackedWords) {
    case tweet: Tweet => println(tweet.text)
  }

}
