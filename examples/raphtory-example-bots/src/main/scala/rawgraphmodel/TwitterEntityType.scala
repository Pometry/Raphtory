package rawgraphmodel

object TwitterEntityType extends Enumeration {
  val user: Value  = Value("user")
  val tweet: Value  = Value("tweet")
  val list: Value = Value("list")
  val hashtag: Value = Value("hashtag")
}
