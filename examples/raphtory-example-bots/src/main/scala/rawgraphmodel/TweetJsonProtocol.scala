package rawgraphmodel

import spray.json._

object TweetJsonProtocol extends DefaultJsonProtocol {

  implicit val public_MetricFormat   = jsonFormat4(Public_Metrics)

  implicit object TweetJsonFormat extends RootJsonFormat[Tweet] {
    // TODO Writer method

    def getRawField(field: String)(implicit jsObj: JsObject): Option[JsValue] =
      jsObj.getFields(field).headOption

    def getField(field: String)(implicit jsObj: JsObject): Option[String] =
      getRawField(field) match {
        case Some(s) => Some(s.toString())
        case None    => None
      }

    def getBoolean(field: String)(implicit jsObj: JsObject): Option[Boolean] =
      getField(field) match {
        case Some(s) => Some(s.toBoolean)
        case None    => None
      }

    def getInt(field: String)(implicit jsObj: JsObject): Option[Int] =
      getField(field) match {
        case Some(s) => Some(s.toInt)
        case None    => None
      }

    def getLong(field: String)(implicit jsObj: JsObject): Option[Long] =
      getField(field) match {
        case Some(s) => Some(s.toLong)
        case None    => None
      }

    def write(p: Tweet) = JsString("TODO")

    def read(value: JsValue) = {
      implicit val jsObj = value.asJsObject

      new Tweet(
        getLong("author_id"),
        getLong("conversation_id"),
        getField("created_at") match {
          case Some(s) => Some(s.replaceAll("\"", ""))
          case None    => None
        },
        getLong("id"),
        getLong("in_reply_to_user"),
        getField("lang"),
        getRawField("public_metrics") match {
          case Some(p) => Some(p.convertTo[Public_Metrics])
          case None    => None //Figure this shit out
        },
        getRawField("referenced_tweets") match {
          case Some(r) => Some(r.convertTo[Referenced_Tweet]) //Figure out how to do possible list
          case None    => None
        },
        getField("source"),
        getField("text")
      )
    }
  }
}
