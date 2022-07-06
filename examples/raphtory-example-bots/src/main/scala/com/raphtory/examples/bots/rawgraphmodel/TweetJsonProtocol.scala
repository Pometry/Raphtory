package com.raphtory.examples.bots.rawgraphmodel

import com.raphtory.examples.bots.rawgraphmodel.{Public_Metrics, Tweet}
import spray.json._

object TweetJsonProtocol extends DefaultJsonProtocol {

  implicit val public_MetricFormat   = jsonFormat4(Public_Metrics)

  implicit object TweetJsonFormat extends RootJsonFormat[Tweet] {

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

    def write(p: Tweet) = JsString("TODO")

    def getLong(field: String)(implicit jsObj: JsObject): Option[Long] =
      getField(field) match {
        case Some(s) => Some(s.toLong)
        case None    => None
      }

    def read(value: JsValue) = {
      implicit val jsObj = value.asJsObject

      new Tweet(
        getLong("author_id"),
        getLong("conversation_id"),
        getLong("created_at"),
        //        getField("created_at") match {
        //          case Some(s) => Some(s.replaceAll("\"", ""))
        //          case None    => None
        //        },
        getField("id"),
        getRawField("in_reply_to_user_id") match {
          case Some(i) =>
            if (i == JsNull) {
              Some(0L)
            }
            else {
              Some(i.convertTo[Long])
            }
          case None    => None

        },
        getField("lang"),
        getRawField("public_metrics") match {
          case Some(p) => Some(p.convertTo[Public_Metrics])
          case None    => None
        },
        getField("referenced_tweets"),
        //        getRawField("referenced_tweets") match {
        //          case Some(r) => Some(r.convertTo[Referenced_Tweet])
        //          case None    => None
        //        },
        getField("source"),
        getField("text"),
        //        getRawField("parent") match {
        //          case Some(p) => Some(p.convertTo[Tweet])
        //          case None    => None
        //        }
      )
    }
  }
}
