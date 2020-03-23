package com.raphtory.examples.twitterRumour

import java.text.SimpleDateFormat

import com.raphtory.core.components.Router.RouterWorker
import com.raphtory.core.model.communication._
import spray.json._

import scala.io.Source

class rumourInteractRouter(val routerId:Int, val initialManagerCount:Int)  extends RouterWorker{

  override protected def parseTuple(cmd: Any): Unit = {
    val List(r_status, tweet) = cmd.asInstanceOf[String].split("__").toList
    val json = Source.fromFile(tweet)
    for (line <- json.getLines){
      var user = line.parseJson.asJsObject.fields("user").asJsObject
      val post = line.parseJson.asJsObject

      val replyTime = post.fields("created_at").toString
      val source = user.fields("id").toString.toLong
      val dist = post.fields("in_reply_to_user_id").toString

      if (dist!="null"){
        sendGraphUpdate(EdgeAddWithProperties(getTwitterDate(replyTime), source, dist.toLong,properties = Properties(ImmutableProperty("rumourStatus",r_status))))
      }else{
        sendGraphUpdate(VertexAddWithProperties(getTwitterDate(replyTime), source, properties = Properties(ImmutableProperty("rumourStatus",r_status))))
      }
    }
  }

  def getTwitterDate(date:String): Long = {
    val twitter = "EEE MMM dd HH:mm:ss ZZZZZ yyyy"
    val sf = new SimpleDateFormat(twitter)
    //println("checking date conversion"+sf.parse(date).getTime())
    return sf.parse(date).getTime()
  }
}
