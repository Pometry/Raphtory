package com.raphtory.examples.twitterRumour

import java.text.SimpleDateFormat

import com.raphtory.core.components.Router.RouterWorker
import com.raphtory.core.model.communication._
import spray.json._

import scala.io.Source

class rumourInteractRouter(val routerId:Int, val initialManagerCount:Int)  extends RouterWorker{

  override protected def parseTuple(cmd: Any): Unit = {
    //println("im at router top...")
    val List(r_status, tweet) = cmd.asInstanceOf[String].split("__").toList
    val json = Source.fromFile(tweet)
    for (line <- json.getLines){
     // println("reading json"+cmd)
      var user = line.parseJson.asJsObject.fields("user").asJsObject
      val post = line.parseJson.asJsObject

      val replyTime = post.fields("created_at").toString.toString.split("\"")(1)
      val source = user.fields("id").toString
//      if (source.toLong <0){println("this is converting worng.."+source)
//      sys.exit()}
      val dist = post.fields("in_reply_to_user_id").toString

      if (dist!="null"){
        sendGraphUpdate(EdgeAddWithProperties(getTwitterDate(replyTime), source.toLong, dist.toLong,properties = Properties(ImmutableProperty("rumourStatus",r_status))))
      }else{
        sendGraphUpdate(VertexAddWithProperties(getTwitterDate(replyTime), source.toLong, properties = Properties(ImmutableProperty("rumourStatus",r_status))))
      }
    }
  }

  def getTwitterDate(date:String): Long = {
   // println(">>> converting time...")
    val twitter = "EEE MMM dd HH:mm:ss ZZZZZ yyyy"
    val sf = new SimpleDateFormat(twitter)
    //println(date)
    try{
    //println("converted time ///"+t)
      return sf.parse(date).getTime()
    } catch{
      case e : Throwable => println("-----time not properly converting"+date)
        sys.exit()
        return 0
    }

  }
}
