package com.raphtory.examples.twitterRumour

import akka.actor.ActorContext
import com.raphtory.core.components.Spout.SpoutTrait

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{Duration, MILLISECONDS, SECONDS}
import java.io.File

class rumourSpout extends SpoutTrait {
  var directory = System.getenv().getOrDefault("PHEME_DIRECTORY", "/home/tsunade/qmul/pheme-rnr-dataset").trim
  var r_case = System.getenv().getOrDefault("RUMOUR_CASE", "/sydneysiege").trim
  val rpath = directory + r_case + "/rumours"
  val nrpath = directory + r_case + "/non-rumours"
  val max_r = 500//Option(new File(rpath).list.size).getOrElse(0)
  val max_nr = 500//Option(new File(nrpath).list.size).getOrElse(0)

  var rtweetCounter = 0
  var nrtweetCounter = 0
println("at spout....")
  override def preStart(): Unit = {
    super.preStart()
    println("checking prestart...")
    AllocateSpoutTask(Duration(1, SECONDS), "rumourTweets")
    println("send message rumour...")
    AllocateSpoutTask(Duration(1, SECONDS), "nonRumourTweets")
  }

  def ingestTweet(path: String, rnr: String): Unit = {
    val tweet_l = new File(path)
    if (tweet_l.exists && tweet_l.isDirectory) {
      tweet_l.listFiles.filter(_.isFile).toList.map(_.toString).foreach { tw_path: String =>
        sendTuple(rnr + tw_path)
      }
    }
  }

  override protected def ProcessSpoutTask(message: Any): Unit = {
    println("processing spout task...")
    println(message)
    if ((rtweetCounter < max_r) || (nrtweetCounter < max_nr)) {
     // if (isSafe()) {
        message match {
          case "rumourTweets" => {
            if (rtweetCounter < max_r){
              println("ingesting rumour ....")
              running(rpath, position = rtweetCounter, "R__")
              rtweetCounter += 1
              context.system.scheduler.scheduleOnce(Duration(1, SECONDS), self, "rumourTweets")
            }
          }
          case "nonRumourTweets" => {
            if (nrtweetCounter < max_nr) {
              println("ingesting non-rumour...")
              running(nrpath, position = nrtweetCounter, "nR__")
              nrtweetCounter += 1
              context.system.scheduler.scheduleOnce(Duration(1, SECONDS), self, "nonRumourTweets")
            }
          }
          case _ => println("message not recognized!")
        }
      }
//    } else {
//      stop()
//    }
  }

  def running(rpath: String, position:Int, rnr: String): Unit = {
    val rumours = new File(rpath)
    if (rumours.exists && rumours.isDirectory) {
      val r_tweet = rumours.listFiles.toList.map(_.toString)
      ingestTweet(r_tweet(position) + "/source-tweet", rnr)
      ingestTweet(r_tweet(position) + "/reactions", rnr)
    }
  }
}