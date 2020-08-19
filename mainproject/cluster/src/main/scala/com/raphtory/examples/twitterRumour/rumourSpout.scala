package com.raphtory.examples.twitterRumour

import akka.actor.ActorContext
import com.raphtory.core.components.Spout.SpoutTrait

import scala.concurrent.duration.Duration
import scala.concurrent.duration.MILLISECONDS
import scala.concurrent.duration.SECONDS
import java.io.File

class rumourSpout extends SpoutTrait {
  var directory = System.getenv().getOrDefault("PHEME_DIRECTORY", "/home/tsunade/qmul/datasets/pheme-rnr-dataset").trim
  var r_case    = System.getenv().getOrDefault("RUMOUR_CASE", "/sydneysiege").trim
  val rpath     = directory + r_case + "/rumours"
  val nrpath    = directory + r_case + "/non-rumours"
  val max_r     = 500 //Option(new File(rpath).list.size).getOrElse(0)
  val max_nr    = 500 //Option(new File(nrpath).list.size).getOrElse(0)

  var rtweetCounter  = 0
  var nrtweetCounter = 0

  override protected def ProcessSpoutTask(message: Any): Unit =
    // println(message)
    if ((rtweetCounter < max_r) || (nrtweetCounter < max_nr))
      //println("inside if stm")
      message match {
        case StartSpout =>
          AllocateSpoutTask(Duration(1, SECONDS), "rumourTweets")
          AllocateSpoutTask(Duration(1, SECONDS), "nonRumourTweets")
        case "rumourTweets" =>
          if (rtweetCounter < max_r) {
            //     println("sending ingest cmd r...")
            running(rpath, position = rtweetCounter, "R__")
            rtweetCounter += 1
            context.system.scheduler.scheduleOnce(Duration(1, SECONDS), self, "rumourTweets")
          }
        case "nonRumourTweets" =>
          if (nrtweetCounter < max_nr) {
            //  println("sending ingest cmd nr...")
            running(nrpath, position = nrtweetCounter, "nR__")
            nrtweetCounter += 1
            context.system.scheduler.scheduleOnce(Duration(1, SECONDS), self, "nonRumourTweets")
          }
        case _ => println("message not recognized!")
      }

  def ingestTweet(path: String, rnr: String): Unit = {
    //  println("inside ingest....")
    val tweet_l = new File(path)
    if (tweet_l.exists && tweet_l.isDirectory)
      tweet_l.listFiles.filter(_.isFile).toList.map(_.toString).foreach { tw_path: String =>
        sendTuple(rnr + tw_path)
      // println("running sendTuple...")
      }
  }

  def running(rpath: String, position: Int, rnr: String): Unit = {
    val rumours = new File(rpath)
    // println("inside runnign...")

    if (rumours.exists && rumours.isDirectory) {

      //println("inside runnign if...")
      val r_tweet = rumours.listFiles.toList.map(_.toString)
      ingestTweet(r_tweet(position) + "/source-tweet", rnr)
      ingestTweet(r_tweet(position) + "/reactions", rnr)
    }
  }
}
