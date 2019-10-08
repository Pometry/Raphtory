package com.raphtory.core.clustersetup

/**
  * Created by Mirate on 29/09/2017.
  */
import akka.actor.{Actor, Props}
import com.raphtory.core.analysis.API.Analyser
import com.raphtory.core.analysis.Managers.RangeManagers.{BWindowedRangeAnalysisManager, RangeAnalysisManager, WindowedRangeAnalysisManager}
import com.raphtory.core.analysis.Managers.ViewManagers.{BWindowedViewAnalysisManager, ViewAnalysisManager, WindowedViewAnalysisManager}
import com.raphtory.core.analysis.Managers.LiveManagers.{BWindowedLiveAnalysisManager, LiveAnalysisManager, WindowedLiveAnalysisManager}

case class LiveAnalysisNode(seedLoc: String, name:String)
    extends DocSvr {
  implicit val system = init(List(seedLoc))


  val jobID = sys.env.getOrElse("JOBID", "Default").toString
  val analyser = Class.forName(name).newInstance().asInstanceOf[Analyser]

  sys.env.getOrElse("LAMTYPE", "Live").toString match {
    case "Live" => { // live graph
      sys.env.getOrElse("WINDOWTYPE", "false") match {
        case "false" => {
          system.actorOf(Props(new LiveAnalysisManager(jobID,analyser)), s"LiveAnalysisManager_$name")
        }
        case "true" => {
          system.actorOf(Props(new WindowedLiveAnalysisManager(jobID,analyser)), s"LiveAnalysisManager_$name")
        }
        case "batched" => {
          system.actorOf(Props(new BWindowedLiveAnalysisManager(jobID,analyser)), s"LiveAnalysisManager_$name")
        }
      }
    }
    case "View" => { //view of the graph
      val time = sys.env.getOrElse("TIMESTAMP", "0").toLong
      sys.env.getOrElse("WINDOWTYPE", "false") match {
        case "false" => {
          system.actorOf(Props(new ViewAnalysisManager(jobID,analyser,time)), s"ViewAnalysisManager_$name")
        }
        case "true" => {
          val window = sys.env.getOrElse("WINDOW", "0").toLong
          system.actorOf(Props(new WindowedViewAnalysisManager(jobID,analyser,time,window)), s"ViewAnalysisManager_$name")
        }
        case "batched" => {
          val windowset = sys.env.getOrElse("WINDOWSET", "0").split(",").map(f=>f.toLong)
          system.actorOf(Props(new BWindowedViewAnalysisManager(jobID,analyser,time,windowset)), s"ViewAnalysisManager_$name")        }
      }
    }
    case "Range" => { // windowed range query through history
      val start = sys.env.getOrElse("START", "0").toLong
      val end = sys.env.getOrElse("END", "0").toLong
      val jump = sys.env.getOrElse("JUMP", "0").toLong
      sys.env.getOrElse("WINDOWTYPE", "false") match {
        case "false" => {
          system.actorOf(Props(new RangeAnalysisManager(jobID,analyser,start,end,jump)), s"RangeAnalysisManager_$name")
        }
        case "true" => {
          val window = sys.env.getOrElse("WINDOW", "0").toLong
          system.actorOf(Props(new WindowedRangeAnalysisManager(jobID,analyser,start,end,jump,window)), s"RangeAnalysisManager_$name")
        }
        case "batched" => {
          val windowset = sys.env.getOrElse("WINDOWSET", "0").split(",").map(f=>f.toLong)
          system.actorOf(Props(new BWindowedRangeAnalysisManager(jobID,analyser,start,end,jump,windowset)), s"RangeAnalysisManager_$name")        }
      }
    }

  }





  //system.actorOf(Props(Class.forName(name)), s"LiveAnalysisManager_$name")
}
