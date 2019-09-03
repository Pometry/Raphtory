package com.raphtory.core.clustersetup

/**
  * Created by Mirate on 29/09/2017.
  */
import akka.actor.{Actor, Props}
import com.raphtory.core.analysis.Analyser
import com.raphtory.core.analysis.Managers.RangeManagers.{RangeAnalysisManager, WindowedRangeAnalysisManager}
import com.raphtory.core.analysis.Managers.ViewManagers.ViewAnalysisManager
import com.raphtory.core.analysis.Managers.AnalysisManager

case class LiveAnalysisNode(seedLoc: String, name:String)
    extends DocSvr {
  implicit val system = init(List(seedLoc))


  val jobID = sys.env.getOrElse("JOBID", "Default").toString
  val analyser = Class.forName(name).newInstance().asInstanceOf[Analyser]

  sys.env.getOrElse("LAMTYPE", "LAM").toString match {
    case "LAM" => { // live graph
      system.actorOf(Props(new AnalysisManager(jobID,analyser)), s"LiveAnalysisManager_$name")
    }
    case "VAM" => { //view of the graph
      val time = sys.env.getOrElse("TIMESTAMP", "0").toLong
      system.actorOf(Props(new ViewAnalysisManager(jobID,analyser,time)), s"ViewAnalysisManager_$name")
    }
    case "RAM" => { //range query through history
      val start = sys.env.getOrElse("START", "0").toLong
      val end = sys.env.getOrElse("END", "0").toLong
      val jump = sys.env.getOrElse("JUMP", "0").toLong
      system.actorOf(Props(new RangeAnalysisManager(jobID,analyser,start,end,jump)), s"RangeAnalysisManager_$name")
    }
    case "WAM" => { // windowed range query through history
      val start = sys.env.getOrElse("START", "0").toLong
      val end = sys.env.getOrElse("END", "0").toLong
      val jump = sys.env.getOrElse("JUMP", "0").toLong
      val window = sys.env.getOrElse("WINDOW", "0").toLong
      system.actorOf(Props(new WindowedRangeAnalysisManager(jobID,analyser,start,end,jump,window)), s"WindowAnalysisManager_$name")
    }

  }





  //system.actorOf(Props(Class.forName(name)), s"LiveAnalysisManager_$name")
}
