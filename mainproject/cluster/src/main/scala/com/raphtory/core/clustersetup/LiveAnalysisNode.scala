package com.raphtory.core.clustersetup

/**
  * Created by Mirate on 29/09/2017.
  */
import akka.actor.{Actor, Props}
import com.raphtory.core.analysis.AnalysisManager.LiveAnalysisManager

case class LiveAnalysisNode(seedLoc: String, name:String)
    extends DocSvr {
  implicit val system = init(List(seedLoc))


  val jobID = sys.env.getOrElse("JOBID", "Default").toString
  sys.env.getOrElse("LAMTYPE", "LAM").toString match {
    case "LAM" => { // live graph
      val cons = Class.forName(name).getConstructor(classOf[String])
      system.actorOf(Props(cons.newInstance(jobID).asInstanceOf[Actor]), s"LiveAnalysisManager_$name")
    }
    case "VAM" => { //view of the graph
      val time = sys.env.getOrElse("TIMESTAMP", "0").toLong
      val cons = Class.forName(name).getConstructor(classOf[String],classOf[Long])
      system.actorOf(Props(cons.newInstance(jobID,time.asInstanceOf[AnyRef]).asInstanceOf[Actor]), s"LiveAnalysisManager_$name")
    }
    case "RAM" => { //range query through history
      val start = sys.env.getOrElse("START", "0").toLong
      val end = sys.env.getOrElse("END", "0").toLong
      val jump = sys.env.getOrElse("JUMP", "0").toLong
      val cons = Class.forName(name).getConstructor(classOf[String],classOf[Long],classOf[Long],classOf[Long])
      system.actorOf(Props(cons.newInstance(jobID,start.asInstanceOf[AnyRef],end.asInstanceOf[AnyRef],jump.asInstanceOf[AnyRef]).asInstanceOf[Actor]), s"LiveAnalysisManager_$name")
    }
    case "WAM" => { // windowed range query through history
      val start = sys.env.getOrElse("START", "0").toLong
      val end = sys.env.getOrElse("END", "0").toLong
      val jump = sys.env.getOrElse("JUMP", "0").toLong
      val window = sys.env.getOrElse("WINDOW", "0").toLong
      val cons = Class.forName(name).getConstructor(classOf[String],classOf[Long],classOf[Long],classOf[Long],classOf[Long])
      system.actorOf(Props(cons.newInstance(jobID,start.asInstanceOf[AnyRef],end.asInstanceOf[AnyRef],jump.asInstanceOf[AnyRef],window.asInstanceOf[AnyRef]).asInstanceOf[Actor]), s"LiveAnalysisManager_$name")
    }

  }





  //system.actorOf(Props(Class.forName(name)), s"LiveAnalysisManager_$name")
}
