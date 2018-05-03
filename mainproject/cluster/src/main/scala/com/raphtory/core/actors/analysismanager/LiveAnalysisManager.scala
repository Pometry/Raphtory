package com.raphtory.core.actors.analysismanager

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class LiveAnalysisManager() extends LiveAnalyser {

  override protected def processResults(result: Any): Unit = println(result)

  override def preStart() {
    println("Prestarting")
    context.system.scheduler.schedule(Duration(10, SECONDS),Duration(10, SECONDS),self,"analyse")
  }

}

