



package com.raphtory.examples.gabMining.actors
import java.text.SimpleDateFormat
import java.util.Date

import com.raphtory.core.analysis.Analyser
import com.raphtory.core.components.AnalysisManager.{LiveAnalysisManager, RangeAnalysisManager, ViewAnalysisManager, WindowRangeAnalysisManager}
import com.raphtory.core.utils.Utils
import com.raphtory.examples.gabMining.analysis.GabMiningCCAnalyser

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class GabMiningCCRange(jobID:String,start:Long,end:Long,hop:Long) extends RangeAnalysisManager(jobID,start,end,hop)  {
  val output_file = System.getenv().getOrDefault("GAB_PROJECT_OUTPUT", "/app/defout.csv").trim
  val inputFormat = new SimpleDateFormat("E MMM dd HH:mm:ss z yyyy")
  val outputFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss")
  Utils.writeLines(output_file,"Date,ConnectedComponents,Vertex,Size")

  override protected def processResults(): Unit = {
    val currentDate=new Date(timestamp())
    val formattedDate = outputFormat.format(inputFormat.parse(currentDate.toString))
    val endResults = results.asInstanceOf[ArrayBuffer[mutable.HashMap[Int, Int]]]
    var connectedComponents=endResults.flatten.groupBy(f=> f._1).mapValues(x=> x.map(_._2).sum).size
    var biggest=endResults.flatten.groupBy(f=> f._1).mapValues(x=> x.map(_._2).sum).maxBy(_._2)
    val text= s"${formattedDate},${connectedComponents},${biggest._1},${biggest._2}"
    Utils.writeLines(output_file,text)
  }


  override protected def defineMaxSteps(): Int = 10
  override protected def generateAnalyzer : Analyser = new GabMiningCCAnalyser()
  override protected def processOtherMessages(value: Any) : Unit = {println ("Not handled message" + value.toString)}

  override protected def checkProcessEnd() : Boolean = {
    partitionsHalting()
  }
}




