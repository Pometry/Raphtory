

package com.raphtory.examples.gabMining.actors
import java.text.SimpleDateFormat
import java.util.Date

import com.raphtory.core.analysis.Analyser
import com.raphtory.core.components.AnalysisManager.{LiveAnalysisManager, RangeAnalysisManager, ViewAnalysisManager, WindowRangeAnalysisManager}
import com.raphtory.core.utils.Utils
import com.raphtory.examples.gabMining.analysis.GabMiningCCAnalyser

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

//Initialisation of the file in where the output will be written is done.
//The partial results sent from the analyser are received and assigned to an equivalent data structure they had in
//the analyser. Then by means of Scala functionality, only the connected component that had the higher number of members
//is written to the file.
// In this class the number of supersteps in the connectec component algorithm is set.
// The number of rows in the output will depend on the dates stated for the window analysis.

class GabMiningCCWindow(jobID:String,start:Long,end:Long,hop:Long,window:Long) extends WindowRangeAnalysisManager(jobID,start,end,hop,window)  {
  val output_file = System.getenv().getOrDefault("GAB_PROJECT_OUTPUT", "/app/defout.csv").trim
  val inputFormat = new SimpleDateFormat("E MMM dd HH:mm:ss z yyyy")
  val outputFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss")
  Utils.writeLines(output_file,"Date,ConnectedComponents,Vertex,Size")

  override protected def processResults(): Unit = {

    val currentDate=new Date(timestamp())
    val formattedDate = outputFormat.format(inputFormat.parse(currentDate.toString))
    //println("starting processing")
    val endResults = results.asInstanceOf[ArrayBuffer[mutable.HashMap[Int, Int]]]
    //println("as instance of")
    //endResults.foreach(println(_))
    //println("mapvalues")
    //var results = mutable.HashMap[Int, Int]()
    //    endResults.foreach(pmap => {
    //      pmap.foreach(f => {results.get(f._1) match {
    //        case Some(previousTotal) => results(f._1) = previousTotal + f._2
    //        case None => results(f._1) = f._2
    //      }})

    //    })
    var connectedComponents=endResults.flatten.groupBy(f=> f._1).mapValues(x=> x.map(_._2).sum).size
    var biggest=endResults.flatten.groupBy(f=> f._1).mapValues(x=> x.map(_._2).sum).maxBy(_._2)
   // println(s"At ${new Date(timestamp())} with a window of ${windowSize()/1000/3600} hour(s) there were ${endResults.flatten.groupBy(f=> f._1).mapValues(x=> x.map(_._2).sum).size } connected components. The biggest being ${endResults.flatten.groupBy(f=> f._1).mapValues(x=> x.map(_._2).sum).maxBy(_._2)}")
    //endResults.flatten.groupBy(f=> f._1).mapValues(x=> x.map(_._2).sum).foreach(f=> println(s"  Group ${f._1} with size ${f._2} "))
    //println()

    val text= s"${formattedDate},${connectedComponents},${biggest._1},${biggest._2}"
    Utils.writeLines(output_file,text)
  }


  override protected def defineMaxSteps(): Int = 5
  override protected def generateAnalyzer : Analyser = new GabMiningCCAnalyser()
  override protected def processOtherMessages(value: Any) : Unit = {println ("Not handled message" + value.toString)}

  override protected def checkProcessEnd() : Boolean = {
    partitionsHalting()
  }
}



