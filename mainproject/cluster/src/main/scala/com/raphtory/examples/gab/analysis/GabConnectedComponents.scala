

package com.raphtory.examples.gab.analysis

import java.text.SimpleDateFormat
import java.util.Date

import com.raphtory.core.analysis.API.{Analyser, WorkerID}
import com.raphtory.core.model.communication.VertexMessage
import com.raphtory.core.utils.Utils

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.mutable.ParTrieMap

class GabConnectedComponents extends Analyser {

  case class ClusterLabel(value: Int) extends VertexMessage

  //Definition of a Pregel-like superstep=0 . In Raphtory is called setup. All the vertices are initiallized to its own id.

  override def setup() = {
    proxy.getVerticesSet().foreach(v => {
      val vertex = proxy.getVertex(v)
      var min = v
      //math.min(v, vertex.getOutgoingNeighbors.union(vertex.getIngoingNeighbors).min)
      val toSend = vertex.getOrSetCompValue("cclabel", min).asInstanceOf[Int]
      vertex.messageAllNeighbours(ClusterLabel(toSend))
    })
  }

  //The vertices are parsed to get the id they have and if minimum to the one they have, they will assign the minimum to themselves
  // and message this value to their neighbours
  //In this function we can observe that in order to maintain the state of the value of the vertex, the getOrSetCompValue is used.
  // the result is passed to the Live Analyser as in a key value pair structure
  override def analyse(): Any= {
    var results = ParTrieMap[Int, Int]()
    var verts = Set[Int]()
    //println(s"Here !!! $workerID ${proxy.getVerticesSet().size}")
    for(v <- proxy.getVerticesSet()){
      val vertex = proxy.getVertex(v)
      val queue = vertex.messageQueue.map(_.asInstanceOf[ClusterLabel].value)
      var label = v
      if(queue.nonEmpty)
        label = queue.min
      vertex.messageQueue.clear
      var currentLabel = vertex.getOrSetCompValue("cclabel",v).asInstanceOf[Int]
      if (label < currentLabel) {
        vertex.setCompValue("cclabel", label)
        vertex messageAllNeighbours  (ClusterLabel(label))
        currentLabel = label
      }
      else{
        vertex messageAllNeighbours (ClusterLabel(currentLabel))
        //vertex.voteToHalt()
      }
      results.put(currentLabel, 1+results.getOrElse(currentLabel,0))
      verts+=v
    }
    results
  }

  override def defineMaxSteps(): Int = 10

  override def processResults(results: ArrayBuffer[Any], oldResults: ArrayBuffer[Any],viewCompleteTime:Long): Unit = ???


  //Initialisation of the file in where the output will be written is done.
  //The partial results sent from the analyser are received and assigned to an equivalent data structure they had in
  //the analyser. Then by means of Scala functionality, only the connected component that had the higher number of members
  //is written to the file.
  // In this class the number of supersteps in the connectec component algorithm is set.
  // The number of rows in the output will depend on the dates stated for the range analysis.

  override def processViewResults(results: ArrayBuffer[Any], oldResults: ArrayBuffer[Any], timestamp: Long,viewCompleteTime:Long): Unit = {
    val output_file = System.getenv().getOrDefault("GAB_PROJECT_OUTPUT", "/app/defout.csv").trim
    val inputFormat = new SimpleDateFormat("E MMM dd HH:mm:ss z yyyy")
    val outputFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss")
    val currentDate=new Date(timestamp)
    val formattedDate = outputFormat.format(inputFormat.parse(currentDate.toString))
    val endResults = results.asInstanceOf[ArrayBuffer[mutable.HashMap[Int, Int]]]
    var connectedComponents=endResults.flatten.groupBy(f=> f._1).mapValues(x=> x.map(_._2).sum).size
    var biggest=endResults.flatten.groupBy(f=> f._1).mapValues(x=> x.map(_._2).sum).maxBy(_._2)
    val text= s"${formattedDate},${connectedComponents},${biggest._1},${biggest._2}"
    Utils.writeLines(output_file,text,"")
  }

  //Initialisation of the file in where the output will be written is done.
  //The partial results sent from the analyser are received and assigned to an equivalent data structure they had in
  //the analyser. Then by means of Scala functionality, only the connected component that had the higher number of members
  //is written to the file.
  // In this class the number of supersteps in the connectec component algorithm is set.
  // The number of rows in the output will depend on the dates stated for the window analysis.

  override def processWindowResults(results: ArrayBuffer[Any], oldResults: ArrayBuffer[Any], timestamp: Long, windowSize: Long,viewCompleteTime:Long): Unit = {
    val output_file = System.getenv().getOrDefault("GAB_PROJECT_OUTPUT", "/app/defout.csv").trim
    val inputFormat = new SimpleDateFormat("E MMM dd HH:mm:ss z yyyy")
    val outputFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss")
    Utils.writeLines(output_file,"Date,ConnectedComponents,Vertex,Size","Date,ConnectedComponents,Vertex,Size")
    val currentDate=new Date(timestamp)
    val formattedDate = outputFormat.format(inputFormat.parse(currentDate.toString))
    val endResults = results.asInstanceOf[ArrayBuffer[mutable.HashMap[Int, Int]]]
    var connectedComponents=endResults.flatten.groupBy(f=> f._1).mapValues(x=> x.map(_._2).sum).size
    var biggest=endResults.flatten.groupBy(f=> f._1).mapValues(x=> x.map(_._2).sum).maxBy(_._2)
    val text= s"${formattedDate},${connectedComponents},${biggest._1},${biggest._2}"
    Utils.writeLines(output_file,text,"")
  }
}
