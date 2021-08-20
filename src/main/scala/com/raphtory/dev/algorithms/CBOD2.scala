//package com.raphtory.dev.algorithms
//
//import com.raphtory.api.Analyser
//import com.raphtory.core.model.analysis.entityVisitors.VertexVisitor
//
//import scala.collection.mutable.ArrayBuffer
//import scala.collection.parallel.ParIterable
//import scala.util.Random
//
//object CBOD2 {
//  def apply(args: Array[String]): CBOD2 = new CBOD2(args)
//}
//class CBOD2(args: Array[String]) extends Analyser(args) {
//  //args = [top , edge property, maxIter, cutoff]
//
//  val arg: Array[String] = args.map(_.trim)
//
//  val top: Int         = if (arg.length == 0) 0 else arg.head.toInt
//  val weight: String       = if (arg.length < 2) "" else arg(1)
//  val maxIter: Int       = if (arg.length < 3) 500 else arg(2).toInt
//  private val debug             = System.getenv().getOrDefault("DEBUG2", "false").trim.toBoolean //for printing debug messages
//
//  val cutoff: Float = if (args.length < 4) 0.0F else args(3).toFloat
//  val output_file: String = System.getenv().getOrDefault("CBOD_OUTPUT_PATH", "").trim
//  val nodeType: String    = System.getenv().getOrDefault("NODE_TYPE", "").trim
//  var b0 = System.currentTimeMillis()
//  val SP = 0.2 // Stickiness probability
//  val rnd    = new scala.util.Random//(111)
//
//  override def setup(): Unit = {
//    view.getVertices().foreach { vertex =>
//      val lab = rnd.nextLong()
//      vertex.setState("lpalabel", lab)
//      vertex.messageAllNeighbours((vertex.ID(),lab))
//    }
//  }
//
//  override def analyse(): Unit = {
//    val t0 = System.currentTimeMillis()
////    if (debug)
////      println(
////        s"{BtwSuperstep: ${view.superStep()}, workerID: ${workerID},  ExecTime: ${t0 - b0}}"
////      )
//    view.getMessagedVertices().foreach { vertex =>
//      try {
//        val vlabel = vertex.getState[Long]("lpalabel")
//
//        // Get neighbourhood Frequencies -- relevant to weighted LPA
//        val vneigh = vertex.getOutEdges ++ vertex.getIncEdges
//        val neigh_freq = vneigh.map { e => (e.ID(), e.getPropertyValue(weight).getOrElse(1.0F).asInstanceOf[Float]) }
//          .groupBy(_._1)
//          .mapValues(x => x.map(_._2).sum)
//
//        // Process neighbour labels into (label, frequency)
//        val gp = vertex.messageQueue[(Long, Long)].map { v => (v._2, neigh_freq.getOrElse(v._1, 1.0F))}
//
//        // Get label most prominent in neighborhood of vertex
//        val maxlab = gp.groupBy(_._1).mapValues(_.map(_._2).sum)
//        var newLabel =  maxlab.filter(_._2 == maxlab.values.max).keySet.max
//
//        // Update node label and broadcast
//        if (newLabel == vlabel)
//          vertex.voteToHalt()
//        newLabel =  if (rnd.nextFloat() < SP) vlabel else newLabel
//        vertex.setState("lpalabel", newLabel)
//        vertex.messageAllNeighbours((vertex.ID(), newLabel))
//        doSomething(vertex, gp.map(_._1).toArray)
//      } catch {
//        case e: Exception => println(e, vertex.ID())
//      }
//    }
//    if (debug & (workerID==1))
//      println(
//        s"{workerID: ${workerID},Superstep: ${view.superStep()},  ExecTime: ${System.currentTimeMillis() - t0}}"
//      )
//    b0 = System.currentTimeMillis()
//  }
////  def doSomething(v: VertexVisitor, gp: Array[Long]): Unit = {}
//
//  def doSomething(v: VertexVisitor, neighborLabels: Array[Long]): Unit = {
//    val vlabel       = v.getState[Long]("lpalabel")
//    val outlierScore = 1 - (neighborLabels.count(_ == vlabel) / neighborLabels.length.toFloat)
//    v.setState("outlierscore", outlierScore)
//  }
//
//  override def returnResults(): Any =
//    view
//      .getVertices()
////      .filter(v => v.Type() == nodeType)
//      .map(vertex => (vertex.getPropertyValue("ID").getOrElse("Unknown"),
//        vertex.getOrSetState[Float]("outlierscore", -1.0F),
//        vertex.getState[Long]("lpalabel")))
//
//  override def processResults(results: ArrayBuffer[Any], timestamp: Long, viewCompleteTime: Long): Unit = {
//    val endResults = results.asInstanceOf[ArrayBuffer[ParIterable[(String, Float, Long)]]].flatten
//
//    val outliers  = endResults.filter(_._2 > cutoff)
//    val sortedstr = outliers.map(x => s""""${x._1}":${x._2}""")
//    val total     = outliers.length
//    val out       = if (top == 0) sortedstr else sortedstr.take(top)
//    val grouped =  endResults.groupBy(f => f._3)
//      .map(f => (f._1, f._2.map(_._1)))
//
//    val groupedNonIslands   = grouped.filter(x => x._2.size > 1)
//    val totalIslands        = grouped.size - groupedNonIslands.size
//    val communities         = if (top == 0) grouped.values else grouped.values.take(top)
//    val commtxt = communities.map(x => s"""[${x.mkString(",")}]""")
//    val text = s"""{"time":$timestamp,"totalC": ${grouped.size}, "totalO":$total,"totalIslands":${totalIslands},"outliers":{${out
//      .mkString(",")}},"""+
//      s""""communities": [${commtxt.mkString(",")}],"""+
//      s""""viewTime":$viewCompleteTime}"""
//    println("islands ", totalIslands)
//    writeOut(text, output_file)
//  }
//
//  override def processWindowResults(
//      results: ArrayBuffer[Any],
//      timestamp: Long,
//      windowSize: Long,
//      viewCompleteTime: Long
//  ): Unit = {
//    val endResults = results.asInstanceOf[ArrayBuffer[ParIterable[(String, Float, Long)]]].flatten
//
//    val outliers  = endResults.filter(_._2 > cutoff)
//    val sortedstr = outliers.map(x => s""""${x._1}":${x._2}""")
//    val total     = outliers.length
//    val out       = if (top == 0) sortedstr else sortedstr.take(top)
//    val grouped =  endResults.groupBy(f => f._3)
//      .map(f => (f._1, f._2.map(_._1)))
//
//    val groupedNonIslands   = grouped.filter(x => x._2.size > 1)
//    val totalIslands        = grouped.size - groupedNonIslands.size
//    val communities         = if (top == 0) grouped.values else grouped.values.take(top)
//    val commtxt = communities.map(x => s"""[${x.mkString(",")}]""")
//    val text = s"""{"time":$timestamp,"windowsize":$windowSize,"totalC": ${grouped.size}, "totalO":$total,"totalIslands":${totalIslands}, "outliers":{${out
//      .mkString(",")}},"""+
//      s""""communities": [${commtxt.mkString(",")}],"""+
//      s""""viewTime":$viewCompleteTime}"""
//    println("islands ", totalIslands)
//    writeOut(text, output_file)
//  }
//
//  override def defineMaxSteps(): Int = maxIter
//}
