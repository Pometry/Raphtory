//package com.raphtory.dev.algorithms
//
//import com.raphtory.algorithms.sortOrdering
//import com.raphtory.api.Analyser
//import com.raphtory.core.model.analysis.entityVisitors.VertexVisitor
//
//import scala.collection.mutable.ArrayBuffer
//import scala.collection.parallel.immutable
//import scala.collection.parallel.mutable.ParArray
//
//object SPLPA {
//  def apply(args: Array[String]): SPLPA = new SPLPA(args)
//}
//
//class SPLPA(args: Array[String]) extends Analyser(args) {
//  //args = [top output, edge property, max iterations]
//
//  val arg: Array[String] = args.map(_.trim)
//
//  val top: Int         = if (arg.length == 0) 0 else arg.head.toInt
//  val weight: String       = if (arg.length < 2) "" else arg(1)
//  val maxIter: Int       = if (arg.length < 3) 500 else arg(2).toInt
//  val b: Float = if (arg.length < 4) 0.999F else arg(2).toFloat
//  val STOP = 3
//  val output_file: String = ""
//  val nodeType: String    = ""
//
//  override def setup(): Unit = {
//    view.getVertices().foreach { vertex =>
//      val lab = vertex.ID()//scala.util.Random.nextLong()
//      vertex.setState("lpalabel", lab)
//      vertex.setState("lpaflag", 1)
//      vertex.messageAllNeighbours((vertex.ID(),lab))
//    }
//  }
//
//  override def analyse(): Unit = {
//    view.getMessagedVertices()
//      .filter(v => v.getState[Int]("lpaflag")>0)
//      .foreach { vertex =>
//      val vlabel = vertex.getState[Long]("lpalabel")
//      val vflag = vertex.getState[Int]("lpaflag")
//      val vneigh = (vertex.getIncEdges ++ vertex.getOutEdges)
//      val neigh_freq = vneigh.map{e=> e.ID()->e.getPropertyValue(weight).getOrElse(1.0).asInstanceOf[Double]}
//        .groupBy(_._1)
//        .mapValues(x => x.map(_._2).sum / x.size)
//      val vfreq = if (vneigh.nonEmpty) neigh_freq.values.sum / neigh_freq.size else 1.0
//      val gp = vertex.messageQueue[(Long, Long)].map{v => (v._2, neigh_freq(v._1))}
//      gp.append((vlabel, vfreq))
//      val newLabel = labelProbability(gp.groupBy(_._1).mapValues(_.map(_._2)))
//      if (newLabel == vlabel) {
//        if (vflag == STOP) {
//          vertex.voteToHalt()
//          vertex.setState("lpaflag", -1)
//        }else
//        vertex.setState("lpaflag", vflag + 1)
//      } else {
//        vertex.setState("lpaflag", 1)
//        vertex.setState("lpalabel", newLabel)
//      }
////      vertex.messageAllNeighbours((vertex.ID(),newLabel))
//      doSomething(vertex, gp.map(_._1).toArray)
//      if (List(5,38,84, 9).contains(vertex.ID())) println( s"{ID: ${vertex.ID()},Superstep: ${view.superStep()}, old: $vlabel, new: $newLabel, flag: $vflag}")
//    }
//    view.getVertices().foreach{v=>
//      v.messageAllNeighbours((v.ID(), v.getState[Long]("lpalabel")))
//    }
////    if (workerID==1)
////      println(
////        s"{workerID: ${workerID},Superstep: ${view.superStep()}}"
////      )
//  }
//
//  def labelProbability(gp: Map[Long, ArrayBuffer[Double]]): Long ={
//    //    label probability function to get rid of oscillation phenomena of synchronous LPA from [1]
//    //    [1] https://doi.org/10.1016/j.neucom.2014.04.084
//
//    var maxp = 1.0
//    var i = 1
//    var p = Array[Double]()
//    while (maxp > b) {
//      val f = gp.map { lab => familiarityFct(lab._2.sum, i) }.toArray
//      p = f.map(i => i / f.sum)
//      maxp = if (i<3) p.max else 0.0
//      i+=1
//    }
//    val gpp = gp.keys zip p
//    randomChoice(gpp).take(1).head
//  }
//
//  def familiarityFct(x: Double, n: Int): Double = {
//    n match {
//      case 1 => Math.pow(10, x) - 1
//      case 2 => Math.pow(x, 4)
//      case _ => Math.pow(x, 2)
//    }
//  }
//  def doSomething(v: VertexVisitor, gp: Array[Long]): Unit = {}
//
//  override def returnResults(): Any =
//    view.getVertices()
//      .map(vertex => (vertex.getState[Long]("lpalabel"), vertex.ID()))
//      .groupBy(f => f._1)
//      .map(f => (f._1, f._2.map(_._2)))
//
//  override def processResults(results: ArrayBuffer[Any], timestamp: Long, viewCompleteTime: Long): Unit = {
//    val er      = extractData(results)
//    val commtxt = er.communities.map(x => s"""[${x.mkString(",")}]""")
//    val text = s"""{"time":$timestamp,"top5":[${er.top5
//      .mkString(",")}],"total":${er.total},"totalIslands":${er.totalIslands},"""+
//      s""""communities": \n[${commtxt.mkString(",")}] \n,"""+
//      s""""viewTime":$viewCompleteTime}"""
//    writeOut(text, output_file)
//  }
//
//  override def processWindowResults(
//                                     results: ArrayBuffer[Any],
//                                     timestamp: Long,
//                                     windowSize: Long,
//                                     viewCompleteTime: Long
//                                   ): Unit = {
//    val er      = extractData(results)
//    val commtxt = er.communities.map(x => s"""[${x.mkString(",")}]""")
//    val text = s"""{"time":$timestamp,"windowsize":$windowSize,"top5":[${er.top5
//      .mkString(",")}],"total":${er.total},"totalIslands":${er.totalIslands},"""+
//      s""""communities": [${commtxt.mkString(",")}],"""+
//      s""""viewTime":$viewCompleteTime}"""
//    writeOut(text, output_file)
//  }
//
//  def extractData(results: ArrayBuffer[Any]): fd = {
//    val endResults = results.asInstanceOf[ArrayBuffer[immutable.ParHashMap[Long, ParArray[String]]]]
//    try {
//      val grouped             = endResults.flatten.groupBy(f => f._1).mapValues(x => x.flatMap(_._2))
//      val groupedNonIslands   = grouped.filter(x => x._2.size > 1)
//      val sorted              = grouped.toArray.sortBy(_._2.size)(sortOrdering)
//      val top5                = sorted.map(_._2.size)//.take(5)
//      val total               = grouped.size
//      val totalWithoutIslands = groupedNonIslands.size
//      val totalIslands        = total - totalWithoutIslands
//      val communities         = if (top == 0) sorted.map(_._2) else sorted.map(_._2).take(top)
//      fd(top5, total, totalIslands, communities)
//    } catch {
//      case _: UnsupportedOperationException => fd(Array(0), 0, 0, Array(ArrayBuffer("0")))
//    }
//  }
//
//  override def defineMaxSteps(): Int = maxIter
//
//  def randomChoice[T](input :Iterable[(T,Double)]): Stream[T] = {
//    val items  :Seq[T]    = input.flatMap{x => Seq.fill((x._2 * 100).toInt)(x._1)}.toSeq
//    def output :Stream[T] = util.Random.shuffle(items).toStream #::: output
//    output
//  }
//}
//
//case class fd(top5: Array[Int], total: Int, totalIslands: Int, communities: Array[ArrayBuffer[String]])
