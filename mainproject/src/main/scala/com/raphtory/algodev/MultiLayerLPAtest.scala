package com.raphtory.algodev

import java.time.LocalDateTime

import com.github.mjakubowski84.parquet4s.ParquetWriter
import com.raphtory.algorithms.LPA
import com.raphtory.core.model.analysis.entityVisitors.VertexVisitor
import org.apache.parquet.hadoop.ParquetFileWriter

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.ParMap

object MultiLayerLPAtest {
  def apply(args: Array[String]): MultiLayerLPAtest = new MultiLayerLPAtest(args)
}

class MultiLayerLPAtest(args: Array[String]) extends LPA(args) {
  //args = [top, weight, maxiter, start, end, layer-size, omega]
  val snapshotSize: Long = args(5).toLong
  val startTime: Long = args(3).toLong * snapshotSize //imlater: change this when done with wsdata
  val endTime: Long = args(4).toLong * snapshotSize
  val snapshots: Iterable[Long] = (for (ts <- startTime to endTime by snapshotSize) yield ts)
  val omega: Array[Double] = (0.1 to 1.0 by 0.1).toArray
  val rnd = new scala.util.Random(111)
  val scaled = true

  override def setup(): Unit =
    view.getVertices().foreach { vertex =>
      // Assign random labels for all instances in time of a vertex as Map(ts, lab)
      val labels =   //im: send tuples instead of TreeMap and build the TM for processing only
        snapshots
          .filter(t => vertex.aliveAtWithWindow(t, snapshotSize))
//          .map(x => (x, (scala.util.Random.nextLong(), scala.util.Random.nextLong()))).toArray
          .map(x => (x, (rnd.nextLong(), rnd.nextLong()))).toArray
      val tlabels = omega.flatMap(_ => labels)
      vertex.setState("mlpalabel", tlabels)
      val message = (vertex.ID(), tlabels.map(x=> (x._1, x._2._2)))
      vertex.messageAllNeighbours(message)
    }

  override def analyse(): Unit = {
    val t1 = System.currentTimeMillis()
    try
      view.getMessagedVertices().foreach { vertex =>
        val vlabel = vertex.getState[Array[(Long, (Long, Long))]]("mlpalabel").groupBy(_._1).mapValues(x=>x.map(_._2))
        val msgQueue = vertex.messageQueue[(Long, Array[(Long, Long)])]
        var voteCount = 0
        val newLabel = vlabel.flatMap { tv =>
          val ts = tv._1
          // Get weights/labels of neighbours of vertex at time ts
          val nei_ts_freq = weightFunction(vertex, ts) // ID -> freq
          val nei_labs = msgQueue
            .filter(x => nei_ts_freq.keySet.contains(x._1)) // filter messages from neighbours at time ts only
            .flatMap { msg =>
              val freq = nei_ts_freq(msg._1)
              val label_ts = msg._2.groupBy(_._1).mapValues(x=>x.map(_._2))
              label_ts(ts).zipWithIndex.map(x=> (x._2, x._1, freq))
//              (label_ts(ts), freq) //get label at time ts -> (lab, freq)
            }

          //Get labels of past/future instances of vertex //IMlater: links between non consecutive layers should persist or at least degrade?
          if (vlabel.contains(ts - snapshotSize))
            vlabel(ts - snapshotSize).zipWithIndex.foreach(x=> nei_labs.append((x._2, x._1._2, omega(x._2))))
//            nei_labs.append((vlabel(ts - snapshotSize)._2, interLayerWeights(omega, vertex, ts - snapshotSize)))
          if (vlabel.contains(ts + snapshotSize))
//            nei_labs.append((vlabel(ts + snapshotSize)._2, interLayerWeights(omega, vertex, ts)))
            vlabel(ts + snapshotSize).zipWithIndex.foreach(x=> nei_labs.append((x._2, x._1._2, omega(x._2))))

          nei_labs.groupBy(_._1).values.map { x =>
            val w = x.head._1
            // Get label most prominent in neighborhood of vertex
            val max_freq = x.groupBy(_._2).mapValues(_.map(_._3).sum)
            val newlab = max_freq.filter(_._2 == max_freq.values.max).keySet.max

            // Update node label and broadcast
            val Oldlab = tv._2(w)._1
            val Vlab = tv._2(w)._2
            (ts, newlab match {
              case Vlab | Oldlab => //im: check if this is the culprit behind islands
                if (newlab == Vlab) voteCount += 1
                (List(Vlab, Oldlab).min, List(Vlab, Oldlab).max)
              case _ => (Vlab, newlab)
            })
          }

        }.toArray

        vertex.setState("mlpalabel", newLabel)
        val message = (vertex.ID(), newLabel.map(x=> (x._1, x._2._2)))
        vertex.messageAllNeighbours(message)

        // Vote to halt if all instances of vertex haven't changed their labels
        if (voteCount == vlabel.size * omega.size) vertex.voteToHalt()
      }
    catch {
      case e: Exception => println("Something went wrong with mLPA!", e)
    }
  if (workerID == 1) println(s"Superstep: ${view.superStep()}    Time: ${LocalDateTime.now()}   ExecTime: ${System.currentTimeMillis() - t1}")
}

//  def interLayerWeights(x: String, v: VertexVisitor, ts: Long): Double =
//    x match {
//      case "None" =>
//        val neilabs = weightFunction(v, ts)             //imlater: a more conceptual implementation of temporal weights
//        neilabs.values.sum / neilabs.size
//      case _ => omega.toDouble                          //imlater: possibly changing this to Double
//    }

  def weightFunction(v: VertexVisitor, ts: Long): ParMap[Long, Double] = {
    var nei_weights =
      (v.getInCEdgesBetween(ts - snapshotSize, ts) ++ v.getOutEdgesBetween(ts - snapshotSize, ts)).map(e =>
        (e.ID(), e.getPropertyValue(weight).getOrElse(1.0).asInstanceOf[Double])
      )
    if (scaled) {
      val scale = scaling(nei_weights.map(_._2).toArray)
      nei_weights = nei_weights.map(x => (x._1, x._2 / scale))

    }
    nei_weights.groupBy(_._1).mapValues(x => x.map(_._2).sum / x.size) // (ID -> Freq)
  }

  def scaling(freq: Array[Double]): Double = math.sqrt(freq.map(math.pow(_, 2)).sum)

  override def returnResults(): Any =
    view
      .getVertices()
      .map(vertex =>
        (
                vertex.getState[Array[(Long, (Long, Long))]]("mlpalabel"),
                vertex.getPropertyValue("Word").getOrElse(vertex.ID()).asInstanceOf[String]
        )
      )
      .flatMap(f => f._1.zipWithIndex.map{case (x, w) => (x._2._2, s"""${w}_${f._2}_${x._1}""")})
      //      .flatMap(f =>  f._1.toArray.map(x => (x._2._2, "\""+x._1.toString+f._2+"\"")))
      .groupBy(f => f._1)
      .map(f => (f._1, f._2.map(_._2)))

  override def processResults(results: ArrayBuffer[Any], timestamp: Long, viewCompleteTime: Long): Unit = {
    val er = extractData(results)
    output_file match {
      case "" =>
        val commtxt = er.communities.map(x => s"""[${x.mkString(",")}]""")
        val text = s"""{"time":$timestamp,"top5":[${er.top5
          .mkString(",")}],"total":${er.total},"totalIslands":${er.totalIslands},""" +
//          s""" "communities": [${commtxt.mkString(",")}],""" +
          s"""viewTime":$viewCompleteTime}"""
        println(text)
      case _ =>
        val text = s"""{"time":$timestamp,"top5":[${er.top5
          .mkString(",")}],"total":${er.total},"totalIslands":${er.totalIslands}, "viewTime":$viewCompleteTime}"""
        println(text)
        case class Data(comm: Array[String])
        val writer =
          ParquetWriter.writer[Data](output_file, ParquetWriter.Options(writeMode = ParquetFileWriter.Mode.OVERWRITE))
        try er.communities.foreach(c => writer.write(Data(c.toArray)))
        finally writer.close()
    }
  }
}
