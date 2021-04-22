package com.raphtory.dev.algorithms

import com.github.mjakubowski84.parquet4s.ParquetWriter
import com.raphtory.algorithms.{LPA, sortOrdering}
import com.raphtory.core.model.analysis.entityVisitors.VertexVisitor
import org.apache.parquet.hadoop.ParquetFileWriter

import java.time.LocalDateTime
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.{ParIterable, ParMap}

object MultiLayerLPAVW {
  def apply(args: Array[String]): MultiLayerLPAVW = new MultiLayerLPAVW(args)
}

class MultiLayerLPAVW(args: Array[String]) extends LPA(args) {
  //args = [top, weight, maxiter, start, end, layer-size, omega, scaled]
  val snapshotSize: Long        = args(5).toLong
  val startTime: Long           = args(3).toLong //* snapshotSize //imlater: change this when done with wsdata
  val endTime: Long             = args(4).toLong //* snapshotSize
  val snapshots: Iterable[Long] = for (ts <- startTime to endTime by snapshotSize) yield ts
  val o: Float                 = if (arg.length < 7) 0.1F else args(6).toFloat
  val omega: Array[Float]      = (o to 1.0F by o).toArray
  val scaled: Boolean = if (arg.length < 8) true else args(7).toBoolean
  var t1 = System.currentTimeMillis()
  override def setup(): Unit =
    view.getVertices().foreach { vertex =>
      // Assign random labels for all instances in time of a vertex as Map(ts, lab)
      val labels =
        snapshots
          .filter(t => vertex.aliveAtWithWindow(t, snapshotSize))
          .map(x => (x,  rnd.nextLong()))
          .toArray
//          .map(x => (x, (vertex.ID()+x,vertex.ID()-x))).toArray
      val tlabels = omega.flatMap(_ => labels)
      vertex.setState("mlpalabel", tlabels)
      val message = (vertex.ID(), tlabels)
      vertex.messageAllNeighbours(message)
    }

  override def analyse(): Unit = {

    try {
      view.getMessagedVertices().foreach { vertex =>
        val vlabel    = vertex.getState[Array[(Long, Long)]]("mlpalabel").groupBy(_._1).mapValues(x => x.map(_._2))
        val msgQueue  = vertex.messageQueue[(Long, Array[(Long, Long)])]
        var voteCount = 0
        val newLabel = vlabel
          .map { tv => //(ts, array(l1,l2)), size(array)=w, size(vlabel) = w*ts
            val ts = tv._1
            // Get weights/labels of neighbours of vertex at time ts
            val nei_ts_freq = weightFunction(vertex, ts) // ID -> freq
            val nei_labs = msgQueue
              .filter(x => nei_ts_freq.keySet.contains(x._1)) // filter messages from neighbours at time ts only
              .flatMap { msg =>                               //(id, array((ts, l2)) size = ts*w
                val freq     = nei_ts_freq(msg._1)
                val label_ts = msg._2.groupBy(_._1).mapValues(x => x.map(_._2)) //map(ts, array(l2))
                label_ts(ts).zipWithIndex.map(x => (x._2, x._1, freq)) // (w, lab, freq)
              }

            //Get labels of past/future instances of vertex //IMlater: links between non consecutive layers should persist or at least degrade?
            if (vlabel.contains(ts - snapshotSize))
              vlabel(ts - snapshotSize).zipWithIndex.foreach(x => nei_labs.append((x._2, x._1, omega(x._2))))
            if (vlabel.contains(ts + snapshotSize))
              vlabel(ts + snapshotSize).zipWithIndex.foreach(x => nei_labs.append((x._2, x._1, omega(x._2))))

            nei_labs.groupBy(_._1).values.toArray.map { x =>
              val w      = x.head._1
              val Curlab = tv._2(w)

              // Get label most prominent in neighborhood of vertex
              var newlab = if (x.nonEmpty) {
                val max_freq = x.groupBy(_._2).mapValues(_.map(_._3).sum)
                max_freq.filter(_._2 == max_freq.values.max).keySet.max
              } else Curlab

              // Update node label and broadcast
              if (newlab == Curlab) voteCount += 1
              newlab = if (rnd.nextFloat() < SP) Curlab else newlab
              (ts,newlab)
            }
          }
          .toArray
          .flatten
        vertex.setState("mlpalabel", newLabel)
        val message = (vertex.ID(), newLabel)
        vertex.messageAllNeighbours(message)

        // Vote to halt if all instances of vertex haven't changed their labels
        if (voteCount > (vlabel.size * omega.length * 0.8).toInt) vertex.voteToHalt()
      }
    } catch {
      case e: Exception => println("Something went wrong with mLPAv!", e)
    }
    if (debug & (workerID==1)) {
      println(
              s"Superstep: ${view.superStep()}    Time: ${LocalDateTime.now()}   ExecTime: ${System.currentTimeMillis() - t1}"
      )
      t1 = System.currentTimeMillis()
    }
  }

  def weightFunction(v: VertexVisitor, ts: Long): ParMap[Long, Float] = {
    var nei_weights =
      (v.getInCEdgesBetween(ts - snapshotSize, ts) ++ v.getOutEdgesBetween(ts - snapshotSize, ts)).map(e =>
        (e.ID(), e.getPropertyValue(weight).getOrElse(1.0F).asInstanceOf[Float])
      )
    if (scaled) {
      val scale = scaling(nei_weights.map(_._2).toArray)
      nei_weights = nei_weights.map(x => (x._1, x._2 / scale))

    }
    nei_weights.groupBy(_._1).mapValues(x => x.map(_._2).sum) // (ID -> Freq)
  }

  def scaling(freq: Array[Float]): Float = math.sqrt(freq.map(math.pow(_, 2)).sum).toFloat

  override def returnResults(): Any =
    view
      .getVertices()
      .map(vertex =>
        (
                vertex.getState[Array[(Long, Long)]]("mlpalabel"),
                vertex.getPropertyValue("Word").getOrElse(vertex.ID()).toString
        )
      )
      .flatMap(f =>
        f._1
          .groupBy(_._1)
          .mapValues(x => x.map(_._2))
          .flatMap(ts => ts._2.zipWithIndex.map { case (lab, w) => (omega(w), lab, s"""${f._2}_${ts._1}""") })
      )

  override def processResults(results: ArrayBuffer[Any], timestamp: Long, viewCompleteTime: Long): Unit = {
    val endResults = results.asInstanceOf[ArrayBuffer[ParIterable[(Float, Long, String)]]]
    try {
      println(s"Printing output to $output_file")
      val groupedW = endResults.flatten.groupBy(f => f._1).mapValues(x => x.groupBy(_._2).mapValues(x => x.map(_._3)))
      groupedW.foreach { g =>
        val w                   = g._1
        val grouped             = g._2
        val groupedNonIslands   = grouped.filter(x => x._2.size > 1)
        val sorted              = grouped.toArray.sortBy(_._2.size)(sortOrdering)
        val top5                = sorted.map(_._2.size).take(5)
        val total               = grouped.size
        val totalWithoutIslands = groupedNonIslands.size
        val totalIslands        = total - totalWithoutIslands
        val communities         = if (top == 0) sorted.map(_._2) else sorted.map(_._2).take(top)

        output_file match {
          case "" =>
                        val commtxt = communities.map(x => s"""[${x.mkString(",")}]""")
            val text =
              s"""{"time":$timestamp, "omega": $w, "top5":[${top5
                .mkString(",")}],"total":$total,"totalIslands":$totalIslands,""" +
                          s""" "communities": [${commtxt.mkString(",")}],""" +
                s"""viewTime":$viewCompleteTime}"""
            println(text)
          case _ =>
            val text =
              s"""{"time":$timestamp, "omega": $w,"top5":[${top5
                .mkString(",")}],"total":$total,"totalIslands":$totalIslands, "viewTime":$viewCompleteTime}"""
            println(text)
            case class Data(omega: Double, comm: Array[String])
            val writer =
              ParquetWriter.writer[Data](
                      output_file + s"omega-$w.parquet",
                      ParquetWriter.Options(writeMode = ParquetFileWriter.Mode.OVERWRITE)
              )
            try communities.foreach(c => writer.write(Data(w, c.toArray)))
            finally writer.close()
        }
      }
    }
  }
}
