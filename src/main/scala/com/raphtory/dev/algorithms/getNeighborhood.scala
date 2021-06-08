package com.raphtory.dev.algorithms

import com.raphtory.api.Analyser

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable
import scala.collection.parallel.immutable.ParIterable
import scala.io.Source
/**
Gets the 2-hop network for select nodes.
**/
class getNeighborhood(args: Array[String]) extends Analyser(args) {
  val url = "https://raphtorydatasets.blob.core.windows.net/top-tier/misc/selnodes.csv"
  val nodesf: String = if (args.length < 1) url else args.head
  val words: Array[String] = if (nodesf.isEmpty) Array[String]() else dllCommFile(nodesf)
  val output_file: String = System.getenv().getOrDefault("OUTPUT_PATH", "").trim

  def dllCommFile(url: String): Array[String] = {
    val html = if (url.startsWith("http")) Source.fromURL(url) else Source.fromFile(url)
    html.mkString.split("\n")
  }

  override def setup(): Unit =
    view.getVertices()
      .filter(v => words.contains(v.getPropertyValue("Word").getOrElse(v.ID()).toString))
      .foreach { vertex =>
        val wd = vertex.getPropertyValue("Word").getOrElse(vertex.ID()).toString
        vertex.setState("state", mutable.Map((2, Array())))
        vertex.messageAllNeighbours((wd, 2))
      }

  override def analyse(): Unit =
    view.getMessagedVertices().foreach { vertex =>
      val msgQ = vertex.messageQueue[(String, Int)]
      val msq = msgQ.groupBy(_._2).map(f => (f._1 - 1, f._2.map(_._1).toArray))
      var st = mutable.Map(msq.toSeq: _*)
      val prevst = vertex.getOrSetState[mutable.Map[Int, Array[String]]]("state", mutable.Map[Int, Array[String]]())
      prevst.foreach(x => st(x._1) = x._2 ++ st.getOrElse(x._1, Array[String]()))
      val wd = vertex.getPropertyValue("Word").getOrElse(vertex.ID()).toString
      vertex.setState("state", st)
      val pos = st.keys.min
      //      if ((wd == "attempting") |(wd == "deliberated" )) println(wd, st.keys.mkString(","), st.values.map(v=> v.mkString(",")).mkString("|"),pos)
      if (pos > 0) vertex.messageAllNeighbours((wd, pos))
    }

  override def returnResults(): Any = {
    view.getVertices()
      //.filter(v => v.Type() == nodeType)
      .map(vertex => (vertex.getPropertyValue("Word").getOrElse(vertex.ID()).toString,
        vertex.getOrSetState[mutable.Map[Int, Array[String]]]("state", mutable.Map[Int, Array[String]]())))
    //      .groupBy(f => f._1)
    //      .map(f => (f._1, f._2.map(_._2)))
  }

  override def defineMaxSteps(): Int = 10

  override def processResults(results: ArrayBuffer[Any], timeStamp: Long, viewCompleteTime: Long): Unit = {
    println("processing results...")
    val endResults = results.asInstanceOf[ArrayBuffer[ParIterable[(String, mutable.Map[Int, Array[String]])]]]
      .flatten.groupBy(f => f._1).mapValues(x => x.flatMap(_._2).toMap)
    val root = words.map { wd =>
      (wd,
        endResults.filter(ch => ch._2.keys.toArray.contains(1)).filter(ch => ch._2(1).contains(wd)).keys)
    }
    println("got roots...")
    val leaf = root.flatMap(_._2).distinct.filterNot(words.contains(_)).map { wd =>
      (wd,
        endResults.filter(ch => ch._2.keys.toArray.contains(0)).filter(ch => ch._2(0).contains(wd)).keys)
    }
    println("got leaves...")
    val text =
      s"""{"time": $timeStamp, ${
        (root ++ leaf).map { cts =>
          s""""${cts._1}":["${cts._2.mkString("\",\"")}"]"""
        }.mkString(",")
      } }"""
    println("storing...")
    writeOut(text, output_file)
  }

  override def processWindowResults(results: ArrayBuffer[Any], timeStamp: Long, windowSize: Long, viewCompleteTime: Long): Unit = {
    println("processing results...")
    val endResults = results.asInstanceOf[ArrayBuffer[ParIterable[(String, mutable.Map[Int, Array[String]])]]]
      .flatten.groupBy(f => f._1).mapValues(x => x.flatMap(_._2).toMap)
    val root = words.map { wd =>
      (wd,
        endResults.filter(ch => ch._2.keys.toArray.contains(1)).filter(ch => ch._2(1).contains(wd)).keys)
    }
    println("got roots...")
    val leaf = root.flatMap(_._2).distinct.filterNot(words.contains(_)).map { wd =>
      (wd,
        endResults.filter(ch => ch._2.keys.toArray.contains(0)).filter(ch => ch._2(0).contains(wd)).keys)
    }
    println("got leaves...")
    val text =
      s"""{"time": $timeStamp, ${
        (root ++ leaf).map { cts =>
          s""""${cts._1}":["${cts._2.mkString("\",\"")}"]"""
        }.mkString(",")
      } }"""
    println("storing...")
    writeOut(text, output_file)
  }
}