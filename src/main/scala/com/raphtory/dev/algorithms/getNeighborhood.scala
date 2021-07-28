package com.raphtory.dev.algorithms

import com.raphtory.core.analysis.api.Analyser

import scala.collection.mutable
import scala.io.Source
import scala.tools.nsc.io.Path

/**
Gets the 2-hop network for select nodes.
**/
class getNeighborhood(args: Array[String]) extends Analyser[Any](args) {
  val url                  = "https://raphtorydatasets.blob.core.windows.net/top-tier/misc/selnodes.csv"
  val nodesf: String       = if (args.length < 1) url else args.head
  val words: Array[String] = if (nodesf.isEmpty) Array[String]() else dllCommFile(nodesf)
  val output_file: String  = System.getenv().getOrDefault("OUTPUT_PATH", "").trim

  def dllCommFile(url: String): Array[String] = {
    val html = if (url.startsWith("http")) Source.fromURL(url) else Source.fromFile(url)
    html.mkString.split("\n")
  }

  override def setup(): Unit =

    view.getVertices().filter(v => words.contains(v.getPropertyValue("Word").getOrElse(v.ID()).toString)).foreach {
      vertex =>
        val wd = vertex.getPropertyValue("Word").getOrElse(vertex.ID()).toString
        vertex.setState("state", mutable.Map((2, List())))
        vertex.messageAllNeighbours((wd, 2))
    }

  override def analyse(): Unit =
    view.getMessagedVertices().foreach { vertex =>
      val msgQ   = vertex.messageQueue[(String, Int)]
      val msq    = msgQ.groupBy(_._2).map(f => (f._1 - 1, f._2.map(_._1)))
      var st     = mutable.Map(msq.toSeq: _*)
      val prevst = vertex.getOrSetState[mutable.Map[Int, List[String]]]("state", mutable.Map[Int, List[String]]())
      prevst.foreach(x => st(x._1) = x._2 ++ st.getOrElse(x._1, List[String]()))
      val wd = vertex.getPropertyValue("Word").getOrElse(vertex.ID()).toString
      vertex.setState("state", st)
      val pos = st.keys.min
      //      if ((wd == "attempting") |(wd == "deliberated" )) println(wd, st.keys.mkString(","), st.values.map(v=> v.mkString(",")).mkString("|"),pos)
      if (pos > 0) vertex.messageAllNeighbours((wd, pos))
    }


  override def returnResults(): Any =
    view
      .getVertices()
      //.filter(v => v.Type() == nodeType)
      .map(vertex =>
        (
                vertex.getPropertyValue("Word").getOrElse(vertex.ID()).toString,
                vertex.getOrSetState[mutable.Map[Int, List[String]]]("state", mutable.Map[Int, List[String]]())
        )
      )
      .toList
//          .groupBy(f => f._1)
//          .map(f => (f._1, f._2.map(_._2)))

  override def defineMaxSteps(): Int = 10

  override def extractResults(results: List[Any]): Map[String, Any] = {
    println("processing results...")
    val endResults = results
      .asInstanceOf[List[List[(String, mutable.Map[Int, List[String]])]]]
      .flatten
      .groupBy(f => f._1)
      .mapValues(x => x.flatMap(_._2).toMap)
    val root = words.map { wd =>
      (wd, endResults.filter(ch => ch._2.keys.toArray.contains(1)).filter(ch => ch._2(1).contains(wd)).keys)
    }
    println("got roots...")
    val leaf = root.flatMap(_._2).distinct.filterNot(words.contains(_)).map { wd =>
      (wd, endResults.filter(ch => ch._2.keys.toArray.contains(0)).filter(ch => ch._2(0).contains(wd)).keys)
    }
    println("got leaves...")
    val text =
      s"""{${(root ++ leaf).map(cts => s""""${cts._1}":["${cts._2.mkString("\",\"")}"]""").mkString(",")} }"""
    println("storing...")
    output_file match {
      case "" => println(text)
      case _  => Path(output_file).createFile().appendAll(text + "\n")
    }
    Map[String, Any]()
  }
}