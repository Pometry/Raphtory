package com.raphtory.algorithms

object testAlgo extends App {
  val fn = System.getenv().getOrDefault("FILE_PATH", "/home/tsunade/small_tgraph.csv")
  var data = scala.io.Source.fromFile(fn).getLines().map(_.split(";").map(_.trim))
    .map{x=>
      val properties = x.last.replaceAll("^\\{|}$", "").split(",")
      .flatMap(_.split(":").map(_.last.toLong))
      ((x.head.toLong, x(1).toLong), properties) }.toArray




}