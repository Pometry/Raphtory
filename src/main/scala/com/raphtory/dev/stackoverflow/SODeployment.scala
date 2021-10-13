package com.raphtory.dev.stackoverflow

import com.raphtory.algorithms.old.DegreeBasic
import com.raphtory.core.build.server.RaphtoryPD
import com.raphtory.spouts.FileSpout


object SODeployment extends App {

  val source = new FileSpout("temp")
  val builder = new SOBuilder()
  val rg = RaphtoryPD[String](source,builder)
  val arguments = Array[String]()

//  rg.rangeQuery(new DegreeBasic(Array()), new DefaultSerialiser, start = 1254192988L, end = 1457262355L, increment = 86400L,arguments)
//  rg.rangeQuery(new DegreeBasic(Array()), new DefaultSerialiser, start = 1254192988L, end = 1457262355L, increment = 86400L,windowBatch = List(7776000L, 15552000L),arguments)
//  rg.rangeQuery(new DegreeBasic(Array()), new DefaultSerialiser, start = 1254192988L, end = 1457262355L, increment = 86400L,windowBatch = List(86400L, 604800L,2592000L,31536000L),arguments)
//  rg.oldrangeQuery(new DegreeBasic(Array()), new DefaultSerialiser, start = 1254192988L, end = 1457262355L, increment = 3600L, window=3600L)
//  rg.oldrangeQuery(new DegreeBasic(Array()), new DefaultSerialiser, start = 1254192988L, end = 1457262355L, increment = 86400L,windowBatch = List(86400L, 604800L,2592000L,31536000L))
}
