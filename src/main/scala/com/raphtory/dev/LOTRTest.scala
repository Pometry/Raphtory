package com.raphtory.dev

import com.raphtory.algorithms.generic.centrality.TriangleCount
import com.raphtory.core.build.server.RaphtoryGraph
import com.raphtory.spouts.FileSpout
import com.raphtory.dev.LOTRGraphBuilder

object LOTRTest extends App{
  val source  = new FileSpout("src/main/scala/com/raphtory/dev","lotr.csv")
  val builder = new LOTRGraphBuilder()
  val rg = RaphtoryGraph[String](source,builder)

  rg.pointQuery(TriangleCount(path = "/tmp/TriangleCount"),32670)
}