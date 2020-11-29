package com.raphtory.examples.lotr

import com.raphtory.RaphtoryGraph

object LOTRDeployment extends App{
  val source  = new LOTRSpout()
  val builder = new LOTRGraphBuilder()
  RaphtoryGraph[String](source,builder)
}
