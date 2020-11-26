package com.raphtory.examples.lotr

import com.raphtory.Raphtory

object LOTRDeployment extends App{
  val source  = new LOTRDataSource()
  val builder = new LOTRGraphBuilder()
  Raphtory[String](source,builder)
}
