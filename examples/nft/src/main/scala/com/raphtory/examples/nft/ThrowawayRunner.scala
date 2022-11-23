package com.raphtory.examples.nft

import com.raphtory.RaphtoryApp
import com.raphtory.algorithms.generic.centrality.Degree
import com.raphtory.api.input.Source
import com.raphtory.internals.context.RaphtoryContext
import com.raphtory.sinks.FileSink
import com.raphtory.spouts.FileSpout

object ThrowawayRunner extends RaphtoryApp.ArrowLocal[NoProps, Price] {
//object ThrowawayRunner extends RaphtoryApp.Local {

  val path = "alphabay_sorted.csv"
  val builder = new AlphaBayBuilder()

  override def run(args: Array[String], ctx: RaphtoryContext): Unit = {
    ctx.runWithNewGraph() { graph =>
      val source = Source(FileSpout(path), builder)
      graph.addDynamicPath("com.raphtory.crypto")
      graph.load(source)

      graph.execute(Degree())
        .writeTo(FileSink("/tmp/raphtory"))
        .waitForJob()
    }
  }
}

case class NoProps()
case class Price(valUSD: Long)
