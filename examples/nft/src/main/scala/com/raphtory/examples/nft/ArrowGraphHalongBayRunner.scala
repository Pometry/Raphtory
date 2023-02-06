package com.raphtory.examples.nft

import com.raphtory.RaphtoryApp
import com.raphtory.algorithms.generic.centrality.Degree
import com.raphtory.api.input.Source
import com.raphtory.internals.context.RaphtoryContext
import com.raphtory.sinks.FileSink
import com.raphtory.spouts.FileSpout

object ArrowGraphHalongBayRunner extends RaphtoryApp.ArrowLocal[NoProps, Price] {

//  val path    = "/pometry/wip/halongbay_partitions_10pc"
  val path    = "/pometry/wip/halongbay_sorted.csv.gz"
//  val path    = "/home/jatinder/projects/Pometry/arrow-core/halongbay_sorted.csv"
  val builder = new HalongBayBuilder()

  override def run(args: Array[String], ctx: RaphtoryContext): Unit =
    ctx.runWithNewGraph() { graph =>
      val source = Source(FileSpout(path = path), builder)
      graph.addDynamicPath("com.raphtory.crypto")
      graph.load(source)

      graph
        .execute(Degree())
        .writeTo(FileSink("/tmp/raphtory"))
        .waitForJob()
    }
}

case class NoProps()
case class Price(valUSD: Long)
