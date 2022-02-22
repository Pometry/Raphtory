package com.raphtory.ethereumtest

import com.raphtory.algorithms.generic.BinaryDiffusion
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.core.components.spout.Spout
import com.raphtory.core.components.spout.instance.FileSpout
import com.raphtory.core.deploy.Raphtory
import com.raphtory.output.FileOutputFormat
import com.raphtory.output.PulsarOutputFormat

object RaphtoryEthTest {

  def main(args: Array[String]): Unit = {
    println("make spout")
    val spout: Spout[String] = FileSpout()
    println("make gb")
    val gb                   = new EthereumGraphBuilder()
    println("run graph")
    val graph                = Raphtory.createGraph(spout, gb)
    println("sleep 20")
    Thread.sleep(3000)
    println("run cc")
    val outputFormata        = FileOutputFormat("/tmp/edge")
    val outputFormatB        = FileOutputFormat("/tmp/diffusion")
    graph.pointQuery(EdgeList(), PulsarOutputFormat("EdgeList"), 1595303181, List())
    graph.pointQuery(BinaryDiffusion(), PulsarOutputFormat("diffusion"), 1595303181, List())
  }
}
