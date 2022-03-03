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
    val spout: Spout[String] = FileSpout("/tmp/transactions_03300000_03399999.csv.gz")
    val gb                   = new EthereumGraphBuilder()
    val graph                = Raphtory.createGraph(spout, gb)
  }
}
