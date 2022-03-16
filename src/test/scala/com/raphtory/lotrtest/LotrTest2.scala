

package com.raphtory.lotrtest

import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.algorithms.generic.centrality.{AverageNeighbourDegree, Degree, WeightedDegree}
import com.raphtory.algorithms.generic.dynamic.{WattsCascade, WeightedRandomWalk}
import com.raphtory.algorithms.generic.motif.{SquareCount, TriangleCount}
import com.raphtory.algorithms.temporal.dynamic.GenericTaint
import com.raphtory.core.components.graphbuilder.GraphBuilder
import com.raphtory.core.components.spout.Spout
import com.raphtory.output.FileOutputFormat
import com.raphtory.spouts.FileSpout
import com.raphtory.{BaseRaphtoryAlgoTest, GlobalState, GraphState}

import java.io.File
import scala.language.postfixOps
import scala.sys.process._


class LotrTest2 extends BaseRaphtoryAlgoTest[String] {
  val outputFormat: FileOutputFormat = FileOutputFormat(testDir)
  override def batchLoading(): Boolean = false

  test("Graph State Test") {
    assert(
      algorithmTest(GraphState(),outputFormat,1, 32674, 10000, List(500, 1000, 10000))equals "9fa9e48ab6e79e186bcacd7c9f9e3e60897c8657e76c348180be87abe8ec53fe"
    )
  }

  test("Global State Test") {
    assert(
      algorithmTest(new GlobalState(),outputFormat,1, 32674, 10000, List(500, 1000, 10000))equals "206d686bb8c5c119980d1743e4ec2aceb1dc62895d0931b5608f521e4da5c334"
    )
  }

  test("Degree Test") {
    assert(
      algorithmTest(Degree(), outputFormat, 1, 32674, 10000, List(500, 1000, 10000))
      equals "53fe18d6e38b2b32a1c8498100b888e3fd6b0d552dae99bb65fc29fd4f76336f"
    )
  }

//  test("Distinctiveness Test") {
//    //    TODO: Implement actual test as output is not deterministic due to floating point errors
//    algorithmTest(Distinctiveness(), outputFormat, 1, 32674, 10000, List(500, 1000, 10000))
//    assert(true)
//  }

  test("AverageNeighbourDegree Test") {
    assert(
      algorithmTest(AverageNeighbourDegree(), outputFormat, 1, 32674, 10000, List(500, 1000, 10000))
        equals "61d767d6ba98d5a06099d4f6d1e42f139dcb893b1caba7983ba4d87d648c6a8a"
    )
  }


  test("Connected Components Test") {
    assert(
      algorithmTest(ConnectedComponents(),outputFormat,1, 32674, 10000, List(500, 1000, 10000))
        equals "c6c26df04212ac7c0ba352d3acd79fb2c38f2c2943079bbe48dde9ea2b399410"
    )
  }

  override def setSpout(): Spout[String] = FileSpout(s"/tmp/lotr.csv")

  override def setGraphBuilder(): GraphBuilder[String] = new LOTRGraphBuilder()

  override def setup(): Unit = {
    if (!new File("/tmp", "lotr.csv").exists()) {
      val status = {s"curl -o /tmp/lotr.csv https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv " !}
      if (status != 0) {
        logger.error("Failed to download LOTR data!")

        "rm /tmp/lotr.csv" !
      }
    }
  }
}
