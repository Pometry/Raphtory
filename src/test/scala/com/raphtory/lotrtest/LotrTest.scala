

package com.raphtory.lotrtest

import com.raphtory.{BaseRaphtoryAlgoTest, GlobalState, GraphState}
import com.raphtory.algorithms.generic.{BinaryDiffusion, ConnectedComponents}
import com.raphtory.algorithms.generic.centrality.{AverageNeighbourDegree, Degree, Distinctiveness, PageRank, WeightedDegree, WeightedPageRank}
import com.raphtory.algorithms.generic.community.{LPA, SLPA}
import com.raphtory.algorithms.generic.dynamic.{DiscreteSI, RandomWalk, WattsCascade, WeightedRandomWalk}
import com.raphtory.algorithms.generic.motif.{SquareCount, TriangleCount}
import com.raphtory.algorithms.temporal.dynamic.GenericTaint
import com.raphtory.core.components.spout.SpoutExecutor
import com.raphtory.core.components.graphbuilder.GraphBuilder
import com.raphtory.core.components.spout.Spout
import com.raphtory.core.components.spout.instance.FileSpout
import com.raphtory.output.FileOutputFormat
import org.apache.pulsar.client.api.Schema

import java.io.File
import scala.language.postfixOps
import sys.process._



class LotrTest extends BaseRaphtoryAlgoTest[String] {
  val outputFormat: FileOutputFormat = FileOutputFormat(testDir)

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

//  test("PageRank Test") {
//    //    TODO: Implement actual test as output is not deterministic due to floating point errors
//    algorithmTest(PageRank(), outputFormat, 1, 32674, 10000, List(500, 1000, 10000))
//    assert(true)
//  }

//  test("WeightedPageRank Test") {
//    //    TODO: Implement actual test as output is not deterministic due to floating point errors
//    algorithmTest(WeightedPageRank(), outputFormat, 1, 32674, 10000, List(500, 1000, 10000))
//    assert(true)
//  }

  test("Strength Test") {
    assert(
      algorithmTest(WeightedDegree[Int](), outputFormat, 1, 32674, 10000, List(500, 1000, 10000))
      equals "887021d9c7254255daf801d1b733fe0d4d0ba31b9be68cd345d790c8374ce57b"
    )
  }

  test("LPA Test") {
    assert(
      algorithmTest(LPA[Int](seed=1234), outputFormat, 32674, 32674, 10000, List(10000))
      equals "cf7bf559d634a0cf02739d9116b4d2f47c25679be724a896223c0917d55d2143"
    )
  }

  test("SLPA Test") {
    assert(
      algorithmTest(SLPA(speakerRule = SLPA.ChooseRandom(seed=1234)), outputFormat, 32674, 32674, 10000, List(10000))
      equals "a7c72dac767dc94d64d76e2c046c1dbe95154a8da7994d2133cf9e1b09b65570"
    )
  }

  test("Connected Components Test") {
    assert(
      algorithmTest(ConnectedComponents(),outputFormat,1, 32674, 10000, List(500, 1000, 10000))
        equals "c6c26df04212ac7c0ba352d3acd79fb2c38f2c2943079bbe48dde9ea2b399410"
    )
  }

//  test("Random Walk Test") {
////    TODO: non-deterministic even with fixed seed, maybe message order is non-deterministic?
//      algorithmTest(RandomWalk(seed=1234), outputFormat, 1, 32674, 10000, List(500, 1000, 10000))
//    assert(true)
//  }

  test("Watts Cascade Test") {
    assert(
      algorithmTest(WattsCascade(infectedSeed = Array("Gandalf"), threshold = 0.1), outputFormat, 1, 32674, 10000, List(500, 1000, 10000))
      equals "772d24456e6b63f2a7b4c4111f34ea0685344d16237b77880058770903b5ae27"
    )
  }

//  test("DiscreteSI test") {
//    assert(
//      algorithmTest(DiscreteSI(Set("Gandalf"), seed=1234), outputFormat, 1, 32674, 10000, List(500, 1000, 10000))
//      equals "57191e340ef3e8268d255751b14fff76292087af2365048d961d59a5c0fbbc3f"
//    )
//  }

  test("Chain Test") {
    assert(
      algorithmTest(TriangleCount() -> ConnectedComponents(),outputFormat,1, 32674, 10000, List(500, 1000, 10000)) equals "c6c26df04212ac7c0ba352d3acd79fb2c38f2c2943079bbe48dde9ea2b399410"
    )
  }

  test("Square counting test") {
    val result = algorithmTest(SquareCount(), outputFormat,1, 32674, 10000, List(500, 1000, 10000))
    assert(result equals "7f025a14361326d15e6ce9736cc5b292873a9b3a638e1f3bda1f029b44153cd8")
  }

  test("Temporal Triangle Count") {
    assert(
      algorithmTest(TriangleCount(),outputFormat,1, 32674, 10000, List(500, 1000, 10000)) equals "91588edb0139e62ff1acc1be54d89a12e1691bf1ef610da8667f91e5089a0d27"
    )
  }

  test("Taint Tracking") {
    assert(
      algorithmTest(GenericTaint(1, infectedNodes = Set("Bilbo"), stopNodes = Set("Aragorn")),outputFormat,1, 32674, 10000, List(500, 1000, 10000))
        equals "85c24ece1ac693814abbac304d18858572a0d7644457f9272cf642caf8517660"
    )
  }

  test("Weighted Random Walk") {
    algorithmPointTest(WeightedRandomWalk[Int](), outputFormat, 32674)
    assert(true)
  }

  // TODO Re-enable with Seed to produce same result
//  test("Binary Diffusion") {
//    assert(
//      algorithmTest(BinaryDiffusion(seed=1, reinfect=false),outputFormat,1, 32674, 10000, List(500, 1000, 10000))
//        equals "dc7a2a28857913f03f3f955353cfe6701abbf4703441ae6bdf92ec454efdd46b"
//    )
//  }

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
