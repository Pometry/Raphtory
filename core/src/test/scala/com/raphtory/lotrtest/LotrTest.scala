package com.raphtory.lotrtest

import com.raphtory.BaseRaphtoryAlgoTest
import com.raphtory.GlobalState
import com.raphtory.GraphState
import com.raphtory.Raphtory
import com.raphtory.algorithms.generic.BinaryDiffusion
import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.algorithms.generic.centrality.AverageNeighbourDegree
import com.raphtory.algorithms.generic.centrality.Degree
import com.raphtory.algorithms.generic.centrality.Distinctiveness
import com.raphtory.algorithms.generic.centrality.PageRank
import com.raphtory.algorithms.generic.centrality.WeightedDegree
import com.raphtory.algorithms.generic.centrality.WeightedPageRank
import com.raphtory.algorithms.generic.community.LPA
import com.raphtory.algorithms.generic.community.SLPA
import com.raphtory.algorithms.generic.dynamic.DiscreteSI
import com.raphtory.algorithms.generic.dynamic.RandomWalk
import com.raphtory.algorithms.generic.dynamic.WattsCascade
import com.raphtory.algorithms.generic.dynamic.WeightedRandomWalk
import com.raphtory.algorithms.generic.motif.SquareCount
import com.raphtory.algorithms.generic.motif.TriangleCount
import com.raphtory.algorithms.temporal.Ancestors
import com.raphtory.algorithms.temporal.Descendants
import com.raphtory.algorithms.temporal.dynamic.GenericTaint
import com.raphtory.api.input.GraphBuilder
import com.raphtory.api.input.Spout
import com.raphtory.sinks.FileSink
import com.raphtory.spouts.FileSpout

import java.io.File
import java.net.URL
import scala.language.postfixOps
import sys.process._

class LotrTest extends BaseRaphtoryAlgoTest[String] {

  withGraph.test("Graph State Test") { graph =>
    val expected = "c21170ae40544156af69000d2b0d6e8eaf5f593d3905810c7527f2e09b8e9172"
    algorithmTest(
            algorithm = GraphState(),
            start = 1,
            end = 32674,
            increment = 10000,
            windows = List(500, 1000, 10000)
    )(graph)
      .map(result => assertEquals(result, expected))
  }

  withGraph.test("Global State Test") { graph =>
    algorithmTest(
            algorithm = new GlobalState(),
            start = 1,
            end = 32674,
            increment = 10000,
            windows = List(500, 1000, 10000)
    )(graph).map(assertEquals(_, "206d686bb8c5c119980d1743e4ec2aceb1dc62895d0931b5608f521e4da5c334"))
  }

  withGraph.test("Degree Test") { graph =>
    algorithmTest(
            algorithm = Degree,
            start = 1,
            end = 32674,
            increment = 10000,
            windows = List(500, 1000, 10000)
    )(graph).map(assertEquals(_, "53fe18d6e38b2b32a1c8498100b888e3fd6b0d552dae99bb65fc29fd4f76336f"))
  }

//  withGraph.test("Distinctiveness Test") { graph =>
//    //    TODO: Implement actual test as output is not deterministic due to floating point errors
//    algorithmTest(Distinctiveness[Double](), 1, 32674, 10000, List(500, 1000, 10000))(graph).map(_ =>
//      assert(cond = true)
//    )
//  }

  withGraph.test("AverageNeighbourDegree Test") { graph =>
    algorithmTest(
            algorithm = AverageNeighbourDegree,
            start = 1,
            end = 32674,
            increment = 10000,
            windows = List(500, 1000, 10000)
    )(graph).map(assertEquals(_, "61d767d6ba98d5a06099d4f6d1e42f139dcb893b1caba7983ba4d87d648c6a8a"))

  }

//  test("PageRank Test") {
//    //    TODO: Implement actual test as output is not deterministic due to floating point errors
//    algorithmTest(PageRank(), 1, 32674, 10000, List(500, 1000, 10000))
//    assert(true)
//  }

//  test("WeightedPageRank Test") {
//    //    TODO: Implement actual test as output is not deterministic due to floating point errors
//    algorithmTest(WeightedPageRank(), 1, 32674, 10000, List(500, 1000, 10000))
//    assert(true)
//  }

  withGraph.test("Strength Test") { graph =>
    algorithmTest(
            algorithm = WeightedDegree[Long](),
            start = 1,
            end = 32674,
            increment = 10000,
            windows = List(500, 1000, 10000)
    )(graph).map(assertEquals(_, "ce2c985cd5db976c5fda5ffaa317d52f8e04236cc602b41468eb80d01be333ac"))

  }

//  test("LPA Test") {
//    assert(
//      algorithmTest(LPA[Int](seed=1234), 32674, 32674, 10000, List(10000))
//      equals "cf7bf559d634a0cf02739d9116b4d2f47c25679be724a896223c0917d55d2143"
//    )
//  }
//
//  test("SLPA Test") {
//    assert(
//      algorithmTest(SLPA(speakerRule = SLPA.ChooseRandom(seed=1234)), 32674, 32674, 10000, List(10000))
//      equals "a7c72dac767dc94d64d76e2c046c1dbe95154a8da7994d2133cf9e1b09b65570"
//    )
//  }

  withGraph.test("Connected Components Test") { graph =>
    algorithmTest(
            algorithm = ConnectedComponents(),
            start = 1,
            end = 32674,
            increment = 10000,
            windows = List(500, 1000, 10000)
    )(graph).map(assertEquals(_, "c6c26df04212ac7c0ba352d3acd79fb2c38f2c2943079bbe48dde9ea2b399410"))
  }
//
//  test("Random Walk Test") {
////    TODO: non-deterministic even with fixed seed, maybe message order is non-deterministic?
//    algorithmTest(RandomWalk(seed = 1234), 1, 32674, 10000, List(500, 1000, 10000))
//    assert(true)
//  }

  withGraph.test("Watts Cascade Test") { graph =>
    algorithmTest(
            algorithm = WattsCascade(infectedSeed = Array("Gandalf"), threshold = 0.1),
            start = 1,
            end = 32674,
            increment = 10000,
            windows = List(500, 1000, 10000)
    )(graph).map(assertEquals(_, "772d24456e6b63f2a7b4c4111f34ea0685344d16237b77880058770903b5ae27"))
  }

//  test("DiscreteSI test") {
//    assert(
//      algorithmTest(DiscreteSI(Set("Gandalf"), seed=1234), 1, 32674, 10000, List(500, 1000, 10000))
//      equals "57191e340ef3e8268d255751b14fff76292087af2365048d961d59a5c0fbbc3f"
//    )
//  }

  withGraph.test("Chain Test") { graph =>
    algorithmTest(
            algorithm = TriangleCount -> ConnectedComponents(),
            start = 1,
            end = 32674,
            increment = 10000,
            windows = List(500, 1000, 10000)
    )(graph).map(assertEquals(_, "c6c26df04212ac7c0ba352d3acd79fb2c38f2c2943079bbe48dde9ea2b399410"))
  }

  withGraph.test("Square counting test") { graph =>
    algorithmTest(
            algorithm = SquareCount,
            start = 1,
            end = 32674,
            increment = 10000,
            windows = List(500, 1000, 10000)
    )(graph).map(assertEquals(_, "7f025a14361326d15e6ce9736cc5b292873a9b3a638e1f3bda1f029b44153cd8"))
  }

  withGraph.test("Temporal Triangle Count") { graph =>
    algorithmTest(
            algorithm = TriangleCount,
            start = 1,
            end = 32674,
            increment = 10000,
            windows = List(500, 1000, 10000)
    )(graph).map(assertEquals(_, "91588edb0139e62ff1acc1be54d89a12e1691bf1ef610da8667f91e5089a0d27"))
  }

  withGraph.test("Taint Tracking") { graph =>
    algorithmTest(
            algorithm = GenericTaint(1, infectedNodes = Set("Bilbo"), stopNodes = Set("Aragorn")),
            start = 1,
            end = 32674,
            increment = 10000,
            windows = List(500, 1000, 10000)
    )(graph).map(assertEquals(_, "85c24ece1ac693814abbac304d18858572a0d7644457f9272cf642caf8517660"))
  }

  withGraph.test("Weighted Random Walk") { graph =>
    algorithmPointTest(
            algorithm = WeightedRandomWalk[Int](),
            timestamp = 32674
    )(graph).map(_ => assert(cond = true))
  }

  withGraph.test("Ancestors Test") { graph =>
    algorithmTest(
            algorithm = Ancestors("Gandalf", 32674, strict = false),
            start = 1,
            end = 32674,
            increment = 10000,
            windows = List(500, 1000, 10000)
    )(graph).map(assertEquals(_, "5f7055f9493d2b328f8e4e13239e683276f48ab3e44d7f3a13a61347405b35a7"))
  }

  withGraph.test("Descendants Test") { graph =>
    algorithmTest(
            algorithm = Descendants("Gandalf", 1000, strict = false),
            start = 1,
            end = 32674,
            increment = 10000,
            windows = List(500, 1000, 10000)
    )(graph).map(assertEquals(_, "3d31b8b47bd25d919993680eed78c51fb991cb062f863025d2e795ecac999873"))
  }

// TODO Re-enable with Seed to produce same result
//  test("Binary Diffusion") {
//    assert(
//      algorithmTest(BinaryDiffusion(seed=1, reinfect=false), 1, 32674, 10000, List(500, 1000, 10000))
//        equals "dc7a2a28857913f03f3f955353cfe6701abbf4703441ae6bdf92ec454efdd46b"
//    )
//  }

  def tmpFilePath = "/tmp/lotr.csv"

  override def liftFileIfNotPresent: Option[(String, URL)] =
    Some(tmpFilePath, new URL("https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv"))

  override def setSpout(): Spout[String] = FileSpout(tmpFilePath)

  override def setGraphBuilder(): GraphBuilder[String] = new LOTRGraphBuilder()

}
