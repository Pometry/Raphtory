package com.raphtory.lotrtest

import com.raphtory.BaseRaphtoryAlgoTest
import com.raphtory.GlobalState
import com.raphtory.GraphState
import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.algorithms.generic.centrality.AverageNeighbourDegree
import com.raphtory.algorithms.generic.centrality.Degree
import com.raphtory.algorithms.generic.centrality.WeightedDegree
import com.raphtory.algorithms.generic.dynamic.WattsCascade
import com.raphtory.algorithms.generic.dynamic.WeightedRandomWalk
import com.raphtory.algorithms.generic.motif.SquareCount
import com.raphtory.algorithms.generic.motif.LocalTriangleCount
import com.raphtory.algorithms.temporal.Ancestors
import com.raphtory.algorithms.temporal.Descendants
import com.raphtory.algorithms.temporal.dynamic.GenericTaint
import com.raphtory.api.input.Graph
import com.raphtory.api.input.Spout
import com.raphtory.internals.graph.GraphBuilder
import com.raphtory.spouts.FileSpout

import java.net.URL
import scala.language.postfixOps

class LotrTest extends BaseRaphtoryAlgoTest[String] {

  test("Graph State Test") {
    val expected = "5386ab26d807ceebc3be32ac6284c74a9e5f509db8443816ee10c5d6fadda582"
    algorithmTest(
            algorithm = GraphState(),
            start = 1,
            end = 32674,
            increment = 10000,
            windows = List(500, 1000, 10000)
    ).map(result => assertEquals(result, expected))
  }

  test("Global State Test") {
    algorithmTest(
            algorithm = new GlobalState(),
            start = 1,
            end = 32674,
            increment = 10000,
            windows = List(500, 1000, 10000)
    ).map(assertEquals(_, "206d686bb8c5c119980d1743e4ec2aceb1dc62895d0931b5608f521e4da5c334"))
  }

  test("Degree Test") {
    algorithmTest(
            algorithm = Degree(),
            start = 1,
            end = 32674,
            increment = 10000,
            windows = List(500, 1000, 10000)
    ).map(assertEquals(_, "53fe18d6e38b2b32a1c8498100b888e3fd6b0d552dae99bb65fc29fd4f76336f"))
  }

//  test("Distinctiveness Test") {
//    //    TODO: Implement actual test as output is not deterministic due to floating point errors
//    algorithmTest(Distinctiveness[Double](), 1, 32674, 10000, List(500, 1000, 10000)).map(_ =>
//      assert(cond = true)
//    )
//  }

  test("AverageNeighbourDegree Test") {
    algorithmTest(
            algorithm = AverageNeighbourDegree,
            start = 1,
            end = 32674,
            increment = 10000,
            windows = List(500, 1000, 10000)
    ).map(assertEquals(_, "61d767d6ba98d5a06099d4f6d1e42f139dcb893b1caba7983ba4d87d648c6a8a"))

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

  test("Strength Test") {
    algorithmTest(
            algorithm = WeightedDegree[Long](),
            start = 1,
            end = 32674,
            increment = 10000,
            windows = List(500, 1000, 10000)
    ).map(assertEquals(_, "ce2c985cd5db976c5fda5ffaa317d52f8e04236cc602b41468eb80d01be333ac"))

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

  test("Connected Components Test") {
    algorithmTest(
            algorithm = ConnectedComponents,
            start = 1,
            end = 32674,
            increment = 10000,
            windows = List(500, 1000, 10000)
    ).map(assertEquals(_, "c6c26df04212ac7c0ba352d3acd79fb2c38f2c2943079bbe48dde9ea2b399410"))
  }
//
//  test("Random Walk Test") {
////    TODO: non-deterministic even with fixed seed, maybe message order is non-deterministic?
//    algorithmTest(RandomWalk(seed = 1234), 1, 32674, 10000, List(500, 1000, 10000))
//    assert(true)
//  }

  test("Watts Cascade Test") {
    algorithmTest(
            algorithm = WattsCascade(infectedSeed = Array("Gandalf"), threshold = 0.1),
            start = 1,
            end = 32674,
            increment = 10000,
            windows = List(500, 1000, 10000)
    ).map(assertEquals(_, "772d24456e6b63f2a7b4c4111f34ea0685344d16237b77880058770903b5ae27"))
  }

//  test("DiscreteSI test") {
//    assert(
//      algorithmTest(DiscreteSI(Set("Gandalf"), seed=1234), 1, 32674, 10000, List(500, 1000, 10000))
//      equals "57191e340ef3e8268d255751b14fff76292087af2365048d961d59a5c0fbbc3f"
//    )
//  }

  test("Chain Test") {
    algorithmTest(
            algorithm = LocalTriangleCount() -> ConnectedComponents,
            start = 1,
            end = 32674,
            increment = 10000,
            windows = List(500, 1000, 10000)
    ).map(assertEquals(_, "c6c26df04212ac7c0ba352d3acd79fb2c38f2c2943079bbe48dde9ea2b399410"))
  }

  test("Square counting test") {
    algorithmTest(
            algorithm = SquareCount,
            start = 1,
            end = 32674,
            increment = 10000,
            windows = List(500, 1000, 10000)
    ).map(assertEquals(_, "7f025a14361326d15e6ce9736cc5b292873a9b3a638e1f3bda1f029b44153cd8"))
  }

  test("Temporal Triangle Count") {
    algorithmTest(
            algorithm = LocalTriangleCount(),
            start = 1,
            end = 32674,
            increment = 10000,
            windows = List(500, 1000, 10000)
    ).map(assertEquals(_, "91588edb0139e62ff1acc1be54d89a12e1691bf1ef610da8667f91e5089a0d27"))
  }

  test("Taint Tracking") {
    algorithmTest(
            algorithm = GenericTaint(1, infectedNodes = Set("Bilbo"), stopNodes = Set("Aragorn")),
            start = 1,
            end = 32674,
            increment = 10000,
            windows = List(500, 1000, 10000)
    ).map(assertEquals(_, "f59f9a0d2fc205ac3909abe4ddfcd4f63950e17eb85c6ffe48328529d19f93b2"))
  }

  test("Weighted Random Walk") {
    algorithmPointTest(
            algorithm = WeightedRandomWalk[Int](),
            timestamp = 32674
    ).map(_ => assert(cond = true))
  }

  test("Ancestors Test") {
    algorithmTest(
            algorithm = Ancestors("Gandalf", 32674, strict = false),
            start = 1,
            end = 32674,
            increment = 10000,
            windows = List(500, 1000, 10000)
    ).map(assertEquals(_, "5f7055f9493d2b328f8e4e13239e683276f48ab3e44d7f3a13a61347405b35a7"))
  }

  test("Descendants Test") {
    algorithmTest(
            algorithm = Descendants("Gandalf", 1000, strict = false),
            start = 1,
            end = 32674,
            increment = 10000,
            windows = List(500, 1000, 10000)
    ).map(assertEquals(_, "3d31b8b47bd25d919993680eed78c51fb991cb062f863025d2e795ecac999873"))
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

  override def setGraphBuilder(): (Graph, String) => Unit = LOTRGraphBuilder.parse

}
