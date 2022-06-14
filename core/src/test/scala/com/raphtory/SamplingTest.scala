package com.raphtory

import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import com.raphtory.utils.Sampling._

import scala.collection.compat.immutable.ArraySeq

class SamplingTest extends AnyFunSuite {

  def testDistribution(
      weights: Array[Double],
      numSamples: Int = 1000000,
      tol: Double = 0.001
  ): Boolean = {
    val rng    = new Random(1234)
    val result = ArrayBuffer.fill[Double](weights.length)(0.0)
    (0 until numSamples).foreach(_ => result(rng.sample(ArraySeq.unsafeWrapArray(weights))) += 1.0 / numSamples)
    val probs  = weights.map(v => v / weights.sum)
    probs.zip(result).forall(p => (p._1 - p._2).abs < tol)
  }

  test("Test sampling distribution for weighted sampling with uniform distribution") {
    val weights: Array[Double] = Array.fill[Double](10)(1.0)
    assert(testDistribution(weights))
  }

  test("Test sampling for weighted sampling with non-uniform distribution") {
    val weights = Array(1.0, 2.0, 5.0, 100.0, 1.0, 1.0)
    assert(testDistribution(weights))
  }

}
