package com.raphtory.algorithms.utils

import scala.util.Random

object Sampling {

  implicit class WeightedSampling(val random: Random) extends AnyVal {

    def sample(weights: Seq[Double]): Int = {
      val probs = weights.scanLeft(0.0)(_ + _).toArray
      val i     = probs.search(random.nextDouble() * probs.last).insertionPoint - 1
      i
    }
  }
}
