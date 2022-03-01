package com.raphtory.algorithms.utils

import scala.util.Random

/**
  * Extended sampling methods for {s}`scala.util.Random`.
  *
  * To make these methods available to instances of {s}`scala.util.Random`, use
  *
  * ```{code-block} scala
  * import com.raphtory.algorithms.utils.Sampling._
  * ```
  *
  * ## Methods
  *
  *  {s}`sample(weights: Seq[Double]): Int`
  *    : Weighted random sampling. Returns integer {s}`i` with probability proportional to {s}`weights(i)`.
  *      ```{note}
  *      This implementation uses binary search to sample the index.
  *      ```
  */
object Sampling {

  implicit class WeightedSampling(val random: Random) extends AnyVal {

    def sample(weights: Seq[Double]): Int = {
      val probs = weights.scanLeft(0.0)(_ + _).toArray
      val i     = probs.search(random.nextDouble() * probs.last).insertionPoint - 1
      i
    }
  }
}
