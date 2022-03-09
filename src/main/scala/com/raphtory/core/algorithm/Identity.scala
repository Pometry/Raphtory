package com.raphtory.core.algorithm

/**
  * {s}`Identity()`
  *  : Identity algorithm that does nothing and outputs an empty table (useful as a default argument for optional steps)
  *
  * ```{seealso}
  * [](com.raphtory.core.algorithm.GraphAlgorithm), [](com.raphtory.algorithms.generic.CBOD)
  * ```
  */
class Identity extends GraphAlgorithm {}

object Identity {
  def apply() = new Identity()
}
