package com.raphtory.algorithms.api

/**
  * {s}`Identity()`
  *  : Identity algorithm that does nothing and outputs an empty table (useful as a default argument for optional steps)
  *
  * ```{seealso}
  * [](com.raphtory.algorithms.api.GraphAlgorithm), [](com.raphtory.algorithms.generic.CBOD)
  * ```
  */
class Identity extends GraphAlgorithm {}

object Identity {
  def apply() = new Identity()
}
