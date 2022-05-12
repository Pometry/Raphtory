package com.raphtory.algorithms.api

/**
  * Identity algorithm that does nothing and outputs an empty table (useful as a default argument for optional steps)
  *
  * @see [[com.raphtory.algorithms.api.GraphAlgorithm]], [[com.raphtory.algorithms.generic.CBOD]]
  */
class Identity extends GraphAlgorithm {}

object Identity {
  def apply() = new Identity()
}
