package com.raphtory.algorithms.api.algorithm

/**
  * Identity algorithm that does nothing and outputs an empty table (useful as a default argument for optional steps)
  *
  * @see [[GenericAlgorithm]], [[com.raphtory.algorithms.generic.CBOD]]
  */
class Identity extends GenericAlgorithm {}

object Identity {
  def apply() = new Identity()
}
