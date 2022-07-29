package com.raphtory.utils

import scala.math.Ordering.Implicits.infixOrderingOps

object OrderingFunctions {
  def max[T: Ordering](arg1: T, arg2: T): T = arg1 max arg2

  def min[T: Ordering](arg1: T, arg2: T): T = arg1 min arg2

}
