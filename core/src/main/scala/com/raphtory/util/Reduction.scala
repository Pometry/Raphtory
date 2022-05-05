package com.raphtory.util

/** Extra reduction functions for collections of `Numeric` type  */
object Reduction {

  implicit class IterableWithAvg[T](data: Iterable[T])(implicit numeric: Numeric[T]) {
    /** Calculate the mean value of the collection */
    def mean: Double = numeric.toDouble(data.sum) / data.size
  }
}
