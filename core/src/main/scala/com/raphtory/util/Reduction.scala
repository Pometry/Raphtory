package com.raphtory.util

/**
  * Extra reduction functions for collections of `Numeric` type
  *
  * ## Methods
  *
  *  `mean: Double`
  *    : Calculate the mean value of the collection
  */
object Reduction {

  implicit class IterableWithAvg[T](data: Iterable[T])(implicit numeric: Numeric[T]) {
    def mean: Double = numeric.toDouble(data.sum) / data.size
  }
}
