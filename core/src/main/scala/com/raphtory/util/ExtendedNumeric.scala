package com.raphtory.util

import scala.language.implicitConversions

/** Extended syntax to make working with abstract numeric types more intuitive */
object ExtendedNumeric {
  /** Implicitly convert integers to arbitrary numeric type. This conversion simply uses `numeric.fromInt` and
    * makes it possible to use, e.g., `val a: T = 1` instead of the much more cumbersome
    * {{{ `val a: T = numeric.fromInt(1)` }}} */
  implicit def numericFromInt[T](x: Int)(implicit numeric: Numeric[T]): T =
    numeric.fromInt(x) // automatic conversion from Int for nicer syntax
}
