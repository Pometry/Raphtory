package com.raphtory.util

import scala.language.implicitConversions

/**
  * {s}`ExtendedNumeric`
  *  : Extended syntax to make working with abstract numeric types more intuitive
  *
  * ## Implicit conversions
  *
  * {s}`numericFromInt[T](x: Int)(implicit numeric: Numeric[T]): T`
  *  : Implicitly convert integers to arbitrary numeric type. This conversion simply uses {s}`numeric.fromInt` and
  *    makes it possible to use, e.g., {s}`val a: T = 1` instead of the much more cumbersome
  *    {s}`val a: T = numeric.fromInt(1)`.
  */
object ExtendedNumeric {

  implicit def numericFromInt[T](x: Int)(implicit numeric: Numeric[T]): T =
    numeric.fromInt(x) // automatic conversion from Int for nicer syntax

}
