package com.raphtory.util

import scala.language.implicitConversions

object ExtendedNumeric {

  implicit def numericFromInt[T: Numeric](x: Int): T =
    implicitly[Numeric[T]].fromInt(x) // automatic conversion from Int for nicer syntax

}
