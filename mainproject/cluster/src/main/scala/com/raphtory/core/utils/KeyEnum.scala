package com.raphtory.core.utils

object KeyEnum extends Enumeration {
  val vertices : Value = Value("vertices")
  val edges    : Value = Value("edges")
}

object SubKeyEnum extends Enumeration {
  val history      : Value = Value("history")
  val creationTime : Value = Value("creationTime")
}
