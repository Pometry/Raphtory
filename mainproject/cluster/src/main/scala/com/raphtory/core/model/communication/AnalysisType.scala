package com.raphtory.core.model.communication
import com.raphtory.core.utils.KeyEnum.Value

object AnalysisType extends Enumeration {
  val live : Value = Value("live")
  val view    : Value = Value("view")
  val range : Value  = Value("range")
}
