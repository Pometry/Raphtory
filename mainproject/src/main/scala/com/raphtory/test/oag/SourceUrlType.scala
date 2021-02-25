package com.raphtory.examples.oag

object SourceUrlType extends Enumeration {
  type SourceUrlType = Value

  //  (1:HTML, 2:Text, 3:PDF, 4:DOC, 5:PPT, 6:XLS, 7:PS)
  val UNKNOWN: Value = Value("UNKNOWN")//added as a fallback
  val HTML: Value = Value("HTML")
  val TEXT: Value = Value("TEXT")
  val PDF: Value = Value("PDF")
  val DOC: Value = Value("DOC")
  val PPT: Value = Value("PPT")
  val XLS: Value = Value("XLS")
  val PS: Value = Value("PS")

}
