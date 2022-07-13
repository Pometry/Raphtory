package com.raphtory.formats

class CsvFormatTest extends FormatTest {

  private val rowOutput =
    """100,id1,34
      |100,id2,24
      |200,200,id1,56
      |200,200,id2,67
      |""".stripMargin

  private val rowOutputWithHeader =
    """timestamp,1,2
      |100,id1,34
      |100,id2,24
      |200,200,id1,56
      |200,200,id2,67
      |""".stripMargin

  private val caseClassOutputWithHeader =
    """timestamp,name,age
      |100,John,25
      |""".stripMargin

  test("CsvFormat output for row table matches data") {
    val output = formatTable(CsvFormat(), rowTable, jobID, partitionID)
    assertEquals(output, rowOutput)
  }

  test("CsvFormat output for row table prints correct header") {
    val output = formatTable(CsvFormat(header = true), rowTable, jobID, partitionID)
    assertEquals(output, rowOutputWithHeader)
  }

  test("CsvFormat output for case class table matches data") {
    val output = formatTable(CsvFormat(header = true), caseClassTable, jobID, partitionID)
    assertEquals(output, caseClassOutputWithHeader)
  }
}
