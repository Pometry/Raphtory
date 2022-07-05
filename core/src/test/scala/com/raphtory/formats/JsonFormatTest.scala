package com.raphtory.formats

import com.google.gson.Gson
import com.raphtory.Raphtory
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.output.format.Format
import com.raphtory.api.time.DiscreteInterval
import com.raphtory.internals.graph.Perspective
import munit.FunSuite

class JsonFormatTest extends FunSuite {
  private val jobID       = "job-id"
  private val partitionID = 13

  private val sampleTable = List(
          (Perspective(100, None, 0, 100), List(Row("id1", 34), Row("id2", 24))),
          (
                  Perspective(200, Some(DiscreteInterval(200)), 0, 200),
                  List(Row("id1", 56), Row("id2", 67))
          )
  )

  private val rowLevelOutput =
    """{"timestamp":100,"window":null,"row":["id1",34]}
      |{"timestamp":100,"window":null,"row":["id2",24]}
      |{"timestamp":200,"window":200,"row":["id1",56]}
      |{"timestamp":200,"window":200,"row":["id2",67]}
      |""".stripMargin

  private val globalLevelOutput =
    """{
      |  "jobID": "job-id",
      |  "partitionID": 13,
      |  "perspectives": [
      |    {
      |      "timestamp": 100,
      |      "window": null,
      |      "rows": [
      |        [
      |          "id1",
      |          34
      |        ],
      |        [
      |          "id2",
      |          24
      |        ]
      |      ]
      |    },
      |    {
      |      "timestamp": 200,
      |      "window": 200,
      |      "rows": [
      |        [
      |          "id1",
      |          56
      |        ],
      |        [
      |          "id2",
      |          67
      |        ]
      |      ]
      |    }
      |  ]
      |}
      |""".stripMargin

  test("JsonFormat.ROW level output matches table data") {
    val output = formatTable(JsonFormat(), sampleTable, jobID, partitionID)
    assertEquals(output, rowLevelOutput)
  }

  test("JsonFormat.ROW level output lines are valid JSON") {
    val output = formatTable(JsonFormat(), sampleTable, jobID, partitionID)
    val gson   = new Gson()
    output.split("\n") foreach { line =>
      gson.fromJson(line, classOf[Any])
    }
  }

  test("JsonFormat.GLOBAL level output matches table data") {
    val output = formatTable(JsonFormat(JsonFormat.GLOBAL), sampleTable, jobID, partitionID)
    assertEquals(output, globalLevelOutput)
  }

  test("JsonFormat.GLOBAL level output is valid JSON") {
    val output = formatTable(JsonFormat(JsonFormat.GLOBAL), sampleTable, jobID, partitionID)
    val gson   = new Gson()
    gson.fromJson(output, classOf[Any])
  }

  private def formatTable(
      format: Format,
      table: List[(Perspective, List[Row])],
      jobID: String,
      partitionID: Int
  ): String = {
    val sink     = StringSink(format = format)
    val executor = sink.executor(jobID, partitionID, Raphtory.getDefaultConfig())

    table foreach {
      case (perspective, rows) =>
        executor.setupPerspective(perspective)
        rows foreach (row => executor.threadSafeWriteRow(row))
        executor.closePerspective()
    }
    executor.close()

    sink.output
  }
}
