package com.raphtory.formats

import com.raphtory.Raphtory
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.output.format.Format
import com.raphtory.api.time.DiscreteInterval
import com.raphtory.internals.graph.Perspective
import munit.FunSuite

class FormatTest extends FunSuite {
  protected val jobID       = "job-id"
  protected val partitionID = 13

  protected val rowTable = List(
          (Perspective(100, None, 0, 100), List(Row("id1", 34), Row("id2", 24))),
          (
                  Perspective(200, Some(DiscreteInterval(200)), 0, 200),
                  List(Row("id1", 56), Row("id2", 67))
          )
  )

  case class Person(name: String, age: Int)
  protected val caseClassTable = List((Perspective(100, None, 0, 100), List(Person("John", 25))))

  protected def formatTable(
      format: Format,
      table: List[(Perspective, List[Any])],
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
