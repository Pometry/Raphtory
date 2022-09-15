package com.raphtory.internals.components.output

import com.raphtory.api.analysis.table.Row
import com.raphtory.api.output.sink.Sink
import com.raphtory.api.output.sink.SinkExecutor
import com.raphtory.api.time.Perspective
import com.raphtory.internals.communication.EndPoint
import com.raphtory.internals.communication.TopicRepository
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

sealed private[raphtory] trait OutputMessages
final private[raphtory] case class RowOutput(perspective: Perspective, row: Row) extends OutputMessages
final private[raphtory] case class EndPerspective(perspective: Perspective)      extends OutputMessages
private[raphtory] case object EndOutput                                          extends OutputMessages

private[raphtory] class TableOutputSinkExecutor(endPoint: EndPoint[OutputMessages]) extends SinkExecutor {
  private var currentPerspective: Perspective = _

  override def setupPerspective(perspective: Perspective): Unit = {
    logger.debug(s"setting current perspective to $perspective")
    currentPerspective = perspective
  }

  override protected def writeRow(row: Row): Unit = {
    logger.debug(s"writing row $row")
    endPoint.sendAsync(RowOutput(currentPerspective, row))
  }

  override def threadSafeWriteRow(row: Row): Unit = writeRow(row)

  override def closePerspective(): Unit = {
    logger.debug(s"closing perspective $currentPerspective")
    endPoint.sendSync(EndPerspective(currentPerspective))
  }

  override def close(): Unit = {
    logger.debug("closing output")
    endPoint.sendSync(EndOutput)
  }
}

private[raphtory] case object TableOutputSink extends Sink {

  override def executor(jobID: String, partitionID: Int, config: Config, topics: TopicRepository): SinkExecutor =
    new TableOutputSinkExecutor(topics.output(jobID).endPoint)
}
