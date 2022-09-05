package com.raphtory.api.input

import com.raphtory.Raphtory
import com.raphtory.internals.communication.EndPoint
import com.raphtory.internals.graph.GraphAlteration
import com.raphtory.internals.graph.GraphBuilder
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

trait Source {
  def buildSource(graphID: String, id: Int): SourceInstance
  def getBuilder: GraphBuilder[Any]
}

trait SourceInstance {

  /** Logger instance for writing out log messages */
  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))
  def hasRemainingUpdates: Boolean
  def sendUpdates(index: Long, failOnError: Boolean): Unit
  def spoutReschedules(): Boolean
  def executeReschedule(): Unit
  def setupStreamIngestion(streamWriters: collection.Map[Int, EndPoint[GraphAlteration]]): Unit
  def sourceID: Int
  def sentMessages(): Long
  def close(): Unit
}

object Source {

  def apply[T](spout: Spout[T], parseFunc: (Graph, T) => Unit): Source = {
    val builder = GraphBuilder(parseFunc)

    new Source { // Avoid defining this as a lambda regardless of IntelliJ advices, that would cause serialization problems
      override def buildSource(graphID: String, id: Int): SourceInstance =
        new SourceInstance {
          private val spoutInstance   = spout.buildSpout()
          private val builderInstance = builder.buildInstance(graphID, sourceID)
          def sourceID: Int           = id

          override def hasRemainingUpdates: Boolean = spoutInstance.hasNext

          override def sendUpdates(index: Long, failOnError: Boolean): Unit = {
            val element = spoutInstance.next()
            builderInstance.sendUpdates(element, index)(failOnError)
          }
          override def spoutReschedules(): Boolean = spoutInstance.spoutReschedules()
          override def executeReschedule(): Unit   = spoutInstance.executeReschedule()

          override def setupStreamIngestion(
              streamWriters: collection.Map[Int, EndPoint[GraphAlteration]]
          ): Unit                    =
            builderInstance.setupStreamIngestion(streamWriters)
          override def close(): Unit = spoutInstance.close()

          override def sentMessages(): Long = builderInstance.getSentUpdates
        }

      override def getBuilder: GraphBuilder[Any] = builder.asInstanceOf[GraphBuilder[Any]]
    }
  }

}
