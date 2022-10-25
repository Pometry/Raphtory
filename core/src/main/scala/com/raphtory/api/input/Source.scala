package com.raphtory.api.input

import com.raphtory.internals.communication.EndPoint
import com.raphtory.internals.graph.GraphAlteration
import com.raphtory.internals.graph.GraphBuilderInstance
import com.twitter.chill.ClosureCleaner
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.concurrent.duration._

trait Source {
  type MessageType
  def spout: Spout[MessageType]
  def builder: GraphBuilder[MessageType]

  def getBuilderClass: Class[_] = builder.getClass

  def buildSource(graphID: String, id: Int): SourceInstance[MessageType] =
    new SourceInstance[MessageType](id, spout.buildSpout(), builder.buildInstance(graphID, id))
}

class ConcreteSource[T](override val spout: Spout[T], override val builder: GraphBuilder[T]) extends Source {
  override type MessageType = T

}

class SourceInstance[T](id: Int, spoutInstance: SpoutInstance[T], builderInstance: GraphBuilderInstance[T]) {

  /** Logger instance for writing out log messages */
  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))
  def sourceID: Int  = id

  def pollInterval: FiniteDuration = 1.seconds

  def hasRemainingUpdates: Boolean = spoutInstance.hasNext

  def sendUpdates(index: Long, failOnError: Boolean): Unit = {
    val element = spoutInstance.next()
    builderInstance.sendUpdates(element, index)(failOnError)
  }

  def spoutReschedules(): Boolean = spoutInstance.spoutReschedules()

  def executeReschedule(): Unit = spoutInstance.executeReschedule()

  def setupStreamIngestion(
      streamWriters: collection.Map[Int, EndPoint[GraphAlteration]]
  ): Unit =
    builderInstance.setupStreamIngestion(streamWriters)

  def close(): Unit = spoutInstance.close()

  def sentMessages(): Long = builderInstance.getSentUpdates

  def highestTimeSeen(): Long = builderInstance.highestTimeSeen
}

object Source {

  def apply[T](spout: Spout[T], builder: GraphBuilder[T]): Source =
    new ConcreteSource(spout, ClosureCleaner.clean(builder))

}
