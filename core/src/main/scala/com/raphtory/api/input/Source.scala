package com.raphtory.api.input

import com.raphtory.internals.graph.GraphAlteration.GraphUpdate
import com.raphtory.internals.serialisers.Marshal
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

trait Source {
  def buildSource(deploymentID: String): SourceInstance
}

trait SourceInstance extends Iterator[GraphUpdate] {

  /** Logger instance for writing out log messages */
  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  def hasNextIterator(): Boolean
  def nextIterator(): Iterator[GraphUpdate]

  def close(): Unit

  def spoutReschedules(): Boolean
  def executeReschedule(): Unit
}

object Source {

  def apply[T](spout: Spout[T], builder: GraphBuilder[T], conf: Config): Source = {
    val failOnError: Boolean = conf.getBoolean("raphtory.builders.failOnError")
    new Source { // Avoid defining this as a lambda regardless of IntelliJ advices, that would cause serialization problems
      override def buildSource(deploymentID: String): SourceInstance =
        new SourceInstance {
          private val spoutInstance   = spout.buildSpout()
          private val builderInstance = builder.buildInstance(deploymentID)

          private var elementIterator = Iterator[GraphUpdate]()

          override def hasNextIterator(): Boolean = spoutInstance.hasNextIterator()

          override def nextIterator(): Iterator[GraphUpdate] =
            spoutInstance.nextIterator().flatMap { element =>
              builderInstance.getUpdates(element)(failOnError)
            }

          override def close(): Unit = spoutInstance.close()

          override def spoutReschedules(): Boolean = spoutInstance.spoutReschedules()

          override def executeReschedule(): Unit = spoutInstance.executeReschedule()

          override def hasNext: Boolean = elementIterator.hasNext || spoutInstance.hasNext

          override def next(): GraphUpdate = {
            if (!elementIterator.hasNext)
              elementIterator = builderInstance.getUpdates(spoutInstance.next())(failOnError).iterator
            elementIterator.next()
          }
        }
    }
  }
}
