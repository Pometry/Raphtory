package com.raphtory.api.input

import com.raphtory.Raphtory
import com.raphtory.internals.communication.EndPoint
import com.raphtory.internals.graph.GraphAlteration
import com.raphtory.internals.graph.GraphBuilderInstance
import com.raphtory.spouts.FileSpout
import com.twitter.chill.ClosureCleaner
//import com.raphtory.internals.graph.GraphBuilder
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import com.raphtory.internals.time.DateTimeParser.{defaultParse => parseDateTime}

trait Source {
  type MessageType
  def spout: Spout[MessageType]
  def builder: GraphBuilder[MessageType]

  def buildSource(graphID: String, id: Int): SourceInstance[MessageType]
  def getBuilderClass: Class[_] = builder.getClass
}

class ConcreteSource[T](override val spout: Spout[T], override val builder: GraphBuilder[T]) extends Source {
  override type MessageType = T

  def buildSource(graphID: String, id: Int): SourceInstance[T] =
    new SourceInstance[T](id, spout.buildSpout(), builder.buildInstance(graphID, id))
}

class EdgeListSource(override val spout: Spout[String]) extends Source {
  override type MessageType = String
  override def builder: GraphBuilder[String] =
    (graph: Graph, tuple: String) => {

      val fileLine = tuple.split(",").map(_.trim)
      val source = fileLine(0)
      val target = fileLine(1)
      val rawTime = fileLine(2)
      var isDateTime = false

      if (graph.index == 0) {
        try {
          rawTime.toLong match {
            case _: Long => isDateTime = false
          }
        }
        catch {
          case e: java.lang.NumberFormatException => isDateTime = true
          case _: Throwable => throw new RuntimeException("Check format.")
        }
      }

      def buildDateTimeGraph(time: Long) = {
        graph.addVertex(time, source)
        graph.addVertex(time, target)
        graph.addEdge(time, source, target)
      }

      if (isDateTime) {
        try {
          val time = parseDateTime(rawTime)
          buildDateTimeGraph(time)
        } catch {
          case _: Throwable => throw new RuntimeException("Timestamp not in DateTime or Epoch format, please check format.")
        }
      } else {
        val time = rawTime.toLong
        buildDateTimeGraph(time)
      }
    }
  def buildSource(graphID: String, id: Int): SourceInstance[String] =
    new SourceInstance[String](id, spout.buildSpout(), builder.buildInstance(graphID, id))

}

object EdgeListSource {
  def apply(path:String) = new EdgeListSource(FileSpout(path))
  def apply(spout: Spout[String]) = new EdgeListSource(spout)
}

class SourceInstance[T](id: Int, spoutInstance: SpoutInstance[T], builderInstance: GraphBuilderInstance[T]) {

  /** Logger instance for writing out log messages */
  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))
  def sourceID: Int  = id

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

  def apply(spout: Spout[String]): Source =
    new EdgeListSource(spout)
}
