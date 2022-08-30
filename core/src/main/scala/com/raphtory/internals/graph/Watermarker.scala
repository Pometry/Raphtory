package com.raphtory.internals.graph

import com.raphtory.internals.components.querymanager.WatermarkTime
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock
import scala.collection.concurrent.Map
import scala.collection.concurrent.TrieMap
import scala.collection.mutable

case class EdgeUpdate(time: Long, index: Long, src: Long, dst: Long)

object EdgeUpdate {
  implicit val ordering: Ordering[EdgeUpdate] = Ordering.by(e => (e.time, e.index, e.src, e.dst))
}

case class VertexUpdate(time: Long, index: Long, id: Long)

object VertexUpdate {
  implicit val ordering: Ordering[VertexUpdate] = Ordering.by(v => (v.time, v.index, v.id))
}

case class SourceCounter(var total: Long) {
  def increment() = total = total + 1;
  def get(): Long = total
}

private[raphtory] class Watermarker(graphID: String, storage: GraphPartition) {

  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))
  private val lock: Lock     = new ReentrantLock()

  private val sources: mutable.Map[Int, SourceCounter] = mutable.Map[Int, SourceCounter]()

  val oldestTime: AtomicLong = new AtomicLong(Long.MaxValue)
  val latestTime: AtomicLong = new AtomicLong(0)

  private var latestWatermark =
    WatermarkTime(storage.getPartitionID, Long.MaxValue, 0, safe = false, Array[(Int, Long)]())

  //Current unsynchronised updates
  private val edgeAdditions: mutable.TreeSet[EdgeUpdate] = mutable.TreeSet()
  private val edgeDeletions: mutable.TreeSet[EdgeUpdate] = mutable.TreeSet()

  private val vertexDeletions: Map[VertexUpdate, AtomicInteger] =
    new TrieMap[VertexUpdate, AtomicInteger]()

  def getLatestWatermark: WatermarkTime = latestWatermark

  def trackEdgeAddition(timestamp: Long, index: Long, src: Long, dst: Long): Unit = {
    lock.lock()
    try edgeAdditions += EdgeUpdate(timestamp, index, src, dst)
    catch {
      case e: Exception =>
        logger.error(e.getMessage) //shouldn't be any errors here, but just in case
    }
    lock.unlock()
  }

  def trackEdgeDeletion(timestamp: Long, index: Long, src: Long, dst: Long): Unit = {
    lock.lock()
    try edgeDeletions += EdgeUpdate(timestamp, index, src, dst)
    catch {
      case e: Exception =>
        logger.error(e.getMessage)
    }
    lock.unlock()
  }

  def trackVertexDeletion(timestamp: Long, index: Long, src: Long, size: Int): Unit = {
    lock.lock()
    try vertexDeletions put (VertexUpdate(timestamp, index, src), new AtomicInteger(size))
    catch {
      case e: Exception =>
        logger.error(e.getMessage)
    }
    lock.unlock()
  }

  def safeRecordCompletedUpdate(sourceID: Int) = {
    lock.lock()
    recordCompletedUpdate(sourceID)
    lock.unlock()
  }

  private def recordCompletedUpdate(sourceID: Int): Unit =
    sources.get(sourceID) match {
      case Some(tracker) => tracker.increment()
      case None          => sources.put(sourceID, new SourceCounter(1))
    }

  def untrackEdgeAddition(sourceID: Int, timestamp: Long, index: Long, src: Long, dst: Long): Unit = {
    lock.lock()
    recordCompletedUpdate(sourceID)
    try edgeAdditions -= EdgeUpdate(timestamp, index, src, dst)
    catch {
      case e: Exception =>
        logger.error(e.getMessage)
    }
    lock.unlock()
  }

  def untrackEdgeDeletion(sourceID: Int, timestamp: Long, index: Long, src: Long, dst: Long): Unit = {
    lock.lock()
    recordCompletedUpdate(sourceID)
    try edgeDeletions -= EdgeUpdate(timestamp, index, src, dst)
    catch {
      case e: Exception =>
        logger.error(e.getMessage)
    }
    lock.unlock()
  }

  def untrackVertexDeletion(sourceID: Int, timestamp: Long, index: Long, src: Long): Unit = {
    lock.lock()
    recordCompletedUpdate(sourceID)
    val update = VertexUpdate(timestamp, index, src)
    try vertexDeletions get update match {
      case Some(counter) => //if after we remove this value its now zero we can remove from the tree
        if (counter.decrementAndGet() == 0)
          vertexDeletions -= update
      case None          =>
    }
    catch {
      case e: Exception =>
        logger.error(e.getMessage)
    }
    lock.unlock()
  }

  def updateWatermark(): Unit = {
    lock.lock()
    try {
      val time             = latestTime.get()
      val edgeAdditionTime =
        if (edgeAdditions.nonEmpty)
          edgeAdditions.minBy(_.time).time
        else
          time

      val edgeDeletionTime =
        if (edgeDeletions.nonEmpty)
          edgeDeletions.minBy(_.time).time
        else
          time

      val vertexDeletionTime: Long =
        if (vertexDeletions.nonEmpty)
          vertexDeletions.keys.minBy(_.time).time //find the min and then extract the time
        else
          time

      val finalTime =
        Array(time, edgeAdditionTime, edgeDeletionTime, vertexDeletionTime).min

      val noBlockingOperations =
        edgeAdditions.isEmpty && edgeDeletions.isEmpty && vertexDeletions.isEmpty

      val sourceCount = sources
        .map({
          case (key, value) => (key, value.get())
        })
        .toArray

      val newWatermark =
        WatermarkTime(
                storage.getPartitionID,
                oldestTime.get(),
                finalTime,
                noBlockingOperations,
                sourceCount
        )

      if (newWatermark != latestWatermark)
        logger.trace(
                s"Partition ${storage.getPartitionID} for '$graphID': Creating watermark with " +
                  s"earliest time '${oldestTime.get()}' and latest time '$finalTime'."
        )

      latestWatermark = newWatermark
    }
    catch {
      case e: Exception =>
        logger.error(e.getMessage)
    }
    lock.unlock()
  }

}
