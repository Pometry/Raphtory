package com.raphtory.internals.graph

import com.raphtory.internals.components.querymanager.WatermarkTime
import com.typesafe.scalalogging.Logger
import monix.execution.atomic.AtomicLong
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock
import scala.collection.concurrent.Map
import scala.collection.concurrent.TrieMap
import scala.collection.mutable

class Watermarker(storage: GraphPartition) {

  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))
  private val lock: Lock     = new ReentrantLock()

  val oldestTime: AtomicLong = AtomicLong(Long.MaxValue)
  val latestTime: AtomicLong = AtomicLong(0)

  private var latestWatermark =
    WatermarkTime(storage.getPartitionID, Long.MaxValue, 0, safe = false)

  //Current unsynchronised updates
  private val edgeAdditions: mutable.TreeSet[(Long, (Long, Long))] = mutable.TreeSet()
  private val edgeDeletions: mutable.TreeSet[(Long, (Long, Long))] = mutable.TreeSet()

  private val vertexDeletions: Map[(Long, Long), AtomicInteger] =
    new TrieMap[(Long, Long), AtomicInteger]()

  def getLatestWatermark: WatermarkTime = latestWatermark

  def trackEdgeAddition(timestamp: Long, src: Long, dst: Long): Unit = {
    lock.lock()
    try edgeAdditions += ((timestamp, (src, dst)))
    catch {
      case e: Exception =>
        logger.error(e.getMessage) //shouldn't be any errors here, but just in case
    }
    lock.unlock()
  }

  def trackEdgeDeletion(timestamp: Long, src: Long, dst: Long): Unit = {
    lock.lock()
    try edgeDeletions += ((timestamp, (src, dst)))
    catch {
      case e: Exception =>
        logger.error(e.getMessage)
    }
    lock.unlock()
  }

  def trackVertexDeletion(timestamp: Long, src: Long, size: Int): Unit = {
    lock.lock()
    try vertexDeletions put ((timestamp, src), new AtomicInteger(size))
    catch {
      case e: Exception =>
        logger.error(e.getMessage)
    }
    lock.unlock()
  }

  def untrackEdgeAddition(timestamp: Long, src: Long, dst: Long): Unit = {
    lock.lock()
    try edgeAdditions -= ((timestamp, (src, dst)))
    catch {
      case e: Exception =>
        logger.error(e.getMessage)
    }
    lock.unlock()
  }

  def untrackEdgeDeletion(timestamp: Long, src: Long, dst: Long): Unit = {
    lock.lock()
    try edgeDeletions -= ((timestamp, (src, dst)))
    catch {
      case e: Exception =>
        logger.error(e.getMessage)
    }
    lock.unlock()
  }

  def untrackVertexDeletion(timestamp: Long, src: Long): Unit = {
    lock.lock()
    try vertexDeletions get (timestamp, src) match {
      case Some(counter) => //if after we remove this value its now zero we can remove from the tree
        if (counter.decrementAndGet() == 0)
          vertexDeletions -= ((timestamp, src))
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
      val time = latestTime.get()
      if (!storage.currentyBatchIngesting()) {
        val edgeAdditionTime =
          if (edgeAdditions.nonEmpty)
            edgeAdditions.minBy(_._1)._1
          else
            time

        val edgeDeletionTime =
          if (edgeDeletions.nonEmpty)
            edgeDeletions.minBy(_._1)._1
          else
            time

        val vertexDeletionTime: Long =
          if (vertexDeletions.nonEmpty)
            vertexDeletions.minBy(_._1._1)._1._1 //find the min and then extract the time
          else
            time

        val finalTime =
          Array(time, edgeAdditionTime, edgeDeletionTime, vertexDeletionTime).min

        val noBlockingOperations =
          edgeAdditions.isEmpty && edgeDeletions.isEmpty && vertexDeletions.isEmpty

        val newWatermark =
          WatermarkTime(storage.getPartitionID, oldestTime.get(), finalTime, noBlockingOperations)

        if (newWatermark != latestWatermark)
          logger.debug(
                  s"Partition ${storage.getPartitionID}: Creating watermark with " +
                    s"earliest time '${oldestTime.get()}' and latest time '$finalTime'."
          )

        latestWatermark = newWatermark
      }
    }
    catch {
      case e: Exception =>
        logger.error(e.getMessage)
    }
    lock.unlock()
  }

}
