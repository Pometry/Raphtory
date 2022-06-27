package com.raphtory.api.analysis.table

import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.collection.AbstractIterator
import scala.collection.mutable.ArrayBuffer

/** Create a row of a data table
  * @see [[Table]]
  */
trait Row {
  protected val values = ArrayBuffer.empty[Any]

  /** Return value at index
    * @param index index to obtain value from
    */
  def apply(index: Int): Any = values(index)

  /** Return value at `index` */
  def get(index: Int): Any = values(index)

  /** Return value at `index` and cast it to type `T` */
  def getAs[T](index: Int): T = values(index).asInstanceOf[T]

  /** Same as `getAs[Int](index)` */
  def getInt(index: Int): Int = getAs[Int](index)

  /** Same as `getAs[String](index)` */
  def getString(index: Int): String = getAs[String](index)

  /** Same as `getAs[Boolean](index)` */
  def getBool(index: Int): Boolean = getAs[Boolean](index)

  /** Same as `getAs[Long](index)` */
  def getLong(index: Int): Long = getAs[Long](index)

  /** Same as `getAs[Double](index)` */
  def getDouble(index: Int): Double = getAs[Double](index)

  /** Return Array of values */
  def getValues(): Array[Any] = values.toArray
}

private[raphtory] class RowImplementation extends Row {

  class SelfIterator extends AbstractIterator[RowImplementation] {
    private var alive = true

    override def hasNext: Boolean =
      if (alive) alive
      else {
        if (RowImplementation.this.acquired)
          RowImplementation.this.release()
        false
      }

    override def next(): RowImplementation =
      if (hasNext) {
        alive = false
        RowImplementation.this
      }
      else
        throw new NoSuchElementException

    def reset(): Unit =
      alive = true
  }

  val logger: Logger       = Logger(LoggerFactory.getLogger(this.getClass))
  private val selfIterator = new SelfIterator
  var acquired             = false
  var exploded             = false

  val constructedOn: String = Thread.currentThread().getName

  def init(values: Seq[Any]): Unit = {
    this.values.clear()
    this.values.addAll(values)
    selfIterator.reset()
    acquired = true
    exploded = false
  }

  def release(): Unit = {
    if (acquired) {
      Row.pool.get().append(this)
      acquired = false
      values.clear()
      logger.trace(s"Row object released on thread ${Thread.currentThread().getName}")
    }
    else
      logger.warn("Row object released multiple times")
    if (Thread.currentThread().getName != constructedOn)
      logger.error(
              s"Row object moved thread, this should never happen!, orignal: $constructedOn, new ${Thread.currentThread().getName}"
      )
  }

//  This returns an iterator that returns the Row and then releases the Row on the second invocation of hasNext.
//  This is used together with flatMap below to make sure that the original row is cleaned up in case of a call to explode
//  and also simplifies lifetime management.
//  !Do not use the original row after calling this method!
  def yieldAndRelease: Iterator[RowImplementation] = selfIterator

//  This takes a row and returns in iterator over rows that cleans up the original and new rows when exhausted.
//  !Do not try to save the result to a List or similar as the underlying Row object is being released after each iteration!
//  !Do not use the original row object after calling this method!
  def explode(f: Row => IterableOnce[Row]): Iterator[RowImplementation] = {
    exploded = true
    f(this).iterator.flatMap(_.asInstanceOf[RowImplementation].yieldAndRelease)
  }
}

/** Factory object for Rows */
object Row {
  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  private[table] val pool =
    ThreadLocal.withInitial[ArrayBuffer[RowImplementation]](() => ArrayBuffer.empty[RowImplementation])

  /** Create a new Row object */
  def apply(values: Any*): Row = {
    val localPool = pool.get()
    val row       =
      if (localPool.isEmpty) {
        logger.trace(s"new row object initialised on thread ${Thread.currentThread().getName}")
        new RowImplementation
      }
      else {
        logger.trace(
                s"Row object reused on thread ${Thread.currentThread().getName}, pool size = ${localPool.size}"
        )
        val row = localPool.last
        localPool.dropRightInPlace(1)
        row
      }
    row.init(values)
    row
  }
}
