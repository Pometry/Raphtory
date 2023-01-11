package com.raphtory.api.analysis.table

import com.raphtory.internals.components.querymanager.ProtoField
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.annotation.varargs
import scala.collection.AbstractIterator
import scala.collection.mutable.ArrayBuffer

/** Create a row of a data table
  * @see [[Table]]
  */

case class KeyPair(key: String, value: Any)

trait Row {
  protected val keyPairs = ArrayBuffer.empty[KeyPair]

  /** Return value at index
    * @param index index to obtain value from
    */
  def apply(index: Int): Any = keyPairs(index).value

  /** Return value at `index` */
  def get(index: Int): Any = keyPairs(index).value

  /** Return value at `index` and cast it to type `T` */
  def getAs[T](index: Int): T = keyPairs(index).value.asInstanceOf[T]

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
  def values(): Array[KeyPair] = keyPairs.toArray
  def keys(): Array[String]    = values().map(pair => pair.key)
  def items(): Array[Any]      = values().map(pair => pair.value)

  override def toString: String = "Row(" + keyPairs.mkString(", ") + ")"

  override def equals(obj: Any): Boolean =
    obj match {
      case that: Row =>
        that.keyPairs.toSeq == this.keyPairs.toSeq
      case _         => false
    }

  override def hashCode(): Int = keyPairs.toSeq.hashCode()
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

  def init(values: Seq[KeyPair]): Unit = {
    this.keyPairs.clear()
    this.keyPairs.addAll(values)
    selfIterator.reset()
    acquired = true
    exploded = false
  }

  def release(): Unit =
    if (Thread.currentThread().getName != constructedOn)
      // TODO: Row release is currently broken when constructing rows from python as they are created in python thread but released in Akka message handling thread.
      logger.debug(
              s"Row object moved thread, not putting it back in pool, original: $constructedOn, new ${Thread.currentThread().getName}"
      )
    else if (acquired) {
      Row.pool.get().append(this)
      acquired = false
      keyPairs.clear()
      logger.trace(s"Row object released on thread ${Thread.currentThread().getName}")
    }
    else
      logger.warn("Row object released multiple times")

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
object Row extends ProtoField[Row] {
  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  private[table] val pool =
    ThreadLocal.withInitial[ArrayBuffer[RowImplementation]](() => ArrayBuffer.empty[RowImplementation])

  /** Create a new Row object */
  def apply(values: KeyPair*): Row = {
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
