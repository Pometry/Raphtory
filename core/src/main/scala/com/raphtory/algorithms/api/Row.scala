package com.raphtory.algorithms.api

import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.collection.AbstractIterator
import scala.collection.mutable.ArrayBuffer

/**
  * {s}`Row(values: Any*)`
  *    : Create a row of a data table
  *
  * ## Parameters
  *
  *  {s}`values: Any*`
  *    : Values to store in row
  *
  * ## Methods
  *
  *  {s}`apply(index: Int): Any`
  *    : Return value at `index`
  *
  *  {s}`get(index: Int): Any`
  *    : same as `apply`
  *
  *  {s}`getAs[T](index: Int): T`
  *      : Return value at `index` and cast it to type `T`
  *
  *  {s}`getInt(index: Int): Int`
  *    : Same as {s}`getAs[Int](index)`
  *
  *  {s}`getString(index: Int): String`
  *    : Same as {s}`getAs[String](index)`
  *
  *  {s}`getBool(index: Int): Boolean`
  *    : Same as {s}`getAs[Boolean](index)`
  *
  *  {s}`getLong(index: Int): Long`
  *    : Same as {s}`getAs[Long](index)`
  *
  *  {s}`getDouble(index: Int): Double`
  *    : Same as {s}`getAs[Double](index)`
  *
  *  {s}`getValues(): Array[Any]`
  *    : Return Array of values
  *
  *  ```{seealso}
  *  [](com.raphtory.algorithms.api.Table)
  *  ```
  */

trait Row {
  protected val values              = ArrayBuffer.empty[Any]
  def apply(index: Int): Any        = values(index)
  def get(index: Int): Any          = values(index)
  def getAs[T](index: Int): T       = values(index).asInstanceOf[T]
  def getInt(index: Int): Int       = getAs[Int](index)
  def getString(index: Int): String = getAs[String](index)
  def getBool(index: Int): Boolean  = getAs[Boolean](index)
  def getLong(index: Int): Long     = getAs[Long](index)
  def getDouble(index: Int): Double = getAs[Double](index)
  def getValues(): Array[Any]       = values.toArray
}

class RowImplementation extends Row {

  class SelfIterator extends AbstractIterator[RowImplementation] {
    private var alive = true

    override def hasNext: Boolean =
      if (alive) alive
      else {
        if (RowImplementation.this.acquired)
          RowImplementation.this.release
        false
      }

    override def next(): RowImplementation =
      if (hasNext) {
        alive = false
        RowImplementation.this
      }
      else
        throw new NoSuchElementException

    def reset: Unit =
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
    selfIterator.reset
    acquired = true
    exploded = false
  }

  def release: Unit = {
    if (acquired) {
      Row.pool.get().append(this)
      acquired = false
      values.clear()
    }
    else
      logger.warn("Row object released multiple times")
    if (Thread.currentThread().getName != constructedOn)
      logger.error("Row object moved thread, this should never happen!")
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

object Row {
  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  val pool = ThreadLocal.withInitial[ArrayBuffer[RowImplementation]](() =>
    ArrayBuffer.empty[RowImplementation]
  )

  //def apply(values: Array[Any]): Row = new Row(values)
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
