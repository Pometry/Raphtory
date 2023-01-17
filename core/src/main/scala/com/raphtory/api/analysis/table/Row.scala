package com.raphtory.api.analysis.table

import com.raphtory.internals.components.querymanager.ProtoField

/** Create a row of a data table
  * @see [[Table]]
  */

case class KeyPair(key: String, value: Any)

class Row(val keyPairs: Array[KeyPair]) {

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

  def insertDictintoKeypair(dictionary: Map[String, Any]): Array[KeyPair] =
    dictionary.map {
      case (key: String, value: Any) => KeyPair(key, value)
      case _                         => throw new IllegalArgumentException("Keypairs in Row should be of type (key: String, value: Any)")
    }.toArray

  /** Return Array of values */
  def values(): Array[KeyPair] = keyPairs.toArray

  /** Return Array of keys */
  def keys(): Array[String] = values().map(pair => pair.key)

  /** Return Array of items */
  def items(): Array[Any] = values().map(pair => pair.value)

  override def toString: String = "Row(" + keyPairs.mkString(", ") + ")"

  override def equals(obj: Any): Boolean =
    obj match {
      case that: Row =>
        that.keyPairs.toSeq == this.keyPairs.toSeq
      case _         => false
    }

  override def hashCode(): Int = keyPairs.toSeq.hashCode()
}

/** Factory object for Rows */
object Row extends ProtoField[Row] {

  /** Create a new Row object */
  def apply(values: KeyPair*): Row = new Row(values.toArray)
}
