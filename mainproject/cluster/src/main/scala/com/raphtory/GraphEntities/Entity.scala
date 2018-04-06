package com.raphtory.GraphEntities

import scala.collection.concurrent.TrieMap
import scala.collection.{SortedMap, mutable}

/** *
  * Represents Graph Entities (Edges and Vertices)
  * Contains a Map of properties (currently String to string)
  * longs representing unique vertex ID's stored in subclassses
  *
  * @param creationMessage ID of the message that created the entity
  * @param isInitialValue  Is the first moment this entity is referenced
  */
class Entity(creationMessage: Int, isInitialValue: Boolean) {

  // Properties from that entity
  var properties:TrieMap[String,Property] = TrieMap[String, Property]()

  // History of that entity
  var previousState: mutable.TreeMap[Int, Boolean] = mutable.TreeMap(
    creationMessage -> isInitialValue)

  // History of that entity
  var removeList: mutable.TreeMap[Int,Boolean] = null
  if(isInitialValue)
    removeList = mutable.TreeMap()
  else
    removeList = mutable.TreeMap(creationMessage -> isInitialValue)
  /** *
    * Set the Entity has alive at a given time
    *
    * @param msgID
    */
  def revive(msgID: Int): Unit = {
    previousState += msgID -> true
  }

  /** *
    * Set the entity absent in a given time
    *
    * @param msgID
    */
  def kill(msgID: Int): Unit = {
    removeList    += msgID -> false
    previousState += msgID -> false
  }

  /** *
    * override the apply method so that we can do edge/vertex("key") to easily retrieve properties
    *
    * @param property property key
    * @return property value
    */
  def apply(property: String): Property = properties(property)

  /** *
    * Add or update the property from an edge or a vertex based, using the operator vertex + (k,v) to add new properties
    *
    * @param msgID Message ID where the update came from
    * @param key   property key
    * @param value property value
    */
  def +(msgID: Int, key: String, value: String): Unit = {
    properties.putIfAbsent(key, new Property(msgID, key, value)) match {
      case Some(oldValue) => oldValue update(msgID, value)
      case None =>
    }
  }

  //************* PRINT ENTITY DETAILS BLOCK *********************\\
  def printCurrent(): String = {
    var toReturn = s"MessageID ${previousState.head._1}: ${previousState.head._2} " + System.lineSeparator
    properties.foreach(p =>
      toReturn = s"$toReturn      ${p._2.toStringCurrent} " + System.lineSeparator)
    toReturn
  }

  /** *
    * Returns string with previous state of entity + properties -- title left off as will be done in subclass
    *
    * @return
    */
  def printHistory(): String = {
    var toReturn = "Previous state of entity: " + System.lineSeparator
    previousState.foreach(p =>
      toReturn = s"$toReturn MessageID ${p._1}: ${p._2} " + System.lineSeparator)
    s"$toReturn \n $printProperties"
  }

  def printProperties(): String = { //test function to make sure the properties are being added to the correct vertices
    var toReturn = "" //indent to be inside the entity
    properties.toSeq
      .sortBy(_._1)
      .foreach(p => toReturn = s"$toReturn      ${p._2.toString} \n")
    toReturn
  }

  //************* END PRINT ENTITY DETAILS BLOCK *********************\\

  def wipe() = {
    previousState = mutable.TreeMap()
  }

}
