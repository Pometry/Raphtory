package com.gwz.dockerexp.GraphEntities

/** *
  * Node or Vertice Property. Created by Mirate on 10/03/2017.
  *
  * @param creationMessage
  * @param key           Property name
  * @param value         Property value
  * @param previousState list of previous states for that property
  */
class Property(creationMessage: Int,
               key: String,
               value: String,
               var previousState: List[(Int, (Boolean, String))])
    extends LogManageable {

  // add in the initial information
  update(creationMessage, value)

  //check front pos of list
  def isCurrentlyAlive(): Boolean = previousState.head._2._1

  /**
    * update the value of the property
    *
    * @param msgID
    * @param newValue
    */
  def update(msgID: Int, newValue: String): Unit =
    previousState =
      findEventPositionInLog(previousState, (msgID, (true, newValue)))

  /**
    * kill the property (used when the vertex/edge is removed)
    *
    * @param msgID
    */
  def kill(msgID: Int): Unit =
    previousState = findEventPositionInLog(previousState, (msgID, (false, "")))

  /** *
    * returns a string with all the history of that property
    *
    * @return
    */
  override def toString: String = {
    var toReturn = System.lineSeparator()
    previousState.foreach(p =>
      toReturn = s"$toReturn           MessageID ${p._1}: ${p._2._1} -- ${p._2._2} " + System
        .lineSeparator())
    s"Property: ${key} ----- Previous State: $toReturn"
  }

  /** *
    * returns a string with only the current value of the property
    *
    * @return
    */
  def toStringCurrent: String = {
    val toReturn = System.lineSeparator() +
      s"           MessageID ${previousState.head._1}: ${previousState.head._2._1} -- ${previousState.head._2._2} " +
      System.lineSeparator()
    s"Property: ${key} ----- Current State: $toReturn"
  }
}
