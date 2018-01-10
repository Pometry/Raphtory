package com.gwz.dockerexp.GraphEntities

/** *
  * Represents Graph Entities (Edges and Vertices)
  * Contains a Map of properties (currently String to string)
  * longs representing unique vertex ID's stored in subclassses
  *
  * @param creationMessage ID of the message that created the entity
  * @param isInitialValue  Is the first moment this entity is referenced
  */
class Entity(creationMessage: Int, isInitialValue: Boolean)
    extends LogManageable {

  // Properties from that entity
  var properties:Map[String,Property] = Map[String, Property]()

  // History of that entity
  var previousState: List[(Int, Boolean)] = List(
    (creationMessage, isInitialValue))

  //need to track all removes to pass to properties
  var removeList: List[(Int, (Boolean, String))] =
    if (isInitialValue) List() else List((creationMessage, (false, "")))

  /** *
    * Set the Entity has alive at a given time
    *
    * @param msgID
    */
  def revive(msgID: Int): Unit = {
    previousState = findEventPositionInLog(previousState, (msgID, true))
  }

  /** *
    * Set the entity absent in a given time
    *
    * @param msgID
    */
  def kill(msgID: Int): Unit = {

    /** *
      * Filter to only removes and convert to a list which can be given to properties
      */
    def updateRemoveList() =
      removeList =
        previousState.filter(_._2 == false).map(p => (p._1, (false, "")))

    previousState = findEventPositionInLog(previousState, (msgID, false))

    updateRemoveList()
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
    if (properties contains key)
      properties(key) update(msgID, value)
    else
      properties = properties updated(key, new Property(msgID,
        key,
        value,
        List()))
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
    previousState = List()
    removeList = List()
  }
}
