package com.gwz.dockerexp.GraphEntities

/**
  * Created by Mirate on 10/03/2017.
  */
class Property(creationMessage:Int,key:String,value:String,removeList:List[(Int,(Boolean,String))]) {
  val initiallyCreated = creationMessage
  val id = key
  var previousState:List[(Int,(Boolean,String))] = removeList //pass all past removes for storing entity incase the initial creation is the incorrect order
  update(creationMessage,value) //add in the intial information
  def currentlyAlive():Boolean = previousState.head._2._1 //check front pos of list

  def update(msgID:Int,newValue:String):Unit={ //update list with new property value
    if(previousState == Nil || msgID > previousState.head._1) previousState = (msgID,(true,newValue)) :: previousState //if the revive can go at the front of the list, then just add it
    else previousState = previousState.head :: updateHelper(msgID,newValue,previousState.tail) //otherwise we need to find where it goes by looking through the list
  }
  private def updateHelper(msgID:Int,newValue:String,ps:List[(Int,(Boolean,String))]):List[(Int,(Boolean,String))] ={
    if(ps isEmpty) (msgID,(true,newValue))::Nil //somehow reached the end of the list
    else if(msgID > ps.head._1) (msgID,(true,newValue)) :: ps //if we have found the position the command should go in the list, return it at the head of the ps
    else ps.head :: updateHelper(msgID,newValue,ps.tail) //otherwise keep looking
  }

  def kill(msgID:Int):Unit={ //kill the property (used when the vertex/edge is removed)
    if(msgID > previousState.head._1) previousState = (msgID,(false,"")) :: previousState //if the kill is at the front, add false and remove the value
    else previousState = previousState.head :: conspiracyToCommitMurder(msgID,previousState.tail) //otherwise we need to find where it goes by looking through the list
  }
  private def conspiracyToCommitMurder(msgID:Int,ps:List[(Int,(Boolean,String))]):List[(Int,(Boolean,String))] ={
    if(ps isEmpty) (msgID,(false,""))::Nil //somehow reached the end of the list
    else if(msgID > ps.head._1) (msgID,(false,"")) :: ps //if we have found the position the command should go in the list, return it at the head of the ps
    else ps.head :: conspiracyToCommitMurder(msgID,ps.tail) //otherwise keep looking
  }

  override def toString: String = {
    var toReturn = "\n"
    previousState.foreach(p=> toReturn = s"$toReturn           MessageID ${p._1}: ${p._2._1} -- ${p._2._2} \n")
    s"Property: ${id} ----- Previous State: $toReturn"
  }

  def toStringCurrent: String = {
    val toReturn = s"\n           MessageID ${previousState.head._1}: ${previousState.head._2._1} -- ${previousState.head._2._2} \n"
    s"Property: ${id} ----- Current State: $toReturn"
  }

}
