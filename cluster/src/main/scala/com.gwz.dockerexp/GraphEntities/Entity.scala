package com.gwz.dockerexp.GraphEntities
/**
  * Class representing Graph Entities
  * Contains a Map of properties (currently String to string)
  * longs representing unique vertex ID's stored in subclassses
  */

class Entity(creationMessage:Int, initialValue:Boolean) {
  var properties = Map[String,Property]()
  var previousState:List[(Int,Boolean)] = (creationMessage,initialValue)::Nil // if initial is delete set to false
  var removeList:List[(Int,(Boolean,String))] =  if(initialValue) Nil else (creationMessage,(false,""))::Nil //need to track all removes to pass to properties

  //************* REVIVE BLOCK *********************\\
  def revive(msgID:Int):Unit={
    if(previousState==Nil) previousState = (msgID,true) :: previousState // if the vertex has been wiped then no need to do anything
    else if(msgID > previousState.head._1) previousState = (msgID,true) :: previousState //if the revive can go at the front of the list, then just add it
    else previousState = previousState.head :: reviveHelper(msgID,previousState.tail) //otherwise we need to find where it goes by looking through the list
  }
  private def reviveHelper(msgID:Int,ps:List[(Int,Boolean)]):List[(Int,Boolean)] ={
    if(ps isEmpty) (msgID,true)::Nil //somehow reached the end of the list
    else if(msgID > ps.head._1) (msgID,true) :: ps //if we have found the position the command should go in the list, return it at the head of the ps
    else ps.head :: reviveHelper(msgID,ps.tail) //otherwise keep looking
  }
  //************* END REVIVE BLOCK *********************\\

  //************* KILL ENTITY BLOCK *********************\\
  def kill(msgID:Int):Unit={
    if(previousState isEmpty) previousState = (msgID,false)::Nil // if the vertex has been wiped then no need to do anything
    else if(msgID > previousState.head._1) previousState = (msgID,false) :: previousState //if the kill is the latest command put at the front
    else previousState = previousState.head :: conspireToCommitMurder(msgID,previousState.tail) //otherwise we need to find where it goes by looking through the list
    properties.foreach(p => p._2.kill(msgID)) //send the message to all properties
    updateRemoveList()
  }

  private def conspireToCommitMurder(msgID:Int,ps:List[(Int,Boolean)]):List[(Int,Boolean)] ={
    if(ps isEmpty) (msgID,false)::Nil //somehow reached the end of the list
    else if(msgID > ps.head._1) (msgID,false) :: ps //if we have found the position the command should go in the list, return it at the head of the ps
    else ps.head :: conspireToCommitMurder(msgID,ps.tail) //otherwise keep looking
  }
  def updateRemoveList() = removeList = previousState.filter(p => p._2==false).map(p => (p._1,(false,""))) //filter to only removes and convert to a list which can be given to properties
  //************* END KILL BLOCK *********************\\

  //************* PROPERTY BLOCK *********************\\
  def apply(property:String): Property = properties(property) //overrite the apply method so that we can do vertex("key") to easily retrieve properties

  def +(msgID:Int,key:String,value:String):Unit = { //create + method so can write vertex + (k,v) to easily add new properties
    if(properties contains key) properties(key) update (msgID,value)
    else properties = properties updated (key,new Property(msgID,key,value,removeList)) //add new property passing all previous removes so the add can be slotted in accordingly
  }
  //************* END PROPERTY BLOCK *********************\\


  //************* PRINT ENTITY DETAILS BLOCK *********************\\
  def printCurrent():String={
    var toReturn= s"MessageID ${previousState.head._1}: ${previousState.head._2} \n"
    properties.foreach(p => toReturn = s"$toReturn      ${p._2.toStringCurrent} \n")
    toReturn
  }
  def printHistory():String={
    var toReturn="Previous state of entity: \n"
    previousState.foreach(p => toReturn = s"$toReturn MessageID ${p._1}: ${p._2} \n")
    s"$toReturn \n $printProperties" //print previous state of entity + properties -- title left off as will be done in subclass
  }
  def printProperties():String ={ //test function to make sure the properties are being added to the correct vertices
    var toReturn ="" //indent to be inside the entity
    properties.toSeq.sortBy(_._1).foreach(p => toReturn = s"$toReturn      ${p._2.toString} \n")
    toReturn
  }
  //************* END PRINT ENTITY DETAILS BLOCK *********************\\

  def wipe() = {
    previousState = Nil
    removeList=Nil
  }

}
