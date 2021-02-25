package com.raphtory.core.actors.Router

import com.raphtory.core.model.communication.GraphUpdate

import scala.util.hashing.MurmurHash3

trait GraphBuilder[T] {

  private var updates: List[GraphUpdate] = List.empty

  def getUpdates(tuple: T): List[GraphUpdate] = { //TODO hide from users
    try{
      parseTuple(tuple)
    }
    catch {
      case e:Exception => println(s"Tuple broken: $tuple")
    }
    val toReturn = updates
    updates = List.empty
    toReturn
  }

  protected def sendUpdate(update: GraphUpdate): Unit =
    updates = updates :+ update

  protected def assignID(uniqueChars: String): Long = MurmurHash3.stringHash(uniqueChars)

  protected def parseTuple(tuple: T): Unit
}
