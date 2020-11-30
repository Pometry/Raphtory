package com.raphtory.core.actors.Router

import com.raphtory.core.model.communication.GraphUpdate

import scala.collection.mutable
import scala.util.hashing.MurmurHash3

trait GraphBuilder[T]{

  var updates:mutable.HashSet[GraphUpdate] = mutable.HashSet[GraphUpdate]()

  def getUpdates() = { //TODO hide from users
    val toReturn = updates
    updates = mutable.HashSet[GraphUpdate]()
    toReturn
  }

  def sendUpdate(update:GraphUpdate):Unit ={
    updates += update
  }
  protected def assignID(uniqueChars: String): Long = MurmurHash3.stringHash(uniqueChars)

  def parseTuple(tuple: T):Unit

}
