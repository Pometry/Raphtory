package com.gwz.dockerexp.GraphEntities

import scala.reflect.ClassTag

/***
  * Trait with all the log operations for time changing entities
  */
trait LogManageable {

  def findEventPositionInLog[T: ClassTag](
      logBeforeUpdate: List[(Int, T)],
      insertedInformation: (Int, T)): List[(Int, T)] = {
    val lastHighMessage =
      logBeforeUpdate.lastIndexWhere(e => e._1 > insertedInformation._1)
    if (lastHighMessage < 0) insertedInformation :: logBeforeUpdate
    else
      logBeforeUpdate.take(lastHighMessage) ::: insertedInformation :: logBeforeUpdate
        .drop(lastHighMessage)
  }

}
