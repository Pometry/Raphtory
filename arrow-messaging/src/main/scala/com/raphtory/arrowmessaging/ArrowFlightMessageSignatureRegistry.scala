package com.raphtory.arrowmessaging

import com.raphtory.arrowmessaging.model._
import java.util.concurrent.ConcurrentHashMap

class ArrowFlightMessageSignatureRegistry {

  private val signatures = new ConcurrentHashMap[String, ArrowFlightMessageSignature]()

  def registerSignature(endPoint: String, messageClass: Class[_]): Unit = {
    try {
      signatures.putIfAbsent(
        endPoint,
        ArrowFlightMessageSignature(Class.forName(messageClass.getCanonicalName + "SchemaFactory"))
      )
    } catch {
      case e: ClassNotFoundException => throw new ClassNotFoundException(s"Failed to find schema factory of the given message type $messageClass", e)
    }
  }

  def deregisterSignature(endPoint: String): Unit =
    signatures.remove(endPoint)

  def contains(endPoint: String): Boolean =
    signatures.containsKey(endPoint)

  def getSignature(endPoint: String): ArrowFlightMessageSignature =
    signatures.get(endPoint)

}

object ArrowFlightMessageSignatureRegistry {
  def apply(): ArrowFlightMessageSignatureRegistry = new ArrowFlightMessageSignatureRegistry()
}
