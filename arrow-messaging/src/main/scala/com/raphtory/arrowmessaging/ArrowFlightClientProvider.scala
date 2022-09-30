package com.raphtory.arrowmessaging

import org.apache.arrow.flight.FlightClient
import org.apache.arrow.flight.Location
import org.apache.arrow.memory.BufferAllocator

import java.util.concurrent.ConcurrentHashMap

object ArrowFlightClientProvider {

  private val addressClientMap = new ConcurrentHashMap[String, FlightClient]()

  // Create only one writer for every address
  def getFlightClient(interface: String, port: Int, allocator: BufferAllocator): FlightClient = {
    val address = s"$interface$port"

    if (!addressClientMap.containsKey(address)) {
      val location     = Location.forGrpcInsecure(interface, port)
      val flightClient = FlightClient.builder(allocator, location).build()
      addressClientMap.put(address, flightClient)
    }

    addressClientMap.get(address)
  }

  Runtime.getRuntime.addShutdownHook(new Thread() {

    override def run(): Unit = {
      addressClientMap.values().forEach(client => client.close())
      addressClientMap.clear()
    }
  })
}
