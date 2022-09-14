package com.raphtory.arrowmessaging

import com.raphtory.arrowmessaging.model.ArrowFlightDataset
import org.apache.arrow.flight.FlightProducer._
import org.apache.arrow.flight._
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.util.AutoCloseables
import org.apache.arrow.vector._
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch

import java.nio.charset.StandardCharsets
import java.util
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap

private[arrowmessaging] class ArrowFlightProducer(
                                                   allocator: BufferAllocator,
                                                   location: Location
                                                 ) extends NoOpFlightProducer with AutoCloseable {

  private val datasets = new ConcurrentHashMap[FlightDescriptor, ArrowFlightDataset]()

  override def acceptPut(context: CallContext, flightStream: FlightStream, ackStream: StreamListener[PutResult]): Runnable = {
    val batches = new util.ArrayList[ArrowRecordBatch]()
    () => {
      var rows: Long = 0
      while (flightStream.next()) {
        val unloader = new VectorUnloader(flightStream.getRoot)
        val arb = unloader.getRecordBatch
        batches.add(arb)
        rows = rows + flightStream.getRoot.getRowCount
      }

      val dataset = model.ArrowFlightDataset(batches, flightStream.getSchema, rows)
      datasets.put(flightStream.getDescriptor, dataset)
      ackStream.onCompleted()
    }
  }

  override def getStream(context: CallContext, ticket: Ticket, listener: ServerStreamListener): Unit = {
    val flightDescriptor = FlightDescriptor.path(new String(ticket.getBytes, StandardCharsets.UTF_8))

    val dataset = this.datasets.get(flightDescriptor)
    if (dataset == null)
      throw CallStatus.NOT_FOUND.withDescription("Unknown descriptor").toRuntimeException

    val vectorSchemaRoot = VectorSchemaRoot.create(this.datasets.get(flightDescriptor).schema, allocator)
    try {
      val loader = new VectorLoader(vectorSchemaRoot)
      listener.start(vectorSchemaRoot)

      this.datasets.get(flightDescriptor).batches.forEach { arrowRecordBatch =>
        loader.load(arrowRecordBatch.cloneWithTransfer(allocator))
        listener.putNext()
      }

      listener.completed()
    } finally {
      if (vectorSchemaRoot != null)
        vectorSchemaRoot.close()
    }

  }

  override def getFlightInfo(context: CallContext, descriptor: FlightDescriptor): FlightInfo = {
    val flightEndpoint = new FlightEndpoint(new Ticket(descriptor.getPath.get(0).getBytes(StandardCharsets.UTF_8)), location);
    new FlightInfo(
      datasets.get(descriptor).schema,
      descriptor,
      Collections.singletonList(flightEndpoint),
      /*bytes=*/ -1,
      datasets.get(descriptor).rows
    )
  }

  override def listFlights(context: CallContext, criteria: Criteria, listener: StreamListener[FlightInfo]): Unit = {
    datasets.forEach((k, v) => listener.onNext(getFlightInfo(null, k)))
    listener.onCompleted()
  }

  override def doAction(context: CallContext, action: Action, listener: StreamListener[Result]): Unit = {
    val flightDescriptor = FlightDescriptor.path(new String(action.getBody, StandardCharsets.UTF_8))
    action.getType match {
      case "DELETE" =>
        val removed: ArrowFlightDataset = datasets.remove(flightDescriptor)
        if (removed != null) {
          try {
            removed.close()
          } catch {
            case e: Exception =>
              listener.onError(CallStatus.INTERNAL.withDescription(e.toString).toRuntimeException)
              return
          }
          val result = new Result("Delete completed".getBytes(StandardCharsets.UTF_8))
          listener.onNext(result)
        } else {
          val result = new Result("Delete not completed. Reason: Key did not exist.".getBytes(StandardCharsets.UTF_8))
          listener.onNext(result)
        }
        listener.onCompleted()
    }
  }

  override def close(): Unit = AutoCloseables.close(datasets.values())
}
