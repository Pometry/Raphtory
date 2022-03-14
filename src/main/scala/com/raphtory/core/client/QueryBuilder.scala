package com.raphtory.core.client

import com.raphtory.core.algorithm.GraphFunction
import com.raphtory.core.algorithm.TableFunction
import com.raphtory.core.components.querymanager.Query
import com.raphtory.core.components.querytracker.QueryProgressTracker
import com.raphtory.core.config.ComponentFactory
import com.raphtory.core.config.PulsarController
import com.raphtory.core.time.Interval
import com.raphtory.serialisers.PulsarKryoSerialiser
import com.typesafe.config.Config
import monix.execution.Scheduler
import org.apache.pulsar.client.api.Schema

class QueryBuilder(
    private val componentFactory: ComponentFactory,
    private val scheduler: Scheduler,
    private val pulsarController: PulsarController,
    val query: Query = Query()
) {

  val kryo                                         = PulsarKryoSerialiser()
  implicit private val schema: Schema[Array[Byte]] = Schema.BYTES

  def addGraphFunction(function: GraphFunction): QueryBuilder =
    this copyWithQuery query.copy(graphFunctions = query.graphFunctions.enqueue(function))

  def addTableFunction(function: TableFunction): QueryBuilder =
    this copyWithQuery query.copy(tableFunctions = query.tableFunctions.enqueue(function))

  def setStartTime(startTime: Long): QueryBuilder = {
    val newStartTime =
      query.startTime.fold(startTime)(current => if (current > startTime) current else startTime)
    this copyWithQuery query.copy(startTime = Some(newStartTime))
  }

  def setEndTime(endTime: Long): QueryBuilder = {
    val newEndTime =
      query.endTime.fold(endTime)(current => if (current < endTime) current else endTime)
    this copyWithQuery query.copy(endTime = Some(newEndTime))
  }

  def setIncrement(increment: Interval): QueryBuilder =
    this copyWithQuery query.copy(increment = Some(increment))

  def setWindows(windows: List[Interval]): QueryBuilder =
    this copyWithQuery query.copy(windows = windows)

  def submit(): QueryProgressTracker = {
    val jobID       = getID(query)
    val outputQuery = query.copy(name = jobID)
    pulsarController.toQueryManagerProducer sendAsync kryo.serialise(outputQuery)
    componentFactory.queryProgressTracker(jobID, scheduler)
  }

  private def getID(query: Query): String =
    query.hashCode().abs + "_" + System.currentTimeMillis()

  private def copyWithQuery(newQuery: Query) =
    new QueryBuilder(componentFactory, scheduler, pulsarController, newQuery)
}
