package com.raphtory.internals.communication

import com.raphtory.internals.communication.repositories.ArrowFlightRepository.ArrowSchemaProviderInstances._
import com.raphtory.internals.components.querymanager.FilteredEdgeMessage
import com.raphtory.internals.components.querymanager.FilteredInEdgeMessage
import com.raphtory.internals.components.querymanager.FilteredOutEdgeMessage
import com.raphtory.internals.components.querymanager.SchemaProvider
import com.raphtory.internals.components.querymanager.VertexMessagesSync
import com.raphtory.internals.graph.GraphAlteration._

object SchemaProviderInstances {
  implicit def genericSchemaProvider[T]: SchemaProvider[T] = new SchemaProvider[T] {}

  implicit lazy val vertexMessagesSyncSchemaProvider: SchemaProvider[VertexMessagesSync] =
    vertexMessagesSyncArrowFlightMessageSchemaProvider

  implicit lazy val filteredEdgeMessageSchemaProvider: SchemaProvider[FilteredEdgeMessage[_]] =
    filteredEdgeMessageArrowFlightMessageSchemaProvider

  implicit lazy val filteredInEdgeMessageSchemaProvider: SchemaProvider[FilteredInEdgeMessage[_]] =
    filteredInEdgeMessageArrowFlightMessageSchemaProvider

  implicit lazy val filteredOutEdgeMessageSchemaProvider: SchemaProvider[FilteredOutEdgeMessage[_]] =
    filteredOutEdgeMessageArrowFlightMessageSchemaProvider

  implicit lazy val intSchemaProvider: SchemaProvider[Int] =
    intArrowFlightMessageSchemaProvider

  implicit lazy val floatSchemaProvider: SchemaProvider[Float] =
    floatArrowFlightMessageSchemaProvider

  implicit lazy val doubleSchemaProvider: SchemaProvider[Double] =
    doubleArrowFlightMessageSchemaProvider

  implicit lazy val longSchemaProvider: SchemaProvider[Long] =
    longArrowFlightMessageSchemaProvider

  implicit lazy val charSchemaProvider: SchemaProvider[Char] =
    charArrowFlightMessageSchemaProvider

  implicit lazy val stringSchemaProvider: SchemaProvider[String] =
    stringArrowFlightMessageSchemaProvider

  implicit lazy val booleanSchemaProvider: SchemaProvider[Boolean] =
    booleanArrowFlightMessageSchemaProvider

  implicit lazy val vertexAddSchemaProvider: SchemaProvider[VertexAdd] =
    vertexAddArrowFlightMessageSchemaProvider

  implicit lazy val edgeAddSchemaProvider: SchemaProvider[EdgeAdd] =
    edgeAddArrowFlightMessageSchemaProvider
}
