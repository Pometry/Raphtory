package com.raphtory.utils.exceptions

final case class EntityIdNotFoundException(private val id: Long,
                                 private val cause: Throwable = None.orNull)
                  extends Exception(s"EntityId $id not found in storage", cause)

final case class CreationTimeNotFoundException(private val id: Long,
                                 private val cause: Throwable = None.orNull)
                  extends Exception(s"CreationTime for $id not found in storage", cause)

final case class HistoryNotFoundException(private val id: Long,
                                 private val cause: Throwable = None.orNull)
                  extends Exception(s"History for $id not found in storage", cause)

final case class AssociatedEdgesNotFoundException(private val id: Long,
                                 private val cause: Throwable = None.orNull)
                  extends Exception(s"No associated edges found for vertex $id in storage", cause)

final case class PropertiesNotFoundException(private val id: Long,
                                 private val cause: Throwable = None.orNull)
                  extends Exception(s"No properties found for $id in storage", cause)
