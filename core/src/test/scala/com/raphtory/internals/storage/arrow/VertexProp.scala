package com.raphtory.internals.storage.arrow

case class VertexProp(
                       age: Long,
                       @immutable name: String,
                       @immutable address_chain: String,
                       @immutable transaction_hash: String
                     )

case class EdgeProp(
                     @immutable name: String,
                     friends: Boolean,
                     weight: Long,
                     @immutable msgId: String,
                     @immutable subject: String
                   )
