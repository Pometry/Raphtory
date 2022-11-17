package com.raphtory.internals.components.partition

import cats.effect.std.Queue
import com.raphtory.internals.components.querymanager.Query
import com.raphtory.internals.graph.GraphPartition
import com.raphtory.internals.graph.LensInterface
import com.raphtory.protocol.CreatePerspective
import com.raphtory.protocol.Empty
import com.raphtory.protocol.GraphId
import com.raphtory.protocol.Operation
import com.raphtory.protocol.OperationAndState
import com.raphtory.protocol.PartitionResult
import com.raphtory.protocol.VertexMessage
import com.typesafe.config.Config

class QueryExecutorF[F[_]](
    query: Query,
    partitionID: Int,
    storage: GraphPartition,
    graphLens: Queue[F, LensInterface],
    conf: Config
) {
  def processMessages(req: VertexMessage): F[Empty] = ???

  def establishPerspective(req: CreatePerspective): F[Empty] = {
    val start = req.perspective.actualStart
    val end = req.perspective.actualEnd
    graphLens.offer(storage.lens(query.name, start, end, ))
  }

  def executeOperation(req: Operation): F[Empty] = ???

  def executeOperationWithState(req: OperationAndState): F[Empty] = ???

  def getResult(req: GraphId): F[PartitionResult] = ???

  def writePerspective(req: GraphId): F[Empty] = ???
}
