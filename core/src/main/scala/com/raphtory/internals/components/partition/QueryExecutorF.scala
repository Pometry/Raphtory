package com.raphtory.internals.components.partition

import com.raphtory.internals.components.querymanager.Query
import com.raphtory.internals.graph.GraphPartition
import com.raphtory.protocol.CreatePerspective
import com.raphtory.protocol.Empty
import com.raphtory.protocol.ExecutorService
import com.raphtory.protocol.FunctionAndState
import com.raphtory.protocol.FunctionNumber
import com.raphtory.protocol.PerspectiveResult
import com.raphtory.protocol.QueryManagement
import com.typesafe.config.Config

class QueryExecutorF[F[_]](
    query: Query,
    partitionID: Int,
    storage: GraphPartition,
    conf: Config
) {
  def establishPerspective(req: CreatePerspective): F[Empty] = ???

  def executeFunction(req: FunctionNumber): F[Empty] = ???

  def executeFunctionWithState(req: FunctionAndState): F[Empty] = ???

  override def writeResults(req: Empty): F[Empty] = ???

  override def getRows(req: Empty): F[fs2.Stream[F, PerspectiveResult]] = ???

  override def receiveMessages(req: fs2.Stream[F, QueryManagement]): F[Empty] = ???
}
