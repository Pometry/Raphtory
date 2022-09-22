package com.raphtory.internals.management.id

import cats.effect.IO
import com.raphtory.internals.management.RpcClient

class ClientIDManager[F[_]](rpcClient: RpcClient[F]) extends IDManager {

  override def getNextAvailableID(graphID: String): Option[Int] =
    rpcClient.requestID()
}
