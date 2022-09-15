package com.raphtory.internals.storage.arrow

import com.raphtory.arrowcore.implementation.LocalEntityIdStore

class LocalEntityRepo(store: LocalEntityIdStore) {

  def resolve(globalId: Long): EntityId = {
    store.getLocalNodeId(globalId) match {
      case -1 => GlobalId(globalId)
      case id => LocalId(id)
    }
  }

}
