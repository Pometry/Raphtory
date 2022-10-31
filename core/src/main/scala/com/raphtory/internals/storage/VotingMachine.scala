package com.raphtory.internals.storage

import java.util.concurrent.atomic.AtomicInteger

sealed trait VotingMachine {

  def vote(): Unit

  def checkVotes(vertexCount: Int): Boolean

  def reset(): Unit
}

private class DefaultVotingMachine(votes: AtomicInteger) extends VotingMachine {
  override def vote(): Unit = votes.incrementAndGet()

  override def checkVotes(vertexCount: Int): Boolean = vertexCount == votes.get()

  override def reset(): Unit = votes.set(0)
}

object VotingMachine {
  def apply(): VotingMachine = new DefaultVotingMachine(new AtomicInteger(0))
}
