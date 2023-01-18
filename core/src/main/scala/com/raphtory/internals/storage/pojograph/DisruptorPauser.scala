package com.raphtory.internals.storage.pojograph

import com.lmax.disruptor.{AlertException, Sequence, SequenceBarrier, WaitStrategy}

/**
  * This class is used to generate specific WaitStrategy instances
  * for disruptor threads. The wait-strategy will allow the
  * event processing threads to be "put to sleep" if the
  * suspend() method is invoked.
  */
private[pojograph] class DisruptorPauser {

  @volatile private var paused = false

  /**
    * Suspends the disruptor event processing threads
    * ensuring they use zero CPU.
    *
    * The producers should be stopped AND the event
    * processors should be idle before invoking
    * this method.
    */
  def suspend(): Unit = paused = true

  /**
    * Releases the disruptor event processing threads
    * so that they will process new events at full speed.
    */
  def resume(): Unit =
    synchronized {
      paused = false
      notifyAll()
    }

  /**
    * @return whether the event processor threads are suspended
    */
  def isSuspended(): Boolean = paused

  /**
    * Creates a new instance of a wait-strategy associated
    * with this pauser instance.
    *
    * The WaitStrategy behavesthe same as a YieldingWaitStrategy
    * but has the ability to be paused and resumed at will.
    *
    * When the pauser is paused, then all disruptor event processing
    * threads using these wait-strategy instances will be paused.
    *
    * @return a new WaitStrategy
    */
  def getWaitStrategy(): WaitStrategy = new PauseableYieldingWaitStrategy

  /**
    * Called by an event processing thread to
    * pause as required.
    */
  private def pauseIfRequired(): Unit =
    if (paused)
      synchronized {
        while (paused)
          try wait()
          catch {
            case _: InterruptedException =>
          }
      }

  /**
    * This class is an instance of a yielding-wait strategy that will
    * check to see if it should pause.
    */
  private class PauseableYieldingWaitStrategy extends WaitStrategy {
    final private val SPIN_TRIES = 100

    @throws[AlertException]
    override def waitFor(
        sequence: Long,
        cursor: Sequence,
        dependentSequence: Sequence,
        barrier: SequenceBarrier
    ): Long = {
      var counter           = SPIN_TRIES
      var availableSequence = dependentSequence.get()

      while (availableSequence < sequence) {
        counter = applyWaitMethod(barrier, counter)
        availableSequence = dependentSequence.get()
      }

      availableSequence
    }

    override def signalAllWhenBlocking(): Unit = {}

    private def applyWaitMethod(barrier: SequenceBarrier, counter: Int): Int = {
      barrier.checkAlert()

      if (counter == 0) {
        Thread.`yield`()
        pauseIfRequired()
        counter
      }
      else counter - 1
    }
  }
}
