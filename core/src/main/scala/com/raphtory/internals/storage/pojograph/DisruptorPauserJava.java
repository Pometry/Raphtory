package com.raphtory.internals.storage.pojograph;

import com.lmax.disruptor.AlertException;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.WaitStrategy;

/**
 * This class is used to generate specific WaitStrategy instances
 * for disruptor threads. The wait-strategy will allow the
 * event processing threads to be "put to sleep" if the
 * suspend() method is invoked.
 */
public final class DisruptorPauserJava {
    private volatile boolean _paused = false;

    /**
     * Suspends the disruptor event processing threads
     * ensuring they use zero CPU.
     * <p>
     * The producers should be stopped AND the event
     * processors should be idle before invoking
     * this method.
     */
    public void suspend() {
        _paused = true;
    }


    /**
     * Releases the disruptor event processing threads
     * so that they will process new events at full speed.
     */
    public synchronized void resume() {
        _paused = false;
        notifyAll();
    }


    /**
     * @return whether the event processor threads are suspended
     */
    public boolean isSuspended() {
        return _paused;
    }

    /**
     * Creates a new instance of a wait-strategy associated
     * with this pauser instance.
     * <p>
     * The WaitStrategy behavesthe same as a YieldingWaitStrategy
     * but has the ability to be paused and resumed at will.
     * <p>
     * When the pauser is paused, then all disruptor event processing
     * threads using these wait-strategy instances will be paused.
     *
     * @return a new WaitStrategy
     */
    public WaitStrategy getWaitStrategy() {
        return new PauseableYieldingWaitStrategy();
    }


    /**
     * Called by an event processing thread to
     * pause as required.
     */
    private void pauseIfRequired() {
        if (_paused) {
            synchronized (this) {
                while (_paused) {
                    try {
                        wait();
                    } catch (InterruptedException ie) {
                    }
                }
            }
        }
    }


    /**
     * This class is an instance of a yielding-wait strategy that will
     * check to see if it should pause.
     */
    private final class PauseableYieldingWaitStrategy implements WaitStrategy {
        private static final int SPIN_TRIES = 100;

        @Override
        public long waitFor(long sequence, Sequence cursor, Sequence dependentSequence, SequenceBarrier barrier) throws AlertException {
            long availableSequence;
            int counter = SPIN_TRIES;

            while ((availableSequence = dependentSequence.get()) < sequence) {
                counter = applyWaitMethod(barrier, counter);
            }

            return availableSequence;
        }


        @Override
        public void signalAllWhenBlocking() {
        }


        private int applyWaitMethod(SequenceBarrier barrier, int counter) throws AlertException {
            barrier.checkAlert();

            if (counter == 0) {
                Thread.yield();
                pauseIfRequired();
            } else {
                return counter - 1;
            }

            return counter;
        }
    }
}