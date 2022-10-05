/* Copyright (C) Pometry Ltd - All Rights Reserved.
 *
 * This file is proprietary and confidential. Unauthorised
 * copying of this file, via any medium is strictly prohibited.
 *
 */

package com.raphtory.arrowcore.implementation;

import java.util.ArrayList;
import java.util.concurrent.*;

/**
 * Simple class that maintains a thread-pool and allows tasks
 * to be submitted and waited for.
 */
public class RaphtoryThreadPool {
    // Global instance that can be used if there are no specific threading requirements
    public static RaphtoryThreadPool THREAD_POOL = new RaphtoryThreadPool(Runtime.getRuntime().availableProcessors(), Integer.MAX_VALUE);

    private final ThreadPoolExecutor _threadPool;


    public RaphtoryThreadPool(int nThreads, int qLength) {
        if (qLength==Integer.MAX_VALUE) {
            _threadPool = new ThreadPoolExecutor(nThreads, nThreads, Long.MAX_VALUE, TimeUnit.SECONDS, new LinkedBlockingQueue<>(Integer.MAX_VALUE));
        }
        else {
            // TODO: Make this use a fixed-size circular queue
            _threadPool = new ThreadPoolExecutor(nThreads, nThreads, Long.MAX_VALUE, TimeUnit.SECONDS, new LinkedBlockingQueue(qLength) {
                @Override
                public boolean offer(Object e) {
                    // turn offer() and add() into a blocking calls (unless interrupted)
                    try {
                        put(e);
                        return true;
                    } catch(InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                    return false;
                }
            });
        }
    }


    /**
     * Submits a task to the thread-pool.
     *
     * @param r the runnable to execute
     * @return a Future that can be used to confirm the task is complete
     */
    public Future<?> submitTask(Runnable r) {
        return _threadPool.submit(r);
    }


    public boolean isIdle() { return _threadPool.getActiveCount()==0; }


    /**
     * Waits until the specified task has been completed
     *
     * @param task the task in question
     */
    public void waitTilComplete(Future<?> task) {
        try {
            task.get();
        }
        catch (ExecutionException | InterruptedException ee) {
            System.err.println("Exception: " + ee);
            ee.printStackTrace(System.err);
            cancel(task);
        }
    }


    /**
     * Waits until all of the specified tasks have been completed
     *
     * @param tasks the list of tasks in question
     */
    public void waitTilComplete(ArrayList<Future<?>> tasks) {
        int n = tasks.size();
        try {
            for (int i = 0; i < n; ++i) {
                //System.err.println("Waiting for: " + i);
                tasks.get(i).get();
            }
        }
        catch (ExecutionException | InterruptedException ee) {
            System.err.println("Exception: " + ee);
            ee.printStackTrace(System.err);
            cancel(tasks);
        }
    }


    /**
     * Attempt to cancel a task
     *
     * @param task the task in question
     */
    public void cancel(Future<?> task) {
        try {
            task.cancel(true);
        }
        catch (Exception e) {
            // IGNORED
        }
    }


    /**
     * Attempt to cancel a list of tasks
     *
     * @param tasks the list of tasks
     */
    public void cancel(ArrayList<Future<?>> tasks) {
        int n = tasks.size();
        for (int i=0; i<n; ++i) {
            try {
                tasks.get(i).cancel(true);
            }
            catch (Exception e) {
                // IGNORED
            }
        }
    }
}