/* Copyright (C) Pometry Ltd - All Rights Reserved.
 *
 * This file is proprietary and confidential. Unauthorised
 * copying of this file, via any medium is strictly prohibited.
 *
 */

package com.raphtory.arrowcore.implementation;

import com.raphtory.arrowcore.model.Edge;
import com.raphtory.arrowcore.util.LRUList;
import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.compression.CompressionCodec;

import java.io.File;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * The EdgePartitionManager manages all EdgePartitions.
 *<p>
 * It co-ordinates the loading and saving of them to disk files and
 * provides various access methods to data held in individual
 * EdgePartitions.
 *<p>
 * <p>TODO: Check the threading around getNewPartitionId(), getPartitionAndLoad() etc.
 *
 * @see EdgePartition
 */
public class EdgePartitionManager {
    private static final String _FILE_SUFFIX = ".rap";

    // Number of Arrow partitions to perform initial sizing with
    // TODO: Make this configurable via the RaphtoryArrowPartition
    public static final int N_ARROW_PARTITIONS = 96;

    // Initial size of list managing file-writing tasks
    public static final int N_WRITERS = 64;


    protected final RaphtoryArrowPartition _raphtoryPartition;

    private final File _dir;
    private final RootAllocator _allocator = new RootAllocator();
    private final Int2ObjectArrayMap<EdgePartition> _partitions = new Int2ObjectArrayMap<>(N_ARROW_PARTITIONS * 64);
    private final LRUList<EdgePartition> _lruList = new LRUList<>();
    private final ArrayList<PartitionWriter> _writers = new ArrayList<>(N_WRITERS);
    private final RaphtoryThreadPool _pool;
    private EdgePartition[] _partitionArray;
    private int _lastFreePartitionId = 0;

    public final int PARTITION_SIZE;
    public final int PARTITION_SHIFT;
    public final int PARTITION_MASK;


    /**
     * Instantiates an edge partition mananger.
     *
     * @param raphtoryPartition the RAP to use
     * @param dir the path to store/load arrow edge files
     * @param pool the thread-pool to use for reading/writing files
     */
    public EdgePartitionManager(RaphtoryArrowPartition raphtoryPartition, File dir, RaphtoryThreadPool pool) {
        _raphtoryPartition = raphtoryPartition;
        _dir = dir;
        _pool = pool;

        PARTITION_SIZE = raphtoryPartition._edgePartitionSize;
        PARTITION_MASK = raphtoryPartition._edgePartitionSize_mask;
        PARTITION_SHIFT = raphtoryPartition._edgePartitionSize_shift;

        if (!dir.exists()) {
            try {
                dir.mkdirs();
            }
            catch (Exception e) {
                System.err.println("Exception: " + e);
                e.printStackTrace(System.err);
            }
        }
    }


    /**
     * @return the Arrow buffer-allocator to use
     */
    public BufferAllocator getAllocator() {
        return _allocator;
    }


    /**
     * @return the number of edge partitions that are loaded
     */
    public int nPartitions() { return _partitions.size(); }


    /**
     * @return the total number of edges
     *
     * TODO: This is a bit slow
     */
    public long getTotalNumberOfEdges() {
        long nVertices = 0;
        int nPartitions = nPartitions();

        for (int i=0; i<nPartitions; ++i) {
            nVertices += getPartition(i).getEdgesCount();
        }

        return nVertices;
    }


    /**
     * Retrieves the next free edge-id, filling in
     * any gaps.
     * <p>
     * This function should not be used from multiple threads
     * as the return value will not change until a new edge
     * has been created and stored in the partition,
     * thus allowing a race condition.
     *
     * @return the next available free edge-id
     */
    public synchronized long getNextFreeEdgeId() {
        int nPartitions = nPartitions();
        if (nPartitions==0) {
            _lastFreePartitionId = 0;
            return 0L;
        }

        for (int i=_lastFreePartitionId; i<nPartitions; ++i) {
            long freeId = getPartition(i).getNextFreeEdgeId();
            if (freeId!=-1L) {
                _lastFreePartitionId = i;
                return freeId;
            }
        }

        _lastFreePartitionId = nPartitions;
        return (long)nPartitions * (long)PARTITION_SIZE;
    }


    /**
     * Calculates the partition-id that an edge is located in
     *
     * @param edgeId the local edge id in question
     *
     * @return the partition-id containing that edge
     */
    public int getPartitionId(long edgeId) {
        return (int)(edgeId >>> PARTITION_SHIFT);
    }


    /**
     * Returns the row number where this edge is located
     *
     * @param edgeId the local edge id in question
     *
     * @return the row number containing that edge
     */
    public int getRowId(long edgeId) {
        return (int) (edgeId & PARTITION_MASK);
    }


    /**
     * Loads an edge partition from it's disk file.
     * If the disk file doesn't exist, then an empty
     * partition is created.
     *
     * @param partId the partition id to load
     *
     * @return the loaded edge partition
     */
    private EdgePartition loadPartition(int partId) {
        waitForWriterToComplete(partId);

        boolean found = false;
        EdgePartition p = null;
        synchronized (this) {
            p = _partitions.get(partId);
            if (p != null) {
                found = true;
                //lruList.touched(p);
            }
            else {
                p = new EdgePartition(this, partId);
                _partitions.put(partId, p);
            }
        }


        if (found) {
            p.ensureLoaded();
        }
        else {
            p.loadFromFile();
        }
        return p;
    }


    /**
     * Experimental infrastructure to handle LRU lists of
     * partitions in an effort to reduce memory constraints.
     * <p>
     * Currently disabled.
     *
     * @param p
     */
    private void touchPartition(EdgePartition p) {
        //lruList.touched(p);
    }


    /**
     * Retrieves the edge partition with the specified id
     *
     * @param partId the partition id in question
     *
     * @return the partition, or null if it hasn't been loaded
     */
    public EdgePartition getPartition(int partId) {
        if (_partitionArray!=null && partId<_partitionArray.length) {
            return _partitionArray[partId];
        }
        EdgePartition p = _partitions.get(partId);
        return p;
    }


    /**
     * Retrieves an edge partition, loading it or creating it as required.
     *
     * @param partId the partition id in question
     *
     * @return the edge partition
     */
    protected EdgePartition getPartitionAndLoad(int partId) {
        EdgePartition p = _partitions.get(partId);
        if (p == null) {
            if (_partitions.size() >= N_ARROW_PARTITIONS) {
                //removeLRUPartition();
            }
            p = loadPartition(partId);
        }
        else {
            //touchPartition(p);
        }

        return p;
    }


    /**
     * Adds or updates a POJO edge to the appropriate edge partition.
     *<p>
     * The fields and properties in the Edge will be stored in Arrow.
     * History records will not be updated.
     *<p>
     * @param e the Edge in question
     * @param prevIncomingPtr the previous incoming edges pointer to use
     * @param prevOutgoingPtr the previous outgoing edges pointer to use
     */
    public void addEdge(Edge e, long prevIncomingPtr, long prevOutgoingPtr) {
        long id = e.getLocalId();
        int partId = getPartitionId(id);
        EdgePartition p = getPartitionAndLoad(partId);
        p.addEdge(e, prevIncomingPtr, prevOutgoingPtr);
    }


    /**
     *  Adds a history record for an edge directly.
     *
     * @param edgeId the vertex to be updated
     * @param time the time of the history point
     * @param active true if the edge was active at that time, false otherwise
     * @param updated true if a property was updated, false otherwise
     *
     * @return the row in the history that this record was stored
     */
    public int addHistory(long edgeId, long time, boolean active, boolean updated) { // MT
        int partId = getPartitionId(edgeId);
        EdgePartition p = getPartitionAndLoad(partId);
        return p.addHistory(edgeId, time, active, updated);
    }


    /**
     * Adds a property for a specific edge directly. This function maintains the
     * property history pointer and ensures the property history remains sorted on time.
     *
     * @param edgeId the edge-id in question
     * @param propertyId the property in question
     * @param efa the property value
     *
     * @return the row in the vertex-property file that this property was stored at
     */
    public int addProperty(long edgeId, int propertyId, VersionedEntityPropertyAccessor efa) {
        int partId = getPartitionId(edgeId);
        EdgePartition p = getPartitionAndLoad(partId);
        return p.addProperty(edgeId, propertyId, efa);
    }


    /**
     * Waits for a file-writer (if any) to complete for the
     * specified partition-id
     *
     * @param partId the edge partition-id in question
     */
    private void waitForWriterToComplete(int partId) {
        PartitionWriter theWriter = null;

        synchronized (_writers) {
            int n = _writers.size();
            for (int i = 0; i < n; ++i) {
                PartitionWriter pw = _writers.get(i);
                EdgePartition ap = pw.ap;
                if (ap != null && ap.getPartitionId() == partId) {
                    theWriter = pw;
                    break;
                }
            }
        }

        if (theWriter != null) {
            theWriter.waitForCompletion();
        }
    }


    /**
     * Submits a new edge file writer to the pool
     *
     * @param ap the edge partition to write
     * @param close if true, will close the file as well
     */
    private void addWriter(EdgePartition ap, boolean close) {
        PartitionWriter pw = new PartitionWriter(ap, close);
        synchronized (_writers) {
            _writers.add(pw);
        }

        _pool.submitTask(pw);
    }


    /**
     * Removes the least-recently used edge partition from memory.
     * <p>Experimental.
     * <p>Currently unused.
     */
    private void removeLRUPartition() {
        EdgePartition p = _lruList.getTail();
        if (p != null) {
            _lruList.remove(p);
            _partitions.remove(p.getPartitionId());
            addWriter(p, true);
        }
    }


    /**
     * Writes all modified partition files back to disk.
     *<p>
     * The files are written in parallel in an effort to
     * improve save times.
     */
    public void saveFiles() {
        _partitions.values().forEach(p -> addWriter(p, false));
        _partitions.keySet().forEach(this::waitForWriterToComplete);
    }


    /**
     * Closes all edge files.
     */
    public void close() {
        _partitions.values().forEach(EdgePartition::close);
    }


    /**
     * Returns the path for the specified vertex partition
     *
     * @param partitionId the vertex partition id in question
     *
     * @return the File path for that vertex partition
     */
    protected File getEdgeFile(int partitionId) {
        return new File(_dir, "edge-p" + partitionId + _FILE_SUFFIX);
    }


    /**
     * Returns the Arrow compression factory to use.
     * <p>Returns null as we don't support compression yet.
     *
     * @return null
     */
    protected CompressionCodec.Factory getCompressionFactory() {
        return null;
    }


    /**
     * Loads all the edge partition files that exist in the
     * target directory.
     *<p>
     * The files are read in parallel in an effort to improve
     * load times.
     */
    public void loadFiles() {
        ArrayList<Future<?>> tasks = new ArrayList<>();

        int fileId = 0;
        for (;;) {
            File file = getEdgeFile(fileId);
            if (file.exists()) {
                final int fId = fileId;
                tasks.add(_pool.submitTask(() -> loadPartition(fId)));
                ++fileId;
            }
            else {
                break;
            }
        }

        _pool.waitTilComplete(tasks);

        int nPartitions = _partitions.size();
        _partitionArray = new EdgePartition[nPartitions];
        for (int i=0; i<nPartitions; ++i) {
            _partitionArray[i] = _partitions.get(i);
        }
    }


    /**
     * Returns the path for the specified edge history partition
     *
     * @param partitionId the edge partition id in question
     *
     * @return the File path for that edge-history partition
     */
    public File getHistoryFile(int partitionId) {
        return new File(_dir, "edgeHistory-p" + partitionId + _FILE_SUFFIX);
    }


    /**
     * Sets the prev outgoing edge pointer on an edge, replacing
     * the previous head, and thus maintaining a linked-list
     *
     * @param edgeId to update
     * @param ptr the new pointer value
     */
    public void setOutgoingEdgePtr(long edgeId, long ptr) {
        int partId = getPartitionId(edgeId);
        if (partId == -1) {
            return;
        }

        EdgePartition p = getPartition(partId);
        if (p==null) {
            return;
        }

        int rowId = getRowId(edgeId);
        p.setOutgoingEdgePtrByRow(rowId, ptr);
    }


    /**
     * Sets the prev incoming edge pointer on an edge, replacing
     * the previous head, and thus maintaining a linked-list
     *
     * @param edgeId to update
     * @param ptr the new pointer value
     */
    public void setIncomingEdgePtr(long edgeId, long ptr) {
        int partId = getPartitionId(edgeId);
        if (partId == -1) {
            return;
        }

        EdgePartition p = getPartition(partId);
        if (p==null) {
            return;
        }

        int rowId = getRowId(edgeId);
        p.setIncomingEdgePtrByRow(rowId, ptr);
    }


    /**
     * Creates a new edge partition
     *
     * @return the new edge partition id
     */
    public synchronized int getNewPartitionId() {
        int nPartitions = _partitions.size();

        int newPartition = nPartitions;

        getPartitionAndLoad(newPartition);

        return newPartition;
    }


    /**
     * Returns the path for the specified edge-property partition
     *
     * @param partitionId the edge partition id in question
     * @param fieldId the property-id in question
     *
     * @return the File path for that edge-property partition
     */
    public File getEdgePropertyFile(int partitionId, int fieldId) {
        return new File(_dir, "edgeProperty-p" + partitionId + "." + fieldId + _FILE_SUFFIX);
    }


    /**
     * This class is used to manage the writing of edge partitions to disk.
     *<p>
     * The PartitionWriter is expected to be run in a separate thread (via a pool)
     * and will notify any callers when it's complete.
     */
    private class PartitionWriter implements Runnable {
        private EdgePartition ap;
        private boolean close;
        private boolean complete;

        /**
         * Instantiates a partition-writer
         *
         * @param ap the edge partition to write to disk
         * @param close if true, will close the partition as well
         */
        public PartitionWriter(EdgePartition ap, boolean close) {
            complete = false;
            this.close = close;
            this.ap = ap;
        }


        /**
         * Invoked by the thread pool and actually saves the file
         */
        @Override
        public void run() {
            ap.saveToFile();
            if (close) {
                ap.close();
            }

            synchronized (this) {
                complete = true;
                notifyAll();
            }
        }


        /**
         * Invoked by a caller that needs to know when
         * the file-saving has been completed.
         */
        public void waitForCompletion() {
            synchronized (this) {
                while (!complete) {
                    try {
                        wait();
                    } catch (InterruptedException ie) {
                        // NOP
                    }
                }
            }

            synchronized (_writers) {
                int n = _writers.size();
                for (int i = 0; i < n; ++i) {
                    if (_writers.get(i) == this) {
                        _writers.remove(i);
                        break;
                    }
                }

                _writers.notifyAll();
            }
        }
    }
}