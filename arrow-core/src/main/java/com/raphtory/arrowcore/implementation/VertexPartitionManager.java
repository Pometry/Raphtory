/* Copyright (C) Pometry Ltd - All Rights Reserved.
 *
 * This file is proprietary and confidential. Unauthorised
 * copying of this file, via any medium is strictly prohibited.
 *
 */

package com.raphtory.arrowcore.implementation;

import com.raphtory.arrowcore.model.Vertex;
import com.raphtory.arrowcore.util.LRUList;
import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.compression.CompressionCodec;

import java.io.File;
import java.util.ArrayList;
import java.util.concurrent.Future;

/**
 * The VertexPartitionManager manages all VertexPartitions.
 *<p>
 * It co-ordinates the loading and saving of them to disk files and
 * provides various access methods to data held in individual
 * VertexPartitions.
 *<p>
 * <p>TODO: Check the threading around getNewPartitionId(), getPartitionAndLoad() etc.
 *
 * @see VertexPartition
 */
public class VertexPartitionManager {
    public static String _FILE_SUFFIX = ".rap";

    // Number of Arrow partitions to perform initial sizing with
    // TODO: Make this configurable via the RaphtoryArrowPartition
    public static final int N_ARROW_PARTITIONS = 96;

    // Initial size of list managing file-writing tasks
    public static final int N_WRITERS = 64;



    protected final RaphtoryArrowPartition _raphtoryPartition;
    protected final File _dir;
    protected final EdgePartitionManager _aepm;
    private final RootAllocator _allocator = new RootAllocator();
    private final Int2ObjectArrayMap<VertexPartition> _partitions = new Int2ObjectArrayMap<>(N_ARROW_PARTITIONS);
    private final LRUList<VertexPartition> _lruList = new LRUList<>();
    private final ArrayList<PartitionWriter> _writers = new ArrayList<>(N_WRITERS);
    private final RaphtoryThreadPool _pool;
    private VertexPartition[] _partitionArray;
    private int _lastFreePartitionId = 0;

    public final int PARTITION_SIZE;
    public final int PARTITION_SHIFT;
    public final int PARTITION_MASK;


    /**
     * Instantiates a vertex partition mananger.
     *
     * @param raphtoryPartition the RAP to use
     * @param dir the path to store/load arrow vertex files
     * @param pool the thread-pool to use for reading/writing files
     */
    public VertexPartitionManager(RaphtoryArrowPartition raphtoryPartition, File dir, RaphtoryThreadPool pool) {
        _raphtoryPartition = raphtoryPartition;
        _aepm = raphtoryPartition.getEdgeMgr();
        _dir = dir;
        _pool = pool;

        PARTITION_SIZE = raphtoryPartition._vertexPartitionSize;
        PARTITION_MASK = raphtoryPartition._vertexPartitionSize_mask;
        PARTITION_SHIFT = raphtoryPartition._vertexPartitionSize_shift;

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
     * @return the number of vertex partitions that are loaded
     */
    public int nPartitions() { return _partitions.size(); }


    /**
     * @return the total number of vertices
     *
     * TODO: This is a bit slow
     */
    public long getTotalNumberOfVertices() {
        long nVertices = 0;
        int nPartitions = nPartitions();

        for (int i=0; i<nPartitions; ++i) {
            nVertices += getPartition(i).getVerticesCount();
        }

        return nVertices;
    }


    /**
     * Retrieves the next free vertex-id, filling in
     * any gaps.
     * <p>
     * This function should not be used from multiple threads
     * as the return value will not change until a new vertex
     * has been created and stored in the partition,
     * thus allowing a race condition.
     *
     * @return the next available free vertex-id
     */
    public long getNextFreeVertexId() {
        int nPartitions = nPartitions();
        if (nPartitions==0) {
            _lastFreePartitionId = 0;
            return 0L;
        }

        for (int i=_lastFreePartitionId; i<nPartitions; ++i) {
            long freeId = getPartition(i).getNextFreeVertexId();
            if (freeId!=-1L) {
                _lastFreePartitionId = i;
                return freeId;
            }
        }

        _lastFreePartitionId = nPartitions;
        return (long)nPartitions * (long)PARTITION_SIZE;
    }


    /**
     * @return the Arrow buffer-allocator to use
     */
    protected BufferAllocator getAllocator() {
        return _allocator;
    }


    /**
     * Calculates the partition-id that a vertex is located in
     *
     * @param vertexId the local vertex id in question
     *
     * @return the partition-id containing that vertex
     */
    public int getPartitionId(long vertexId) {
        return (int) (vertexId >>> PARTITION_SHIFT);
    }


    /**
     * Returns the row number where this vertex is located
     *
     * @param vertexId the local vertex id in question
     *
     * @return the row number containing that vertex
     */
    public int getRowId(long vertexId) {
        return (int)(vertexId & PARTITION_MASK);
    }


    /**
     * Loads a vertex partition from it's disk file.
     * If the disk file doesn't exist, then an empty
     * partition is created.
     *
     * @param partId the partition id to load
     *
     * @return the loaded vertex partition
     */
    private VertexPartition loadPartition(int partId) {
        waitForWriterToComplete(partId);

        boolean found = false;
        VertexPartition p = null;
        synchronized (this) {
            p = _partitions.get(partId);
            if (p != null) {
                found = true;
                //lruList.touched(p);
            }
            else {
                p = new VertexPartition(this, partId);
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
     * Currently disabled.
     *
     * @param p
     */
    private void touchPartition(VertexPartition p) {
        //lruList.touched(p);
    }


    /**
     * Loads all the vertex partition files that exist in the
     * target directory.
     *<p>
     * The files are read in parallel in an effort to improve
     * load times.
     */
    public void loadFiles() {
        ArrayList<Future<?>> tasks = new ArrayList<>();

        int fileId = 0;
        for (;;) {
            File file = getVertexFile(fileId);
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
        _partitionArray = new VertexPartition[nPartitions];
        for (int i=0; i<nPartitions; ++i) {
            _partitionArray[i] = _partitions.get(i);
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
        _partitions.keySet().forEach(p -> waitForWriterToComplete(p));
    }


    /**
     * Retrieves the vertex partition with the specified id
     *
     * @param partId the partition id in question
     *
     * @return the partition, or null if it hasn't been loaded
     */
    public VertexPartition getPartition(int partId) {
        if (_partitionArray!=null && partId<_partitionArray.length) {
            return _partitionArray[partId];
        }
        VertexPartition p = _partitions.get(partId);
        if (p != null) {
            touchPartition(p);
        }

        return p;
    }


    /**
     * Retrieves a vertex partition, loading it or creating it as required.
     *
     * @param partId the partition id in question
     *
     * @return the vertex partition
     */
    protected VertexPartition getPartitionAndLoad(int partId) {
        VertexPartition p = _partitions.get(partId);
        if (p == null) {
            if (_partitions.size() >= N_ARROW_PARTITIONS) {
                //removeLRUPartition();
            }
            p = loadPartition(partId);
        }
        else {
            touchPartition(p);
        }

        return p;
    }


    /**
     * Retrieves a POJO Vertex object for the specified vertex-id.
     * The reference count on the returned Vertex will be 1.
     *
     * @param id the local vertex id
     *
     * @return the POJO Vertex
     */
    public Vertex getVertex(long id) {
        int partId = getPartitionId(id);
        if (partId == -1) {
            return null;
        }

        VertexPartition p = getPartition(partId);
        return p==null ? null : p.getVertex(id);
    }


    /**
     * Adds or updates a POJO Vertex to the appropriate vertex partition.
     * Updates the local-entity-id store as well.
     *<p>
     * The fields and properties in the Vertex will be stored in Arrow.
     * History records will not be updated.
     *
     * @param v the Vertex in question
     * @return true iff the vertex was added, false if it was updated
     */
    public boolean addVertex(Vertex v) {
        long id = v.getLocalId();
        int partId = getPartitionId(id);
        VertexPartition p = getPartitionAndLoad(partId);
        return p.addVertex(v);
    }


    /**
     * Adds an edge to the incoming list for the specified vertex
     *
     * @param vertexId the vertex in question
     * @param edgeId the new edge to add
     *
     * @return the previous head of the incoming list of edges
     */
    public long addIncomingEdgeToList(long vertexId, long edgeId) {
        int partId = getPartitionId(vertexId);
        VertexPartition p = getPartitionAndLoad(partId);

        return p.addIncomingEdgeToList(vertexId, edgeId);
    }


    /**
     * Adds an edge to the outgoing list for the specified vertex
     *
     * @param vertexId the vertex in question
     * @param edgeId the new edge to add
     * @param dstVertexId the dst vertex for this edge
     * @param dstIsGlobal true if the dest vertex id is global, false otherwise
     *
     * @return the previous head of the outgoing list of edges
     */
    public long addOutgoingEdgeToList(long vertexId, long edgeId, long dstVertexId, boolean dstIsGlobal) {
        int partId = getPartitionId(vertexId);
        VertexPartition p = getPartitionAndLoad(partId);

        return p.addOutgoingEdgeToList(vertexId, edgeId, dstVertexId, dstIsGlobal);
    }


    /**
     * Adds a new property value for a vertex directly.
     *<p>
     * The list of properties in time order is maintained correctly.
     *
     * @param vertexId the local vertex-id in question
     * @param propertyId the property-id
     * @param efa the value to set
     *
     * @return the row index in which the property was stored.
     */
    public int addProperty(long vertexId, int propertyId, VersionedEntityPropertyAccessor efa) {
        int partId = getPartitionId(vertexId);
        VertexPartition p = getPartitionAndLoad(partId);
        return p.addProperty(vertexId, propertyId, efa);
    }


    /**
     * Inserts a new vertex history record for this vertex.
     *
     * @param vertexId the vertex in question
     * @param time the history point in time
     * @param active true if the vertex was active at that time, false otherwise
     * @param updated true if a property was updated, false otherwise
     * @param localEdgeId the edge id if this history point relates to an edge, otherwise -1
     * @param outgoing true if the related edge is an outgoing edge, otherwise false
     *
     * @return the row in the history file that the history record was stored
     */
    public int addHistory(long vertexId, long time, boolean active, boolean updated, long localEdgeId, boolean outgoing) {
        int partId = getPartitionId(vertexId);
        VertexPartition p = getPartitionAndLoad(partId);
        return p.addHistory(vertexId, time, active, updated, localEdgeId, outgoing);
    }


    /**
     * Retrieves the global-vertex-id directly from the vertex partition
     *
     * @param localVertexId vertex id in question
     *
     * @return the global vertex id
     */
    public long getGlobalVertexId(long localVertexId) {
        int partId = getPartitionId(localVertexId);
        if (partId == -1) {
            return -1L;
        }

        VertexPartition p = getPartition(partId);
        if (p==null) {
            return -1L;
        }

        return p._getGlobalVertexIdByRow(getRowId(localVertexId));
    }


    /**
     * Waits for a file-writer (if any) to complete for the
     * specified partition-id
     *
     * @param partId the vertex partition-id in question
     */
    private void waitForWriterToComplete(int partId) {
        PartitionWriter theWriter = null;

        synchronized (_writers) {
            int n = _writers.size();
            for (int i = 0; i < n; ++i) {
                PartitionWriter pw = _writers.get(i);
                VertexPartition ap = pw._ap;
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
     * Submits a new vertex file writer to the pool
     *
     * @param ap the vertex partition to write
     * @param close if true, will close the file as well
     */
    private void addWriter(VertexPartition ap, boolean close) {
        PartitionWriter pw = new PartitionWriter(ap, close);
        synchronized (_writers) {
            _writers.add(pw);
        }

        _pool.submitTask(pw);
    }


    /**
     * Removes the least-recently used vertex partition from memory.
     * <p>Experimental.
     * <p>Currently unused.
     */
    private void removeLRUPartition() {
        VertexPartition p = _lruList.getTail();
        if (p != null) {
            _lruList.remove(p);
            _partitions.remove(p.getPartitionId());
            addWriter(p, true);
        }
    }


    /**
     * Closes all vertex files.
     */
    public void close() {
        _partitions.values().forEach(p -> p.close());
    }


    /**
     * Returns the path for the specified vertex partition
     *
     * @param partitionId the vertex partition id in question
     *
     * @return the File path for that vertex partition
     */
    public File getVertexFile(int partitionId) {
        return new File(_dir, "vertex-p" + partitionId + _FILE_SUFFIX);
    }


    /**
     * Returns the path for the specified vertex-property partition
     *
     * @param partitionId the vertex partition id in question
     * @param fieldId the property-id in question
     *
     * @return the File path for that vertex-property partition
     */
    public File getVertexPropertyFile(int partitionId, int fieldId) {
        return new File(_dir, "vertexProperty-p" + partitionId + "." + fieldId + _FILE_SUFFIX);
    }


    /**
     * Returns the path for the specified vertex history partition
     *
     * @param partitionId the vertex partition id in question
     *
     * @return the File path for that vertex-history partition
     */
    public File getHistoryFile(int partitionId) {
        return new File(_dir, "vertexHistory-p" + partitionId + _FILE_SUFFIX);
    }


    /**
     * Returns the path for the specified vertex-edge index partition
     *
     * @param partitionId the vertex partition id in question
     *
     * @return the File path for that vertex-edge index partition
     */
    public File getEdgeIndexFile(int partitionId) {
        return new File(_dir, "vertexEdgeIndex-p" + partitionId + _FILE_SUFFIX);
    }

    /**
     * Returns the path for the specified vertex snapshot partition
     *
     * @param partitionId the vertex partition id in question
     *
     * @return the File path for that vertex-snapshot partition
     */
    public File getSnapshotFile(int partitionId) {
        return new File(_dir, "vertexSnapshot-p" + partitionId + _FILE_SUFFIX);
    }



    /**
     * Returns the Arrow compression factory to use.
     * <p>
     * Returns null as we don't support compression yet.
     *
     * @return null
     */
    public CompressionCodec.Factory getCompressionFactory() {
        return null;
    }


    /**
     * Creates a new vertex partition
     *
     * @return the new vertex partition id
     */
    public synchronized int getNewPartitionId() {
        int nPartitions = _partitions.size();
        int newPartition = nPartitions;

        getPartitionAndLoad(newPartition);

        return newPartition;
    }


    /**
     * Checks whether a local vertex-id represents a valid vertex
     *
     * @param vertexId the local vertex-id in question
     *
     * @return true if the vertex exists, false otherwise
     */
    public boolean isValid(long vertexId) {
        int partId = getPartitionId(vertexId);
        if (partId>=0 && partId<_partitions.size()) {
            VertexPartition p = getPartition(partId);
            int row = (int)(vertexId % PARTITION_SIZE);
            return p!=null && p.isValidRow(row);
        }

        return false;
    }


    /**
     * Retrieves a specific field from a particular vertex directly
     *
     * @param vertexId the local vertex id
     * @param fieldId the field to retrieve
     * @param efa field-accessors, the relevant one will be updated with the value
     *
     * @return the updated field-accessor
     */
    public EntityFieldAccessor getField(long vertexId, int fieldId, EntityFieldAccessor[] efa) {
        int partId = getPartitionId(vertexId);
        if (partId == -1) {
            return null;
        }

        VertexPartition p = getPartition(partId);
        if (p==null) {
            return null;
        }

        int row = getRowId(vertexId);

        p._fieldAccessors[fieldId].load(efa[fieldId], row);
        return efa[fieldId];
    }


    /**
     * This class is used to manage the writing of vertex partitions to disk.
     *<p>
     * The PartitionWriter is expected to be run in a separate thread (via a pool)
     * and will notify any callers when it's complete.
     */
    private class PartitionWriter implements Runnable {
        private final VertexPartition _ap;
        private final boolean _close;
        private boolean _complete;

        /**
         * Instantiates a partition-writer
         *
         * @param ap the vertex partition to write to disk
         * @param close if true, will close the partition as well
         */
        public PartitionWriter(VertexPartition ap, boolean close) {
            _complete = false;
            _close = close;
            _ap = ap;
        }


        /**
         * Invoked by the thread pool and actually saves the file
         */
        @Override
        public void run() {
            _ap.saveToFile();
            if (_close) {
                _ap.close();
            }

            synchronized (this) {
                // Tell any callers that we're finished
                _complete = true;
                notifyAll();
            }
        }


        /**
         * Invoked by a caller that needs to know when
         * the file-saving has been completed.
         */
        public void waitForCompletion() {
            synchronized (this) {
                while (!_complete) {
                    try {
                        wait();
                    }
                    catch (InterruptedException ie) {
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

                _writers.notifyAll();;
            }
        }
    }
}