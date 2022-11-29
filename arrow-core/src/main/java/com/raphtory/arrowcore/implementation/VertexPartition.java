/* Copyright (C) Pometry Ltd - All Rights Reserved.
 *
 * This file is proprietary and confidential. Unauthorised
 * copying of this file, via any medium is strictly prohibited.
 *
 */

package com.raphtory.arrowcore.implementation;

import com.raphtory.arrowcore.model.Vertex;
import com.raphtory.arrowcore.util.LRUListItem;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;

/**
 * VertexPartition manages all of the Arrow files and access methods
 * for a single vertex partition - consisting of the arrow vertex data
 * itself, the vertex history and the user-defined fields and properties.
 *<p>
 * <p>TODO: Currently the local-vertex-id is stored in the vertex Arrow schema.
 * <p>TODO: It's redundant as the row number combined with the partition-id will
 * <p>TODO: always generate the local-vertex-id.
 */
public class VertexPartition implements LRUListItem<VertexPartition> {
    // Basic partition details:
    protected final int _partitionId;
    protected final long _baseLocalVertexIds;
    protected final VertexPartitionManager _apm;
    protected final LocalEntityIdStore _localIdStore;


    // The actual arrow vertex stuff:
    private VectorSchemaRoot _rootRO;       // Arrow root
    protected VertexArrowStore _store;      // Used to access the actual arrow data
    private ArrowFileReader _reader;        // File reader
    private int _currentSize = 0;           // Number of rows so far
    private boolean _modified = false;      // Whether or not this partition has been modified
    private boolean _loaded = false;        // Whether or not this partition has been loaded
    private boolean _sorted = false;        // Whether or not this partition has been sorted


    // History, Fields and Properties:
    protected final VertexHistoryPartition _history;           // The vertex history partition
    //protected final VertexSnapshotPartition _snapshots;      // The vertex snapshots partition
    protected final VertexEdgeIndexPartition _edgeIndex;       // Index of edges by dst-vertex-id
    protected final CachedMutatingEdgeMap _cachedEdgeMap;      // Cached dst/edges per src vertex

    protected SchemaFieldAccessor[] _fieldAccessors;           // Field accessors for the user-defined fields

    protected int _nProperties;
    protected VersionedProperty[] _propertyFields;             // The user-defined properties
    protected VertexPropertyPartition[] _propertyStores;       // The stores for each property
    protected IntVector[] _propertyPrevPtrVector;              // The arrow vectors for maintaining property history

    // LRU stuff, disabled.
    private VertexPartition _prev = null;
    private VertexPartition _next = null;


    /**
     * Instantiates a new Arrow vertex partition
     *
     * @param apm the partition manager to use
     * @param partitionId this partition-id
     */
    public VertexPartition(VertexPartitionManager apm, int partitionId) {
        _apm = apm;
        _partitionId = partitionId;
        _baseLocalVertexIds = apm.PARTITION_SIZE * _partitionId;

        _localIdStore = _apm._raphtoryPartition.getLocalEntityIdStore();
        _rootRO = null;
        _store = new VertexArrowStore();
        _history = new VertexHistoryPartition(_partitionId, this);
        _edgeIndex = new VertexEdgeIndexPartition(_partitionId, this);

        _cachedEdgeMap = new CachedMutatingEdgeMap(this, apm, apm._aepm);

        ArrayList<VersionedProperty> props = _apm._raphtoryPartition._propertySchema.versionedVertexProperties();
        _nProperties = props==null ? 0 : props.size();
        if (_nProperties>0) {
            _propertyFields = _apm._raphtoryPartition._propertySchema.versionedVertexProperties().toArray(new VersionedProperty[_nProperties]);
            _propertyStores = new VertexPropertyPartition[_nProperties];
            _propertyPrevPtrVector = new IntVector[_nProperties];

            for (int i=0; i<_nProperties; ++i) {
                _propertyStores[i] = new VertexPropertyPartition(_apm, _partitionId, i, _propertyFields[i]);
            }

            //_snapshots = new VertexSnapshotPartition(_partitionId, this);
        }
        else {
            //_snapshots = null;
        }
    }


    /**
     * Sets up an empty vertex partition
     */
    private void initialize() {
        Schema arrowSchema = _apm._raphtoryPartition._arrowVertexSchema;

        _rootRO = VectorSchemaRoot.create(arrowSchema, _apm.getAllocator());
        _rootRO.setRowCount(_apm.PARTITION_SIZE);
        _fieldAccessors = _apm._raphtoryPartition.createSchemaFieldAccessors(_rootRO, _apm._raphtoryPartition._propertySchema.nonversionedVertexProperties());

        _store.init(this, _rootRO, _fieldAccessors);

        _history.initialize();
        _edgeIndex.initialize();

        if (_nProperties>0) {
            for (int i = 0; i < _nProperties; ++i) {
                _propertyPrevPtrVector[i] = ((IntVector) _rootRO.getVector(_propertyFields[i]._prevPtrFieldIndexInEntitySchema));
                _propertyStores[i].initialize();
            }

            //_snapshots.initialize();
        }
    }


    /**
     * @return the partition id for this vertex partition
     */
    public int getPartitionId() {
        return _partitionId;
    }


    /**
     * @return the number of vertices in this partition
     */
    public synchronized int getVerticesCount() {
        return _currentSize;
    }


    /**
     * @return the number of history points in this partition
     */
    public synchronized int getHistoryCount() {
        return _history.getHistoryStore()._maxRow;
    }



    /**
     * @return the next free vertex-id that will fit in this
     * partition, or -1 if this partition is full
     */
    public synchronized long getNextFreeVertexId() {
        if (_currentSize<_apm.PARTITION_SIZE) {
            return (long)_partitionId * (long)_apm.PARTITION_SIZE + (long)_currentSize;
        }

        return -1L;
    }


    /**
     * Stores (adds or updates) the supplied vertex in this Arrow partition
     * and updates the local-entity-id store as well
     *
     * @param v the vertex in question
     *
     * @return true iff the vertex was created, false if it was updated
     */
    public boolean addVertex(Vertex v) {
        boolean added = internalAddVertex(v);

        if (added) {
            long globalId = _getGlobalVertexIdByRow(_apm.getRowId(v.getLocalId()));
            _localIdStore.setMapping(globalId, v.getLocalId());
        }

        return added;
    }


    /**
     * Stores (adds or updates) the supplied vertex in this Arrow partition
     *
     * @param v the vertex in question
     *
     * @return true iff the vertex was created, false if it was updated
     */
    private synchronized boolean internalAddVertex(Vertex v) {
        int row = _apm.getRowId(v.getLocalId());
        boolean created = _store.addVertex(row, v);
        _modified = true;
        _sorted = false;

        // Store the properties...
        for (int i=0; i<_nProperties; ++i) {
            VersionedEntityPropertyAccessor a = v.getProperty(i);

            if (a!=null && a.isSet()) {
                VersionedEntityPropertyAccessor efa = v.getProperty(i);
                IntVector prevPtrVector = _propertyPrevPtrVector[i];
                VertexPropertyPartition avpp = _propertyStores[i];
                int prevPtr = prevPtrVector.isSet(row)==0 ? -1 : prevPtrVector.get(row);

                // Properties are always accessible in time-order
                long creationTime = efa.getCreationTime();

                if (prevPtr==-1 || creationTime >= avpp.getCreationTime(prevPtr)) {
                    // New item is in ascending order...
                    int newPropertyRowId = avpp.addProperty(prevPtr, efa);
                    prevPtrVector.set(row, newPropertyRowId);
                }
                else {
                    int newPropertyRowId = avpp.insertProperty(prevPtr, creationTime, efa);
                }
            }
        }

        if (row >= _currentSize) {
            _currentSize = row+1;
        }

        return created;
    }


    /**
     * Adds a property for a specific vertex directly. This function maintains the
     * property history pointer and ensures the property history remains sorted on time.
     *
     * @param vertexId the vertex-id in question
     * @param propertyId the property in question
     * @param efa the property value
     *
     * @return the row in the vertex-property file that this property was stored at
     */
    public synchronized int addProperty(long vertexId, int propertyId, VersionedEntityPropertyAccessor efa) {
        if (efa.isSet()) {
            _modified = true;
            _sorted = false;

            int row = _apm.getRowId(vertexId);

            IntVector prevPtrVector = _propertyPrevPtrVector[efa.getEntityPrevPtrFieldId()];
            VertexPropertyPartition avpp = _propertyStores[propertyId];

            int prevPtr = prevPtrVector.isSet(row) == 0 ? -1 : prevPtrVector.get(row);
            long creationTime = efa.getCreationTime();

            int newPropertyRowId;

            if (prevPtr == -1 || creationTime >= avpp.getCreationTime(prevPtr)) {
                // New item is in ascending order...
                newPropertyRowId = avpp.addProperty(prevPtr, efa);
                prevPtrVector.set(row, newPropertyRowId);
            }
            else {
                newPropertyRowId = avpp.insertProperty(prevPtr, creationTime, efa);
            }

            return newPropertyRowId;
        }

        return -1;
    }


    /**
     *  Adds a history record for a vertex directly.
     *
     * @param vertexId the vertex to be updated
     * @param time the time of the history point
     * @param active true if the vertex was active at that time, false otherwise
     * @aparm updated true if a property was updated
     * @param localEdgeId the relevant edge-id, or -1 for none
     * @param outgoing true if the relevant edge is an outgoing edge, false otherwise
     *
     * @return the row in the history that this record was stored
     */
    public synchronized int addHistory(long vertexId, long time, boolean active, boolean updated, long localEdgeId, boolean outgoing) { // MT
        int vertexRow = _apm.getRowId(vertexId);
        int prevHistoryPtr = _store.getHistoryPtr(vertexRow);

        int historyRow = _history.addHistory(vertexRow, time, active, updated, prevHistoryPtr, localEdgeId, outgoing);
        _store.setHistoryPtr(vertexRow, historyRow);

        if (_store._oldestPoints.isSet(vertexRow)==0) {
            _store._oldestPoints.set(vertexRow, time);
        }
        else {
            long oldestPoint = _store._oldestPoints.get(vertexRow);
            if (oldestPoint==-1L || oldestPoint>time) {
                _store._oldestPoints.set(vertexRow, time);
            }
        }

        if (active) {
            long creationTime = _store._creationTimes.get(vertexRow);
            if (creationTime>time) {
                _store._creationTimes.set(vertexRow, time);
            }
        }

        _modified = true;
        _sorted = false;

        return historyRow;
    }


    /**
     * Retrieves a POJO Vertex object for the specified vertex-id.
     * The returned Vertex will have all the user-defined properties
     * and fields set and will have it's reference-count set to 1.
     *
     * @param id the local vertex id to retrieve
     *
     * @return the POJO Vertex
     */
    public synchronized Vertex getVertex(long id) { // MT
        if (_rootRO == null) {
            return null;
        }

        int row = _apm.getRowId(id);

        if (isValidRow(row)) {
            Vertex it = _apm._raphtoryPartition.getVertex();
            _store.retrieveVertex(row, it);
            it.incRefCount();

            for (int i=0; i<_nProperties; ++i) {
                IntVector prevPtrVector = _propertyPrevPtrVector[i];

                if (prevPtrVector.isSet(row)!=0) {
                    int prevPtrPropertyRow = prevPtrVector.get(row);
                    _propertyStores[i].retrieveProperty(prevPtrPropertyRow, it.getProperty(i));
                }
            }
            return it;
        }
        return null;
    }


    /**
     * Closes this vertex partition, closing the history
     * and properties as well.
     */
    public synchronized void close() {
        //System.out.println("CLOSING PARTITION: " + partitionId);
        clearReader();
        _history.close();
        closeProperties();
        _edgeIndex.close();
    }


    /**
     * Directly retrieves a property value at a particular propertyRow in the property file.
     *
     * @param field the property field in question
     * @param propertyRow the property propertyRow to use
     * @param ea the entity property where the value will be stored
     *
     * @return the updated entity property
     */
    protected VersionedEntityPropertyAccessor getPropertyByPropertyRow(int field, int propertyRow, VersionedEntityPropertyAccessor ea) {
        _propertyStores[field].retrieveProperty(propertyRow, ea);
        return ea;
    }


    /**
     * Directly retrieves a field value at a particular vertex row.
     *
     * @param field the field in question
     * @param vertexRow the vertex row
     * @param ea the entity accessor that will be updated with the value
     *
     * @return the updated entity accessor
     */
    protected EntityFieldAccessor getFieldByVertexRow(int field, int vertexRow, EntityFieldAccessor ea) {
        _fieldAccessors[field].load(ea, vertexRow);
        return ea;
    }


    /**
     * Retrieves the local vertex id stored at the specified row in the vertex partition.
     *
     * @param row the row to inspect
     *
     * @return the local vertex id stored there
     */
    protected long _getLocalVertexIdByRow(int row) {
        return _baseLocalVertexIds + row;
    }


    /**
     * Retrieves the global vertex id stored at the specified row in the vertex partition
     *
     * @param row the row to inspect
     *
     * @return the global vertex id
     */
    protected long _getGlobalVertexIdByRow(int row) {
        return _store._globalIds.get(row);
    }


    /**
     * Retrieves the nuber of incoming edges stored at the specified row in the vertex partition
     *
     * @param row the row to inspect
     *
     * @return the number of incoming edges
     */
    protected int _getNIncomingEdgesByRow(int row) {
        return _store._nIncomingEdges.get(row);
    }


    /**
     * Utility function to dump a row
     *
     * @param row which row to dump
     */
    private void dumpRow(int row) {
        System.out.println("HERE WITH: partId=" + _partitionId +", row=" + row);
        System.out.println("gid=" + _store._globalIds.isSet(row));
        System.out.println("lid=" + _getLocalVertexIdByRow(row));
    }


    /**
     * Retrieves the incoming edge ptr stored at the specified row in the vertex partition
     *
     * @param row the row to inspect
     *
     * @return the incoming edge ptr
     */
    protected long _getIncomingEdgePtrByRow(int row) {
        return _store._incomingEdges.get(row);
    }


    /**
     * Retrieves the outgoing edge ptr stored at the specified row in the vertex partition
     *
     * @param row the row to inspect
     *
     * @return the outgoing edge ptr
     */
    protected long _getOutgoingEdgePtrByRow(int row) {
        return _store._outgoingEdges.get(row);
    }


    /**
     * Retrieves the number of outgoing edges stored at the specified row in the vertex partition
     *
     * @param row the row to inspect
     *
     * @return the number of outgoing edges
     */
    protected int _getNOutgoingEdgesByRow(int row) {
        return _store._nOutgoingEdges.get(row);
    }

    /**
     * Checks whether the specified row refers to a valid vertex
     *
     * @param row the row in question
     *
     * @return true if a valid vertex is stored at that row, false otherwise
     */
    protected boolean isValidRow(int row) {
        return row>=0 && row<_currentSize && _store._globalIds.isSet(row)!=0;
    }


    /**
     * Adds an edge to the outgoing list for the specified vertex
     *
     * @param srcVertexId the vertex in question
     * @param edgeId the new edge to add
     * @param dstVertexId the dst vertex for this edge
     * @param dstIsGlobal true if the dest vertex id is global, false otherwise
     *
     * @return the previous head of the outgoing list of edges
     */
    public synchronized long addOutgoingEdgeToList(long srcVertexId, long edgeId, long dstVertexId, boolean dstIsGlobal) {
        int row = _apm.getRowId(srcVertexId);
        long prev = _store._outgoingEdges.get(row);
        _store._outgoingEdges.set(row, edgeId);
        _store._nOutgoingEdges.set(row, _store._nOutgoingEdges.get(row)+1);
        _edgeIndex.addEdgeIndexRecord(row, dstVertexId, edgeId, dstIsGlobal); // XXX We only need 1 of these methods of finding matching edges?
        _cachedEdgeMap.addEdge(srcVertexId, dstVertexId, edgeId, _store._nOutgoingEdges.get(row));
        _modified = true;
        _sorted = false;
        return prev;
    }


    /**
     * Adds an edge to the incoming list for the specified vertex
     *
     * @param vertexId the vertex in question
     * @param edgeId the new edge to add
     *
     * @return the previous head of the incoming list of edges
     */
    public synchronized long addIncomingEdgeToList(long vertexId, long edgeId) {
        int row = _apm.getRowId(vertexId);
        long prev = _store._incomingEdges.get(row);
        _store._incomingEdges.set(row, edgeId);
        _store._nIncomingEdges.set(row, _store._nIncomingEdges.get(row)+1);
        _modified = true;
        _sorted = false;
        return prev;
    }


    /**
     * Retrieves the vertex creation time by row
     *
     * @param rowId the row in question
     *
     * @return the creation time stored there
     */
    protected long getCreationTimeByRow(int rowId) {
        return _store._creationTimes.get(rowId);
    }


    /**
     * Saves this vertex partition to disk, if it's been modified.
     * The history and property files are also saved as appropriate.
     *<p>
     * <p>TODO: Exception handling if the writes fail?
     */
    public synchronized void saveToFile() {
        try {
            buildSortedVertexPointers();
            if (_modified) {
                _rootRO.syncSchema();
                _rootRO.setRowCount(_currentSize);

                File outFile = _apm.getVertexFile(_partitionId);
                ArrowFileWriter writer = new ArrowFileWriter(_rootRO, null, new FileOutputStream(outFile).getChannel());
                writer.start();
                writer.writeBatch();
                writer.end();

                writer.close();
            }

            _history.saveToFile();
            saveProperties();
            _edgeIndex.saveToFile();

            _modified = false;
        }
        catch (Exception e) {
            System.out.println("Exception: " + e);
            e.printStackTrace(System.err);
        }
    }


    /**
     * Synchronizes the sorted history store with this vertex store
     * by updating the sorted history vertex start/end pointers
     */
    protected synchronized void buildSortedVertexPointers() {
        if (_sorted) {
            return;
        }

        _history.sortHistoryTimes();
        int n = _history._history._maxRow;
        IntVector sorted = _history._history._sortedVertexTimeIndices;

        int vertexRow = -1;
        for (int i=0; i<n; ++i) {
            int actualRow = sorted.get(i);

            if (vertexRow==-1) {
                vertexRow = _history._history._vertexRowIds.get(actualRow);
                _store._sortedHStart.set(vertexRow, i);
            }
            else if (vertexRow != _history._history._vertexRowIds.get(actualRow)) {
                _store._sortedHEnd.set(vertexRow, i-1);
                vertexRow = _history._history._vertexRowIds.get(actualRow);
                _store._sortedHStart.set(vertexRow, i);
            }
        }

        _store._sortedHEnd.set(vertexRow, n-1);
        _sorted = true;
    }


    /**
     * Saves the property files for this vertex partition to disk
     */
    private void saveProperties() {
        if (_propertyStores==null || _propertyStores.length==0) {
            return;
        }

        for (int i=0; i<_propertyStores.length; ++i) {
            if (_propertyStores[i]!=null) {
                _propertyStores[i].saveToFile();
            }
        }
    }


    /**
     * Loads the property files for this vertex partition
     */
    private void loadProperties() {
        if (_propertyStores==null || _propertyStores.length==0) {
            return;
        }

        for (int i=0; i<_propertyStores.length; ++i) {
            _propertyPrevPtrVector[i] = ((IntVector)_rootRO.getVector(_propertyFields[i]._prevPtrFieldIndexInEntitySchema));

            if (_propertyStores[i]!=null) {
                if (!_propertyStores[i].loadFromFile()) {
                    _propertyStores[i].initialize();
                }
            }
        }
    }


    /**
     * Closes the property files for this vertex partition
     */
    private void closeProperties() {
        if (_propertyStores==null || _propertyStores.length==0) {
            return;
        }

        for (int i=0; i<_propertyStores.length; ++i) {
            if (_propertyStores[i]!=null) {
                _propertyStores[i].close();
            }
        }
    }


    /**
     * Loads this vertex partition from disk into memory.
     * Loads the history and properties as well as syncing
     * the global-id to local-id map.
     */
    public void loadFromFile() {
        loadFromFile(false);
    }


    /**
     * Waits until the partition has been completely loaded.
     */
    public synchronized void ensureLoaded() {
        while (!_loaded) {
            try {
                wait();
            }
            catch (InterruptedException ie) {
                // NOP
            }
        }
    }


    /**
     * Loads this vertex partition from disk into memory.
     * Loads the history and properties as well as syncing
     * the global-id to local-id map.
     *
     * @param force if true, will force a reload even if the partition has already been loaded
     */
    protected synchronized void loadFromFile(boolean force) {
        if (!force && _rootRO!=null && _loaded) {
            // Already initialized...
            return;
        }

        File inFile = _apm.getVertexFile(_partitionId);
        if (!inFile.exists()) {
            initialize();
            notifyAll();
            return;
        }

        try {
            clearReader();

            _reader = new ArrowFileReader(new FileInputStream(inFile).getChannel(), _apm.getAllocator(), _apm.getCompressionFactory());
            _reader.loadNextBatch();
            _rootRO = _reader.getVectorSchemaRoot();
            _rootRO.syncSchema();

            _fieldAccessors = _apm._raphtoryPartition.createSchemaFieldAccessors(_rootRO, _apm._raphtoryPartition._propertySchema.nonversionedVertexProperties());
            _store.init(this, _rootRO, _fieldAccessors);

            _modified = false;
            _sorted = true;
            _currentSize = _rootRO.getRowCount();
            if (_currentSize != _apm.PARTITION_SIZE) {
                _rootRO.setRowCount(_apm.PARTITION_SIZE);
            }

            if (!_history.loadFromFile()) {
                _history.initialize();
            }

            if (_apm._raphtoryPartition._syncIDMap) {
                LocalEntityIdStore leis = _apm._raphtoryPartition.getLocalEntityIdStore();
                for (int i=0; i<_currentSize; ++i) {
                    leis.setMapping(_getGlobalVertexIdByRow(i), _getLocalVertexIdByRow(i));
                }
            }

            loadProperties();

            if (!_edgeIndex.loadFromFile()) {
                _edgeIndex.initialize();
            }

            _loaded = true;
            notifyAll();
        }
        catch (Exception e) {
            System.err.println("Exception: " + e);
            e.printStackTrace(System.err);
        }
    }


    /**
     * Closes the resources associated with the vertex store.
     * Releases Arrow memory etc.
     */
    private void clearReader() {
        try {
            _loaded = false;

            if (_store !=null) {
                _store.init(this,null, null);
            }

            if (_rootRO != null) {
                _rootRO.clear();
                _rootRO.close();
                _rootRO = null;
                _fieldAccessors = null;
            }

            if (_reader != null) {
                _reader.close();
                _reader = null;
            }
        }
        catch (Exception e) {
            System.err.println("Exception: " + e);
            e.printStackTrace(System.err);
        }
    }


    /**
     * Retrieves the previous-ptr for a property for a specified row in the vertex partition
     *
     * @param field the field in question
     * @param row the vertex row in question
     *
     * @return the orevious-ptr at that row
     */
    protected int getPropertyPrevPtrByRow(int field, int row) {
        int prevPtr = _propertyPrevPtrVector[field].get(row);

        return prevPtr;
    }


    /**
     * Set the next pointer for the LRU list
     *
     * @param p the next pointer
     */
    @Override
    public void setNext(VertexPartition p) {
        _next = p;
    }


    /**
     * Set the prev pointer for the LRU list
     *
     * @param p the prev pointer
     */
    @Override
    public void setPrev(VertexPartition p) {
        _prev = p;
    }


    /**
     * @return the next pointer for the LRU list
     */
    @Override
    public VertexPartition getNext() {
        return _next;
    }


    /**
     * @return the prev pointer for the LRU list
     */
    @Override
    public VertexPartition getPrev() {
        return _prev;
    }


    /**
     * Returns the max history time for a vertex
     *
     * @param vertexId the vertex in question
     *
     * @return the max history time for that vertex
     */
    public long getVertexMaxHistoryTime(long vertexId) {
        int row = _apm.getRowId(vertexId);
        return _history.getVertexMaxHistoryTime(row);
    }


    /**
     * Returns the min history time for a vertex
     *
     * @param vertexId the vertex in question
     *
     * @return the min history time for that vertex
     */
    public long getVertexMinHistoryTime(long vertexId) {
        int row = _apm.getRowId(vertexId);
        return _history.getVertexMinHistoryTime(row);
    }


    /**
     * Initialises the matching-edge-scanner iterator to find matching edges for
     * the appropriate parameters.
     *
     * @param srcId the src vertex id
     * @param dstID the dst vertex id
     * @param isDstGlobal true if the dst vertex is remote, false otherwise
     * @param scanner the iterator to configure
     *
     * @return the configured iterator
     */
    public synchronized EdgeIterator findMatchingEdges(long srcId, long dstID, boolean isDstGlobal, CachedMutatingEdgeMap.MatchingEdgeCachedIterator scanner) {
        int row = _apm.getRowId(srcId);
        int nOutgoingEdges = _store._nOutgoingEdges.get(row);
        return _cachedEdgeMap.findMatchingEdges(srcId, dstID, isDstGlobal, nOutgoingEdges, scanner);
    }


    /**
     * Identifies whether or not the specified vertex is alive within the time window
     *
     * @param vertexId the vertexId
     * @param start the window start time (inclusive)
     * @param end the window end time (inclusive)
     *
     * @return true if the vertex was alive in the window, false otherwise
     */
    public boolean isAliveAt(long vertexId, long start, long end) {
        buildSortedVertexPointers();
        return _history.isAliveAt(_apm.getRowId(vertexId), start, end);
    }


    /**
     * Identifies whether or not the specified vertex is alive within the time window
     *
     * @param vertexId the vertexId
     * @param start the window start time (inclusive)
     * @param end the window end time (inclusive)
     * @param searcher the binary searcher to use
     *
     * @return true if the vertex was alive in the window, false otherwise
     */
    public boolean isAliveAt(long vertexId, long start, long end, VertexHistoryPartition.BoundedVertexEdgeTimeWindowComparator searcher) {
        buildSortedVertexPointers();
        return _history.isAliveAt(_apm.getRowId(vertexId), start, end, searcher);
    }


    /**
     * Identifies whether or not the specified vertex is alive within the time window
     *
     * @param vertexRow the vertex
     * @param start the window start time (inclusive)
     * @param end the window end time (inclusive)
     * @param searcher the binary searcher to use
     *
     * @return true if the edge was alive in the window, false otherwise
     */
    protected boolean isAliveAtByRow(int vertexRow, long start, long end, VertexHistoryPartition.BoundedVertexEdgeTimeWindowComparator searcher) {
        buildSortedVertexPointers();
        return _history.isAliveAt(vertexRow, start, end, searcher);
    }
}