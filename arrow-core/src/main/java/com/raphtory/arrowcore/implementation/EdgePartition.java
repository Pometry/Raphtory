/* Copyright (C) Pometry Ltd - All Rights Reserved.
 *
 * This file is proprietary and confidential. Unauthorised
 * copying of this file, via any medium is strictly prohibited.
 *
 */

package com.raphtory.arrowcore.implementation;

import com.raphtory.arrowcore.model.Edge;
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
 * EdgePartition manages all of the Arrow files and access methods
 * for a single edge partition - consisting of the arrow edge data
 * itself, the edge history and the user-defined fields and properties.
 *<p>
 * <p>TODO: Currently the local-edge-id is stored in the edge Arrow schema.
 * <p>TODO: It's redundant as the row number combined with the partition-id will
 * <p>TODO: always generate the local-edge-id.
 */
public class EdgePartition implements LRUListItem<EdgePartition> {
    // Basic partition details:
    protected final int _partitionId;
    protected final long _baseLocalEdgeIds;
    protected final EdgePartitionManager _aepm;

    // The actual arrow vertex stuff:
    private VectorSchemaRoot _rootRO;           // Arrow root
    protected EdgeArrowStore _store;            // Used to access the actual arrow data
    private ArrowFileReader _reader;            // File reader
    private boolean _modified = false;          // Whether or not this partition has been modified
    private boolean _loaded = false;            // Whether or not this partition has been loaded
    private int _currentSize = 0;               // Number of rows so far
    private boolean _sorted = false;            // Whether or not this partition is sorted

    // History, Fields and Properties:
    protected EdgeHistoryPartition _history;   // The edge history partition

    protected SchemaFieldAccessor[] _fieldAccessors;            // Field accessors for the user-defined fields

    protected int _nProperties;
    protected VersionedProperty[] _propertyFields;              // The user-defined fields
    protected EdgePropertyPartition[] _propertyStores;     // The stores for each property
    protected IntVector _propertyPrevPtrVector[];               // The arrow vectors for maintaining property history

    // LRU stuff, disabled.
    private EdgePartition _prev = null;
    private EdgePartition _next = null;



    /**
     * Instantiates a new Arrow edge partition
     *
     * @param aepm the partition manager to use
     * @param partitionId this partition-id
     */
    public EdgePartition(EdgePartitionManager aepm, int partitionId) {
        _aepm = aepm;
        _partitionId = partitionId;
        _baseLocalEdgeIds = _partitionId * aepm.PARTITION_SIZE;
        _store = new EdgeArrowStore();
        _history = new EdgeHistoryPartition(_partitionId, this);

        ArrayList<VersionedProperty> props = _aepm._raphtoryPartition._propertySchema.versionedEdgeProperties();
        _nProperties = props==null ? 0 : props.size();
        if (_nProperties>0) {
            _propertyFields = _aepm._raphtoryPartition._propertySchema.versionedEdgeProperties().toArray(new VersionedProperty[_nProperties]);
            _propertyStores = new EdgePropertyPartition[_nProperties];
            _propertyPrevPtrVector = new IntVector[_nProperties];

            for (int i=0; i<_nProperties; ++i) {
                _propertyStores[i] = new EdgePropertyPartition(_aepm, _partitionId, i, _propertyFields[i]);
            }
        }
    }


    /**
     * Sets up an empty edge partition
     */
    private void initialize() {
        Schema arrowSchema = _aepm._raphtoryPartition._arrowEdgeSchema;

        _rootRO = VectorSchemaRoot.create(arrowSchema, _aepm.getAllocator());
        _rootRO.setRowCount(_aepm.PARTITION_SIZE);
        _fieldAccessors = _aepm._raphtoryPartition.createSchemaFieldAccessors(_rootRO, _aepm._raphtoryPartition._propertySchema.nonversionedEdgeProperties());

        _store.init(this, _rootRO, _fieldAccessors);

        _history.initialize();

        for (int i=0; i<_nProperties; ++i) {
            _propertyPrevPtrVector[i] = ((IntVector)_rootRO.getVector(_propertyFields[i]._prevPtrFieldIndexInEntitySchema));
            _propertyStores[i].initialize();
        }
    }


    /**
     * @return the partition id for this edge partition
     */
    public int getPartitionId() {
        return _partitionId;
    }


    /**
     * @return the number of edges in this partition
     */
    public synchronized int getEdgesCount() {
        return _currentSize;
    }


    /**
     * @return the next free edge-id that will fit in this
     * partition, or -1 if this partition is full
     */
    public synchronized long getNextFreeEdgeId() {
        if (_currentSize<_aepm.PARTITION_SIZE) {
            return (long)_partitionId * (long)_aepm.PARTITION_SIZE + (long)_currentSize;
        }

        return -1L;
    }


    /**
     * Stores (adds or updates) the supplied edge in this Arrow partition
     *
     * @param e the edge in question
     * @param prevIncomingPtr the prev incoming edge list pointer
     * @param prevOutgoingPtr the prev outgoing edge list pointer
     *
     * @return the row number that the edge was stored in
     */
    public synchronized int addEdge(Edge e, long prevIncomingPtr, long prevOutgoingPtr) { // MT
        int row = _aepm.getRowId(e.getLocalId());
        boolean isNew = _store.addEdge(row, e, prevIncomingPtr, prevOutgoingPtr);
        _modified = true;
        _sorted = false;

        for (int i=0; i<_nProperties; ++i) {
            VersionedEntityPropertyAccessor a = e.getProperty(i);

            if (a!=null && a.isSet()) {
                VersionedEntityPropertyAccessor efa = e.getProperty(i);
                IntVector prevPtrVector = _propertyPrevPtrVector[i];
                int prevPtr = prevPtrVector.isSet(row)==0 ? -1 : prevPtrVector.get(row);
                int newRowId = _propertyStores[i].addProperty(prevPtr, efa);
                prevPtrVector.set(row, newRowId);
            }
        }

        if (row >= _currentSize) {
            _currentSize = row+1;
        }

        return row;
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
    public synchronized int addProperty(long edgeId, int propertyId, VersionedEntityPropertyAccessor efa) {
        if (!efa.isSet()) {
            return -1;
        }

        int row = _aepm.getRowId(edgeId);

        IntVector prevPtrVector = _propertyPrevPtrVector[efa.getEntityPrevPtrFieldId()];
        int prevPtr = prevPtrVector.isSet(row)==0 ? -1 : prevPtrVector.get(row);
        int newRowId = _propertyStores[propertyId].addProperty(prevPtr, efa);
        prevPtrVector.set(row, newRowId);
        return newRowId;
    }


    /**
     * Inserts a new edge history record for this vertex.
     *
     * @param edgeId the edge in question
     * @param time the history point in time
     * @param active true if the vertex was active at that time, false otherwise
     * @param updated true if a property was updated, false otherwise
     *
     * @return the row in the history file that the history record was stored
     */
    public synchronized int addHistory(long edgeId, long time, boolean active, boolean updated) {
        int edgeRow = _aepm.getRowId(edgeId);
        int historyRow = _history.addHistory(edgeRow, time, active, updated, _store.getHistoryPtr(edgeRow));
        _store.setHistoryPtr(edgeRow, historyRow);

        long oldestPoint = _store._oldestPoints.get(edgeRow);
        if (oldestPoint>time) {
            _store._oldestPoints.set(edgeRow, time);
        }

        if (active) {
            long creationTime = _store._creationTimes.get(edgeRow);
            if (creationTime>time) {
                _store._creationTimes.set(edgeRow, time);
            }
        }

        _modified = true;
        _sorted = false;

        return historyRow;
    }


    /**
     * Sets the outgoing edge pointer for an edge - maintaining the linked list.
     *
     * @param edgeId the edgeId to update
     * @param ptr the new pointer value
     */
    public void setOutgoingEdgePtrByEdgeId(long edgeId, long ptr) {
        setOutgoingEdgePtrByRow(_aepm.getRowId(edgeId), ptr);
    }


    /**
     * Sets the outgoing edge pointer for an edge - maintaining the linked list.
     *
     * @param row the row to update
     * @param ptr the new pointer value
     */
    public synchronized void setOutgoingEdgePtrByRow(int row, long ptr) {
        _store._prevOutgoingEdgesPtr.set(row, ptr);
        _modified = true;
        _sorted = false;
    }


    /**
     * Sets the incoming edge pointer for an edge - maintaining the linked list.
     *
     * @param row the row to update
     * @param ptr the new pointer value
     */
    public synchronized void setIncomingEdgePtrByRow(int row, long ptr) {
        _store._prevIncomingEdgesPtr.set(row, ptr);
        _modified = true;
        _sorted = false;
    }


    /**
     * Sets the incoming edge pointer for an edge - maintaining the linked list.
     *
     * @param edgeId the edge to update
     * @param ptr the new pointer value
     */
    public void setIncomingEdgePtrByEdgeId(long edgeId, long ptr) {
        setIncomingEdgePtrByRow(_aepm.getRowId(edgeId), ptr);
    }


    /**
     * Retrieves a POJO Edge object for the specified edge-id.
     * The returned Edge will have all the user-defined properties
     * and fields set and will have it's reference-count set to 1.
     *
     * @param id the local edge id to retrieve
     *
     * @return the POJO Edge
     */
    public Edge getEdge(long id) {
        if (_rootRO == null) {
            return null;
        }

        int row = _aepm.getRowId(id);

        if (isValidRow(row)) {
            Edge it = _aepm._raphtoryPartition.getEdge();
            _store.retrieveEdge(row, it);
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
     * Directly retrieves a property value at a particular propertyRow in the property file.
     *
     * @param field the property field in question
     * @param propertyRow the property propertyRow to use
     * @param ea the entity property where the value will be stored
     *
     * @return the updated entity property
     */
    public VersionedEntityPropertyAccessor getPropertyByPropertyRow(int field, int propertyRow, VersionedEntityPropertyAccessor ea) {
        _propertyStores[field].retrieveProperty(propertyRow, ea);
        return ea;
    }


    /**
     * Directly retrieves a field value at a particular edge row.
     *
     * @param field the field in question
     * @param edgeRow the vertex row
     * @param ea the entity accessor that will be updated with the value
     *
     * @return the updated entity accessor
     */
    protected EntityFieldAccessor getFieldByVertexRow(int field, int edgeRow, EntityFieldAccessor ea) {
        _fieldAccessors[field].load(ea, edgeRow);
        return ea;
    }


    /**
     * Set the next pointer for the LRU list
     *
     * @param p the next pointer
     */
    @Override
    public void setNext(EdgePartition p) {
        _next = p;
    }


    /**
     * Set the prev pointer for the LRU list
     *
     * @param p the prev pointer
     */
    @Override
    public void setPrev(EdgePartition p) {
        _prev = p;
    }


    /**
     * @return the next pointer for the LRU list
     */
    @Override
    public EdgePartition getNext() {
        return _next;
    }


    /**
     * @return the prev pointer for the LRU list
     */
    @Override
    public EdgePartition getPrev() {
        return _prev;
    }


    /**
     * Closes this edge partition, closing the history
     * and properties as well.
     */
    public synchronized void close() {
        //System.out.println("EDGE CLOSING PARTITION: " + partitionId);
        clearReader();
        _history.close();
        closeProperties();
    }


    /**
     * Saves this edge partition to disk, if it's been modified.
     * The history and property files are also saved as appropriate.
     *<p>
     * <p>TODO: Exception handling if the writes fail?
     */
    public synchronized void saveToFile() {
        try {
            buildSortedEdgePointers();

            if (_modified) {
                _rootRO.syncSchema();
                _rootRO.setRowCount(_currentSize);

                File outFile = _aepm.getEdgeFile(_partitionId);
                ArrowFileWriter writer = new ArrowFileWriter(_rootRO, null, new FileOutputStream(outFile).getChannel());
                writer.start();
                writer.writeBatch();
                writer.end();

                writer.close();
            }

            _history.saveToFile();
            saveProperties();

            _modified = false;
        }
        catch (Exception e) {
            System.out.println("Exception: " + e);
            e.printStackTrace(System.err);
        }
    }


    /**
     * Synchronizes the sorted history store with this edge store
     * by updating the sorted history edge start/end pointers
     */
    protected synchronized void buildSortedEdgePointers() {
        if (_sorted) {
            return;
        }

        _history.sortHistoryTimes();
        int n = _history._history._maxRow;
        IntVector sorted = _history._history._sortedEdgeTimeIndices;

        int edgeRow = -1;
        for (int i=0; i<n; ++i) {
            int actualRow = sorted.get(i);

            if (edgeRow==-1) {
                edgeRow = _history._history._edgeRowIds.get(actualRow);
                _store._sortedHStart.set(edgeRow, i);
            }
            else if (edgeRow != _history._history._edgeRowIds.get(actualRow)) {
                _store._sortedHEnd.set(edgeRow, i-1);
                edgeRow = _history._history._edgeRowIds.get(actualRow);
                _store._sortedHStart.set(edgeRow, i);
            }
        }

        _store._sortedHEnd.set(edgeRow, n-1);
        _sorted = true;

//        System.out.println("SORTED EDGE TIMES: " + sorted.toString());
//        for (int i=0; i<_currentSize; ++i) {
//            System.out.println("EDGE: " + i + " F: " + _store._sortedHStart.get(i) + " -> " + _store._sortedHEnd.get(i));
//        }
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
     * Loads this edge partition from disk into memory.
     * Loads the history and properties as well as syncing
     * the global-id to local-id map.
     */
    public void loadFromFile() {
        loadFromFile(false);
    }


    /**
     * Loads this edge partition from disk into memory.
     * Loads the history and properties as well as syncing
     * the global-id to local-id map.
     *
     * @param force if true, will force a reload even if the partition has already been loaded
     */
    public synchronized void loadFromFile(boolean force) {
        if (!force && _rootRO!=null && _loaded) {
            // Already initialised...
            return;
        }

        File inFile = _aepm.getEdgeFile(_partitionId);
        if (!inFile.exists()) {
            initialize();
            notifyAll();
            return;
        }

        try {
            clearReader();

            _reader = new ArrowFileReader(new FileInputStream(inFile).getChannel(), _aepm.getAllocator(), _aepm.getCompressionFactory());
            _reader.loadNextBatch();
            _rootRO = _reader.getVectorSchemaRoot();
            _rootRO.syncSchema();

            _fieldAccessors = _aepm._raphtoryPartition.createSchemaFieldAccessors(_rootRO, _aepm._raphtoryPartition._propertySchema.nonversionedEdgeProperties());
            _store.init(this, _rootRO, _fieldAccessors);

            _modified = false;
            _sorted = true;
            _currentSize = _rootRO.getRowCount();
            _rootRO.setRowCount(_aepm.PARTITION_SIZE);

            if (!_history.loadFromFile()) {
                _history.initialize();
            }

            loadProperties();

            _loaded = true;
            notifyAll();
        }
        catch (Exception e) {
            System.err.println("Exception: " + e);
            e.printStackTrace(System.err);
        }
    }


    /**
     * Closes the resources associated with the edge store.
     * Releases Arrow memory etc.
     */
    private void clearReader() {
        try {
            _loaded = false;

            if (_store !=null) {
                _store.init(this, null, null);
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
     * Saves the property files for this edge partition to disk
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
     * Loads the property files for this edge partition
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
     * Returns the prev incoming ptr directly for the specified row
     *
     * @param row the row in question
     *
     * @return the prev incoming ptr
     */
    protected long _getPrevIncomingPtrByRow(int row) {
        return _store._prevIncomingEdgesPtr.get(row);
    }


    /**
     * Returns true if the specified row is valid
     *
     * @param row the row in question
     *
     * @return true if valid, false otherwise
     */
    protected boolean isValidRow(int row) { return row>=0 && row<_currentSize && _store._srcVertexIds.isSet(row)!=0; }


    /**
     * Returns the prev outgoing ptr directly for the specified row
     *
     * @param row the row in question
     *
     * @return the prev outgoing ptr
     */
    protected long _getPrevOutgoingPtrByRow(int row) {
        return _store._prevOutgoingEdgesPtr.get(row);
    }


    /**
     * Returns the src vertex id directly for the specified row
     *
     * @param row the row in question
     *
     * @return the src vertex id
     */
    protected long getSrcVertexId(int row) {
        return _store._srcVertexIds.get(row);
    }


    /**
     * Returns the dst vertex id directly for the specified row
     *
     * @param row the row in question
     *
     * @return the dst vertex id
     */
    protected long getDstVertexId(int row) {
        return _store._dstVertexIds.get(row);
    }


    /**
     * Returns true if the src vertex id is a local vertex id
     *
     * @param row the row in question
     *
     * @return true if the src-vertex-id is a local-id, false otherwise
     */
    protected boolean isSrcVertexLocal(int row) {
        return _store._srcIsGlobals.get(row) == 0;
    }
    /**
     * Returns true if the src vertex id is a local vertex id
     *
     * @param row the row in question
     *
     * @return true if the src-vertex-id is a local-id, false otherwise
     */
    protected boolean isSrcVertexGlobal(int row) { return _store._srcIsGlobals.get(row)!=0;
}


    /**
     * Returns true if the dst vertex id is a local vertex id
     *
     * @param row the row in question
     *
     * @return true if the dst-vertex-id is a local-id, false otherwise
     */
    protected boolean isDstVertexLocal(int row) {
        return _store._dstIsGlobals.get(row) == 0;
    }
    /**
     * Returns true if the dst vertex id is a local vertex id
     *
     * @param row the row in question
     *
     * @return true if the dst-vertex-id is a local-id, false otherwise
     */
    protected boolean isDstVertexGlobal(int row) { return _store._dstIsGlobals.get(row) != 0; }



    /**
     * Returns the edge creation time for the row directly
     *
     * @param row the row in question
     *
     * @return the creation time
     */
    protected long _getCreationTime(int row) {
        return _store._creationTimes.get(row);
    }


    /**
     * Returns the local-edge-id for the row directly
     *
     * @param row the row in question
     *
     * @return the local edge-id
     */
    protected long _getLocalEdgeIdByRow(int row) { return _baseLocalEdgeIds + row; }


    /**
     * Retrieves the previous-ptr for a property for a specified row in the edge partition
     *
     * @param field the field in question
     * @param row the edge row in question
     *
     * @return the orevious-ptr at that row
     */
    protected int getPropertyPrevPtrByRow(int field, int row) {
        IntVector iv = _propertyPrevPtrVector[field];

        if (iv.isSet(row)!=0) {
            return iv.get(row);
        }

        return -1;
    }


    /**
     * Returns the max history time for the specified edge
     *
     * @param edgeId the edge in question
     * @return the max history time
     */
    public long getEdgeMaxHistoryTime(long edgeId) {
        int row = _aepm.getRowId(edgeId);
        return _history.getEdgeMaxHistoryTime(row);
    }


    /**
     * Returns the min history time for the specified edge
     *
     * @param edgeId the edge in question
     * @return the min history time
     */
    public long getEdgeMinHistoryTime(long edgeId) {
        int row = _aepm.getRowId(edgeId);
        return _history.getEdgeMinHistoryTime(row);
    }


    /**
     * Identifies whether or not the specified edge is alive within the time window
     *
     * @param edgeId the edge-edgeId
     * @param start the window start time (inclusive)
     * @param end the window end time (inclusive)
     * @param searcher the binary searcher to use
     *
     * @return true if the edge was alive in the window, false otherwise
     */
    public boolean isAliveAt(long edgeId, long start, long end, EdgeHistoryPartition.EdgeHistoryBoundedBinarySearch searcher) {
        buildSortedEdgePointers();
        return _history.isAliveAt(edgeId, start, end, searcher);
    }


    /**
     * Identifies whether or not the specified edge is alive within the time window
     *
     * @param edgeId the edge-edgeId
     * @param start the window start time (inclusive)
     * @param end the window end time (inclusive)
     *
     * @return true if the edge was alive in the window, false otherwise
     */
    public boolean isAliveAt(long edgeId, long start, long end) {
        buildSortedEdgePointers();
        return _history.isAliveAt(edgeId, start, end);
    }
}
