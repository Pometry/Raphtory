/* Copyright (C) Pometry Ltd - All Rights Reserved.
 *
 * This file is proprietary and confidential. Unauthorised
 * copying of this file, via any medium is strictly prohibited.
 *
 */

package com.raphtory.arrowcore.implementation;

import com.raphtory.arrowcore.util.ArrowBinarySearch;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntComparator;
import org.apache.arrow.algorithm.sort.VectorValueComparator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

public class VertexSnapshotPartition {
    private static final ThreadLocal<IntArrayList> _tmpListTL = ThreadLocal.withInitial(IntArrayList::new);

    private static final ThreadLocal<VertexSnapshotPartition.SnapshotVertexTimeComparatorIAL> _vertexTimeCmpTL = ThreadLocal.withInitial(VertexSnapshotPartition.SnapshotVertexTimeComparatorIAL::new);
    private static final ThreadLocal<VertexSnapshotPartition.VertexTimeWindowComparator> _timeVertexWindowComparatorTL = ThreadLocal.withInitial(VertexSnapshotPartition.VertexTimeWindowComparator::new);


    /**
     * Comparator to sort all snapshot records by vertex AND then time
     */
    private static class SnapshotVertexTimeComparatorIAL implements IntComparator {
        private VertexSnapshotPartition _vsp;
        private BigIntVector _times;
        private BigIntVector _vertexIds;

        /**
         * Initialises this instance
         *
         * @param vsp the history partition to use
         */
        public void init(VertexSnapshotPartition vsp) {
            _vsp = vsp;
            _times = vsp._store._times;
            _vertexIds = vsp._store._localIds;
        }


        /**
         * Compares 2 rows of the history data by VERTEX and then by TIME
         *
         * @param row1
         * @param row2
         *
         * @return -1, 0 or +1 depending on whether row1 is < = > than row 2
         */
        @Override
        public int compare(int row1, int row2) {
            long vertex1 = _vertexIds.get(row1);
            long vertex2 = _vertexIds.get(row2);

            int retval = Long.compare(vertex1, vertex2);
            if (retval!=0) {
                return retval;
            }

            long time1 = _times.get(row1);
            long time2 = _times.get(row2);

            return Long.compare(time1, time2);
        }
    }


    private final int _partitionId;
    private final VertexPartition _avp;
    private final VertexPartitionManager _apm;

    private VertexSnapshotStore _store;
    private VectorSchemaRoot _snapshotRO;
    private ArrowFileReader _reader;
    private boolean _modified = false;
    private boolean _sorted = false;


    /**
     * Instantiate a new vertex history partition
     *
     * @param partitionId the partition-id of this partition
     * @param avp the owning vertex-partition
     */
    public VertexSnapshotPartition(int partitionId, VertexPartition avp) {
        _partitionId = partitionId;
        _avp = avp;
        _apm = avp._apm;

        _store = new VertexSnapshotStore();
    }


    /**
     * Initialises this instance for an empty file
     */
    public void initialize() {
        _store.init(null, _avp);
        _snapshotRO = VectorSchemaRoot.create(_store.createVertexSnapshotSchema(), _apm.getAllocator());
        _store.init(_snapshotRO, _avp);
    }


    public int takeSnapshot(long localVertexId, int vertexRow, boolean iv, long time) {
        return _store.takeSnapshot(localVertexId, vertexRow, iv, time, _avp);
    }


    /**
     * Returns whether or not the vertex is active for that snapshot record
     *
     * @param rowId the snapshot record in question
     *
     * @return true if the vertex is active, false otherwise
     */
    public boolean getIsAliveBySnapshotRowId(int rowId) {
        return _store._initialValues.get(rowId) != 0;
    }


    /**
     * Returns the modification time of a snapshot record
     *
     * @param rowId the snapshot record in question
     *
     * @return the modification time associated with this record
     */

    public long getModificationTimeBySnapshotRowId(int rowId) {
        return _store._times.get(rowId);
    }


    /**
     * Returns the vertex-id stored at the specified snapshot row
     *
     * @param rowId the snapshot row id to inspect
     *
     * @return the vertexId stored there
     */
    public long getVertexIdBySnapshotRowId(int rowId) {
        return _store._localIds.get(rowId);
    }


    /**
     * Closes this vertex snapshot partition
     */
    public void close() {
        //System.out.println("CLOSING PARTITION: " + partitionId);
        clearReader();
    }


    /**
     * Saves this vertex snapshot partition to disk, if it's been modified.
     * The data will be sorted as required.
     *
     * TODO: Exception handling if the write fails?
     */
    public void saveToFile() {
        try {
            sortSnapshotTimes();

            if (_modified) {
                _snapshotRO.syncSchema();
                _snapshotRO.setRowCount(_store._maxRow);

                File outFile = _apm.getSnapshotFile(_partitionId);
                ArrowFileWriter writer = new ArrowFileWriter(_snapshotRO, null, new FileOutputStream(outFile).getChannel());
                writer.start();
                writer.writeBatch();
                writer.end();

                writer.close();
            }

            _modified = false;
            _sorted = true;
        }
        catch (Exception e) {
            System.out.println("Exception: " + e);
            e.printStackTrace(System.err);
        }
    }


    /**
     * Loads this vertex snapshot partition from a disk file
     *
     * @return true if the file exists and was read, false otherwise
     */
    public boolean loadFromFile() {
        File inFile = _apm.getSnapshotFile(_partitionId);
        if (!inFile.exists()) {
            return false;
        }

        try {
            clearReader();

            _reader = new ArrowFileReader(new FileInputStream(inFile).getChannel(), _apm.getAllocator(), _apm.getCompressionFactory());
            _reader.loadNextBatch();
            _snapshotRO = _reader.getVectorSchemaRoot();
            _snapshotRO.syncSchema();

            _store.init(_snapshotRO, _avp);

            _store._maxRow = _snapshotRO.getRowCount();

            _modified = false;
            _sorted = true;

            return true;
        }
        catch (Exception e) {
            System.err.println("Exception: " + e);
            e.printStackTrace(System.err);
            return false;
        }
    }


    /**
     * Closes the resources associated with the vertex snapshot store.
     * Releases Arrow memory etc.
     */
    private void clearReader() {
        try {
            if (_store !=null) {
                _store.init(null, _avp);
            }

            if (_snapshotRO != null) {
                _snapshotRO.clear();
                _snapshotRO.close();
                _snapshotRO = null;
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
     * Sorts the snapshot records by vertex and time.
     *<p>
     * The records are not actually sorted (ie. moved into sorted order),
     * instead, an index is created that points to the rows in the correct
     * sorted order. ie. an indirect sorted index is created.
     */
    protected synchronized void sortSnapshotTimes() {
        if (_sorted) {
            return;
        }

        int n = _store._maxRow;

        IntArrayList tmpList = _tmpListTL.get();
        tmpList.clear();
        tmpList.ensureCapacity(n);
        tmpList.size(n);
        int[] elements = tmpList.elements();

        // Sort by vertex-row-id and time
        for (int i=0; i<n; ++i) {
            elements[i] = i;
        }
        SnapshotVertexTimeComparatorIAL vtCmp = _vertexTimeCmpTL.get();
        vtCmp.init(this);
        tmpList.sort(vtCmp);
        IntVector sortedVertexTimeIndices = _store._sortedVertexTimeIndices;
        sortedVertexTimeIndices.setValueCount(n);
        for (int i=0; i<n; ++i) {
            sortedVertexTimeIndices.set(i, elements[i]);
        }

        _sorted = true;
    }


    /**
     * Test function to output the contents of this history file
     */
    private void dump() {
        IntVector indices = _store._sortedVertexTimeIndices;
        int n = _store._maxRow;
        long tdiff = 0;
        for (int i=0; i<n; ++i) {
            int row = indices.get(i);

            tdiff = _store._times.get(row); // - tdiff;

            System.out.println(i + ": " + row + ", v=" + _store._localIds.get(row) + ", t=" + tdiff);
        }
    }


    protected int getSnapshotRowForVertex(long targetVertexId, long targetTime) {
        if (_snapshotRO.getRowCount() != _store._maxRow) {
            _snapshotRO.setRowCount(_store._maxRow);
        }

        sortSnapshotTimes();

        VertexTimeWindowComparator wc = _timeVertexWindowComparatorTL.get();
        wc.init(targetVertexId, _store._localIds, _store._sortedVertexTimeIndices, _store._times, targetTime);
        int index = ArrowBinarySearch.binarySearch(_store._sortedVertexTimeIndices, wc, null, -1);

        return index;
    }


    /**
     * Comparator for Arrow Vectors that compares vertex-ids and times only
     */
    private static class VertexTimeWindowComparator extends VectorValueComparator<IntVector> {
        private long _targetVertexId;
        private long _targetTime;
        private BigIntVector _vertexIds;
        private IntVector _sortedIndices;
        private BigIntVector _times;

        /**
         * Initialises this comparator.
         *
         * @param targetVertexId the vertex we're interested in
         * @param vertexIds the vector of vertex-ids
         * @param sortedIndices the sorted-indices to search
         * @param times the unsorted times
         * @param targetTime the minimum time to search for (inclusive)
         */
        public void init(long targetVertexId, BigIntVector vertexIds, IntVector sortedIndices, BigIntVector times, long targetTime) {
            _targetVertexId = targetVertexId;
            _vertexIds = vertexIds;
            _sortedIndices = sortedIndices;
            _times = times;
            _targetTime = targetTime;
        }


        /**
         * Compares two rows by time only. Has to check for unset values.
         *
         * @param index1 1st index to check
         * @param index2 2nd index to check
         *
         * @return -1, 0, +1 depending on whether vertex/time in index1 < = > the vertex/time in index2
         */
        @Override
        public int compare(int index1, int index2) {
            boolean isNull2 = vector2.isNull(index2);

            if (!isNull2) {
                return this.compareNotNull(index1, index2);
            }

            return 1;
        }


        /**
         * Compares two rows by time only. Has to check for unset values.
         *
         * @param index1 1st index to check
         * @param index2 2nd index to check
         *
         * @return -1, 0, +1 depending on whether vertex/time in index1 < = > the vertex/time in index2
         */
        @Override
        public int compareNotNull(int index1, int index2) {
            int row = _sortedIndices.get(index2);

            long vertexId = _vertexIds.get(row);
            if (_targetVertexId != vertexId) {
                return _targetVertexId < vertexId ? -1 : 1;
            }

            long creationTime = _times.get(row);

            if (_targetTime==creationTime) {
                return 0;
            }
            return _targetTime < creationTime ? -1 : 1;
        }


        /**
         * @return a new instance of this comparator
         */
        @Override
        public VectorValueComparator<IntVector> createNew() {
            return new VertexTimeWindowComparator();
        }
    }
}