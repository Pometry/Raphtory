/* Copyright (C) Pometry Ltd - All Rights Reserved.
 *
 * This file is proprietary and confidential. Unauthorised
 * copying of this file, via any medium is strictly prohibited.
 *
 */

package com.raphtory.arrowcore.implementation;

import com.raphtory.arrowcore.implementation.VertexIterator.WindowedVertexEdgeIterator;
import com.raphtory.arrowcore.implementation.VertexIterator.WindowedVertexIterator;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntComparator;
import org.apache.arrow.algorithm.search.VectorRangeSearcher;
import org.apache.arrow.algorithm.sort.VectorValueComparator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

/**
 * VertexHistoryPartition manages the history data for a single vertex partition.
 */
/*
Fix it so that the snapshot row is added to the history table
That way, when we search via time, we have the snapshot as well
The value in the snapshot row needs to be recalculated if the hsitory
changes - as an out of order update may have occurred?
So, initially, just recalc it all whenever we save the file and a mod has
been performed.
*/

public class VertexHistoryPartition {
    private static final ThreadLocal<IntArrayList> _tmpListTL = ThreadLocal.withInitial(IntArrayList::new);

    private static final ThreadLocal<HistoryTimeComparatorIAL> _timeCmpTL = ThreadLocal.withInitial(HistoryTimeComparatorIAL::new);
    private static final ThreadLocal<HistoryVertexTimeComparatorIAL> _vertexTimeCmpTL = ThreadLocal.withInitial(HistoryVertexTimeComparatorIAL::new);

    private static final ThreadLocal<TimeWindowComparator> _timeWindowComparatorTL = ThreadLocal.withInitial(TimeWindowComparator::new);
    private static final ThreadLocal<VertexEdgeTimeWindowComparator> _vertexTimeEdgeWindowComparatorTL = ThreadLocal.withInitial(VertexEdgeTimeWindowComparator::new);
    private static final ThreadLocal<VertexTimeWindowComparator> _timeVertexWindowComparatorTL = ThreadLocal.withInitial(VertexTimeWindowComparator::new);

    /**
     * Comparator to sort all history records by time
     */
    private static class HistoryTimeComparatorIAL implements IntComparator {
        private VertexHistoryPartition _avhpm;
        private BigIntVector _times;


        /**
         * Initialises this instance
         *
         * @param avhpm the history partition to use
         */
        public void init(VertexHistoryPartition avhpm) {
            _avhpm = avhpm;
            _times = avhpm._history._times;
        }


        /**
         * Compares 2 rows of the history data by TIME only
         *
         * @param row1
         * @param row2
         *
         * @return -1, 0 or +1 depending on whether row1 is < = > than row 2
         */
        @Override
        public int compare(int row1, int row2) {
            long time1 = _times.get(row1);
            long time2 = _times.get(row2);

            int retval = Long.compare(time1, time2);
            return retval;
        }
    }


    /**
     * Comparator to sort all history records by vertex AND then time
     */
    private static class HistoryVertexTimeComparatorIAL implements IntComparator {
        private VertexHistoryPartition _avhpm;
        private BigIntVector _times;
        private BigIntVector _edgeIds;
        private IntVector _vertexRowIds;

        /**
         * Initialises this instance
         *
         * @param avhpm the history partition to use
         */
        public void init(VertexHistoryPartition avhpm) {
            _avhpm = avhpm;
            _times = avhpm._history._times;
            _edgeIds = avhpm._history._edgeIds;
            _vertexRowIds = avhpm._history._vertexRowIds;
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
            int vertex1 = _vertexRowIds.get(row1);
            int vertex2 = _vertexRowIds.get(row2);

            int retval = Integer.compare(vertex1, vertex2);
            if (retval!=0) {
                return retval;
            }

            long time1 = _times.get(row1);
            long time2 = _times.get(row2);

            retval = Long.compare(time1, time2);
            if (retval!=0) {
                return retval;
            }

            long edge1 = _edgeIds.get(row1);
            long edge2 = _edgeIds.get(row2);

            return Long.compare(edge1, edge2);
        }
    }



    private final int _partitionId;
    private final VertexPartition _avp;
    private final VertexPartitionManager _apm;
    //private final VertexSnapshotPartition _snapshot;

    private VertexHistoryStore _history;
    private VectorSchemaRoot _historyRO;
    private ArrowFileReader _historyReader;
    private boolean _modified = false;
    private boolean _sorted = false;


    /**
     * Instantiate a new vertex history partition
     *
     * @param partitionId the partition-id of this partition
     * @param avp the owning vertex-partition
     */
    public VertexHistoryPartition(int partitionId, VertexPartition avp) {
        _partitionId = partitionId;
        _avp = avp;
        _apm = avp._apm;

        //_snapshot = new VertexSnapshotPartition(partitionId, avp);
        _history = new VertexHistoryStore();
    }


    /**
     * Initialises this instance for an empty file
     */
    public void initialize() {
        _historyRO = VectorSchemaRoot.create(VertexHistoryStore.HISTORY_SCHEMA, _apm.getAllocator());
        _history.init(_historyRO);
        //_snapshot.initialize();
    }


    /**
     * Adds a history record to this history partition
     *
     * @param localRowId the row-id of the vertex in the vertex partition
     * @param time the time of this history point
     * @param active true if the vertex was active at that time, false otherwise
     * @aparm updated true if a property was updated
     * @param historyPtr the prev-history pointer to set
     * @param localEdgeId the associated edge-id or -1 if none
     * @param outgoing true if the associated edge is an outgoing edge, false otherwise
     *
     * @return the row in which this history record is stored
     */
    public int addHistory(int localRowId, long time, boolean active, boolean updated, int historyPtr, long localEdgeId, boolean outgoing) {
        _modified = true;
        _sorted = false;

        boolean inOrder;
        if (historyPtr!=-1) {
            long prevHistoryTime = _history._times.get(historyPtr);
            inOrder = time >= prevHistoryTime;
        }
        else {
            inOrder = true;
        }

        if (updated) {
            //_snapshot.takeSnapshot();
        }

        int snapshotRow;
        if (inOrder) {
            snapshotRow = takeSnapshot();
        }
        else {
            snapshotRow = generateSnapshot();
        }

        int historyRow = _history.addHistory(localRowId, time, active, updated, historyPtr, localEdgeId, outgoing, snapshotRow);
        return historyRow;
    }


    private int takeSnapshot() {
        return -1;
    }



    private int generateSnapshot() {
        return -1;
    }


    /**
     * Returns the history-row-id for the nth sorted item (sortedIndex) based on time only
     *
     * @param sortedIndex the nth sorted item to retrieve
     *
     * @return the history row index containing that item
     */
    public int getHistoryRowIdBySortedIndex(int sortedIndex) {
        int rowId = _history._sortedTimeIndices.get(sortedIndex);
        return rowId;
    }


    /**
     * Returns the history row-id for the nth sorted item (sortedIndex) based on vertex and time
     *
     * @param sortedIndex the nth sorted item to retrieve
     *
     * @return the history row index containing that item
     */
    public int getEdgeHistoryRowIdBySortedIndex(int sortedIndex) {
        int rowId = _history._sortedVertexTimeIndices.get(sortedIndex);
        return rowId;
    }


    /**
     * Returns whether or not the vertex is active for that history record
     *
     * @param rowId the history record in question
     *
     * @return true if the vertex is active, false otherwise
     */
    public boolean getIsAliveByHistoryRowId(int rowId) {
        return _history._states.get(rowId) != 0;
    }


    /**
     * Returns the modification time of a history record
     *
     * @param rowId the history record in question
     *
     * @return the modification time associated with this record
     */

    public long getModificationTimeByHistoryRowId(int rowId) {
        return _history._times.get(rowId);
    }


    /**
     * Returns the vertex-row-id stored at the specified history row
     *
     * @param rowId the history row id to inspect
     *
     * @return the vertex-row-id stored there
     */
    public int getVertexLocalRowIdByHistoryRowId(int rowId) {
        return _history._vertexRowIds.get(rowId);
    }


    /**
     * Closes this vertex history partition
     */
    public void close() {
        //System.out.println("CLOSING PARTITION: " + partitionId);
        clearReader();
    }


    /**
     * Saves this vertex history partition to disk, if it's been modified.
     * The data will be sorted as required.
     *
     * TODO: Exception handling if the write fails?
     */
    public void saveToFile() {
        try {
            if (!_sorted) {
                sortHistoryTimes();
            }

            if (_modified) {
                _historyRO.syncSchema();
                _historyRO.setRowCount(_history._maxRow);

                File outFile = _apm.getHistoryFile(_partitionId);
                ArrowFileWriter writer = new ArrowFileWriter(_historyRO, null, new FileOutputStream(outFile).getChannel());
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
     * Loads this vertex history partition from a disk file
     *
     * @return true if the file exists and was read, false otherwise
     */
    public boolean loadFromFile() {
        File inFile = _apm.getHistoryFile(_partitionId);
        if (!inFile.exists()) {
            return false;
        }

        try {
            clearReader();

            _historyReader = new ArrowFileReader(new FileInputStream(inFile).getChannel(), _apm.getAllocator(), _apm.getCompressionFactory());
            _historyReader.loadNextBatch();
            _historyRO = _historyReader.getVectorSchemaRoot();
            _historyRO.syncSchema();

            _history.init(_historyRO);

            _history._maxRow = _historyRO.getRowCount();

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
     * Closes the resources associated with the vertex history store.
     * Releases Arrow memory etc.
     */
    private void clearReader() {
        try {
            if (_history !=null) {
                _history.init(null);
            }

            if (_historyRO != null) {
                _historyRO.clear();
                _historyRO.close();
                _historyRO = null;
            }

            if (_historyReader != null) {
                _historyReader.close();
                _historyReader = null;
            }
        }
        catch (Exception e) {
            System.err.println("Exception: " + e);
            e.printStackTrace(System.err);
        }
    }


    /**
     * Returns the edge-id stored in the specified row
     *
     * @param rowId the row to inspect
     *
     * @return the edge-id
     */
    public long getEdgeIdByHistoryRowId(int rowId) {
        return _history._edgeIds.get(rowId);
    }


    /**
     * Determines if the edge stored in the specified row is an outgoing edge
     *
     * @param rowId the row to inspect
     *
     * @return true if the edge is an outgoing edge, false otherwise
     */
    public boolean getIsOutgoingByHistoryRowId(int rowId) {
        return _history._isOutgoings.get(rowId)!=0;
    }


    /**
     * Sorts the history records by time only and then
     * again by vertex and time.
     *<p>
     * The records are not actually sorted (ie. moved into sorted order),
     * instead, an index is created that points to the rows in the correct
     * sorted order. ie. an indirect sorted index is created.
     */
    protected void sortHistoryTimes() {
        int n = _history._maxRow;

        IntArrayList tmpList = _tmpListTL.get();
        tmpList.clear();
        tmpList.ensureCapacity(n);
        tmpList.size(n);
        int[] elements = tmpList.elements();

        // Sort by vertex-row-id and time
        for (int i=0; i<n; ++i) {
            elements[i] = i;
        }
        HistoryVertexTimeComparatorIAL vtCmp = _vertexTimeCmpTL.get();
        vtCmp.init(this);
        tmpList.sort(vtCmp);
        IntVector sortedVertexTimeIndices = _history._sortedVertexTimeIndices;
        sortedVertexTimeIndices.setValueCount(n);
        for (int i=0; i<n; ++i) {
            sortedVertexTimeIndices.set(i, elements[i]);
        }


        // Sort by time only
        for (int i=0; i<n; ++i) {
            elements[i] = i;
        }
        HistoryTimeComparatorIAL cmp = _timeCmpTL.get();
        cmp.init(this);
        tmpList.sort(cmp);
        IntVector sortedIndices = _history._sortedTimeIndices;
        sortedIndices.setValueCount(n);
        for (int i=0; i<n; ++i) {
            sortedIndices.set(i, elements[i]);
        }

        _sorted = true;
    }


    /**
     * Initialises a WindowedVertexIterator in order to iterate over
     * active vertices within a time window.
     *
     * @param state the iterator to initialise
     */
    protected void isAliveAtWithWindowVector(WindowedVertexIterator state) {
        if (_historyRO == null) {
            state._firstIndex = -1;
            state._lastIndex = -1;
            return;
        }

        if (_historyRO.getRowCount() != _history._maxRow) {
            _historyRO.setRowCount(_history._maxRow);
        }

        if (!_sorted) {
            sortHistoryTimes();
        }

        TimeWindowComparator wc = _timeWindowComparatorTL.get();
        wc.init(_history._sortedTimeIndices, _history._times, state._minTime, state._maxTime);
        int first = VectorRangeSearcher.getFirstMatch(_history._sortedTimeIndices, wc, null, 0);
        if (first<0) {
            state._firstIndex = -1;
            state._lastIndex = -1;
            return;
        }

        int last = VectorRangeSearcher.getLastMatch(_history._sortedTimeIndices, wc, null, 0);
        state._firstIndex = first;
        state._lastIndex = last;
    }


    /**
     * Test function to output the contents of this history file
     */
    private void dump() {
        IntVector indices = _history._sortedVertexTimeIndices;
        int n = _history._maxRow;
        long tdiff = 0;
        for (int i=0; i<n; ++i) {
            int row = indices.get(i);

            tdiff = _history._times.get(row); // - tdiff;

            System.out.println(i + ": " + row + ", v=" + _history._vertexRowIds.get(row) + ", e=" + _history._edgeIds.get(row) + ", t=" + tdiff);
        }
    }


    /**
     * Initialises a WindowedVertexEdgeIterator in order to iterate over
     * active vertices within a time window.
     *
     * @param state the iterator to initialise
     */
    protected void isAliveAtWithWindowVector(WindowedVertexEdgeIterator state) {
        if (_historyRO == null) {
            state._firstIndex = -1;
            state._lastIndex = -1;
            return;
        }

        if (_historyRO.getRowCount() != _history._maxRow) {
            _historyRO.setRowCount(_history._maxRow);
        }

        if (!_sorted) {
            sortHistoryTimes();
        }

        VertexEdgeTimeWindowComparator wc = _vertexTimeEdgeWindowComparatorTL.get();
        wc.init(state._vertexRowId, _history._vertexRowIds, _history._sortedVertexTimeIndices, _history._times, state._minTime, state._maxTime);
        int first = VectorRangeSearcher.getFirstMatch(_history._sortedVertexTimeIndices, wc, null, 0);
        if (first<0) {
            state._firstIndex = -1;
            state._lastIndex = -1;
        }
        else {
            int last = VectorRangeSearcher.getLastMatch(_history._sortedVertexTimeIndices, wc, null, 0);
            state._firstIndex = first;
            state._lastIndex = last;
        }
    }


    protected void findHistory(VertexHistoryIterator.WindowedVertexHistoryIterator state) {
        if (_historyRO == null) {
            state._firstIndex = -1;
            state._lastIndex = -1;
            return;
        }

        if (_historyRO.getRowCount() != _history._maxRow) {
            _historyRO.setRowCount(_history._maxRow);
        }

        if (!_sorted) {
            sortHistoryTimes();
        }

        if (state._vertexId==-1L) {
            TimeWindowComparator wc = _timeWindowComparatorTL.get();
            wc.init(_history._sortedTimeIndices, _history._times, state._minTime, state._maxTime);
            int first = VectorRangeSearcher.getFirstMatch(_history._sortedTimeIndices, wc, null, 0);
            if (first < 0) {
                state._firstIndex = -1;
                state._lastIndex = -1;
            }
            else {
                int last = VectorRangeSearcher.getLastMatch(_history._sortedTimeIndices, wc, null, 0);
                state._firstIndex = first;
                state._lastIndex = last;
            }
        }
        else {
            VertexEdgeTimeWindowComparator wc = _vertexTimeEdgeWindowComparatorTL.get();
            wc.init(_apm.getRowId(state._vertexId), _history._vertexRowIds, _history._sortedVertexTimeIndices, _history._times, state._minTime, state._maxTime);
            int first = VectorRangeSearcher.getFirstMatch(_history._sortedVertexTimeIndices, wc, null, 0);
            if (first<0) {
                state._firstIndex = -1;
                state._lastIndex = -1;
            }
            else {
                int last = VectorRangeSearcher.getLastMatch(_history._sortedVertexTimeIndices, wc, null, 0);
                state._firstIndex = first;
                state._lastIndex = last;
            }
        }
    }


    public long getLowestTime() {
        if (_history._maxRow==0) {
            return Long.MAX_VALUE;
        }

        if (_historyRO.getRowCount() != _history._maxRow) {
            _historyRO.setRowCount(_history._maxRow);
        }

        if (!_sorted) {
            sortHistoryTimes();
        }


        int lowestRow = _history._sortedTimeIndices.get(0);

        return _history._times.get(lowestRow);
    }


    public long getHighestTime() {
        if (_history._maxRow==0) {
            return Long.MIN_VALUE;
        }

        if (_historyRO.getRowCount() != _history._maxRow) {
            _historyRO.setRowCount(_history._maxRow);
        }

        if (!_sorted) {
            sortHistoryTimes();
        }

        int highestRow = _history._sortedTimeIndices.get(_history._maxRow-1);

        return _history._times.get(highestRow);
    }


    public long getNHistoryItems() {
        int n = _history._maxRow;
        return n+1;
    }


    /**
     * Comparator for Arrow Vectors that compares times only
     */
    private static class TimeWindowComparator extends VectorValueComparator<IntVector> {
        private long _minTime;
        private long _maxTime;
        private IntVector _sortedIndices;
        private BigIntVector _creationTimes;

        /**
         * Initialises this comparator.
         *
         * sortedIndices[0] contains the index of the history row having the lowest time field
         *
         * @param sortedIndices the sorted-indices to search
         * @param creationTimes the unsorted times
         * @param minTime the minimum time to search for (inclusive)
         * @param maxTime the maximum time to search for (inclusive)
         */
        public void init(IntVector sortedIndices, BigIntVector creationTimes, long minTime, long maxTime) {
            _sortedIndices = sortedIndices;
            _creationTimes = creationTimes;
            _minTime = minTime;
            _maxTime = maxTime;
        }


        /**
         * Compares two rows by time only. Has to check for unset values.
         *
         * @param index1 1st index to check
         * @param index2 2nd index to check
         *
         * @return -1, 0, +1 depending on whether time in index1 < = > the time in index2
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
         * Compares two rows by time only.
         *
         * @param index1 1st index to check
         * @param index2 2nd index to check
         *
         * @return -1, 0, +1 depending on whether time in index1 < = > the time in index2
         */
        @Override
        public int compareNotNull(int index1, int index2) {
            int row = _sortedIndices.get(index2);
            long creationTime = _creationTimes.get(row);

            //System.out.println("TIME at " + row + " : " + creationTime + ", inRange=" + (creationTime>=min && creationTime<=max));

            if (creationTime<_minTime) { return 1; }
            if (creationTime>_maxTime) { return -1; }

            return 0;
        }


        /**
         * @return a new instance of this comparator
         */
        @Override
        public VectorValueComparator<IntVector> createNew() {
            return new TimeWindowComparator();
        }
    }


    /**
     * Comparator for Arrow Vectors that compares vertex-ids and times only
     */
    private static class VertexEdgeTimeWindowComparator extends VectorValueComparator<IntVector> {
        private int _vertexRowId;
        private long _minTime;
        private long _maxTime;
        private IntVector _rowIds;
        private IntVector _sortedIndices;
        private BigIntVector _creationTimes;

        /**
         * Initialises this comparator.
         *
         * @param vertexRowId the vertex we're interested in
         * @param rowIds the vector of vertex-ids
         * @param sortedIndices the sorted-indices to search
         * @param creationTimes the unsorted times
         * @param minTime the minimum time to search for (inclusive)
         * @param maxTime the maximum time to search for (inclusive)
         */
        public void init(int vertexRowId, IntVector rowIds, IntVector sortedIndices, BigIntVector creationTimes, long minTime, long maxTime) {
            _vertexRowId = vertexRowId;
            _rowIds = rowIds;
            _sortedIndices = sortedIndices;
            _creationTimes = creationTimes;
            _minTime = minTime;
            _maxTime = maxTime;
        }


        /**
         * Compares two rows by vertex and time only. Has to check for unset values.
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

            int vertexRow = _rowIds.get(row);
            if (_vertexRowId!=vertexRow) {
                return _vertexRowId<vertexRow ? -1 : 1;
            }

            long creationTime = _creationTimes.get(row);

            if (creationTime<_minTime) { return 1; }
            if (creationTime>_maxTime) { return -1; }

            return 0;
        }


        /**
         * @return a new instance of this comparator
         */
        @Override
        public VectorValueComparator<IntVector> createNew() {
            return new VertexEdgeTimeWindowComparator();
        }
    }



    private static class VertexTimeWindowComparator extends VectorValueComparator<IntVector> {
        private int _vertexRowId;
        private IntVector _rowIds;
        private IntVector _sortedIndices;
        private BigIntVector _creationTimes;

        /**
         * Initialises this comparator.
         *
         * @param vertexRowId the vertex we're interested in
         * @param rowIds the vector of vertex-ids
         * @param sortedIndices the sorted-indices to search
         * @param creationTimes the unsorted times
         */
        public void init(int vertexRowId, IntVector rowIds, IntVector sortedIndices, BigIntVector creationTimes) {
            _vertexRowId = vertexRowId;
            _rowIds = rowIds;
            _sortedIndices = sortedIndices;
            _creationTimes = creationTimes;
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
            int row2 = _sortedIndices.get(index2);

            int vertexRow = _rowIds.get(row2);
            if (_vertexRowId!=vertexRow) {
                return _vertexRowId<vertexRow ? -1 : 1;
            }

            int row1 = _sortedIndices.get(index1);

            long creationTime1 = _creationTimes.get(row1);
            long creationTime2 = _creationTimes.get(row2);

            if (creationTime1>creationTime2) { return 1; }
            if (creationTime1<creationTime2) { return -1; }

            return 0;
        }


        /**
         * @return a new instance of this comparator
         */
        @Override
        public VectorValueComparator<IntVector> createNew() {
            return new VertexEdgeTimeWindowComparator();
        }
    }


    protected VertexHistoryStore getHistoryStore() { return _history; }


    public long getVertexMinHistoryTime(int vertexRowId) {
        if (_historyRO == null) {
            return Long.MIN_VALUE;
        }

        if (_historyRO.getRowCount() != _history._maxRow) {
            _historyRO.setRowCount(_history._maxRow);
        }

        if (!_sorted) {
            sortHistoryTimes();
        }

        VertexEdgeTimeWindowComparator wc = _vertexTimeEdgeWindowComparatorTL.get();
        wc.init(vertexRowId, _history._vertexRowIds, _history._sortedVertexTimeIndices, _history._times, Long.MIN_VALUE, Long.MAX_VALUE);
        int sortedRow = VectorRangeSearcher.getFirstMatch(_history._sortedVertexTimeIndices, wc, null, 0);
        if (sortedRow>=0) {
            int row = _history._sortedVertexTimeIndices.get(sortedRow);
            return _history._times.get(row);
        }

        return Long.MIN_VALUE;
    }


    public long getVertexMaxHistoryTime(int vertexRowId) {
        if (_historyRO == null) {
            return Long.MAX_VALUE;
        }

        if (_historyRO.getRowCount() != _history._maxRow) {
            _historyRO.setRowCount(_history._maxRow);
        }

        if (!_sorted) {
            sortHistoryTimes();
        }

        VertexEdgeTimeWindowComparator wc = _vertexTimeEdgeWindowComparatorTL.get();
        wc.init(vertexRowId, _history._vertexRowIds, _history._sortedVertexTimeIndices, _history._times, Long.MIN_VALUE, Long.MAX_VALUE);
        int sortedRow = VectorRangeSearcher.getLastMatch(_history._sortedVertexTimeIndices, wc, null, 0);
        if (sortedRow>=0) {
            int row = _history._sortedVertexTimeIndices.get(sortedRow);
            return _history._times.get(row);
        }

        return Long.MAX_VALUE;
    }
}