/* Copyright (C) Pometry Ltd - All Rights Reserved.
 *
 * This file is proprietary and confidential. Unauthorised
 * copying of this file, via any medium is strictly prohibited.
 *
 */

package com.raphtory.arrowcore.implementation;

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
 * Holds the history for edges for a single arrow edge file.
 *<p>
 * Maintains a sorted array of "events" ordered by time.
 */
public class EdgeHistoryPartition {
    private static final ThreadLocal<IntArrayList> _tmpListTL = ThreadLocal.withInitial(IntArrayList::new);

    private static final ThreadLocal<HistoryTimeComparatorIAL> _timeCmpTL = ThreadLocal.withInitial(HistoryTimeComparatorIAL::new);
    private static final ThreadLocal<HistoryEdgeTimeComparatorIAL> _edgeTimeCmpTL = ThreadLocal.withInitial(HistoryEdgeTimeComparatorIAL::new);

    private static final ThreadLocal<WindowComparator> _windowComparatorTL = ThreadLocal.withInitial(WindowComparator::new);
    private static final ThreadLocal<EdgeWindowComparator> _edgeWindowComparatorTL = ThreadLocal.withInitial(EdgeWindowComparator::new);

    private static final ThreadLocal<EdgeHistoryBoundedBinarySearch> _edgeBoundWindowComparatorTL = ThreadLocal.withInitial(EdgeHistoryBoundedBinarySearch::new);


    /**
     *  Comparator to sort all history records by time
     */
    private static class HistoryTimeComparatorIAL implements IntComparator {
        private EdgeHistoryPartition _aehpm;
        private BigIntVector _times;

        /**
         * Initialises this instance
         *
         * @param aehpm the history partition to use
         */
        public void init(EdgeHistoryPartition aehpm) {
            _aehpm = aehpm;
            _times = aehpm._history._times;
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
     * Comparator to sort all history records by edge AND then time
     * Currently unused...
     */
    private static class HistoryEdgeTimeComparatorIAL implements IntComparator {
        private EdgeHistoryPartition _aehpm;
        private BigIntVector _times;
        private IntVector _edgeRowIds;

        /**
         * Initialises this instance
         *
         * @param aehpm the edge history partition to use
         */
        public void init(EdgeHistoryPartition aehpm) {
            _aehpm = aehpm;
            _times = aehpm._history._times;
            _edgeRowIds = aehpm._history._edgeRowIds;
        }


        /**
         * Compares 2 rows of the history data by edge-id and time only
         *
         * @param row1
         * @param row2
         *
         * @return -1, 0 or +1 depending on whether row1 is < = > than row 2
         */
        @Override
        public int compare(int row1, int row2) {
            int edge1 = _edgeRowIds.get(row1);
            int edge2 = _edgeRowIds.get(row2);

            int retval = Integer.compare(edge1, edge2);
            if (retval!=0) {
                return retval;
            }

            long time1 = _times.get(row1);
            long time2 = _times.get(row2);

            retval = Long.compare(time1, time2);
            return retval;
        }
    }



    private final int _partitionId;
    private final EdgePartition _aep;
    private final EdgePartitionManager _aepm;
    protected final EdgeHistoryStore _history;

    private VectorSchemaRoot _historyRO;
    private ArrowFileReader _historyReader;
    private boolean _modified = false;
    protected boolean _sorted = false;


    /**
     * Instantiate a new edge history partition
     *
     * @param partitionId the partition-id of this partition
     * @param aep the owning edge-partition
     */
    public EdgeHistoryPartition(int partitionId, EdgePartition aep) {
        _partitionId = partitionId;
        _aep = aep;
        _aepm = aep._aepm;

        _history = new EdgeHistoryStore();
    }


    /**
     * Initialises this instance for an empty file
     */
    public void initialize() {
        _historyRO = VectorSchemaRoot.create(EdgeHistoryStore.HISTORY_SCHEMA, _aepm.getAllocator());
        _history.init(_historyRO);
    }


    /**
     * @return the Arrow partition-id for this instance
     */
    public int getPartitionId() {
        return _partitionId;
    }



    /**
     * Adds a history record to this history partition
     *
     * @param localRowId the row-id of the edge in the edge partition
     * @param time the time of this history point
     * @param active true if the edge was active at that time, false otherwise
     * @aparm updated true if a property was updated
     * @param historyPtr the prev-history pointer to set
     *
     * @return the row in which this history record is stored
     */
    public int addHistory(int localRowId, long time, boolean active, boolean updated, int historyPtr) {
        _modified = true;
        _sorted = false;
        int historyRow = _history.addHistory(localRowId, time, active, updated, historyPtr);
        return historyRow;
    }


    /**
     * Returns whether or not the edge at the specified history row
     * was active at that time
     *
     * @param rowId the history row-id in question
     *
     * @return true, if the edge was active at that time, false otherwise
     */
    protected boolean getIsAliveByRowId(int rowId) {
        return _history._states.get(rowId) != 0;
    }


    /**
     * Returns the row-id of the edge in the edge partition from the
     * specified row in this edge history partition
     *
     * @param rowId the row-id in question
     *
     * @return the edge-id at that row
     */
    protected int getEdgeLocalRowIdByHistoryRowId(int rowId) {
        return _history._edgeRowIds.get(rowId);
    }


    /**
     * Closes this edge history partition
     */
    public void close() {
        clearReader();
    }


    /**
     * Saves this edge history partition to disk, if it's been modified.
     * The data will be sorted as required.
     *<p>
     * TODO: Exception handling if the write fails?
     */
    public void saveToFile() {
        try {
            sortHistoryTimes();

            if (_modified) {
                _historyRO.syncSchema();
                _historyRO.setRowCount(_history._maxRow);

                File outFile = _aepm.getHistoryFile(_partitionId);
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
     * Loads this edge history partition from a disk file
     *
     * @return true if the file exists and was read, false otherwise
     */
    public boolean loadFromFile() {
        File inFile = _aepm.getHistoryFile(_partitionId);
        if (!inFile.exists()) {
            return false;
        }

        try {
            clearReader();

            _historyReader = new ArrowFileReader(new FileInputStream(inFile).getChannel(), _aepm.getAllocator(), _aepm.getCompressionFactory());
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
     * Closes the resources associated with the edge history store.
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
     * Sorts the history records by time only.
     *<p>
     * The capability to sort by edge and time has been disabled.
     *<p>
     * The records are not actually sorted (ie. moved into sorted order),
     * instead, an index is created that points to the rows in the correct
     * sorted order. ie. an indirect sorted index is created.
     */
    protected synchronized void sortHistoryTimes() {
        if (_sorted) {
            return;
        }

        if (_historyRO.getRowCount() != _history._maxRow) {
            _historyRO.setRowCount(_history._maxRow);
        }

        int n = _history._maxRow;

        IntArrayList tmpList = _tmpListTL.get();
        tmpList.clear();
        tmpList.ensureCapacity(n);
        tmpList.size(n);
        int[] elements = tmpList.elements();

        // Sort by edge-row-id and time
        for (int i=0; i<n; ++i) {
            elements[i] = i;
        }
        HistoryEdgeTimeComparatorIAL vtCmp = _edgeTimeCmpTL.get();
        vtCmp.init(this);
        tmpList.sort(vtCmp);
        IntVector sortedEdgeTimeIndices = _history._sortedEdgeTimeIndices;
        sortedEdgeTimeIndices.setValueCount(n);
        for (int i=0; i<n; ++i) {
            sortedEdgeTimeIndices.set(i, elements[i]);
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
     * Initialises a WindowedEdgeIterator in order to iterate over
     * active edges within a time window.
     *
     * @param state the iterator to initialise
     */
    protected void isAliveAtWithWindowVector(EdgeIterator.WindowedEdgeIterator state) {
        if (_historyRO == null) {
            state._firstIndex = -1;
            state._lastIndex = -1;
            return;
        }

        sortHistoryTimes();

        WindowComparator wc = _windowComparatorTL.get();
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
     * Comparator for Arrow Vectors that compares times only
     */
    private static class WindowComparator extends VectorValueComparator<IntVector> {
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
            return new WindowComparator();
        }
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
     * Comparator for Arrow Vectors that compares edge-ids and times only
     */
    private static class EdgeWindowComparator extends VectorValueComparator<IntVector> {
        private int _edgeRowId;
        private long _minTime;
        private long _maxTime;
        private IntVector _rowIds;
        private IntVector _sortedIndices;
        private BigIntVector _creationTimes;

        /**
         * Initialises this comparator.
         *
         * sortedIndices[0] contains the index of the history row having the lowest time field
         *
         * @param edgeRowId the edge we're interested in
         * @param rowIds the vector of edge-ids
         * @param sortedIndices the sorted-indices to search
         * @param creationTimes the unsorted times
         * @param minTime the minimum time to search for (inclusive)
         * @param maxTime the maximum time to search for (inclusive)
         */
        public void init(int edgeRowId, IntVector rowIds, IntVector sortedIndices, BigIntVector creationTimes, long minTime, long maxTime) {
            _edgeRowId = edgeRowId;
            _rowIds = rowIds;
            _sortedIndices = sortedIndices;
            _creationTimes = creationTimes;
            _minTime = minTime;
            _maxTime = maxTime;
        }


        /**
         * Compares two rows by edge and time only. Has to check for unset values.
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
         * Compares two rows by edge and time only.
         *
         * @param index1 1st index to check
         * @param index2 2nd index to check
         *
         * @return -1, 0, +1 depending on whether time in index1 < = > the time in index2
         */
        @Override
        public int compareNotNull(int index1, int index2) {
            int row = _sortedIndices.get(index2);

            int edgeRow = _rowIds.get(row);
            if (_edgeRowId!=edgeRow) {
                return _edgeRowId<edgeRow ? -1 : 1;
            }

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
            return new EdgeWindowComparator();
        }
    }





    /**
     * Returns the history-row-id for the nth sorted item (sortedIndex) based on edge-id and time only
     *
     * @param sortedIndex the nth sorted item to retrieve
     *
     * @return the history row index containing that item
     */
    public int getHistoryRowIdBySortedEdgeIndex(int sortedIndex) { // XXX, edgeId?
        int rowId = _history._sortedEdgeTimeIndices.get(sortedIndex);
        return rowId;
    }


    /**
     * @return the lowest history time in this partition
     */
    public long getLowestTime() {
        if (_history._maxRow==0) {
            return Long.MAX_VALUE;
        }

        sortHistoryTimes();

        int lowestRow = _history._sortedTimeIndices.get(0);

        return _history._times.get(lowestRow);
    }


    /**
     * @return the highest history time in this partition
     */
    public long getHighestTime() {
        if (_history._maxRow==0) {
            return Long.MIN_VALUE;
        }

        sortHistoryTimes();

        int highestRow = _history._sortedTimeIndices.get(_history._maxRow-1);

        return _history._times.get(highestRow);
    }


    /**
     * @return the number of history records in this partition
     */
    public long getNHistoryItems() {
        return _history._maxRow;
    }


    /**
     * @param edgeRowId  the edge-row in question
     *
     * @return the lowest history time for the specified edge
     *
     * TODO: Re-implement using the bounded window comparator
     */
    protected long getEdgeMinHistoryTime(int edgeRowId) {
        if (_historyRO == null) {
            return Long.MIN_VALUE;
        }

        sortHistoryTimes();

        EdgeWindowComparator wc = _edgeWindowComparatorTL.get();
        wc.init(edgeRowId, _history._edgeRowIds, _history._sortedEdgeTimeIndices, _history._times, Long.MIN_VALUE, Long.MAX_VALUE);
        int sortedRow = VectorRangeSearcher.getFirstMatch(_history._sortedEdgeTimeIndices, wc, null, 0);
        if (sortedRow>=0) {
            int row = _history._sortedEdgeTimeIndices.get(sortedRow);
            return _history._times.get(row);
        }

        return Long.MIN_VALUE;
    }


    /**
     * @param edgeRowId  the edge-row in question
     *
     * @return the highest history time for the specified edge
     *
     * TODO: Re-implement using the bounded window comparator
     */
    protected long getEdgeMaxHistoryTime(int edgeRowId) {
        if (_historyRO == null) {
            return Long.MAX_VALUE;
        }

        sortHistoryTimes();

        EdgeWindowComparator wc = _edgeWindowComparatorTL.get();
        wc.init(edgeRowId, _history._edgeRowIds, _history._sortedEdgeTimeIndices, _history._times, Long.MIN_VALUE, Long.MAX_VALUE);
        int sortedRow = VectorRangeSearcher.getLastMatch(_history._sortedEdgeTimeIndices, wc, null, 0);
        if (sortedRow>=0) {
            int row = _history._sortedEdgeTimeIndices.get(sortedRow);
            return _history._times.get(row);
        }

        return Long.MAX_VALUE;
    }


    /**
     * Initialises the windowed-edge-history iterator for the current search
     *
     * @param state the iterator state to initialise
     */
    protected void findHistory(EdgeHistoryIterator.WindowedEdgeHistoryIterator state) {
        if (_historyRO == null) {
            state._firstIndex = -1;
            state._lastIndex = -1;
            return;
        }

        sortHistoryTimes();

        if (state._edgeId==-1L) {
            WindowComparator wc = _windowComparatorTL.get();
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
            EdgeWindowComparator wc = _edgeWindowComparatorTL.get();
            wc.init(_aepm.getRowId(state._edgeId), _history._edgeRowIds, _history._sortedEdgeTimeIndices, _history._times, state._minTime, state._maxTime);
            int first = VectorRangeSearcher.getFirstMatch(_history._sortedEdgeTimeIndices, wc, null, 0);
            if (first<0) {
                state._firstIndex = -1;
                state._lastIndex = -1;
            }
            else {
                int last = VectorRangeSearcher.getLastMatch(_history._sortedEdgeTimeIndices, wc, null, 0);
                state._firstIndex = first;
                state._lastIndex = last;
            }
        }
    }


    /**
     * @return the history store for this partition
     */
    public EdgeHistoryStore getHistoryStore() {
        return _history;
    }


    /**
     * Returns whether or not this edge was alive by the end of this window
     *
     * @param edgeId the edge in question
     * @param start the start time of the window (inclusive)
     * @param end the end time of the window (inclusive)
     *
     * @return true iff the edge was alive, false otherwise
     */
    public boolean isAliveAt(long edgeId, long start, long end) {
        return isAliveAt(edgeId, start, end, _edgeBoundWindowComparatorTL.get());
    }


    /**
     * Returns whether or not this edge was alive by the end of this window
     *
     * @param edgeId the edge in question
     * @param start the start time of the window (inclusive)
     * @param end the end time of the window (inclusive)
     * @param searcher the binary searcher to use
     *
     * @return true iff the edge was alive, false otherwise
     */
    public boolean isAliveAt(long edgeId, long start, long end, EdgeHistoryBoundedBinarySearch searcher) {
        int targetRow = _aepm.getRowId(edgeId);
        int nRows = _history._maxRow;
        int low = _aep._store._sortedHStart.get(targetRow);
        int high = _aep._store._sortedHEnd.get(targetRow);

        searcher.init(targetRow, _history._edgeRowIds, _history._sortedEdgeTimeIndices, _history._times, end, low, high);

        int row = searcher.find();

        if (row < 0) {
            row = -row;
            row = row - 1;

            if (row>=nRows) {
                row = nRows-1;
            }

            if (row<low) {  // row<0
                return false;
            }

            int actualRow = _history._sortedEdgeTimeIndices.get(row);
            if ((_history._edgeRowIds.get(actualRow)!=targetRow || _history._times.get(actualRow)>end) && row>0) {
                // We found the next vertex, so go back 1
                --row;
                actualRow = _history._sortedEdgeTimeIndices.get(row);
            }
            else {
                // TODO: Find the last one with this timestamp?
                // Not sure this is correct as the "last" is defined
                // by the sort order...not insertion time etc.
                // In any case, there may be 2 rows with this
                // timestamp and inconsistent alive flags...
                // what happens there?
            }

            return actualRow<nRows &&
                    _history._states.get(actualRow)!=0 &&
                    _history._edgeRowIds.get(actualRow)==targetRow &&
                    _history._times.get(actualRow)>=start &&
                    _history._times.get(actualRow)<=end;
        }

        int actualRow = _history._sortedEdgeTimeIndices.get(row);
        return _history._states.get(actualRow)!=0;
    }


    /**
     * Bounded binary search class for edge searching
     */
    protected static class EdgeHistoryBoundedBinarySearch {
        private int _edgeRowId;
        private long _maxTime;
        private IntVector _rowIds;
        private IntVector _sortedIndices;
        private BigIntVector _creationTimes;
        private int _low;
        private int _high;

        /**
         * Initialises the searcher
         *
         * @param edgeRowId the edge row id to look for
         * @param rowIds the vector of row-ids
         * @param sortedIndices the sorted indices to use
         * @param creationTimes the time vector use
         * @param maxTime the time to look for
         * @param lowBound the lower bound of the sorted indices to search within
         * @param highBound the upper bound of the sorted indices to search within
         */
        public void init(int edgeRowId, IntVector rowIds, IntVector sortedIndices, BigIntVector creationTimes, long maxTime, int lowBound, int highBound) {
            init(edgeRowId, rowIds, sortedIndices, creationTimes, maxTime);
            setBounds(lowBound, highBound);
        }


        /**
         * Initialises the searcher
         *
         * @param edgeRowId the edge row id to look for
         * @param rowIds the vector of row-ids
         * @param sortedIndices the sorted indices to use
         * @param creationTimes the time vector use
         * @param maxTime the time to look for
         */
        public void init(int edgeRowId, IntVector rowIds, IntVector sortedIndices, BigIntVector creationTimes, long maxTime) {
            _edgeRowId = edgeRowId;
            _rowIds = rowIds;
            _sortedIndices = sortedIndices;
            _creationTimes = creationTimes;
            _maxTime = maxTime;

            _low = 0;
            _high = _sortedIndices.getValueCount() - 1;
        }


        /**
         * Sets the bound of the search
         *
         * @param low the lower bound of the sorted indices to search within
         * @param high the upper bound of the sorted indices to search within
         */
        public void setBounds(int low, int high) {
            _low = low;
            _high = high;
        }


        /**
         * Searches for the appropriate edge and time.
         *
         * @return -(n+1) if not found, +n if found, where n is the index of the row where the
         * item can/should be found
         */
        public int find() {
            int low = _low;
            int high = _high;

            while (low <= high) {
                int mid = low + (high - low) / 2;
                int cmp = compare(mid);
                if (cmp < 0) {          // mid > key
                    high = mid - 1;
                }
                else if (cmp > 0) {     // mid < key
                    low = mid + 1;
                }
                else {                  // mid == key
                    return mid;
                }
            }

            return -(low + 1);  // key not found
        }


        /**
         * Compares a history record with the searched for item
         *
         * @param sortedIndexRow the sorted index to row to look at
         *
         * @return -1,0,+1 according to whether the searched-for item is
         * less/equal/greater than the item stored at index.
         */
        private int compare(int sortedIndexRow) {
            int row = _sortedIndices.get(sortedIndexRow);

            int edgeRow = _rowIds.get(row);
            if (_edgeRowId != edgeRow) {
                return edgeRow<_edgeRowId ? 1 : -1;
            }

            long creationTime = _creationTimes.get(row);

            if (creationTime<_maxTime) { return 1; }
            if (creationTime>_maxTime) { return -1; }

            return 0;
        }
    }
}