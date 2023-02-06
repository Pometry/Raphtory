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
 * VertexEdgeIndexPartition manages the edge index data for a single vertex partition.
 * <p>
 *     The index essentially consists of rows of data (in the order they were added)
 *     and a vector which contains the row-ids in sorted order.
 *     <p>
 *     This means that after sorting and searching, the sorted-row-index needs to be
 *     translated to its unsorted row-index via a lookup in the sorted vector.
 */
public class VertexEdgeIndexPartition {
    private static final ThreadLocal<IntArrayList> _tmpListTL = ThreadLocal.withInitial(IntArrayList::new);

    private static final ThreadLocal<VertexDstEdgeComparator> _vertexDstEdgeComparatorTL = ThreadLocal.withInitial(VertexDstEdgeComparator::new);

    private static final ThreadLocal<SortedVertexDstEdgeComparator> _sortedVertexDstEdgeComparatorTL = ThreadLocal.withInitial(SortedVertexDstEdgeComparator::new);


    /**
     * Comparator to sort all history records by src-vertex-id, dst-vertex-id AND then edge-id
     */
    private static class VertexDstEdgeComparator implements IntComparator {
        private VertexEdgeIndexPartition _veip;
        private IntVector _vertexRowIds;
        private BigIntVector _dstVertexIds;
        private BigIntVector _edgeIds;


        /**
         * Initialises the comparator for the specified partition
         *
         * @param veip the VertexEdgeIndexPartition to use
         */
        public void init(VertexEdgeIndexPartition veip) {
            _veip = veip;
            _vertexRowIds = veip._index._vertexRowIds;
            _dstVertexIds = veip._index._dstVertexIds;
            _edgeIds = veip._index._edgeIds;
        }


        /**
         * Compares 2 vertex-edge-index rows
         *
         * @param row1 first row
         * @param row2 second row
         *
         * @return -1 if row1<row2, 0 if the same, +1 otherwise
         */
        @Override
        public int compare(int row1, int row2) {
            int vertex1 = _vertexRowIds.get(row1);
            int vertex2 = _vertexRowIds.get(row2);

            int retval = Integer.compare(vertex1, vertex2);
            if (retval!=0) {
                return retval;
            }

            long dst1 = _dstVertexIds.get(row1);
            long dst2 = _dstVertexIds.get(row2);

            retval = Long.compare(dst1, dst2);
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

    private VertexEdgeIndexArrowStore _index;
    private VectorSchemaRoot _indexRO;
    private ArrowFileReader _indexReader;
    private boolean _modified = false;
    private boolean _sorted = false;


    /**
     * Instantiate a new vertex edge index partition
     *
     * @param partitionId the partition-id of this partition
     * @param avp the owning vertex-partition
     */
    public VertexEdgeIndexPartition(int partitionId, VertexPartition avp) {
        _partitionId = partitionId;
        _avp = avp;
        _apm = avp._apm;

        _index = new VertexEdgeIndexArrowStore();
    }


    /**
     * Initialises this instance for an empty file
     */
    public void initialize() {
        _indexRO = VectorSchemaRoot.create(VertexEdgeIndexArrowStore.INDEX_SCHEMA, _apm.getAllocator());
        _index.init(_partitionId, _indexRO);
    }


    /**
     * Adds a new src-dst-edge index record
     *
     * @param localRowId the row-id of the src vertex (in the vertex partition)
     * @param dstVertexId the dst vertex id
     * @param edgeId the edge id of this edge
     * @param dstIsGlobal true if the dst-vertex is remote, false otherwise
     */
    public void addEdgeIndexRecord(int localRowId, long dstVertexId, long edgeId, boolean dstIsGlobal) {
        _modified = true;
        _sorted = false;

        _index.addIndexRecord(localRowId, dstVertexId, edgeId, dstIsGlobal);
    }


    /**
     * Returns the unsorted row id associated with the sorted
     * row-id from the sorted-index vector.
     *
     * @param sortedRowId the sorted row id in question
     *
     * @return the row-id where this record can be found
     * in the unsorted vector.
     */
    public int getVertexRowIdByIndexRowId(int sortedRowId) {
        return _index._sortedIndex.get(sortedRowId);
    }


    /**
     * Returns the edge-id associated with this row id
     *
     * @param rowId the row-id in question
     *
     * @return the edge id
     */
    public long getEdgeIdByRowId(int rowId) {
        return _index._edgeIds.get(rowId);
    }


    /**
     * Returns true if the destination vertex at this row is a remote edge
     *
     * @param rowId the row-id in question
     *
     * @return true, if the dst vertex is remote, false otherwise
     */
    public boolean getDstIsGlobalByRowId(int rowId) { return _index._isGlobals.get(rowId) != 0; }


    /**
     * Closes this index, releasing all resources.
     */
    public void close() {
        //System.out.println("CLOSING PARTITION: " + partitionId);
        clearReader();
    }


    /**
     * Saves the index to disk
     */
    public void saveToFile() {
        try {
            sortVertexEdgeIndex();

            if (_modified) {
                _indexRO.syncSchema();
                _indexRO.setRowCount(_index._maxRow);

                File outFile = _apm.getEdgeIndexFile(_partitionId);
                ArrowFileWriter writer = new ArrowFileWriter(_indexRO, null, new FileOutputStream(outFile).getChannel());
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
     * Loads the index from disk
     *
     * @return true if successful, false otherwise
     */
    public boolean loadFromFile() {
        File inFile = _apm.getEdgeIndexFile(_partitionId);
        if (!inFile.exists()) {
            return false;
        }

        try {
            clearReader();

            _indexReader = new ArrowFileReader(new FileInputStream(inFile).getChannel(), _apm.getAllocator(), _apm.getCompressionFactory());
            _indexReader.loadNextBatch();
            _indexRO = _indexReader.getVectorSchemaRoot();
            _indexRO.syncSchema();

            _index.init(_partitionId, _indexRO);

            _index._maxRow = _indexRO.getRowCount();

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
     * Releases resources associated with this index
     */
    private void clearReader() {
        try {
            if (_index !=null) {
                _index.init(-1, null);
            }

            if (_indexRO != null) {
                _indexRO.clear();
                _indexRO.close();
                _indexRO = null;
            }

            if (_indexReader != null) {
                _indexReader.close();
                _indexReader = null;
            }
        }
        catch (Exception e) {
            System.err.println("Exception: " + e);
            e.printStackTrace(System.err);
        }
    }


    /**
     * Sorts the index.
     */
    protected synchronized void sortVertexEdgeIndex() {
        if (_sorted) {
            return;
        }

        if (_indexRO.getRowCount() != _index._maxRow) {
            _indexRO.setRowCount(_index._maxRow);
        }

        int n = _index._maxRow;

        IntArrayList tmpList = _tmpListTL.get();
        tmpList.clear();
        tmpList.ensureCapacity(n);
        tmpList.size(n);
        int[] elements = tmpList.elements();

        // Sort by vertex-row-id, dstVertexId and edgeId
        for (int i=0; i<n; ++i) {
            elements[i] = i;
        }
        VertexDstEdgeComparator vtCmp = _vertexDstEdgeComparatorTL.get();
        vtCmp.init(this);
        tmpList.sort(vtCmp);
        IntVector sortedVertexTimeIndices = _index._sortedIndex;
        sortedVertexTimeIndices.setValueCount(n);
        for (int i=0; i<n; ++i) {
            sortedVertexTimeIndices.set(i, elements[i]);
        }

        _sorted = true;
    }


    /**
     * Initialises the edge iterator such that it's pointing to the
     * first matching edge for the specified src and dst vertices.
     *
     * @param state the edge iterator to initialise
     */
    protected void findMatchingEdges(EdgeIterator.MatchingEdgesIterator state) {
        if (_indexRO == null) {
            state._firstIndex = -1;
            state._lastIndex = -1;
            return;
        }

        sortVertexEdgeIndex();

        SortedVertexDstEdgeComparator wc = _sortedVertexDstEdgeComparatorTL.get();
        wc.init(state._vertexRowId, _index._vertexRowIds, _index._sortedIndex, _index._dstVertexIds, state._dstVertexId);
        int first = VectorRangeSearcher.getFirstMatch(_index._sortedIndex, wc, null, 0);
        if (first<0) {
            state._firstIndex = -1;
            state._lastIndex = -1;
        }
        else {
            int last = VectorRangeSearcher.getLastMatch(_index._sortedIndex, wc, null, 0);
            state._firstIndex = first;
            state._lastIndex = last;
        }
    }


    /**
     * Comparator used for searching the index to find matching edges.
     */
    protected static class SortedVertexDstEdgeComparator extends VectorValueComparator<IntVector> {
        private int _vertexRowId;
        private long _dstVertexId;

        private IntVector _vertexRowIds;
        private IntVector _sortedIndices;
        private BigIntVector _dstVertexIds;


        /**
         * Initialises this comparator.
         *
         * @param vertexRowId The src vertex-row-id to search for
         * @param rowIds the  vector of src vertex-row-ids
         * @param sortedIndices the vector of sorted row-ids
         * @param dstVertexIds NULL
         * @param dstVertexId the dst vertex-id to search for
         */
        public void init(int vertexRowId, IntVector rowIds, IntVector sortedIndices, BigIntVector dstVertexIds, long dstVertexId) {
            _vertexRowId = vertexRowId;
            _vertexRowIds = rowIds;
            _sortedIndices = sortedIndices;
            _dstVertexIds = dstVertexIds;
            _dstVertexId = dstVertexId;
        }


        /**
         * Compares two rows in the sorted index.
         * <p>
         * NB. index1 refers to the dst-vertex-row and is essentially ignored
         *
         * @param index1 ignored
         * @param index2 the sorted-row to compare with
         *
         * @return -1, 0, +1 as appropriate
         */
        @Override
        public int compare(int index1, int index2) {
            return this.compareNotNull(index1, index2);
        }


        /**
         * Compares two rows in the sorted index.
         * <p>
         * NB. index1 refers to the dst-vertex-row and is essentially ignored
         * as we only compare the data held in row [index2] against the
         * data this comparator was initialised with.
         * <p>
         * This comaparator only compares the src-vertex-id and the dst-vertex-id.
         *
         * @param index1 ignored
         * @param index2 the sorted-row to compare with
         *
         * @return -1, 0, +1 as appropriate
         */
        @Override
        public int compareNotNull(int index1, int index2) {
            int row = _sortedIndices.get(index2);

            int vertexRow = _vertexRowIds.get(row);
            if (_vertexRowId!=vertexRow) {
                return _vertexRowId<vertexRow ? -1 : 1;
            }

            long dstVertexId = _dstVertexIds.get(row);

            if (dstVertexId<_dstVertexId) { return 1; }
            if (dstVertexId>_dstVertexId) { return -1; }

            return 0;
        }


        /**
         * @return a new instance of this comparator
         */
        @Override
        public VectorValueComparator<IntVector> createNew() {
            return new SortedVertexDstEdgeComparator();
        }
    }
}