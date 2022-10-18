/* Copyright (C) Pometry Ltd - All Rights Reserved.
 *
 * This file is proprietary and confidential. Unauthorised
 * copying of this file, via any medium is strictly prohibited.
 *
 */

package com.raphtory.arrowcore.implementation;

public abstract class VertexHistoryIterator {
    private boolean _hasNext = false;
    private boolean _getNext = true;

    protected VertexPartitionManager _avpm;
    protected VertexPartition _vertexPartition;
    protected VertexHistoryStore _historyStore;
    protected int _partitionId = -1;
    protected long _vertexId;
    protected int _historyRow;


    protected void init(VertexPartitionManager avpm) {
        _avpm = avpm;
        _vertexPartition = null;
        _partitionId = -1;
        _vertexId = -1L;
        _historyStore = null;
        _historyRow = -1;
        _hasNext = false;
        _getNext = true;
    }


    protected void init(VertexPartitionManager avpm, long vertexId) {
        _avpm = avpm;
        _vertexId = vertexId;

        _partitionId = _avpm.getPartitionId(vertexId);
        _vertexPartition = _avpm.getPartition(_partitionId);
        _historyStore = _vertexPartition._history.getHistoryStore();
        _historyRow = -1;
        _hasNext = false;
        _getNext = true;
    }


    public final long next() {
        if (_getNext) {
            moveToNext();
        }

        _getNext = true;

        return _vertexId; // XXX ???
    }


    public final boolean hasNext() {
        if (_getNext) {
            _hasNext = moveToNext();
            _getNext = false;
        }

        return _hasNext;
    }


    protected abstract boolean moveToNext();

    public long getVertexId()         { return _vertexId; }
    public long getModificationTime() { return _historyStore._times.get(_historyRow); }
    public boolean wasActive()        { return _historyStore._states.get(_historyRow)!=0; }
    public boolean wasUpdated()       { return _historyStore._updates.get(_historyRow)!=0; }
    public long getEdgeId()           { return _historyStore._edgeIds.get(_historyRow); }
    public boolean isOutgoingEdge()   { return _historyStore._isOutgoings.get(_historyRow)!=0; }



    public static class WindowedVertexHistoryIterator extends VertexHistoryIterator {
        protected long _minTime;
        protected long _maxTime;

        protected int _firstIndex;
        protected int _lastIndex;
        protected int _index;
        protected boolean _firstTime = true;
        protected boolean _getNextPartition = false;
        protected boolean _vertexIdSupplied = false;

        private WindowedVertexHistoryIterator() {}


        protected WindowedVertexHistoryIterator(VertexPartitionManager avpm, long minTime, long maxTime) {
            init(avpm, minTime, maxTime);
        }


        protected WindowedVertexHistoryIterator(VertexPartitionManager avpm, long vertexId, long minTime, long maxTime) {
            init(avpm, vertexId, minTime, maxTime);
        }



        protected void init(VertexPartitionManager avpm, long minTime, long maxTime) {
            super.init(avpm);

            _minTime = minTime;
            _maxTime = maxTime;
            _firstTime = true;
            _index = -1;
            _getNextPartition = true;
            _vertexIdSupplied = false;
        }


        protected void init(VertexPartitionManager avpm, long vertexId, long minTime, long maxTime) {
            super.init(avpm, vertexId);

            _minTime = minTime;
            _maxTime = maxTime;
            _firstTime = true;

            _firstIndex = -1;
            _lastIndex = -1;
            _index = -1;
            _historyRow = -1;
            _getNextPartition = false;
            _vertexIdSupplied = true;
        }


        private long getNext() {
            for (;;) {
                if (_vertexPartition==null && _getNextPartition) {
                    _vertexPartition = _avpm.getPartition(++_partitionId);
                    _firstIndex = -1;
                    _lastIndex = -1;
                    _index = -1;
                    _vertexId = -1L;
                    _historyRow = -1;
                    _historyStore = null;
                }

                if (_vertexPartition == null) {
                    _vertexId = -1L;
                    _historyRow = -1;
                    _historyStore = null;
                    return -1L;
                }

                if (_lastIndex!=-1) {
                    if (_index >= _firstIndex) {
                        //System.out.println("RET: " + _index);
                        if (_vertexIdSupplied) {
                            _historyRow = _vertexPartition._history.getEdgeHistoryRowIdBySortedIndex(_index);
                        }
                        else {
                            _historyRow = _vertexPartition._history.getHistoryRowIdBySortedIndex(_index);
                        }
                        int vertexRowId = _vertexPartition._history.getVertexLocalRowIdByHistoryRowId(_historyRow);
                        return _vertexPartition._getLocalVertexIdByRow(vertexRowId);
                    }

                    _firstIndex = -1;
                    _lastIndex = -1;
                    _index = -1;
                    _vertexPartition = null;
                    _vertexId = -1L;
                    _historyRow = -1;
                    _historyStore = null;
                }
                else {
                    _vertexPartition._history.findHistory(this);
                    if (_lastIndex==-1) {
                        _vertexPartition = null;
                        _vertexId = -1L;
                        _historyRow = -1;
                        _historyStore = null;
                    }
                    else {
                        _index = _lastIndex;
                        _historyStore = _vertexPartition._history.getHistoryStore();
                    }
                    //System.out.println("WINDOW: " + _lastIndex + " -> " + _firstIndex);
                }
            }
        }


        /**
         * @return true if there is another matching vertex, false otherwise
         */
        @Override
        protected boolean moveToNext() {
            if (!_firstTime) {
                --_index;
            }
            else {
                _firstTime = false;
            }

            _vertexId = getNext();
            return _vertexId!=-1L;
        }
    }
}