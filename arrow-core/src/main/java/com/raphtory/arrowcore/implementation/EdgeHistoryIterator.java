/* Copyright (C) Pometry Ltd - All Rights Reserved.
 *
 * This file is proprietary and confidential. Unauthorised
 * copying of this file, via any medium is strictly prohibited.
 *
 */

package com.raphtory.arrowcore.implementation;

public abstract class EdgeHistoryIterator {
    private boolean _hasNext = false;
    private boolean _getNext = true;

    protected EdgePartitionManager _aepm;
    protected EdgePartition _edgePartition;
    protected EdgeHistoryStore _historyStore;
    protected int _partitionId = -1;
    protected long _edgeId;
    protected int _historyRow;


    protected void init(EdgePartitionManager aepm) {
        _aepm = aepm;
        _edgePartition = null;
        _partitionId = -1;
        _edgeId = -1L;
        _historyStore = null;
        _historyRow = -1;
        _hasNext = false;
        _getNext = true;
    }


    protected void init(EdgePartitionManager aepm, long edgeId) {
        _aepm = aepm;
        _edgeId = edgeId;

        _partitionId = _aepm.getPartitionId(edgeId);
        _edgePartition = _aepm.getPartition(_partitionId);
        _historyStore = _edgePartition._history.getHistoryStore();
        _historyRow = -1;
        _hasNext = false;
        _getNext = true;
    }


    public final long next() {
        if (_getNext) {
            moveToNext();
        }

        _getNext = true;

        return _edgeId; // XXX ???
    }


    public final boolean hasNext() {
        if (_getNext) {
            _hasNext = moveToNext();
            _getNext = false;
        }

        return _hasNext;
    }


    protected abstract boolean moveToNext();

    public long getEdgeId()           { return _edgeId; }

    public long getModificationTime() { return _historyStore._times.get(_historyRow); }
    public boolean wasActive()        { return _historyStore._states.get(_historyRow)!=0; }
    public boolean wasUpdated()       { return _historyStore._updates.get(_historyRow)!=0; }



    public static class WindowedEdgeHistoryIterator extends EdgeHistoryIterator {
        protected long _minTime;
        protected long _maxTime;

        protected int _firstIndex;
        protected int _lastIndex;
        protected int _index;
        protected boolean _firstTime = true;
        protected boolean _getNextPartition = false;
        protected boolean _vertexIdSupplied = false;

        private WindowedEdgeHistoryIterator() {}


        protected WindowedEdgeHistoryIterator(EdgePartitionManager aepm, long minTime, long maxTime) {
            init(aepm, minTime, maxTime);
        }


        protected WindowedEdgeHistoryIterator(EdgePartitionManager aepm, long vertexId, long minTime, long maxTime) {
            init(aepm, vertexId, minTime, maxTime);
        }



        protected void init(EdgePartitionManager aepm, long minTime, long maxTime) {
            super.init(aepm);

            _minTime = minTime;
            _maxTime = maxTime;
            _firstTime = true;
            _index = -1;
            _getNextPartition = true;
            _vertexIdSupplied = false;
        }


        protected void init(EdgePartitionManager aepm, long edgeId, long minTime, long maxTime) {
            super.init(aepm, edgeId);

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
                if (_edgePartition ==null && _getNextPartition) {
                    _edgePartition = _aepm.getPartition(++_partitionId);
                    _firstIndex = -1;
                    _lastIndex = -1;
                    _index = -1;
                    _edgeId = -1L;
                    _historyRow = -1;
                    _historyStore = null;
                }

                if (_edgePartition == null) {
                    _edgeId = -1L;
                    _historyRow = -1;
                    _historyStore = null;
                    return -1L;
                }

                if (_lastIndex!=-1) {
                    if (_index >= _firstIndex) {
                        //System.out.println("RET: " + _index);
                        if (_vertexIdSupplied) {
                            _historyRow = _edgePartition._history.getHistoryRowIdBySortedEdgeIndex(_index);
                        }
                        else {
                            _historyRow = _edgePartition._history.getHistoryRowIdBySortedIndex(_index);
                        }
                        int edgeRowId = _edgePartition._history.getEdgeLocalRowIdByHistoryRowId(_historyRow);
                        return _edgePartition._getLocalEdgeIdByRow(edgeRowId);
                    }

                    _firstIndex = -1;
                    _lastIndex = -1;
                    _index = -1;
                    _edgePartition = null;
                    _edgeId = -1L;
                    _historyRow = -1;
                    _historyStore = null;
                }
                else {
                    _edgePartition._history.findHistory(this);
                    if (_lastIndex==-1) {
                        _edgePartition = null;
                        _edgeId = -1L;
                        _historyRow = -1;
                        _historyStore = null;
                    }
                    else {
                        _index = _lastIndex;
                        _historyStore = _edgePartition._history.getHistoryStore();
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

            _edgeId = getNext();
            return _edgeId !=-1L;
        }
    }
}