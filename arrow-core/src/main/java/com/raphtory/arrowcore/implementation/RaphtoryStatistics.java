package com.raphtory.arrowcore.implementation;

public class RaphtoryStatistics {
    private final RaphtoryArrowPartition _rap;
    private final VertexPartitionManager _vmgr;
    private final EdgePartitionManager _emgr;


    public RaphtoryStatistics(RaphtoryArrowPartition rap) {
        _rap = rap;
        _vmgr = rap.getVertexMgr();
        _emgr = rap.getEdgeMgr();
    }


    public long getNVertices() {
        int n = _vmgr.nPartitions();
        long nVertices = 0L;

        for (int i=0; i<n; ++i) {
            VertexPartition p = _vmgr.getPartition(i);
            nVertices += p.getVerticesCount();
        }

        return nVertices;
    }


    public long getNEdges() {
        int n = _emgr.nPartitions();
        long nEdges = 0L;

        for (int i=0; i<n; ++i) {
            EdgePartition p = _emgr.getPartition(i);
            nEdges += p.getEdgesCount();
        }

        return nEdges;
    }


    public long getMinHistoryTime() {
        int n = _vmgr.nPartitions();
        long minTime = Long.MAX_VALUE;

        for (int i=0; i<n; ++i) {
            VertexPartition p = _vmgr.getPartition(i);
            long t = p._history.getLowestTime();
            if (t < minTime) {
                minTime = t;
            }
        }

        return minTime;
    }


    public long getMaxHistoryTime() {
        int n = _vmgr.nPartitions();
        long maxTime = Long.MIN_VALUE;

        for (int i=0; i<n; ++i) {
            VertexPartition p = _vmgr.getPartition(i);
            long t = p._history.getHighestTime();
            if (t > maxTime) {
                maxTime = t;
            }
        }

        return maxTime;
    }


    public long getNHistoryPoints() {
        int n = _vmgr.nPartitions();
        long nTotal = 0;

        for (int i=0; i<n; ++i) {
            VertexPartition p = _vmgr.getPartition(i);
            nTotal += p._history.getNHistoryItems();
        }

        return nTotal;
    }
}