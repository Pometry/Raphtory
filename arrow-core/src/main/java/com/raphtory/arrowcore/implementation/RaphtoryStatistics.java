/* Copyright (C) Pometry Ltd - All Rights Reserved.
 *
 * This file is proprietary and confidential. Unauthorised
 * copying of this file, via any medium is strictly prohibited.
 *
 */
package com.raphtory.arrowcore.implementation;

/**
 * Encapsulates basic stats for the Raphtory arrow partitions
 * TODO The code that iterates over the partitions is wrong...doesn't handle out-of-order partIds.
 */
public class RaphtoryStatistics {
    private final RaphtoryArrowPartition _rap;
    private final VertexPartitionManager _vmgr;
    private final EdgePartitionManager _emgr;


    public RaphtoryStatistics(RaphtoryArrowPartition rap) {
        _rap = rap;
        _vmgr = rap.getVertexMgr();
        _emgr = rap.getEdgeMgr();
    }


    @Override
    public String toString() {
        return "nVertices: " + getNVertices() + ", nEdges: " + getNEdges() + ", nHistory: " + getNHistoryPoints();
    }


    /**
     * @return the total number of vertices
     */
    public long getNVertices() {
        int n = _vmgr.nPartitions();
        long nVertices = 0L;

        for (int i=0; i<n; ++i) {
            VertexPartition p = _vmgr.getPartition(i);
            nVertices += p.getVerticesCount();
        }

        return nVertices;
    }


    /**
     * @return the total number of edges
     */
    public long getNEdges() {
        int n = _emgr.nPartitions();
        long nEdges = 0L;

        for (int i=0; i<n; ++i) {
            EdgePartition p = _emgr.getPartition(i);
            nEdges += p.getEdgesCount();
        }

        return nEdges;
    }


    /**
     * @return the lowest vertex history time
     */
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


    /**
     * @return the highest vertex history time
     */
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


    /**
     * @return the total number of vertex history points
     */
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