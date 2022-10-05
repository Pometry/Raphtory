/* Copyright (C) Pometry Ltd - All Rights Reserved.
 *
 * This file is proprietary and confidential. Unauthorised
 * copying of this file, via any medium is strictly prohibited.
 *
 */

package com.raphtory.bitcoin;

import com.raphtory.arrowcore.implementation.*;
import com.raphtory.arrowcore.model.Vertex;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.apache.commons.lang3.mutable.MutableLong;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


public class BitcoinDumpHistory {
    private final RaphtoryArrowPartition _rap;
    private final VertexPartitionManager _avpm;
    private final EdgePartitionManager _aepm;
    private final StringBuilder _tmpSB = new StringBuilder();
    private final MutableLong _minTime = new MutableLong(Long.MAX_VALUE);
    private final MutableLong _maxTime = new MutableLong(Long.MIN_VALUE);



    public static void main(String[] args) throws Exception {
        String src = args[0];

        RaphtoryArrowPartition.RaphtoryArrowPartitionConfig cfg = new RaphtoryArrowPartition.RaphtoryArrowPartitionConfig();
        cfg._propertySchema = new BitcoinSchema();
        cfg._arrowDir = src;
        cfg._raphtoryPartitionId = 0;
        cfg._nRaphtoryPartitions = 1;
        cfg._nLocalEntityIdMaps = 128;
        cfg._localEntityIdMapSize = 1 * 1024 * 1024;
        cfg._syncIDMap = true;

        RaphtoryArrowPartition rap = new RaphtoryArrowPartition(cfg);

        BitcoinDumpHistory it = new BitcoinDumpHistory(rap);
        it.findMinMaxTimes();
        //it.dumpAllVertexHistory();
        //it.dumpSomeVertices();
        //it.dumpHistoryPerVertex();
        it.dumpAllEdgeHistory();
        it.dumpHistoryPerEdge();

        System.exit(0);
    }


    public BitcoinDumpHistory(RaphtoryArrowPartition rap) {
        _rap = rap;
        _aepm = rap.getEdgeMgr();
        _avpm = rap.getVertexMgr();

        System.out.println(new Date() + ": STARTING");

        long then = System.currentTimeMillis();
        _avpm.loadFiles();
        _aepm.loadFiles();
        long now = System.currentTimeMillis();
        System.out.println(new Date() + ": BITCOIN: VERTEX ARROW LOADING TOOK: " + (now - then) + "ms");
    }


    public void dumpAllVertexHistory() throws Exception {
        System.out.println(new Date() + ": BITCOIN: VERTEX EDGE CHECK STARTING:");
        long then = System.currentTimeMillis();

        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(System.out), 16384);

        long nTotal = 0;
        StringBuilder sb = new StringBuilder();

        VertexHistoryIterator.WindowedVertexHistoryIterator vi = _rap.getNewVertexHistoryIterator();
        while (vi.hasNext()) {
            sb.setLength(0);

            long vertexId = vi.next();
            sb.append(vertexId);
            sb.append(", a=").append(vi.wasActive()).append(", t=").append(vi.getModificationTime());
            sb.append(", u=").append(vi.wasUpdated());
            sb.append(", e=").append(vi.getEdgeId()).append(", o=").append(vi.isOutgoingEdge());
            sb.append("\n");

            //out.append(sb);

            ++nTotal;
        }

        out.flush();
        long now = System.currentTimeMillis();
        System.out.println(new Date() + ": BITCOIN: VERTEX PRINTING TOOK: " + (now - then) + "ms, total=" + nTotal);
    }


    public void dumpSomeVertices() throws Exception {
        System.out.println(new Date() + ": BITCOIN: VERTEX EDGE CHECK STARTING:");

        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(System.out), 16384);

        StringBuilder sb = new StringBuilder();

        final long ONE_MONTH = 28L * 24L * 60L * 60L * 1000L;

        for (long time = _minTime.getValue(); time < _maxTime.getValue(); time += ONE_MONTH) {
            long then = System.currentTimeMillis();
            long nTotal = 0;

            VertexHistoryIterator.WindowedVertexHistoryIterator vi = _rap.getNewVertexHistoryIterator(time, time + ONE_MONTH);
            while (vi.hasNext()) {
                sb.setLength(0);

                long vertexId = vi.next();
                sb.append(vertexId);
                sb.append(", a=").append(vi.wasActive()).append(", t=").append(vi.getModificationTime());
                sb.append(", u=").append(vi.wasUpdated());
                sb.append(", e=").append(vi.getEdgeId()).append(", o=").append(vi.isOutgoingEdge());
                sb.append("\n");

                //out.append(sb);

                ++nTotal;
            }

            long now = System.currentTimeMillis();
            System.out.println("From: " + new Date(time) + " took " + (now-then) + "ms, count=" + nTotal);
        }

        out.flush();
    }


    private void findMinMaxTimes() {
        _minTime.setValue(_rap.getStatistics().getMinHistoryTime());
        _maxTime.setValue(_rap.getStatistics().getMaxHistoryTime());

        System.out.println(new Date() + ": minTime=" + _minTime.getValue() + " -> " + new Date(_minTime.getValue()));
        System.out.println(new Date() + ": maxTime=" + _maxTime.getValue() + " -> " + new Date(_maxTime.getValue()));
        System.out.println(new Date() + ": nVertices=" + _rap.getStatistics().getNVertices());
        System.out.println(new Date() + ": nEdges=" + _rap.getStatistics().getNEdges());
        System.out.println(new Date() + ": nHistory=" + _rap.getStatistics().getNHistoryPoints());
    }


    public void dumpHistoryPerVertex() throws Exception {
        System.out.println(new Date() + ": BITCOIN: VERTEX EDGE CHECK STARTING:");
        long then = System.currentTimeMillis();

        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(System.out), 16384);

        long nTotal = 0;
        StringBuilder sb = new StringBuilder();

        VertexIterator vi = _rap.getNewAllVerticesIterator();
        while (vi.hasNext()) {
            long vertexId = vi.next();

            VertexPartition p = vi.getPartition();
            long theMin = p.getVertexMinHistoryTime(vertexId);
            long theMax = p.getVertexMaxHistoryTime(vertexId);

            /*
            long foundMin = Long.MAX_VALUE;
            long foundMax = Long.MIN_VALUE;

            VertexHistoryIterator.WindowedVertexHistoryIterator wvhi = _rap.getNewVertexHistoryIterator(vertexId, Long.MIN_VALUE, Long.MAX_VALUE);
            while (wvhi.hasNext()) {
                if (wvhi.next() != vertexId) {
                    System.out.println("Wrong vertex!");
                    return;
                }

                long time = wvhi.getModificationTime();
                foundMax = Math.max(foundMax, time);
                foundMin = Math.min(foundMin, time);

                ++nTotal;
            }

            if (foundMax!=theMax) {
                if (!(foundMax==Long.MIN_VALUE && theMax==Long.MAX_VALUE)) {
                    System.out.println("v: " + vertexId + ", fmax=" + foundMax + ", amax=" + theMax);
                }
            }
            if (foundMin!=theMin) {
                if (!(foundMin==Long.MAX_VALUE && theMin==Long.MIN_VALUE)) {
                    System.out.println("v: " + vertexId + ", fmin=" + foundMin + ", amin=" + theMin);
                }
            }

             */
        }

        out.flush();
        long now = System.currentTimeMillis();
        System.out.println(new Date() + ": BITCOIN: VERTEX CHECKING TOOK: " + (now - then) + "ms, total=" + nTotal);
    }


    public void dumpAllEdgeHistory() throws Exception {
        System.out.println(new Date() + ": BITCOIN: EDGE HISTORY DUMP STARTING:");
        long then = System.currentTimeMillis();

        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(System.out), 16384);

        long nTotal = 0;
        StringBuilder sb = new StringBuilder();

        EdgeHistoryIterator.WindowedEdgeHistoryIterator ehi = _rap.getNewEdgeHistoryIterator(Long.MIN_VALUE, Long.MAX_VALUE);
        while (ehi.hasNext()) {
            sb.setLength(0);

            long edgeId = ehi.next();
            sb.append(edgeId);
            sb.append(", a=").append(ehi.wasActive()).append(", t=").append(ehi.getModificationTime());
            sb.append(", u=").append(ehi.wasUpdated());
            sb.append("\n");

            //out.append(sb);

            ++nTotal;
        }

        out.flush();
        long now = System.currentTimeMillis();
        System.out.println(new Date() + ": BITCOIN: EDGE PRINTING TOOK: " + (now - then) + "ms, total=" + nTotal);
    }


    public void dumpHistoryPerEdge() throws Exception {
        System.out.println(new Date() + ": BITCOIN: EDGE CHECK STARTING:");
        long then = System.currentTimeMillis();

        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(System.out), 16384);

        final int EPROPERTY_MAGIC = _rap.getEdgePropertyId("magic_edge");
        long nTotal = 0;
        StringBuilder sb = new StringBuilder();

        EdgeIterator ei = _rap.getNewAllEdgesIterator();
        while (ei.hasNext()) {
            long edgeId = ei.next();

            ArrowPropertyIterator api = ei.getPropertyHistory(EPROPERTY_MAGIC);
            while (api.hasNext()) {
                VersionedEntityPropertyAccessor vepa = api.next();
                //System.out.println(vepa.getLong());
            }

            EdgePartition p = ei.getPartition();
            long theMin = p.getEdgeMinHistoryTime(edgeId);
            long theMax = p.getEdgeMaxHistoryTime(edgeId);

            long foundMin = Long.MAX_VALUE;
            long foundMax = Long.MIN_VALUE;

            EdgeHistoryIterator.WindowedEdgeHistoryIterator wehi = _rap.getNewEdgeHistoryIterator(edgeId, Long.MIN_VALUE, Long.MAX_VALUE);
            while (wehi.hasNext()) {
                if (wehi.next() != edgeId) {
                    System.out.println("Wrong edge!");
                    return;
                }

                long time = wehi.getModificationTime();
                foundMax = Math.max(foundMax, time);
                foundMin = Math.min(foundMin, time);

                ++nTotal;
            }

            if (foundMax!=theMax) {
                if (!(foundMax==Long.MIN_VALUE && theMax==Long.MAX_VALUE)) {
                    System.out.println("e: " + edgeId + ", fmax=" + foundMax + ", amax=" + theMax);
                }
            }
            if (foundMin!=theMin) {
                if (!(foundMin==Long.MAX_VALUE && theMin==Long.MIN_VALUE)) {
                    System.out.println("e: " + edgeId + ", fmin=" + foundMin + ", amin=" + theMin);
                }
            }
        }

        out.flush();
        long now = System.currentTimeMillis();
        System.out.println(new Date() + ": BITCOIN: EDGE CHECKING TOOK: " + (now - then) + "ms, total=" + nTotal);
    }
}