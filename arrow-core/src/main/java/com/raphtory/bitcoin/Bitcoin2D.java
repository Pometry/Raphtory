/* Copyright (C) Pometry Ltd - All Rights Reserved.
 *
 * This file is proprietary and confidential. Unauthorised
 * copying of this file, via any medium is strictly prohibited.
 *
 */

package com.raphtory.bitcoin;

import com.raphtory.arrowcore.implementation.*;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Sample implementation of a 2nd-degree
 * neighbours algorithm.
 * Performs it on a set of 1000 random roots.
 */
public class Bitcoin2D {
    public static void main(String[] args) throws Exception {
        if (args==null || args.length<1) {
            System.err.println("Usage: Bitcoin2D ARROW_DST_DIR");
            System.err.println("Note, the dst dir should contain the edge and vertex arrow files!");
            System.exit(1);
        }

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

        long then = System.currentTimeMillis();
        System.out.println(new Date() + ": STARTING");

        rap.getEdgeMgr().loadFiles();
        rap.getVertexMgr().loadFiles();

        long now = System.currentTimeMillis();
        System.out.println(new Date() + ": BITCOIN: ARROW LOADING TOOK: " + (now-then) + "ms");


        int nPartitions = rap.getVertexMgr().nPartitions();
        long THE_MAX = (long)nPartitions * (long) rap.getVertexMgr().PARTITION_SIZE;

        ThreadLocalRandom random = ThreadLocalRandom.current();
        VertexIterator.AllVerticesIterator iter = rap.getNewAllVerticesIterator();
        LongOpenHashSet roots = new LongOpenHashSet();

        while (roots.size()<1000) {
            for (;;) {
                long id = random.nextLong(THE_MAX);
                iter.reset(id);
                if (!iter.isValid()) {
                    continue;
                }

                iter.reset(id);
                if (iter.getNOutgoingEdges()>=1) {
                    roots.add(id);
                    break;
                }
            }
        }
        System.out.println("GOT: " + roots.size() + " root nodes");

        // Single-threaded implementation
        then = System.currentTimeMillis();
        LongOpenHashSet nextSet = new LongOpenHashSet();
        LongOpenHashSet ids = new LongOpenHashSet(roots);

        // Get 1st degree neighbours
        final LongOpenHashSet ns1 = nextSet;
        ids.forEach(id -> {
            iter.reset(id);
            EdgeIterator ei = iter.getOutgoingEdges();
            while (ei.hasNext()) {
                ei.next();
                ns1.add(ei.getDstVertexId());
            }
        });
        now = System.currentTimeMillis();
        System.out.println("GOT: " + nextSet.size() + " 1st degree nodes in " + (now-then) + "ms");

        ids = nextSet;
        nextSet = new LongOpenHashSet();
        final LongOpenHashSet ns2 = nextSet;
        // Get 2nd degree neighbours
        ids.forEach(id -> {
            iter.reset(id);
            EdgeIterator ei = iter.getOutgoingEdges();
            while (ei.hasNext()) {
                ei.next();
                ns2.add(ei.getDstVertexId());
            }
        });
        now = System.currentTimeMillis();
        System.out.println("GOT: " + nextSet.size() + " 2nd degree nodes in " + (now-then) + "ms");


        // Multi-threaded implementation
        Bitcoin2D mgr = new Bitcoin2D(rap, roots);
        then = System.currentTimeMillis();
        int n = mgr.start();
        now = System.currentTimeMillis();
        System.out.println("MTGOT: " + n + " 2nd degree nodes in " + (now-then) + "ms");

        System.exit(0);
    }


    private final RaphtoryArrowPartition _rap;
    private final VertexPartitionManager _avpm;
    private final EdgePartitionManager _aepm;
    private LongOpenHashSet _roots;
    private LongOpenHashSet[] _output;


    public Bitcoin2D(RaphtoryArrowPartition rap, LongOpenHashSet roots) {
        _rap = rap;
        _avpm = _rap.getVertexMgr();
        _aepm = _rap.getEdgeMgr();
        _roots = roots;

    }


    public int start() {
        int nPartitions = _avpm.nPartitions();

        final LongOpenHashSet[] sets = new LongOpenHashSet[nPartitions];
        distribute(_roots, sets);
        ArrayList<Future<?>> tasks = new ArrayList<>();

        // Find 1st degree neighbours
        buildOutput(nPartitions);
        for (int i=0; i<sets.length; ++i) {
            if (sets[i]!=null && sets[i].size()>0) {
                final int ii = i;
                tasks.add(RaphtoryThreadPool.THREAD_POOL.submitTask(() -> step(sets[ii])));
            }
        }
        RaphtoryThreadPool.THREAD_POOL.waitTilComplete(tasks);
        tasks.clear();


        final LongOpenHashSet[] sets2 = _output;
        _output = null;

        // Find 2nd degree neighbours
        buildOutput(nPartitions);
        for (int i=0; i<sets.length; ++i) {
            if (sets2[i]!=null && sets2[i].size()>0) {
                final int ii = i;
                tasks.add(RaphtoryThreadPool.THREAD_POOL.submitTask(() -> step(sets2[ii])));
            }
        }
        RaphtoryThreadPool.THREAD_POOL.waitTilComplete(tasks);
        tasks.clear();

        // Calculate total number of 2nd degree neighbours
        int max = 0;
        for (int i=0; i<nPartitions; ++i) {
            if (_output[i]!=null) {
                max += _output[i].size();
            }
        }

        return max;
    }


    // Create maps to store the neighbours
    private void buildOutput(int nPartitions) {
        _output = new LongOpenHashSet[nPartitions];

        for (int i=0; i<nPartitions; ++i) {
            _output[i] = new LongOpenHashSet();
        }
    }


    // Perform a step - identifying and storing the neighbours
    private void step(LongOpenHashSet items) {
        final VertexIterator.AllVerticesIterator iter = _rap.getNewAllVerticesIterator();

        items.forEach(id -> {
            iter.reset(id);
            EdgeIterator ei = iter.getOutgoingEdges();
            while (ei.hasNext()) {
                ei.next();
                long neighbourId = ei.getDstVertexId();
                int partId = _avpm.getPartitionId(neighbourId);
                synchronized (_output[partId]) {
                    _output[partId].add(neighbourId);
                }
            }
        });
    }


    // Distributes the roots across the partitions they're in
    private void distribute(final LongOpenHashSet roots, final LongOpenHashSet[] sets) {
        roots.forEach(id -> {
            int partition = _avpm.getPartitionId(id);
            if (sets[partition]==null) {
                sets[partition] = new LongOpenHashSet();
            }
            sets[partition].add(id);
        });
    }
}