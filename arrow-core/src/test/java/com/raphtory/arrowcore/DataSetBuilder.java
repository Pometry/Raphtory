/* Copyright (C) Pometry Ltd - All Rights Reserved.
 *
 * This file is proprietary and confidential. Unauthorised
 * copying of this file, via any medium is strictly prohibited.
 *
 */

package com.raphtory.arrowcore;

import com.raphtory.arrowcore.implementation.*;
import com.raphtory.arrowcore.model.Edge;
import com.raphtory.arrowcore.model.Vertex;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import static org.junit.Assert.*;

/**
 * Simple program to confirm that vertices and edges are
 * retrieved correctly.
 */
public class DataSetBuilder {
    private final int _nRaps;
    private final RaphtoryArrowPartition[] _raps;
    private final StringBuilder _tmp = new StringBuilder();

    private final int NAME_FIELD;


    public DataSetBuilder(int nRaphtoryPartitions) {
        _raps = new RaphtoryArrowPartition[nRaphtoryPartitions];
        _nRaps = nRaphtoryPartitions;

        for (int i=0; i<nRaphtoryPartitions; ++i) {
            RaphtoryArrowPartition.RaphtoryArrowPartitionConfig cfg = new RaphtoryArrowPartition.RaphtoryArrowPartitionConfig();
            cfg._propertySchema = new SimpleTestSchema();
            cfg._arrowDir = "/tmp/rap" + i;
            cfg._raphtoryPartitionId = i;
            cfg._nRaphtoryPartitions = nRaphtoryPartitions;
            cfg._nLocalEntityIdMaps = 1;
            cfg._localEntityIdMapSize = 8;
            cfg._vertexPartitionSize = 32;
            cfg._edgePartitionSize = 512;
            cfg._syncIDMap = true;

            _raps[i] = new RaphtoryArrowPartition(cfg);
        }

        NAME_FIELD = _raps[0].getVertexFieldId("name");
    }


    public void setData(long[][] data, boolean verify) {
        Long2ObjectOpenHashMap<ObjectArrayList<long[]>> srcVertexToEdgeMap = new Long2ObjectOpenHashMap<>();
        Long2ObjectOpenHashMap<ObjectArrayList<long[]>> dstVertexToEdgeMap = new Long2ObjectOpenHashMap<>();
        Int2ObjectOpenHashMap<LongOpenHashSet> verticesPerRAP = new Int2ObjectOpenHashMap<>();

        for (int i=0; i<data.length; ++i) {
            long[] item = data[i];

            addVertex(item[0], item[2]);
            addVertex(item[1], item[2]);
            addEdge(item[0], item[1], item[2]);

            if (verify) {
                addToRap(item[0], verticesPerRAP);
                addToRap(item[1], verticesPerRAP);

                ObjectArrayList<long[]> srcEdgeList = srcVertexToEdgeMap.get(item[0]);
                if (srcEdgeList==null) {
                    srcEdgeList = new ObjectArrayList<>();
                    srcVertexToEdgeMap.put(item[0], srcEdgeList);
                }
                srcEdgeList.add(item);

                ObjectArrayList<long[]> dstEdgeList = dstVertexToEdgeMap.get(item[1]);
                if (dstEdgeList==null) {
                    dstEdgeList = new ObjectArrayList<>();
                    dstVertexToEdgeMap.put(item[1], dstEdgeList);
                }
                dstEdgeList.add(item);
            }
        }


        if (verify) {
            // Check vertices exist...
            LongOpenHashSet globalVertexIds = new LongOpenHashSet();
            for (int i=0; i<data.length; ++i) {
                globalVertexIds.add(data[i][0]);
                globalVertexIds.add(data[i][1]);
            }

            // Check that the vertices and edges exist...
            checkAllVerticesExist(globalVertexIds);
            checkAllEdgesExist(data);

            // Check edges for each vertex are correct
            globalVertexIds.forEach(globalId -> checkEdgesPerVertex(globalId, reverse(srcVertexToEdgeMap.get(globalId)), reverse(dstVertexToEdgeMap.get(globalId))));

            // Check iterators
            checkNormalVertexIterator(verticesPerRAP);
            checkWindowedVertexIterator(data);
            checkWindowedEdgeIterator(data);

            checkWindowedEdgesFromWindowedVertexIterator(data);

            checkVertexHistory(data);
            //checkEdgeHistory(data);
        }
    }


    /*
     * Need to check the find matching edges thing...
     *
     * For edge-history prob best to dup the data and
     * when we receive an edge, find a matching data item
     * and remove it.
     * Obv need to map local/global vertex ids as appropriate.
     *
     * Code below is a mish-mash.
     */
    /*
    private void checkEdgeHistory(long[][] data) {
        // Currently we only test for edge history records per edge creation...
        // As the dataset-builder doesn't currently support property updates etc.
        Long2ObjectOpenHashMap<LongArrayList> edgeHistoryPoints = new Long2ObjectOpenHashMap<>();

        for (int i=0; i<data.length; ++i) {
            long[] edge = data[i];

            RaphtoryArrowPartition srcRap = getRap(edge[0]);
            long srcLocalId = srcRap.getLocalEntityIdStore().getLocalNodeId(edge[0]);

            RaphtoryArrowPartition dstRap = getRap(edge[1]);
            long dstId;
            if (dstRap==srcRap) {
                dstId = dstRap.getLocalEntityIdStore().getLocalNodeId(edge[1]);
            }
            else {
                dstId = edge[1];
            }

            // Find the edge...
            VertexIterator vi = srcRap.getNewAllVerticesIterator();
            vi.reset(srcLocalId);
            assertTrue(vi.hasNext());
            assertEquals(srcLocalId, vi.next());

            EdgeIterator mei = vi.findAllOutgoingEdges(dstId, dstRap!=srcRap);
            assertNotNull(mei);
            assertTrue(mei.hasNext());
            long edgeId = mei.next();
            assertEquals(edge[2], mei.getCreationTime());

            LongArrayList history = edgeHistoryPoints.get(edge[0]);
            if (history==null) {
                history = new LongArrayList();
                vertexHistoryPoints.put(edge[0], history);
                history.add(edge[2]); // Vertex creation
            }
            history.add(edge[2]); // Vertex modification (edge added)

            history = vertexHistoryPoints.get(edge[1]);
            if (history==null) {
                history = new LongArrayList();
                vertexHistoryPoints.put(edge[1], history);
                history.add(edge[2]); // Vertex creation
            }
            history.add(edge[2]); // Vertex modification (edge added)
        }
    }
    */


    private void checkVertexHistory(long[][] data) {
        // Map of history points for each vertex
        Long2ObjectOpenHashMap<LongArrayList> vertexHistoryPoints = new Long2ObjectOpenHashMap<>();

        for (int i=0; i<data.length; ++i) {
            long[] edge = data[i];
            LongArrayList history = vertexHistoryPoints.get(edge[0]);
            if (history==null) {
                history = new LongArrayList();
                vertexHistoryPoints.put(edge[0], history);
                history.add(edge[2]); // Vertex creation
            }
            history.add(edge[2]); // Vertex modification (edge added)

            history = vertexHistoryPoints.get(edge[1]);
            if (history==null) {
                history = new LongArrayList();
                vertexHistoryPoints.put(edge[1], history);
                history.add(edge[2]); // Vertex creation
            }
            history.add(edge[2]); // Vertex modification (edge added)
        }


        for (int rap=0; rap<_raps.length; ++rap) {
            VertexHistoryIterator vhi = _raps[rap].getNewVertexHistoryIterator();
            while (vhi.hasNext()) {
                long vId = vhi.next();
                long globalId = _raps[rap].getVertexMgr().getGlobalVertexId(vId);
                LongArrayList history = vertexHistoryPoints.get(globalId);

                assertNotNull("Extra history record found?", history);
                assertTrue("Extra history record found? " + globalId, history.size()>0);

                long time = vhi.getModificationTime();
                boolean found = false;
                for (int i=0; i<history.size(); ++i) {
                    if (history.getLong(i)==time) {
                        history.removeLong(i);
                        found = true;
                        break;
                    }
                }

                assertTrue("Failed to match history record to data!", found);
            }
        }

        vertexHistoryPoints.values().forEach(hp -> assertTrue("Not enough history records found!", hp.isEmpty()));
    }


    /**
     * Checks that windowed-edge iterators from a windowed-vertex iterator
     * returns the correct set of edges.
     */
    private void checkWindowedEdgesFromWindowedVertexIterator(long[][] data) {
        // Iterate over each time point
        long start = data[0][2];
        long end = data[data.length-1][2];

        for (long startTime=start; startTime<=end; ++startTime) {
            for (long endTime=end; endTime>=startTime; --endTime) {
                checkWindowedEdgesFromWindowedVertexIterator(startTime, endTime, data);
            }
        }
    }


    /**
     * Checks that windowed-edge iterators from a windowed-vertex iterator
     * returns the correct set of edges for INCOMING, OUTGOING and
     * ALL edge iterators over the specified time frame.
     */
    @SuppressWarnings("unchecked")
    private void checkWindowedEdgesFromWindowedVertexIterator(long start, long end, long[][]data) {
        ObjectArrayList<long[]>[] edgesPerRap = new ObjectArrayList[_raps.length];
        for (int i=0; i<_raps.length; ++i) {
            edgesPerRap[i] = new ObjectArrayList<>();
        }

        // List of unique edges per RAP
        for (long[] edge: data) {
            if (edge[2]>=start && edge[2]<=end) {
                long src = edge[0];
                long dst = edge[1];

                int srcRapId = getRapId(src);
                int dstRapId = getRapId(dst);

                edgesPerRap[srcRapId].add(edge); // Outgoing edge
                edgesPerRap[dstRapId].add(edge); // Incoming edge
            }
        }

        // Check that incoming and outgoing edges exist...
        for (int i=0; i<_raps.length; ++i) {
            VertexIterator.WindowedVertexIterator vi = _raps[i].getNewWindowedVertexIterator(start, end);
            while (vi.hasNext()) {
                long vertexId = vi.next();

                EdgeIterator ei = vi.getIncomingEdges();
                while (ei.hasNext()) {
                    long eId = ei.next();

                    long srcId = ei.isSrcVertexLocal() ? getGlobalVertexIdFromLocalId(_raps[i], ei.getSrcVertexId()) : ei.getSrcVertexId();
                    long dstId = ei.isDstVertexLocal() ? getGlobalVertexIdFromLocalId(_raps[i], ei.getDstVertexId()) : ei.getDstVertexId();

                    assertTrue("Couldn't find edge!", findAndRemoveEdge(edgesPerRap[i], srcId, dstId, start, end));
                }

                ei = vi.getOutgoingEdges();
                while (ei.hasNext()) {
                    long eId = ei.next();

                    long srcId = ei.isSrcVertexLocal() ? getGlobalVertexIdFromLocalId(_raps[i], ei.getSrcVertexId()) : ei.getSrcVertexId();
                    long dstId = ei.isDstVertexLocal() ? getGlobalVertexIdFromLocalId(_raps[i], ei.getDstVertexId()) : ei.getDstVertexId();

                    assertTrue("Couldn't find edge!", findAndRemoveEdge(edgesPerRap[i], srcId, dstId, start, end));
                }
            }
            assertTrue("Unknown edges found!", edgesPerRap[i].isEmpty());
        }

        // Rebuild data - but for combined edges (getAllEdges)
        for (long[] edge: data) {
            if (edge[2]>=start && edge[2]<=end) {
                long src = edge[0];
                long dst = edge[1];

                int srcRapId = getRapId(src);
                int dstRapId = getRapId(dst);

                edgesPerRap[srcRapId].add(edge); // Outgoing edge

                // When we get all edges, we only get a self-edge ONCE
                if (src!=dst) {
                    edgesPerRap[dstRapId].add(edge); // Incoming edge
                }
            }
        }

        // Check that ALL edges exist...
        for (int i=0; i<_raps.length; ++i) {
            VertexIterator.WindowedVertexIterator vi = _raps[i].getNewWindowedVertexIterator(start, end);
            while (vi.hasNext()) {
                long vertexId = vi.next();

                EdgeIterator ei = vi.getAllEdges();
                while (ei.hasNext()) {
                    long eId = ei.next();

                    long srcId = ei.isSrcVertexLocal() ? getGlobalVertexIdFromLocalId(_raps[i], ei.getSrcVertexId()) : ei.getSrcVertexId();
                    long dstId = ei.isDstVertexLocal() ? getGlobalVertexIdFromLocalId(_raps[i], ei.getDstVertexId()) : ei.getDstVertexId();

                    assertTrue("Couldn't find edge! src=" + srcId + ", dst=" + dstId, findAndRemoveEdge(edgesPerRap[i], srcId, dstId, start, end));
                }
            }
            assertTrue("Unknown edges found!", edgesPerRap[i].isEmpty());
        }
    }


    /**
     * Check that a windowed-vertex iterator returns the correct vertices
     */
    private void checkWindowedVertexIterator(long[][] data) {
        // Iterate over each time point
        long start = data[0][2];
        long end = data[data.length-1][2];

        for (long startTime=start; startTime<=end; ++startTime) {
            for (long endTime=end; endTime>=startTime; --endTime) {
                checkWindowedVertexIterator(startTime, endTime, data);
            }
        }
    }


    /**
     * Checks that windowed-vertex iterator returns the correct
     * set of vertices for the specified time frame.
     */
    private void checkWindowedVertexIterator(long start, long end, long[][] data) {
        //System.out.println("TIME: " + start + " -> " + end);
        // List of unique vertices
        LongOpenHashSet vertices = new LongOpenHashSet();
        {
            int i = 0;
            int n = data.length;
            while (i<n) {
                if (data[i][2]>=start && data[i][2]<=end) {
                    vertices.add(data[i][0]);
                    vertices.add(data[i][1]);
                }

                ++i;
            }
        }

        // List of unique vertices per RAP
        LongOpenHashSet[] verticesPerRap = new LongOpenHashSet[_raps.length];
        for (int i=0; i<_raps.length; ++i) {
            verticesPerRap[i] = new LongOpenHashSet();
        }

        for (long v: vertices) {
            int rapId = getRapId(v);
            verticesPerRap[rapId].add(v);
        }

        // Check that each rap has the correct vertices...
        for (int i=0; i<_raps.length; ++i) {
            VertexIterator.WindowedVertexIterator vi = _raps[i].getNewWindowedVertexIterator(start, end);
            while (vi.hasNext()) {
                long lId = vi.next();
                long vId = vi.getGlobalVertexId();
                assertTrue("Map doesn't contain the vertex!", verticesPerRap[i].remove(vId));
            }
        }

        for (int i=0; i<_raps.length; ++i) {
            assertTrue(start + "->" + end + ": Didn't identify all vertices!: " + verticesPerRap[i], verticesPerRap[i].isEmpty());
        }
    }


    /**
     * Checks that windowed-edge iterator returns the correct
     * set of edges.
     */
    private void checkWindowedEdgeIterator(long[][] data) {
        // Iterate over each time point
        long start = data[0][2];
        long end = data[data.length-1][2];

        for (long startTime=start; startTime<=end; ++startTime) {
            for (long endTime=end; endTime>=startTime; --endTime) {
                checkWindowedEdgeIterator(startTime, endTime, data);
            }
        }
    }


    /**
     * Checks that windowed-edge iterator returns the correct
     * set of edges for the specified time frame.
     */
    @SuppressWarnings("unchecked")
    private void checkWindowedEdgeIterator(long start, long end, long[][] data) {
        // List of unique edges per RAP
        ObjectArrayList<long[]>[] edgesPerRap = new ObjectArrayList[_raps.length];
        for (int i=0; i<_raps.length; ++i) {
            edgesPerRap[i] = new ObjectArrayList<>();
        }

        for (long[] edge: data) {
            if (edge[2]>=start && edge[2]<=end) {
                long src = edge[0];
                long dst = edge[1];

                int srcRapId = getRapId(src);
                int dstRapId = getRapId(dst);

                edgesPerRap[srcRapId].add(edge);
                if (dstRapId != srcRapId) {
                    edgesPerRap[dstRapId].add(edge);
                }
            }
        }

        // Check that each rap has the correct edges...
        for (int i=0; i<_raps.length; ++i) {
            EdgeIterator.WindowedEdgeIterator ei = _raps[i].getNewWindowedEdgeIterator(start, end);
            while (ei.hasNext()) {
                long eId = ei.next();

                long srcId = ei.isSrcVertexLocal() ? getGlobalVertexIdFromLocalId(_raps[i], ei.getSrcVertexId()) : ei.getSrcVertexId();
                long dstId = ei.isDstVertexLocal() ? getGlobalVertexIdFromLocalId(_raps[i], ei.getDstVertexId()) : ei.getDstVertexId();

                assertTrue("Couldn't find edge!: src=" + srcId + ", dst=" + dstId, findAndRemoveEdge(edgesPerRap[i], srcId, dstId, start, end));
            }

            assertTrue("Unknown edges found!", edgesPerRap[i].isEmpty());
        }
    }


    /**
     * Finds and removes a matching edge from a list.
     */
    private boolean findAndRemoveEdge(ObjectArrayList<long[]> edges, long srcId, long dstId, long start, long end) {
        int n = edges.size();
        for (int i=0; i<n; ++i) {
            long[] edge = edges.get(i);

            if (edge[0]==srcId && edge[1]==dstId && edge[2]>=start && edge[2]<=end) {
                edges.remove(i);
                return true;
            }
        }

        return false;
    }


    /**
     * Returns a global-vertex-id from a local-vertex-id
     */
    private long getGlobalVertexIdFromLocalId(RaphtoryArrowPartition rap, long localId) {
        return rap.getVertexMgr().getGlobalVertexId(localId);
    }


    /**
     * Checks a normal (non-windowed) vertex iterator returns the correct set of vertices
     */
    private void checkNormalVertexIterator(Int2ObjectOpenHashMap<LongOpenHashSet> verticesPerRAP) {
        for (int rapId: verticesPerRAP.keySet()) {
            RaphtoryArrowPartition rap = _raps[rapId];
            LongOpenHashSet vertices = verticesPerRAP.get(rapId);
            if (vertices==null) {
                continue;
            }

            vertices = vertices.clone();

            VertexIterator vi = rap.getNewAllVerticesIterator();
            while (vi.hasNext()) {
                long localId = vi.next();
                assertNotSame("valid local vertex id", -1L, localId);

                long globalId = vi.getGlobalVertexId();
                assertNotSame("valid global vertex id", -1L, globalId);

                assertTrue("duplicate or unknown global vertex id found", vertices.remove(globalId));
            }

            assertEquals("not all of the vertices were found", 0, vertices.size());
        }
    }


    /**
     * Adds a global-id to a map of vertices per RAP
     */
    private void addToRap(long globalId, Int2ObjectOpenHashMap<LongOpenHashSet> verticesPerRAP) {
        int rap = getRapId(globalId);
        LongOpenHashSet set = verticesPerRAP.get(rap);
        if (set==null) {
            set = new LongOpenHashSet();
            verticesPerRAP.put(rap, set);
        }

        set.add(globalId);
    }


    /**
     * Reverses a list, used when we can/need to find things in a particular order
     */
    private <T> ObjectArrayList<T> reverse(ObjectArrayList<T> list) {
        if (list==null || list.size()==0) {
            return list;
        }

        int n = list.size();

        for (int i=0, last=n-1; i<n/2; ++i, --last) {
            T it = list.get(i);
            list.set(i, list.get(last));
            list.set(last, it);
        }

        return list;
    }


    /**
     * Checks that a vertex has the correct set of incoming and outgoing edges
     */
    public void checkEdgesPerVertex(long globalId, ObjectArrayList<long[]> outgoingEdges, ObjectArrayList<long[]> incomingEdges) {
        RaphtoryArrowPartition rap = getRap(globalId);
        int rapId = getRapId(globalId);
        long localId = rap.getLocalEntityIdStore().getLocalNodeId(globalId);

        VertexIterator vi = rap.getNewAllVerticesIterator();
        assertTrue(vi.reset(localId));
        assertEquals(localId, vi.next());

        if (outgoingEdges!=null) {
            int n = outgoingEdges.size();
            assertEquals("Outgoing edge count in vertex is wrong!", n, vi.getNOutgoingEdges());
            EdgeIterator ei = vi.getOutgoingEdges();
            assertNotNull(ei);

            int nFound = 0;
            while (ei.hasNext()) {
                assertNotSame(ei.next(), -1L);
                long dstGlobalId;
                if (ei.isDstVertexLocal()) {
                    dstGlobalId = rap.getVertexMgr().getVertex(ei.getDstVertexId()).getGlobalId();
                }
                else {
                    dstGlobalId = ei.getDstVertexId();
                }

                assertTrue("Found too many edges!", nFound < outgoingEdges.size());
                assertEquals("Dest vertices must match!", dstGlobalId, outgoingEdges.get(nFound)[1]);

                ++nFound;
            }
        }
        else {
            assertEquals("Outgoing edge count in vertex is wrong!", 0, vi.getNOutgoingEdges());
        }

        if (incomingEdges!=null) {
            int n = incomingEdges.size();
            assertEquals("Incoming edge count in vertex is wrong!", n, vi.getNIncomingEdges());
            EdgeIterator ei = vi.getIncomingEdges();
            assertNotNull(ei);

            int nFound = 0;
            while (ei.hasNext()) {
                assertNotSame(ei.next(), -1L);
                long dstGlobalId;
                if (ei.isDstVertexLocal()) {
                    dstGlobalId = rap.getVertexMgr().getVertex(ei.getDstVertexId()).getGlobalId();
                }
                else {
                    dstGlobalId = ei.getDstVertexId();
                }

                assertTrue("Found too many edges!", nFound < incomingEdges.size());
                assertEquals("Dest vertices must match!", dstGlobalId, incomingEdges.get(nFound)[1]);
            }
        }
        else {
            assertEquals("Incoming edge count in vertex is wrong!", 0, vi.getNIncomingEdges());
        }
    }


    /**
     * Checks that all the edges in the data-set exist
     */
    public void checkAllEdgesExist(long[][] data) {
        for (int i=0; i<data.length; ++i) {
            long srcGlobalId = data[i][0];
            long dstGlobalId = data[i][1];
            long time = data[i][2];

            int srcRapId = getRapId(srcGlobalId);
            int dstRapId = getRapId(dstGlobalId);

            if (srcRapId==dstRapId) {
                checkLocalEdgeExists(srcGlobalId, dstGlobalId, time, srcRapId);
            }
            else {
                checkSrcSplitEdgeExists(srcGlobalId, dstGlobalId, time, srcRapId);
                checkDstSplitEdgeExists(srcGlobalId, dstGlobalId, time, dstRapId);
            }
        }
    }


    /**
     * Checks that a split edge exists in the source RAP
     */
    public void checkSrcSplitEdgeExists(long srcGlobalId, long dstGlobalId, long time, int srcRapId) {
        RaphtoryArrowPartition rap = _raps[srcRapId];
        assertNotNull("RAP must be valid!", rap);

        long srcLocalId = rap.getLocalEntityIdStore().getLocalNodeId(srcGlobalId);
        assertNotSame("Didn't map the src-node-id", srcLocalId, -1L);

        VertexIterator vi = rap.getNewAllVerticesIterator();
        assertTrue("Iterator must find vertex!", vi.reset(srcLocalId));
        assertEquals("Iterator must find vertex!", srcLocalId, vi.next());

        EdgeIterator mei = vi.findAllOutgoingEdges(dstGlobalId, true);
        assertTrue("Must have an iterator!", mei.hasNext());

        int nFound = 0;
        do {
            long edgeId = mei.next();

            assertNotSame("Found edge can't be -1", edgeId, -1L);
            assertEquals("Found src must be same as that searched for!", mei.getSrcVertexId(), srcLocalId);
            assertEquals("Found dst must same as that searched for!", mei.getDstVertexId(), dstGlobalId);
            assertTrue("src must be local!", mei.isSrcVertexLocal());
            assertFalse("dst must be global!", mei.isDstVertexLocal());

            if (mei.getCreationTime()==time) {
                ++nFound;
            }
        }
        while (mei.hasNext());

        assertEquals("Didn't find the right number of edges!", 1, nFound);
    }


    /**
     * Checks that a split edge exists in the destination RAP
     */
    public void checkDstSplitEdgeExists(long srcGlobalId, long dstGlobalId, long time, int dstRapId) {
        RaphtoryArrowPartition rap = _raps[dstRapId];
        assertNotNull("Rap must be valid!", rap);

        long dstLocalId = rap.getLocalEntityIdStore().getLocalNodeId(dstGlobalId);
        assertNotSame("Didn't map the node-id", dstLocalId, -1L);

        VertexIterator vi = rap.getNewAllVerticesIterator();
        assertTrue("Iterator must find vertex!", vi.reset(dstLocalId));
        assertEquals("Iterator must find vertex!", dstLocalId, vi.next());

        EdgeIterator ei = vi.getIncomingEdges();
        assertTrue(ei.hasNext());

        int nFound = 0;
        do {
            long edgeId = ei.next();

            assertNotSame("Found edge can't be -1", edgeId, -1L);

            if (ei.getCreationTime()==time && !ei.isSrcVertexLocal() && ei.getSrcVertexId()==srcGlobalId) {
                assertEquals(ei.getSrcVertexId(), srcGlobalId);
                assertEquals(ei.getDstVertexId(), dstLocalId);
                assertFalse(ei.isSrcVertexLocal());
                assertTrue(ei.isDstVertexLocal());

                ++nFound;
            }
        }
        while (ei.hasNext());

        assertEquals("Didn't find the right number of edges!", 1, nFound);
    }


    /**
     * Checks that a local edge exists
     */
    public void checkLocalEdgeExists(long srcGlobalId, long dstGlobalId, long time, int rapId) {
        RaphtoryArrowPartition rap = _raps[rapId];
        assertNotNull(rap);

        long srcLocalId = rap.getLocalEntityIdStore().getLocalNodeId(srcGlobalId);
        assertNotSame(srcLocalId, -1L);

        long dstLocalId = rap.getLocalEntityIdStore().getLocalNodeId(dstGlobalId);
        assertNotSame(dstLocalId, -1L);

        VertexIterator vi = rap.getNewAllVerticesIterator();
        assertTrue(vi.reset(srcLocalId));
        assertEquals(srcLocalId, vi.next());

        EdgeIterator mei = vi.findAllOutgoingEdges(dstLocalId, false);
        assertTrue(mei.hasNext());

        int nFound = 0;
        do {
            long edgeId = mei.next();

            assertNotSame("edge-id can't be -1", edgeId, -1L);
            assertEquals("src must match that searched for!", mei.getSrcVertexId(), srcLocalId);
            assertEquals("dst must match that searched for!", mei.getDstVertexId(), dstLocalId);
            assertTrue("node must be local!", mei.isSrcVertexLocal());
            assertTrue("node must be local!", mei.isDstVertexLocal());

            if (mei.getCreationTime()==time) {
                ++nFound;
            }
        }
        while (mei.hasNext());

        assertEquals("Wrong number of edges found!", 1, nFound);
    }


    /**
     * Checks that all the vertices exist...
     */
    public void checkAllVerticesExist(LongOpenHashSet globalIds) {
        globalIds.forEach(globalId -> {
            RaphtoryArrowPartition rap = getRap(globalId);
            assertNotNull(rap);

            long localId = rap.getLocalEntityIdStore().getLocalNodeId(globalId);
            assertNotSame(localId, -1L);

            Vertex v = rap.getVertexMgr().getVertex(localId);
            assertNotNull(v);
            assertEquals(globalId, v.getGlobalId());

            // Add more asserts here?

            v.incRefCount();
            v.decRefCount();
        });
    }


    /**
     * Returns which rap this vertex should be in
     */
    public int getRapId(long globalId) {
        return (int) (globalId % _raps.length);
    }


    /**
     * Returns the RAP for a specific vertex
     */
    public RaphtoryArrowPartition getRap(long globalId) {
        return _raps[getRapId(globalId)];
    }


    /**
     * Adds a vertex to the appropriate RAP
     */
    public boolean addVertex(long globalId, long time) {
        RaphtoryArrowPartition rap = getRap(globalId);
        long localId = rap.getLocalEntityIdStore().getLocalNodeId(globalId);

        if (localId==-1L) {
            createVertex(rap, globalId, time);
            return true;
        }
        else {
            // It already exists...
            return false;
        }
    }


    public boolean updateVertex(long globalId, long time) {
        RaphtoryArrowPartition rap = getRap(globalId);
        long localId = rap.getLocalEntityIdStore().getLocalNodeId(globalId);

        if (localId!=-1L) {
            VertexPartition p = rap.getVertexMgr().getPartitionForVertex(localId);
            p.addHistory(localId, time, true, false, -1L, true);
            return true;
        }
        else {
            // It doesn't exist!
            return false;
        }
    }


    /**
     * Adds an edge to a RAP, handles split edges by adding the edge to 2 RAPs.
     */
    public void addEdge(long srcGlobalId, long dstGlobalId, long time) {
        int srcRapId = getRapId(srcGlobalId);
        int dstRapId = getRapId(dstGlobalId);

        if (srcRapId==dstRapId) {
            addLocalEdge(_raps[srcRapId], srcGlobalId, dstGlobalId, time);
        }
        else {
            addSpltEdgeOutgoing(srcGlobalId, dstGlobalId, time);
            addSplitEdgeIncoming(srcGlobalId, dstGlobalId, time);
        }
    }


    private Vertex retrieveVertex(RaphtoryArrowPartition rap, long globalId) {
        long localId = rap.getLocalEntityIdStore().getLocalNodeId(globalId);
        if (localId==-1L) {
            throw new RuntimeException("Invalid global id for this rap");
        }

        return rap.getVertexMgr().getVertex(localId);
    }


    /**
     * Returns the local-vertex-id from a global-id
     */
    public long getLocalId(long globalId) {
        int rap = getRapId(globalId);
        return _raps[rap].getLocalEntityIdStore().getLocalNodeId(globalId);
    }


    /**
     * Adds a local edge (ie. src and dst are both in the same RAP)
     */
    private void addLocalEdge(RaphtoryArrowPartition rap, long srcGlobalId, long dstGlobalId, long time) {
        long localSrcId = getLocalId(srcGlobalId);
        long localDstId = getLocalId(dstGlobalId);

        Edge e = rap.getEdge();
        e.incRefCount();

        e.init(rap.getEdgeMgr().getNextFreeEdgeId(), true, time);

        e.resetEdgeData(localSrcId, localDstId, false, false);
        rap.getEdgeMgr().addEdge(e, -1L, -1L);
        EdgePartition ep = rap.getEdgeMgr().getPartition(rap.getEdgeMgr().getPartitionId(e.getLocalId()));
        ep.addHistory(e.getLocalId(), time, true, true);

        VertexPartition p = rap.getVertexMgr().getPartitionForVertex(localSrcId);
        ep.setOutgoingEdgePtrByEdgeId(e.getLocalId(), p.addOutgoingEdgeToList(e.getSrcVertex(), e.getLocalId(), e.getDstVertex(), false));
        p.addHistory(localSrcId, time, true, false, e.getLocalId(), true);

        p = rap.getVertexMgr().getPartitionForVertex(localDstId);
        ep.setIncomingEdgePtrByEdgeId(e.getLocalId(), p.addIncomingEdgeToList(e.getDstVertex(), e.getLocalId(), e.getSrcVertex()));
        p.addHistory(localDstId, time, true, false, e.getLocalId(), false);

        e.decRefCount();
    }


    /**
     * Adds a split edge from the outgoing edge viewpoint
     */
    private void addSpltEdgeOutgoing(long globalSrcId, long globalDstId, long time) {
        // Adds a split edge from the viewpoint of the SOURCE
        // ie. SOURCE is local
        RaphtoryArrowPartition rap = getRap(globalSrcId);
        long localSrcId = getLocalId(globalSrcId);

        Edge e = rap.getEdge();
        e.incRefCount();
        e.init(rap.getEdgeMgr().getNextFreeEdgeId(), true, time);
        e.resetEdgeData(localSrcId, globalDstId,false, true);
        rap.getEdgeMgr().addEdge(e, -1L, -1L);
        EdgePartition ep = rap.getEdgeMgr().getPartition(rap.getEdgeMgr().getPartitionId(e.getLocalId()));

        ep.addHistory(e.getLocalId(), time, true, true);

        VertexPartition  p = rap.getVertexMgr().getPartitionForVertex(localSrcId);
        ep.setOutgoingEdgePtrByEdgeId(e.getLocalId(), p.addOutgoingEdgeToList(e.getSrcVertex(), e.getLocalId(), e.getDstVertex(), true));
        p.addHistory(localSrcId, time, true, false, e.getLocalId(), true);

        e.decRefCount();
    }


    /**
     * Adds a split edge from the incoming edge viewpoint
     */
    private void addSplitEdgeIncoming(long globalSrcId, long globalDstId, long time) {
        // Adds a split edge from the viewpoint of the DEST
        // ie. DEST is local
        RaphtoryArrowPartition rap = getRap(globalDstId);
        long localDstId = getLocalId(globalDstId);

        Edge e = rap.getEdge();
        e.incRefCount();

        e.init(rap.getEdgeMgr().getNextFreeEdgeId(), true, time);
        e.resetEdgeData(globalSrcId, localDstId, true, false);
        rap.getEdgeMgr().addEdge(e, -1L, -1L);
        EdgePartition ep = rap.getEdgeMgr().getPartition(rap.getEdgeMgr().getPartitionId(e.getLocalId()));

        ep.addHistory(e.getLocalId(), time, true, true);

        VertexPartition  p = rap.getVertexMgr().getPartitionForVertex(localDstId);
        ep.setIncomingEdgePtrByEdgeId(e.getLocalId(), p.addIncomingEdgeToList(e.getDstVertex(), e.getLocalId(), e.getSrcVertex()));
        p.addHistory(localDstId, time, true, false, e.getLocalId(), false);

        e.decRefCount();
    }


    /**
     * Creates a vertex in the specified rap
     */
    private void createVertex(RaphtoryArrowPartition rap, long globalId, long time) {
        _tmp.setLength(0);
        _tmp.append("V:");
        _tmp.append(globalId);

        long localId = rap.getVertexMgr().getNextFreeVertexId();

        Vertex v = rap.getVertex();
        v.reset(localId, globalId, true, time);
        v.getField(NAME_FIELD).set(_tmp);
        rap.getVertexMgr().addVertex(v);
        rap.getVertexMgr().addHistory(v.getLocalId(), time, true, false, -1, false);

        v.incRefCount();
        v.decRefCount();
    }


    private long getGlobalDstId(RaphtoryArrowPartition rap, EdgeIterator ei) {
        if (ei.isDstVertexLocal()) {
            return rap.getVertexMgr().getGlobalVertexId(ei.getDstVertexId());
        }
        else {
            return ei.getDstVertexId();
        }
    }


    private long getGlobalSrcId(RaphtoryArrowPartition rap, EdgeIterator ei) {
        if (ei.isSrcVertexLocal()) {
            return rap.getVertexMgr().getGlobalVertexId(ei.getSrcVertexId());
        }
        else {
            return ei.getSrcVertexId();
        }
    }
}
