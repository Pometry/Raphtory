/* Copyright (C) Pometry Ltd - All Rights Reserved.
 *
 * This file is proprietary and confidential. Unauthorised
 * copying of this file, via any medium is strictly prohibited.
 *
 */

package com.raphtory.googleplus;

import com.raphtory.arrowcore.implementation.*;
import com.raphtory.arrowcore.model.Edge;
import com.raphtory.arrowcore.model.Vertex;
import com.raphtory.bitcoin.BitcoinSchema;
import net.openhft.chronicle.core.util.StringUtils;

import java.io.*;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;

/**
 * Sample program to load Google+ data
 *
 * The original source data from https://snap.stanford.edu/data/ego-Gplus.html has
 * been pre-processed as follows:
 *
 * # Turn the followers files into an edge file:
 * for i in *.followers; do theuser=`basename $i .followers`; sed "s/$/ $theuser/" <$i >$i.edges; done
 *
 * # Collate all the edges into 1 file:
 * cat *.edges > alledges.txt
 *
 * # Sort and uniq them:
 * sort alledges.txt > sortedalledges.txt
 * uniq sortedalledges.txt > uniq-all-edges.txt
 *
 * This loader will then only load the uniq-all-edges.txt file.
 *
 * NB. ALl text files should be in UNIX format.
 */
public class GPLoader {
    // Thread-pool to process updates
    private static final int PARALLELISM_LEVEL = Runtime.getRuntime().availableProcessors();
    private static final RaphtoryThreadPool _arrowPool = new RaphtoryThreadPool(PARALLELISM_LEVEL, PARALLELISM_LEVEL);

    private static final int BUFFER_SIZE = 4 * 1024 * 1024; // File buffering
    private static final int BATCH_SIZE = 65536;            // Processing batch size

    // Some counts
    private static final AtomicLong _N_VERTICES = new AtomicLong(0);
    private static final AtomicLong _N_EDGES = new AtomicLong(0);

    private final RaphtoryArrowPartition _rap;


    private GPLoader(RaphtoryArrowPartition rap) {
        _rap = rap;
    }


    private boolean readLine(BufferedInputStream br, StringBuilder sb) throws IOException {
        sb.setLength(0);
        int c;
        while ((c = br.read()) != -1) {
            if (c == '\n') {
                return sb.length() > 0;
            }
            sb.append((char) c);
        }

        return sb.length() > 0;
    }


    private StringBuilder copy(StringBuilder src, int offset, int length, StringBuilder dst) {
        dst.setLength(0);

        for (int i=0; i<length; ++i) {
            dst.append(src.charAt(i + offset));
        }

        return dst;
    }


    // Create a new edge
    private Edge getNewEdge(long edgeId, long time) {
        Edge it = _rap.getEdge();
        it.init(edgeId, true, time);
        return it;
    }


    private void loadData(String filename) throws IOException {
        BufferedInputStream br;
        if (filename.endsWith(".csv.gz")) {
            br = new BufferedInputStream(new GZIPInputStream(new FileInputStream(filename), BUFFER_SIZE), BUFFER_SIZE);
        }
        else {
            br = new BufferedInputStream(new FileInputStream(filename), BUFFER_SIZE);
        }

        StringBuilder line = new StringBuilder(1024);
        int[] fields = new int[2];

        StringBuilder srcNode = new StringBuilder();
        StringBuilder dstNode = new StringBuilder();

        GlobalEntityIdStore gis = _rap.getGlobalEntityIdStore();
        LocalEntityIdStore lis = _rap.getLocalEntityIdStore();
        VertexPartitionManager avpm = _rap.getVertexMgr();
        EdgePartitionManager aepm = _rap.getEdgeMgr();

        int USERID_FIELD = _rap.getVertexFieldId("userid");
        int XXX_FIELD = _rap.getVertexFieldId("xxx");


        long lastNodeId = -1l;
        long lastEdgeId = -1L;

        int nRowsRead = 0;
        while (readLine(br, line)) {
            if ((++nRowsRead & 0x7ffff) == 0) {
                System.out.println(new Date() + ": Vertices: " + _N_VERTICES + ", Edges: " + _N_EDGES);
            }
            if (line.length()==0) {
                break;
            }

            // Generate the fields
            for (int i=0; i<fields.length; ++i) {
                if (i == 0) {
                    fields[i] = 0;
                }
                else {
                    int index = line.indexOf(" ", fields[i - 1]) + 1;
                    fields[i] = index;
                }
            }

            copy(line, fields[0], fields[1] - 1 - fields[0], srcNode);
            copy(line, fields[1], line.length() - fields[1], dstNode);

            long srcGlobalId = gis.getGlobalNodeId(srcNode);
            long dstGlobalId = gis.getGlobalNodeId(dstNode);
            boolean srcCreated = false;
            boolean dstCreated = false;

            long srcLocalId = lis.getLocalNodeId(srcGlobalId);
            if (srcLocalId == -1L) {
                srcLocalId = ++lastNodeId;
                lis.setMapping(srcGlobalId, srcLocalId);

                Vertex srcVertex = _rap.getVertex();

                srcVertex.incRefCount();
                srcVertex.reset(srcLocalId, srcGlobalId, true, 0L);
                srcVertex.getField(USERID_FIELD).set(srcNode);
                srcVertex.getField(XXX_FIELD).setEnum((int)(srcLocalId % 3));

                avpm.addVertex(srcVertex);
                srcVertex.decRefCount();

                srcVertex = _rap.getVertexMgr().getVertex(srcLocalId);
                System.out.println("Enum=" + srcVertex.getField(XXX_FIELD).getEnum());
                srcVertex.decRefCount();

                srcCreated = true;
                _N_VERTICES.incrementAndGet();
            }

            long dstLocalId = lis.getLocalNodeId(dstGlobalId);
            if (dstLocalId == -1L) {
                dstLocalId = ++lastNodeId;
                lis.setMapping(dstGlobalId, dstLocalId);

                Vertex dstVertex = _rap.getVertex();

                dstVertex.incRefCount();
                dstVertex.reset(dstLocalId, dstGlobalId, true, 0L);
                dstVertex.getField(USERID_FIELD).set(dstNode);
                dstVertex.getField(XXX_FIELD).setEnum((int)(dstLocalId % 3));

                avpm.addVertex(dstVertex);
                dstVertex.decRefCount();

                dstCreated = true;
                _N_VERTICES.incrementAndGet();

                dstVertex = _rap.getVertexMgr().getVertex(dstLocalId);
                System.out.println("Enum=" + dstVertex.getField(XXX_FIELD).getEnum() + " --> " + dstLocalId);
                dstVertex.decRefCount();
            }

            // Add the actual edge...
            long edgeId = ++lastEdgeId;

            Edge edge = getNewEdge(edgeId, 0);
            edge.incRefCount();
            edge.resetEdgeData(srcLocalId, dstLocalId, -1L, -1L, false, false);

            long prevIncomingListPtr = avpm.addIncomingEdgeToList(dstLocalId, edgeId);
            long prevOutgoingListPtr = avpm.addOutgoingEdgeToList(srcLocalId, edgeId, dstLocalId);

            aepm.addEdge(edge, prevIncomingListPtr, prevOutgoingListPtr);
            _N_EDGES.incrementAndGet();

            edge.decRefCount();
        }

        System.out.println(new Date() + ": Vertices: " + _N_VERTICES + ", Edges: " + _N_EDGES);
    }



    // Simple function to log the global and local node-ids for debug purposes
    private void dumpGlobalAndLocalNodeIds(String dst, String mode) throws IOException {
        LocalEntityIdStore entityIdStore = _rap.getLocalEntityIdStore();

        long then = System.currentTimeMillis();
        long nNodes = 0L;

        BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream(new File(dst, mode+"-NODE-IDS.txt")), 4*1024*1024);
        StringBuilder sb = new StringBuilder();

        VertexIterator.AllVerticesIterator iter = _rap.getNewAllVerticesIterator();
        while (iter.hasNext()) {
            long localNodeId = iter.next();
            long globalNodeId = iter.getGlobalVertexId();
            if (entityIdStore.getLocalNodeId(globalNodeId) != localNodeId) {
                System.out.println("NODE MAP INCORRECT");
            }
            ++nNodes;

            sb.setLength(0);
            sb.append(globalNodeId).append(", ").append(localNodeId).append("\n");
            os.write(sb.toString().getBytes());
        }

        long now = System.currentTimeMillis();
        System.out.println(new Date() + ": DUMP IDs: " + nNodes + " nodes in " + (now-then) + "ms");
        os.close();
    }


    public static void main(String[] args) throws Exception {
        if (args == null || args.length < 1) {
            System.err.println("Usage: GPLoader GP_SRC_FILE ARROW_DST_DIR");
            System.err.println("Note, the dst dir should be empty - and no files will be created!");
            System.exit(1);
        }

        String src = args[0];
        String dst = args[1];

        RaphtoryArrowPartition.RaphtoryArrowPartitionConfig cfg = new RaphtoryArrowPartition.RaphtoryArrowPartitionConfig();
        cfg._propertySchema = new GPSchema();
        cfg._arrowDir = dst;
        cfg._raphtoryPartitionId = 0;
        cfg._nRaphtoryPartitions = 1;
        cfg._nLocalEntityIdMaps = 128;
        cfg._vertexPartitionSize = 4096;
        cfg._edgePartitionSize = 512 * 1024;
        cfg._syncIDMap = true;

        RaphtoryArrowPartition rap = new RaphtoryArrowPartition(cfg);

        GPLoader loader = new GPLoader(rap);

        System.out.println(new Date() + ": Google+ LOADING: STARTING");

        File testFile = new File(dst, "vertex-p0.rap");
        if (!testFile.exists()) {
            loader.loadData(src);
            System.out.println(new Date() + ": Google+ LOADING: FINISHED");
        }
        else {
            rap.getVertexMgr().loadFiles();
            System.out.println(new Date() + ": FINISHED LOADING VERTICES");
            loader.dumpGlobalAndLocalNodeIds(dst, "READING");

            rap.getEdgeMgr().loadFiles();
            System.out.println(new Date() + ": FINISHED LOADING EDGES");
        }

        rap.close();

        System.exit(0);
    }
}