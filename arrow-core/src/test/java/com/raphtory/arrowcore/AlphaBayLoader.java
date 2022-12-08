package com.raphtory.arrowcore;

import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;
import com.raphtory.arrowcore.implementation.*;
import com.raphtory.arrowcore.model.Edge;
import com.raphtory.arrowcore.model.PropertySchema;
import com.raphtory.arrowcore.model.Vertex;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.zip.GZIPInputStream;

public class AlphaBayLoader {
    public static class AlphaBaySchema implements PropertySchema {
        private static final ArrayList<NonversionedField> _nonVersionedVertexProperties;
        private static final ArrayList<VersionedProperty> _versionedVertexProperties;

        private static final ArrayList<NonversionedField> _nonVersionedEdgeProperties;
        private static final ArrayList<VersionedProperty> _versionedEdgeProperties;

        static {
            _nonVersionedVertexProperties = new ArrayList<>(Arrays.asList(
                    new NonversionedField("globalid", long.class)
            ));

            _nonVersionedEdgeProperties = null;

            _versionedVertexProperties = null;

            _versionedEdgeProperties = new ArrayList<>(Arrays.asList(
                    // Dummy field used for testing
                    new VersionedProperty("price", long.class)
            ));
        }


        @Override
        public ArrayList<NonversionedField> nonversionedVertexProperties() {
            return _nonVersionedVertexProperties;
        }


        @Override
        public ArrayList<VersionedProperty> versionedVertexProperties() {
            return _versionedVertexProperties;
        }


        @Override
        public ArrayList<NonversionedField> nonversionedEdgeProperties() {
            return _nonVersionedEdgeProperties;
        }


        @Override
        public ArrayList<VersionedProperty> versionedEdgeProperties() {
            return _versionedEdgeProperties;
        }
    }


    private final RaphtoryArrowPartition _rap;
    private final VertexPartitionManager _avpm;
    private final EdgePartitionManager _aepm;
    private final StringBuilder _tmpSB = new StringBuilder();
    private final int NODEID_FIELD;
    private final int PRICE_PROPERTY;
    private final VertexIterator _vertexIter;
    private final VersionedEntityPropertyAccessor _priceVEPA;

    public final static String RaphtoryInput = "/home/jatinder/projects/Pometry/arrow-core";
    public final static String ArrowDir = "/tmp/alphabay";


    //public final static String RaphtoryInput = "/pometry/wip";
    //public final static String ArrowDir = "/pometry/wip/data";


    public static void main(String[] args) throws Exception {
        RaphtoryArrowPartition.RaphtoryArrowPartitionConfig cfg = new RaphtoryArrowPartition.RaphtoryArrowPartitionConfig();
        cfg._propertySchema = new AlphaBaySchema();
        cfg._arrowDir = ArrowDir;
        cfg._raphtoryPartitionId = 0;
        cfg._nRaphtoryPartitions = 1;
        cfg._nLocalEntityIdMaps = 128;
        cfg._localEntityIdMapSize = 1024;
        cfg._syncIDMap = false;
        cfg._edgePartitionSize = 1024 * 1024;
        cfg._vertexPartitionSize = 256 * 1024;

        RaphtoryArrowPartition rap = new RaphtoryArrowPartition(cfg);

        AlphaBayLoader loader = new AlphaBayLoader(rap);
        if (new File(ArrowDir + "/vertex-p0.rap").exists()) {
            rap.getVertexMgr().loadFiles();
            rap.getEdgeMgr().loadFiles();
            System.out.println(rap.getStatistics());
        }
        else {
            //loader.load(RaphtoryInput + "/alphabay_sorted.csv.gz");
            loader.loadMT(RaphtoryInput + "/alphabay_sorted.csv.gz");

            rap.getVertexMgr().saveFiles();
            rap.getEdgeMgr().saveFiles();
        }

        for (int i=0; i<2; ++i) {
            System.out.println("\n\n\n");
            loader.degreeAlgoMT();
            loader.degreeAlgoTestHack();
            loader.degreeAlgoTest();
        }
    }


    public AlphaBayLoader(RaphtoryArrowPartition rap) {
        PRICE_PROPERTY = rap.getEdgePropertyId("price");
        NODEID_FIELD = rap.getVertexFieldId("globalid");

        _rap = rap;
        _aepm = rap.getEdgeMgr();
        _avpm = rap.getVertexMgr();

        _vertexIter = _rap.getNewAllVerticesIterator();

        _priceVEPA = _rap.getEdgePropertyAccessor(PRICE_PROPERTY);
    }


    public void degreeAlgo() throws Exception {
        System.out.println(new Date() + ": Degrees starting");
        VertexIterator vi = _rap.getNewWindowedVertexIterator(Long.MIN_VALUE, Long.MAX_VALUE);
        //VertexIterator vi = _rap.getNewAllVerticesIterator();
        int nVertices = 0;
        int nEdges = 0;
        Long2IntOpenHashMap inDegreeMap = new Long2IntOpenHashMap(16384);
        Long2IntOpenHashMap outDegreeMap = new Long2IntOpenHashMap(16384);
        Long2IntOpenHashMap degreeMap = new Long2IntOpenHashMap(16384);


        while (vi.hasNext()) {
            long vId = vi.next();
            ++nVertices;

            EdgeIterator ei;

            //inDegreeMap.put(vId, vi.getNIncomingEdges());
            //outDegreeMap.put(vId, vi.getNOutgoingEdges());

            int nIncoming = 0;
            ei = vi.getIncomingEdges();
            while (ei.hasNext()) {
                ei.next();
                ++nIncoming;
            }
            inDegreeMap.put(vId, nIncoming);

            int nOutgoing = 0;
            ei = vi.getOutgoingEdges();
            while (ei.hasNext()) {
                ei.next();
                ++nOutgoing;
            }
            outDegreeMap.put(vId, nOutgoing);

            /*
            int nTotal = 0;
            ei = vi.getAllEdges();
            while (ei.hasNext()) {
                ei.next();
                ++nTotal;
            }
            degreeMap.put(vId, nTotal);
            nEdges += nTotal;
            */
        }

        /*
        OutputStream output = new BufferedOutputStream(new FileOutputStream("degree_output.csv"), 65536);
        StringBuilder tmp = new StringBuilder();

        degreeMap.keySet().forEach(id -> {
            tmp.setLength(0);
            tmp.append("DUMMY,");
            tmp.append(id);
            tmp.append(",");
            tmp.append(inDegreeMap.get(id));
            tmp.append(",");
            tmp.append(outDegreeMap.get(id));
            tmp.append(",");
            tmp.append(degreeMap.get(id));
            tmp.append("\n");

            try {
                output.write(tmp.toString().getBytes());
            }
            catch (Exception e) {
                // NOP
            }
        });

        output.flush();
        output.close();
*/

        System.out.println(new Date() + ": Degrees ended");
        System.out.println("nVertices: " + nVertices + ", nEdges=" + nEdges);
    }



    public void degreeAlgoTest() throws Exception {
        //long time = 1325264638L;
        long time = Long.MAX_VALUE;

        System.out.println(new Date() + ": Degrees starting");
        VertexIterator vi = _rap.getNewWindowedVertexIterator(Long.MIN_VALUE, time);

        int nVertices = 0;
        int nEdges = 0;
        Long2IntOpenHashMap inDegreeMap = new Long2IntOpenHashMap(16384);
        Long2IntOpenHashMap outDegreeMap = new Long2IntOpenHashMap(16384);
        Long2IntOpenHashMap degreeMap = new Long2IntOpenHashMap(16384);

        long then = System.nanoTime();
        while (vi.hasNext()) {

            long vId = vi.next();

            //System.out.println("FOUND: " + vId);

            ++nVertices;

            EdgeIterator ei;

            int nIncoming = 0;
            int nOutgoing = 0;
            int nSelfEdges = 0;

            ei = vi.getIncomingEdges();
            while (ei.hasNext()) {
                long edgeId = ei.next();

                ++nIncoming;

                if (ei.getSrcVertexId()==ei.getDstVertexId() && ei.isSrcVertexLocal() && ei.isDstVertexLocal()) {
                    ++nSelfEdges;
                }

                //System.out.println("I: " + edgeId);
            }

            inDegreeMap.put(vId, nIncoming);
            nEdges += nIncoming;

            ei = vi.getOutgoingEdges();
            while (ei.hasNext()) {
                long edgeId = ei.next();

                ++nOutgoing;
                //System.out.println("O: " + edgeId);
            }
            outDegreeMap.put(vId, nOutgoing);
            nEdges += nOutgoing;

            if (true) {
                int nTotal = 0;
                ei = vi.getAllEdges();
                while (ei.hasNext()) {
                    long edgeId = ei.next();
                    ++nTotal;
                    //System.out.println("A: " + edgeId);
                }

                degreeMap.put(vId, nTotal);
                nEdges += nTotal;
            }
            else {
                int nTotal = nIncoming + nOutgoing - nSelfEdges;
                degreeMap.put(vId, nTotal);
                nEdges += nTotal;
            }
        }

        /*
        OutputStream output = new BufferedOutputStream(new FileOutputStream("degree_output.csv"), 65536);
        StringBuilder tmp = new StringBuilder();

        degreeMap.keySet().forEach(id -> {
            tmp.setLength(0);
            tmp.append("DUMMY,");
            tmp.append(id);
            tmp.append(",");
            tmp.append(inDegreeMap.get(id));
            tmp.append(",");
            tmp.append(outDegreeMap.get(id));
            tmp.append(",");
            tmp.append(degreeMap.get(id));
            tmp.append("\n");

            try {
                output.write(tmp.toString().getBytes());
            }
            catch (Exception e) {
                // NOP
            }
        });

        output.flush();
        output.close();
        */

        long now = System.nanoTime();
        System.out.println("Took: " + (now-then)/1000000.0d + "ms");
        System.out.println(new Date() + ": Degrees ended");
        System.out.println("nVertices: " + nVertices + ", nEdges=" + nEdges);
    }



    public void degreeAlgoTestHack() throws Exception {
        //long time = 1325264638L;
        long time = Long.MAX_VALUE;

        System.out.println(new Date() + ": Degrees starting");
        VertexIterator vi = _rap.getNewAllVerticesIterator();

        int nVertices = 0;
        int nEdges = 0;
        Long2IntOpenHashMap inDegreeMap = new Long2IntOpenHashMap(16384);
        Long2IntOpenHashMap outDegreeMap = new Long2IntOpenHashMap(16384);
        Long2IntOpenHashMap degreeMap = new Long2IntOpenHashMap(16384);

        long then = System.nanoTime();
        while (vi.hasNext()) {
            long vId = vi.next();
            if (!vi.isAliveAt(Long.MIN_VALUE, time)) { continue; }

            //System.out.println("FOUND: " + vId);

            ++nVertices;

            EdgeIterator ei;

            int nIncoming = 0;
            int nOutgoing = 0;
            int nSelfEdges = 0;

            ei = vi.getIncomingEdges();
            while (ei.hasNext()) {
                long edgeId = ei.next();
                if (!ei.isAliveAt(Long.MIN_VALUE, time)) { continue; }

                ++nIncoming;

                if (ei.getSrcVertexId()==ei.getDstVertexId() && ei.isSrcVertexLocal() && ei.isDstVertexLocal()) {
                    ++nSelfEdges;
                }

                //System.out.println("I: " + edgeId);
            }

            inDegreeMap.put(vId, nIncoming);
            nEdges += nIncoming;

            ei = vi.getOutgoingEdges();
            while (ei.hasNext()) {
                long edgeId = ei.next();
                if (!ei.isAliveAt(Long.MIN_VALUE, time)) { continue; }

                ++nOutgoing;
                //System.out.println("O: " + edgeId);
            }
            outDegreeMap.put(vId, nOutgoing);
            nEdges += nOutgoing;

            if (true) {
                int nTotal = 0;
                ei = vi.getAllEdges();
                while (ei.hasNext()) {
                    long edgeId = ei.next();
                    if (!ei.isAliveAt(Long.MIN_VALUE, time)) { continue; }
                    ++nTotal;
                    //System.out.println("A: " + edgeId);
                }

                degreeMap.put(vId, nTotal);
                nEdges += nTotal;
            }
            else {
                int nTotal = nIncoming + nOutgoing - nSelfEdges;
                degreeMap.put(vId, nTotal);
                nEdges += nTotal;
            }
        }

        /*
        OutputStream output = new BufferedOutputStream(new FileOutputStream("degree_output.csv"), 65536);
        StringBuilder tmp = new StringBuilder();

        degreeMap.keySet().forEach(id -> {
            tmp.setLength(0);
            tmp.append("DUMMY,");
            tmp.append(id);
            tmp.append(",");
            tmp.append(inDegreeMap.get(id));
            tmp.append(",");
            tmp.append(outDegreeMap.get(id));
            tmp.append(",");
            tmp.append(degreeMap.get(id));
            tmp.append("\n");

            try {
                output.write(tmp.toString().getBytes());
            }
            catch (Exception e) {
                // NOP
            }
        });

        output.flush();
        output.close();
        */

        long now = System.nanoTime();
        System.out.println("Took: " + (now-then)/1000000.0d + "ms");
        System.out.println(new Date() + ": Degrees ended");
        System.out.println("nVertices: " + nVertices + ", nEdges=" + nEdges);
    }


    public void degreeAlgoMT() throws Exception {
        final long start = Long.MIN_VALUE;
        final long end = Long.MAX_VALUE;

        System.out.println(new Date() + ": Degrees starting");
        long then = System.nanoTime();

        //VertexIterator vi = _rap.getNewWindowedVertexIterator(Long.MIN_VALUE, Long.MAX_VALUE);
        VertexIterator.MTWindowedVertexManager avm = _rap.getNewMTWindowedVertexManager(RaphtoryThreadPool.THREAD_POOL, start, end);

        Long2IntOpenHashMap[] inDegreesMap = new Long2IntOpenHashMap[_rap.getVertexMgr().nPartitions()];
        Long2IntOpenHashMap[] outDegreesMap = new Long2IntOpenHashMap[_rap.getVertexMgr().nPartitions()];
        Long2IntOpenHashMap[] degreesMap = new Long2IntOpenHashMap[_rap.getVertexMgr().nPartitions()];

        for (int i=0; i<_rap.getVertexMgr().nPartitions(); ++i) {
            inDegreesMap[i] = new Long2IntOpenHashMap(16384);
            outDegreesMap[i] = new Long2IntOpenHashMap(16384);
            degreesMap[i] = new Long2IntOpenHashMap(16384);
        }

        AtomicInteger theTotalVertices = new AtomicInteger();
        AtomicInteger theTotalEdges = new AtomicInteger();

        avm.start((id, vi) -> {
            Long2IntOpenHashMap inDegreeMap = inDegreesMap[id];
            Long2IntOpenHashMap outDegreeMap = inDegreesMap[id];
            Long2IntOpenHashMap degreeMap = inDegreesMap[id];

            int totalIncoming = 0;
            int totalOutgoing = 0;
            int totalTotal = 0;
            int nVertices = 0;

            while (vi.hasNext()) {
                long vId = vi.next();
                ++nVertices;

                //if (!vi.isAliveAt(Long.MIN_VALUE, Long.MAX_VALUE)) { continue; }

                EdgeIterator ei;

                //inDegreeMap.put(vId, vi.getNIncomingEdges());
                //outDegreeMap.put(vId, vi.getNOutgoingEdges());

                int nIncoming = 0;
                ei = vi.getOutgoingEdges();
                while (ei.hasNext()) {
                    ei.next();
                    //if (!ei.isAliveAt(Long.MIN_VALUE, Long.MAX_VALUE)) { continue; }
                    ++nIncoming;
                }
                inDegreeMap.put(vId, nIncoming);

                int nOutgoing = 0;
                ei = vi.getOutgoingEdges();
                while (ei.hasNext()) {
                    ei.next();
                    //if (!ei.isAliveAt(Long.MIN_VALUE, Long.MAX_VALUE)) { continue; }
                    ++nOutgoing;
                }
                outDegreeMap.put(vId, nOutgoing);

                int nTotal = 0;
                ei = vi.getAllEdges();
                while (ei.hasNext()) {
                    ei.next();
                    //if (!ei.isAliveAt(Long.MIN_VALUE, Long.MAX_VALUE)) { continue; }
                    ++nTotal;
                }
                degreeMap.put(vId, nTotal);

                totalIncoming += nIncoming;
                totalOutgoing += nOutgoing;
                totalTotal    += nTotal;
            }

            theTotalVertices.addAndGet(nVertices);
            theTotalEdges.addAndGet(totalIncoming + totalOutgoing + totalTotal);
        });

        avm.waitTilComplete();

        /*
        OutputStream output = new BufferedOutputStream(new FileOutputStream("degree_output.csv"), 65536);
        StringBuilder tmp = new StringBuilder();

        for (int i=0; i<degreesMap.length; ++i) {
            Long2IntOpenHashMap inDegreeMap = inDegreesMap[i];
            Long2IntOpenHashMap outDegreeMap = inDegreesMap[i];
            Long2IntOpenHashMap degreeMap = inDegreesMap[i];

            degreeMap.keySet().forEach(id -> {
                tmp.setLength(0);
                tmp.append("DUMMY,");
                tmp.append(id);
                tmp.append(",");
                tmp.append(inDegreeMap.get(id));
                tmp.append(",");
                tmp.append(outDegreeMap.get(id));
                tmp.append(",");
                tmp.append(degreeMap.get(id));
                tmp.append("\n");

                try {
                    output.write(tmp.toString().getBytes());
                }
                catch (Exception e) {
                    // NOP
                }
            });
        }

        output.flush();
        output.close();
        */

        long now = System.nanoTime();
        System.out.println("Took: " + (now-then)/1000000.0d + "ms");

        System.out.println(new Date() + ": Degrees ended");
        System.out.println("nVertices: " + theTotalVertices + ", nEdges=" + theTotalEdges);

    }


    int _nEdgesAdded = 0;
    int _nEdgesUpdated = 0;

    public void load(String file) throws Exception {
        BufferedReader br;

        if (file.endsWith(".gz")) {
            br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file), 65536)), 65536);
        }
        else {
            br = new BufferedReader(new InputStreamReader(new FileInputStream(file)), 65536);
        }
        String line;
        StringBuilder tmp = new StringBuilder();

        System.out.println(new Date() + ": Starting load");

        int nLines = 0;
        long then = System.currentTimeMillis();
        while ((line = br.readLine())!=null) {
            ++nLines;
            if (nLines % (1024 * 1024)==0) {
                System.out.println(nLines);
            }

            String[] fields = line.split(",");

            long srcGlobalId = _rap.getGlobalEntityIdStore().getGlobalNodeId(fields[3]);
            long src = Long.parseLong(fields[3]);

            long dstGlobalId = _rap.getGlobalEntityIdStore().getGlobalNodeId(fields[4]);
            long dst = Long.parseLong(fields[4]);

            long time = Long.parseLong(fields[5]);
            long price = Long.parseLong(fields[7]);

            addVertex(srcGlobalId, src, time);
            addVertex(dstGlobalId, dst, time);
            addOrUpdateEdge(srcGlobalId, dstGlobalId, time, price);
        }

        long now = System.currentTimeMillis();
        double rate = _rap.getStatistics().getNVertices() / (((double)(now-then)) / 1000.0d);
        System.out.println(new Date() + ": Ending load: rate=" + rate + " per second");

        System.out.println(_rap.getStatistics());
        System.out.println("nEdgesAdded=" + _nEdgesAdded + ", nEdgesUpdated=" + _nEdgesUpdated);
    }


    private void addVertex(long globalId, long nodeId, long time) {
        long localId = _rap.getLocalEntityIdStore().getLocalNodeId(globalId);
        if (localId==-1L) {
            long id = _rap.getVertexMgr().getNextFreeVertexId();
            Vertex v = createVertex(id, globalId, nodeId, time);
            v.decRefCount();
        }
    }


    private Vertex createVertex(long localId, long globalId, long nodeId, long time) {
        Vertex v = _rap.getVertex();
        v.incRefCount();
        v.reset(localId, globalId, true, time);
        v.getField(NODEID_FIELD).set(nodeId);
        _avpm.addVertex(v);
        _avpm.addHistory(localId, time, true, true, -1L, false);
        return v;
    }


    private void addOrUpdateEdge(long src, long dst, long time, long price) {
        long srcId = _rap.getLocalEntityIdStore().getLocalNodeId(src);
        long dstId = _rap.getLocalEntityIdStore().getLocalNodeId(dst);

        // Check if edge already exists...
        _vertexIter.reset(srcId);
        EdgeIterator iter = _vertexIter.findAllOutgoingEdges(dstId, false);
        Edge e = null;
        if (iter.hasNext()) {
            iter.next();
            e = iter.getEdge();
        }

        if (e==null) {
            addEdge(srcId, dstId, time, price);
        }
        else {
            ++_nEdgesUpdated;
            _priceVEPA.reset();
            _priceVEPA.setHistory(true, time).set(price);
            _rap.getEdgeMgr().addProperty(e.getLocalId(), PRICE_PROPERTY, _priceVEPA);
            _rap.getEdgeMgr().addHistory(e.getLocalId(), time, true, true);
            _rap.getVertexMgr().addHistory(e.getSrcVertex(), time, true, false, e.getLocalId(), true);
            _rap.getVertexMgr().addHistory(e.getDstVertex(), time, true, false, e.getLocalId(), false);

            e.decRefCount();
        }
    }


    private void addEdge(long srcId, long dstId, long time, long price) {
        Edge e = _rap.getEdge();
        e.incRefCount();

        long edgeId = _rap.getEdgeMgr().getNextFreeEdgeId();
        e.init(edgeId, true, time);

        e.resetEdgeData(srcId, dstId, false, false);
        e.getProperty(PRICE_PROPERTY).set(price);

        _rap.getEdgeMgr().addEdge(e, -1L, -1L);
        EdgePartition ep = _rap.getEdgeMgr().getPartition(_rap.getEdgeMgr().getPartitionId(e.getLocalId()));
        ep.addHistory(e.getLocalId(), time, true, true);

        VertexPartition p = _rap.getVertexMgr().getPartitionForVertex(srcId);
        ep.setOutgoingEdgePtrByEdgeId(e.getLocalId(), p.addOutgoingEdgeToList(e.getSrcVertex(), e.getLocalId(), e.getDstVertex(), false));
        p.addHistory(srcId, time, true, false, e.getLocalId(), true);

        p = _rap.getVertexMgr().getPartitionForVertex(dstId);
        ep.setIncomingEdgePtrByEdgeId(e.getLocalId(), p.addIncomingEdgeToList(e.getDstVertex(), e.getLocalId(), e.getSrcVertex()));
        p.addHistory(dstId, time, true, false, e.getLocalId(), false);

        e.decRefCount();

        ++_nEdgesAdded;
    }


    private boolean readLine(InputStream br, StringBuilder sb) throws IOException {
        sb.setLength(0);
        int c;
        while ((c = br.read()) != -1) {
            if (c == '\n') {
                trim(sb);
                return sb.length() > 0;
            }
            sb.append((char) c);
        }

        trim(sb);
        return sb.length() > 0;
    }


    private void trim(StringBuilder sb) {
        int len = sb.length();
        while (len>0) {
            char c = sb.charAt(len-1);
            if (c=='\r' || c=='\n' || c==' ' || c=='\t') {
                sb.setLength(len-1);
                len = len-1;
                continue;
            }
            break;
        }
    }


    private StringBuilder copy(StringBuilder src, int offset, int length, StringBuilder dst) {
        dst.setLength(0);

        for (int i=0; i<length; ++i) {
            dst.append(src.charAt(i + offset));
        }

        trim(dst);
        return dst;
    }









    private final int N_LOAD_THREADS = 8;
    private final int QUEUE_SIZE = 32768;

    private final Worker _workers[] = new Worker[N_LOAD_THREADS];
    private final RingBuffer<AddVertexEvent>[] _queues = new RingBuffer[N_LOAD_THREADS];
    private final Disruptor<AddVertexEvent>[] _disruptors = new Disruptor[N_LOAD_THREADS];


    public void loadMT(String file) throws Exception {
        for (int i=0; i<N_LOAD_THREADS; ++i) {
            _workers[i] = new Worker(i);
            _disruptors[i] = buildDisruptor();
            _disruptors[i].handleEventsWith(_workers[i]);
            _queues[i] = _disruptors[i].start();
        }


        BufferedReader br;

        if (file.endsWith(".gz")) {
            br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file), 65536)), 65536);
        }
        else {
            br = new BufferedReader(new InputStreamReader(new FileInputStream(file)), 65536);
        }
        String line;
        StringBuilder tmp = new StringBuilder();

        System.out.println(new Date() + ": Starting load");

        int nLines = 0;
        long then = System.currentTimeMillis();
        while ((line = br.readLine())!=null) {
            ++nLines;
            if (nLines % (1024 * 1024)==0) {
                System.out.println(nLines);
            }
            String[] fields = line.split(",");

            long srcGlobalId = _rap.getGlobalEntityIdStore().getGlobalNodeId(fields[3]);
            long src = Long.parseLong(fields[3]);

            long dstGlobalId = _rap.getGlobalEntityIdStore().getGlobalNodeId(fields[4]);
            long dst = Long.parseLong(fields[4]);

            long time = Long.parseLong(fields[5]);
            long price = Long.parseLong(fields[7]);

            int srcWorker = Math.abs((int)(srcGlobalId % N_LOAD_THREADS));
            int dstWorker = Math.abs((int)(dstGlobalId % N_LOAD_THREADS));

            queueVertex(dstWorker, dstGlobalId, dst, time);
            queueVertex(srcWorker, srcGlobalId, src, time);
            queueEdge(srcWorker, srcGlobalId, dstGlobalId, time, price);
        }

        System.out.println("WAITING");
        boolean finished = false;
        while (!finished) {
            finished = true;

            for (int i=0; i<_workers.length; ++i) {
                if (_queues[i].remainingCapacity()!=QUEUE_SIZE) {
                    finished = false;
                }
            }

            if (!finished) {
                Thread.sleep(10L);
            }
        }

        long now = System.currentTimeMillis();
        double rate = _rap.getStatistics().getNVertices() / (((double)(now-then)) / 1000.0d);
        System.out.println(new Date() + ": Ending load: rate=" + rate + " per second, dummy=");

        System.out.println(_rap.getStatistics());
        System.out.println("nEdgesAdded=" + AI_NEDGES_ADDED.get() + ", nEdgesUpdated=" + AI_NEDGES_UPDATED);
    }


    private void queueVertex(int worker, long globalId, long nodeId, long time) {
        long sequenceId = _queues[worker].next();
        AddVertexEvent event = _queues[worker].get(sequenceId);
        event.initAddVertex(globalId, nodeId, time);
        _queues[worker].publish(sequenceId);
    }


    private void queueEdge(int worker, long srcGlobalId, long dstGlobalId, long time, long price) {
        long sequenceId = _queues[worker].next();
        AddVertexEvent event = _queues[worker].get(sequenceId);
        event.initAddEdge(srcGlobalId, dstGlobalId, time, price);
        _queues[worker].publish(sequenceId);
    }



    private final AtomicInteger AI_NEDGES_ADDED = new AtomicInteger(0);
    private final AtomicInteger AI_NEDGES_UPDATED = new AtomicInteger(0);

    private class Worker implements EventHandler<AddVertexEvent> {
        private final int _id;
        private long _lastVertexId = -1L;
        private long _endVertexId = -1L;
        private long _lastEdgeId = -1L;
        private long _endEdgeId = -1L;

        private final VertexIterator _vertexIter2;

        public Worker(int id) {
            _id = id;
            _vertexIter2 = _rap.getNewAllVerticesIterator();
        }


        @Override
        public void onEvent(AddVertexEvent av, long l, boolean b) throws Exception {
            if (av._dstGlobalId != -1L) {
                addEdge(av);
            }
            else {
                addVertex(av);
            }
        }


        private void addEdge(AddVertexEvent av) {
            long srcId = _rap.getLocalEntityIdStore().getLocalNodeId(av._globalId);
            long dstId;
            while ((dstId = _rap.getLocalEntityIdStore().getLocalNodeId(av._dstGlobalId)) == -1L) {
                //LockSupport.parkNanos(1L);
                Thread.yield();
            }

            // Check if edge already exists...
            Edge e = null;
            _vertexIter2.reset(srcId);
            EdgeIterator iter = _vertexIter2.findAllOutgoingEdges(dstId, false);
            if (iter.hasNext()) {
                iter.next();
                e = iter.getEdge();
            }

            ///System.out.println(av._globalId + "," + av._dstGlobalId + ", " + (e!=null));

            if (e == null) {
                long edgeId = addEdge(srcId, dstId, av._time, av._price);
            }
            else {
                _priceVEPA.reset();
                _priceVEPA.setHistory(true, av._time).set(av._price);
                _rap.getEdgeMgr().addProperty(e.getLocalId(), PRICE_PROPERTY, _priceVEPA);
                _rap.getEdgeMgr().addHistory(e.getLocalId(), av._time, true, true);
                _rap.getVertexMgr().addHistory(e.getSrcVertex(), av._time, true, false, e.getLocalId(), true);
                _rap.getVertexMgr().addHistory(e.getDstVertex(), av._time, true, false, e.getLocalId(), false);

                e.decRefCount();
                AI_NEDGES_UPDATED.incrementAndGet();
            }
        }



        private long addEdge(long srcId, long dstId, long time, long price) {
            if (_lastEdgeId == -1L || _lastEdgeId >= _endEdgeId) {
                int partId = _rap.getEdgeMgr().getNewPartitionId();
                _lastEdgeId = partId * _rap.getEdgeMgr().PARTITION_SIZE;
                _endEdgeId = _lastEdgeId + _rap.getEdgeMgr().PARTITION_SIZE;
            }

            Edge e = _rap.getEdge();
            e.incRefCount();

            e.init(_lastEdgeId, true, time);

            e.resetEdgeData(srcId, dstId, false, false);
            e.getProperty(PRICE_PROPERTY).set(price);

            _rap.getEdgeMgr().addEdge(e, -1L, -1L);
            EdgePartition ep = _rap.getEdgeMgr().getPartition(_rap.getEdgeMgr().getPartitionId(e.getLocalId()));
            ep.addHistory(e.getLocalId(), time, true, true);

            VertexPartition p = _rap.getVertexMgr().getPartitionForVertex(srcId);
            ep.setOutgoingEdgePtrByEdgeId(e.getLocalId(), p.addOutgoingEdgeToList(e.getSrcVertex(), e.getLocalId(), e.getDstVertex(), false));
            p.addHistory(srcId, time, true, false, e.getLocalId(), true);

            p = _rap.getVertexMgr().getPartitionForVertex(dstId);
            ep.setIncomingEdgePtrByEdgeId(e.getLocalId(), p.addIncomingEdgeToList(e.getDstVertex(), e.getLocalId(), e.getSrcVertex()));
            p.addHistory(dstId, time, true, false, e.getLocalId(), false);

            e.decRefCount();

            AI_NEDGES_ADDED.incrementAndGet();

            return _lastEdgeId++;
        }



        private void addVertex(AddVertexEvent av) {
            if (_lastVertexId == -1L || _lastVertexId >= _endVertexId) {
                int partId = _rap.getVertexMgr().getNewPartitionId();
                _lastVertexId = partId * _rap.getVertexMgr().PARTITION_SIZE;
                _endVertexId = _lastVertexId + _rap.getVertexMgr().PARTITION_SIZE;
            }

            long localId = _rap.getLocalEntityIdStore().getLocalNodeId(av._globalId);
            if (localId == -1L) {
                createVertex(_lastVertexId, av._globalId, av._nodeId, av._time);
                ++_lastVertexId;
            }
        }
    }


    private static class AddVertexEvent {
        public final static EventFactory EVENT_FACTORY = () -> new AddVertexEvent();

        protected long _globalId;
        protected long _nodeId;
        protected long _time;
        protected long _dstGlobalId;
        protected long _price;


        public void initAddVertex(long globalId, long nodeId, long time) {
            _globalId = globalId;
            _nodeId = nodeId;
            _time = time;
            _dstGlobalId = -1L;
            _price = 0L;
        }


        public void initAddEdge(long srcGlobalId, long dstGlobalId, long time, long price) {
            _globalId = srcGlobalId;
            _nodeId = -1L;
            _dstGlobalId = dstGlobalId;
            _time = time;
            _price = price;
        }
    }


    private Disruptor<AddVertexEvent> buildDisruptor() {
        ThreadFactory threadFactory = DaemonThreadFactory.INSTANCE;
        WaitStrategy waitStrategy = new SleepingWaitStrategy();
        //WaitStrategy waitStrategy = new BusySpinWaitStrategy();
        //WaitStrategy waitStrategy = new BlockingWaitStrategy();
        Disruptor<AddVertexEvent> disruptor = new Disruptor<>(AddVertexEvent.EVENT_FACTORY, QUEUE_SIZE, threadFactory, ProducerType.SINGLE, waitStrategy);
        return disruptor;
    }
}