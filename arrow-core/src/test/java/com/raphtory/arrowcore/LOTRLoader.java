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
import net.openhft.chronicle.core.util.StringUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;


public class LOTRLoader {
    private static final int BUFFER_SIZE = 64 * 1024;
    private static final int N_LOAD_THREADS = 1;
    private static final int QUEUE_SIZE = 32768 * 2;
    private static final boolean BATCH_EDGES = true;
    private static final int EDGE_BATCH_SIZE = 4096;


    public static class LOTRSchema implements PropertySchema {
        private static final ArrayList<NonversionedField> _nonVersionedVertexProperties;
        private static final ArrayList<VersionedProperty> _versionedVertexProperties;

        private static final ArrayList<NonversionedField> _nonVersionedEdgeProperties;
        private static final ArrayList<VersionedProperty> _versionedEdgeProperties;

        static {
            _nonVersionedVertexProperties = new ArrayList<>(Arrays.asList(
                    new NonversionedField("name", StringBuilder.class)
            ));

            _nonVersionedEdgeProperties = null;

            _versionedVertexProperties = null;

            _versionedEdgeProperties = null;
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
    private final int NAME_FIELD;
    private final VertexIterator _vertexIter;
    private final EntityFieldAccessor[] FIELDS;
    private final LocalEntityIdStore _leis;

    private long _nextFreeVertexId;
    private long _lastFreeVertexId;
    private VertexPartition _nextVertexPartition;

    private long _nextFreeEdgeId;
    private long _lastFreeEdgeId;
    private EdgePartition _nextEdgePartition;


    public final static String RaphtoryInput = "/home/jatinder/projects/Pometry/arrow-core";
    public final static String ArrowDir = "/tmp/halongbay";



    //public final static String RaphtoryInput = "/pometry/wip";
    //public final static String ArrowDir = "/pometry/wip/data";


    public static void main(String[] args) throws Exception {
        RaphtoryArrowPartition.RaphtoryArrowPartitionConfig cfg = new RaphtoryArrowPartition.RaphtoryArrowPartitionConfig();
        cfg._propertySchema = new LOTRSchema();
        cfg._arrowDir = ArrowDir;
        cfg._raphtoryPartitionId = 0;
        cfg._nRaphtoryPartitions = 1;
        cfg._nLocalEntityIdMaps = 128;
        cfg._localEntityIdMapSize = 1024;
        cfg._syncIDMap = false;
        cfg._edgePartitionSize = 1024 * 1024;
        cfg._vertexPartitionSize = 256 * 1024;

        RaphtoryArrowPartition rap = new RaphtoryArrowPartition(cfg);

        LOTRLoader loader = new LOTRLoader(rap);
        if (new File(ArrowDir + "/vertex-p0.rap").exists()) {
            long then = System.currentTimeMillis();
            rap.getVertexMgr().loadFiles();
            rap.getEdgeMgr().loadFiles();
            System.out.println("ARROW FILES LOADED: " + rap.getStatistics() + ", time taken: " + (System.currentTimeMillis()-then) + "ms");
        }
        else {
            //loader.load(RaphtoryInput + "/lotr.csv");
            loader.loadMT(RaphtoryInput + "/lotr.csv");

            //rap.getVertexMgr().saveFiles();
            //rap.getEdgeMgr().saveFiles();
        }


        //rap = new RaphtoryArrowPartition(cfg);
        //loader = new HalongBayLoader(rap);
        //loader.load(RaphtoryInput + "/halongbay_sorted.csv");

        for (int i=0; i<1; ++i) {
            System.out.println("\n\n\n");
            //loader.dump();
            //loader.dump(0, 11);

            //loader.degreeAlgoNoWindow();



            System.out.println("\n\n\n");
            loader.degreeAlgoMT(9500, 10000);
        }
    }


    public LOTRLoader(RaphtoryArrowPartition rap) {
        NAME_FIELD = rap.getVertexFieldId("name");
        FIELDS = rap.getTmpVertexEntityFieldAccessors();

        _rap = rap;
        _aepm = rap.getEdgeMgr();
        _avpm = rap.getVertexMgr();
        _leis = _rap.getLocalEntityIdStore();

        _vertexIter = _rap.getNewAllVerticesIterator();
    }


    public void dump() throws Exception {
        VertexIterator vi = _rap.getNewAllVerticesIterator();
        while (vi.hasNext()) {
            long vId = vi.next();

            System.out.println("V: " + vId + ", " + vi.getField(NAME_FIELD).getString());

            EdgeIterator ei = vi.getIncomingEdges();
            while (ei.hasNext()) {
                ei.next();

                System.out.print("  E: " + ei.getEdgeId() + ", F: " + getName(ei.getSrcVertexId()) + ",");
                EdgeHistoryIterator ehi = _rap.getNewEdgeHistoryIterator(ei.getEdgeId(), Long.MIN_VALUE, Long.MAX_VALUE);
                while (ehi.hasNext()) {
                    ehi.next();
                    System.out.print(" " + ehi.getModificationTime());
                }
                System.out.println();
            }

            ei = vi.getOutgoingEdges();
            while (ei.hasNext()) {
                ei.next();

                System.out.print("  E: " + ei.getEdgeId() + ", T: " + getName(ei.getDstVertexId()) + ",");
                EdgeHistoryIterator ehi = _rap.getNewEdgeHistoryIterator(ei.getEdgeId(), Long.MIN_VALUE, Long.MAX_VALUE);
                while (ehi.hasNext()) {
                    ehi.next();
                    System.out.print(" " + ehi.getModificationTime());
                }
                System.out.println();
            }
        }
    }




    public void dump(long src, long dst) throws Exception {
        VertexIterator vi = _rap.getNewAllVerticesIterator();
        System.out.println("DETAIL FOR F=" + getName(src) + " TO " + getName(dst));

        vi.reset(dst);

        EdgeIterator ei = vi.getIncomingEdges();
        while (ei.hasNext()) {
            ei.next();
            if (ei.getSrcVertexId()==src) {
                break;
            }
        }

        if (ei.getSrcVertexId()==src) {
            System.out.println(ei.isAliveAt(5000, 10000));
        }
    }


    private StringBuilder _tmp = new StringBuilder();
    private VertexIterator _tmpVI = null;

    private StringBuilder getName(long vId) {
        VertexIterator vi = _rap.getNewAllVerticesIterator();
        vi.reset(vId);
        _tmp.setLength(0);
        _tmp.append(vi.getField(NAME_FIELD).getString());

        return _tmp;
    }


    public void degreeAlgoNoWindow() throws Exception {
        System.out.println(new Date() + ": MT Degrees starting");
        long then = System.nanoTime();

        //VertexIterator vi = _rap.getNewWindowedVertexIterator(Long.MIN_VALUE, Long.MAX_VALUE);
        VertexIterator.MTAllVerticesManager avm = _rap.getNewMTAllVerticesManager(RaphtoryThreadPool.THREAD_POOL);

        Long2IntOpenHashMap[] inDegreesMap = new Long2IntOpenHashMap[_avpm.nPartitions()];
        Long2IntOpenHashMap[] outDegreesMap = new Long2IntOpenHashMap[_avpm.nPartitions()];
        Long2IntOpenHashMap[] degreesMap = new Long2IntOpenHashMap[_avpm.nPartitions()];

        for (int i=0; i<_avpm.nPartitions(); ++i) {
            inDegreesMap[i] = new Long2IntOpenHashMap(16384);
            outDegreesMap[i] = new Long2IntOpenHashMap(16384);
            degreesMap[i] = new Long2IntOpenHashMap(16384);
        }

        AtomicInteger theTotalVertices = new AtomicInteger();
        AtomicInteger theTotalInc = new AtomicInteger();
        AtomicInteger theTotalOut = new AtomicInteger();
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

                EdgeIterator ei;

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

                int nTotal = 0;
                ei = vi.getAllEdges();
                while (ei.hasNext()) {
                    ei.next();
                    ++nTotal;
                }
                degreeMap.put(vId, nTotal);

                totalIncoming += nIncoming;
                totalOutgoing += nOutgoing;
                totalTotal    += nTotal;

                //System.out.println(vi.getField(NAME_FIELD).getString() + "," + nIncoming + "," + nOutgoing + "," + nTotal);
            }

            theTotalVertices.addAndGet(nVertices);
            theTotalInc.addAndGet(totalIncoming);
            theTotalOut.addAndGet(totalOutgoing);
            theTotalEdges.addAndGet(totalTotal);
        });

        avm.waitTilComplete();

        /*
        OutputStream output = new BufferedOutputStream(new FileOutputStream("degree_output.csv"), BUFFER_SIZE);
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
        System.out.println("MT Took: " + (now-then)/1000000.0d + "ms");

        System.out.println(new Date() + ": Degrees ended");
        System.out.println("nVertices: " + theTotalVertices + ", nEdges=" + theTotalEdges + ", totalInc=" + theTotalInc + ", totalOut=" + theTotalOut + "\n\n");
    }



    public void degreeAlgoMT(final long start, final long end) throws Exception {
        System.out.println(new Date() + ": MT Degrees starting");
        long then = System.nanoTime();

        //VertexIterator vi = _rap.getNewWindowedVertexIterator(Long.MIN_VALUE, Long.MAX_VALUE);
        VertexIterator.MTWindowedVertexManager avm = _rap.getNewMTWindowedVertexManager(RaphtoryThreadPool.THREAD_POOL, start, end);

        Long2IntOpenHashMap[] inDegreesMap = new Long2IntOpenHashMap[_avpm.nPartitions()];
        Long2IntOpenHashMap[] outDegreesMap = new Long2IntOpenHashMap[_avpm.nPartitions()];
        Long2IntOpenHashMap[] degreesMap = new Long2IntOpenHashMap[_avpm.nPartitions()];

        for (int i=0; i<_avpm.nPartitions(); ++i) {
            inDegreesMap[i] = new Long2IntOpenHashMap(16384);
            outDegreesMap[i] = new Long2IntOpenHashMap(16384);
            degreesMap[i] = new Long2IntOpenHashMap(16384);
        }

        AtomicInteger theTotalVertices = new AtomicInteger();
        AtomicInteger theTotalInc = new AtomicInteger();
        AtomicInteger theTotalOut = new AtomicInteger();
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

                EdgeIterator ei;

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

                int nTotal = 0;
                ei = vi.getAllEdges();
                while (ei.hasNext()) {
                    ei.next();
                    ++nTotal;
                }
                degreeMap.put(vId, nTotal);

                totalIncoming += nIncoming;
                totalOutgoing += nOutgoing;
                totalTotal    += nTotal;

                System.out.println(end + "," + (end-start) + "," + vi.getField(NAME_FIELD).getString() + "," + nIncoming + "," + nOutgoing + "," + nTotal);
            }

            theTotalVertices.addAndGet(nVertices);
            theTotalInc.addAndGet(totalIncoming);
            theTotalOut.addAndGet(totalOutgoing);
            theTotalEdges.addAndGet(totalTotal);
        });

        avm.waitTilComplete();

        /*
        OutputStream output = new BufferedOutputStream(new FileOutputStream("degree_output.csv"), BUFFER_SIZE);
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
        System.out.println("MT Took: " + (now-then)/1000000.0d + "ms");

        System.out.println(new Date() + ": Degrees ended");
        System.out.println("nVertices: " + theTotalVertices + ", nEdges=" + theTotalEdges + ", totalInc=" + theTotalInc + ", totalOut=" + theTotalOut + "\n\n");
    }


    int _nEdgesAdded = 0;
    int _nEdgesUpdated = 0;

    public void load(String file) throws Exception {
        BufferedReader br;

        _nextFreeVertexId = _avpm.getNextFreeVertexId();
        int partitionId = _avpm.getPartitionId(_nextFreeVertexId);
        _nextVertexPartition = _avpm.getPartition(partitionId);
        _lastFreeVertexId = (partitionId+1L) * _avpm.PARTITION_SIZE;

        _nextFreeEdgeId = _aepm.getNextFreeEdgeId();
        partitionId = _aepm.getPartitionId(_nextFreeEdgeId);
        _nextEdgePartition = _aepm.getPartition(partitionId);
        _lastFreeEdgeId = (partitionId+1L) * _aepm.PARTITION_SIZE;


        if (file.endsWith(".gz")) {
            br = new BufferedReader(new InputStreamReader(new BufferedInputStream(new GZIPInputStream(new FileInputStream(file), BUFFER_SIZE), BUFFER_SIZE)), BUFFER_SIZE);
        }
        else {
            br = new BufferedReader(new InputStreamReader(new BufferedInputStream(new FileInputStream(file), BUFFER_SIZE)), BUFFER_SIZE);
        }
        String line;
        StringBuilder tmp = new StringBuilder();

        System.out.println(new Date() + ": Starting load");
        long then = System.currentTimeMillis();

        int nLines = 0;
        while ((line = br.readLine())!=null) {
            ++nLines;
            if (nLines % (1024 * 1024)==0) {
                System.out.println(nLines);
            }

            String[] fields = line.split(",");

            String src = fields[0];
            String dst = fields[1];

            long srcGlobalId = _rap.getGlobalEntityIdStore().getGlobalNodeId(src);
            long dstGlobalId = _rap.getGlobalEntityIdStore().getGlobalNodeId(dst);

            long time = Long.parseLong(fields[2]);

            addVertex(srcGlobalId, src, time);
            addVertex(dstGlobalId, dst, time);
            addOrUpdateEdge(srcGlobalId, dstGlobalId, time);
        }

        long now = System.currentTimeMillis();
        double rate = _rap.getStatistics().getNVertices() / (((double)(now-then)) / 1000.0d);
        System.out.println(new Date() + ": Ending load: rate=" + rate + " per second");
        System.out.println("Total time: " + (now-then) + "ms");

        System.out.println(_rap.getStatistics());
        System.out.println("nEdgesAdded=" + _nEdgesAdded + ", nEdgesUpdated=" + _nEdgesUpdated);
    }


    private void addVertex(long globalId, String name, long time) {
        long localId = _leis.getLocalNodeId(globalId);
        if (localId==-1L) {
            long id = _avpm.getNextFreeVertexId();
            Vertex v = createVertex(id, globalId, name, time);
            v.decRefCount();
        }
    }


    private Vertex createVertex(long localId, long globalId, String name, long time) {
        Vertex v = _rap.getVertex();
        v.incRefCount();
        v.reset(localId, globalId, true, time);
        v.getField(NAME_FIELD).set(name);
        _nextVertexPartition.addVertex(v);
        _nextVertexPartition.addHistory(localId, time, true, true, -1L, false);

        if (++_nextFreeVertexId >= _lastFreeVertexId) {
            _nextFreeVertexId = _avpm.getNextFreeVertexId();
            int partitionId = _avpm.getPartitionId(_nextFreeVertexId);
            _nextVertexPartition = _avpm.getPartitionAndLoad(partitionId);
            _lastFreeVertexId = (partitionId + 1L) * _avpm.PARTITION_SIZE;
        }

        return v;
    }


    private void addOrUpdateEdge(long src, long dst, long time) {
        long srcId = _leis.getLocalNodeId(src);
        long dstId = _leis.getLocalNodeId(dst);

        // Check if edge already exists...
        _vertexIter.reset(srcId);
        long e = -1L;

        EdgeIterator iter = _vertexIter.findAllOutgoingEdges(dstId, false);
        if (iter.hasNext()) {
            e = iter.next();
        }

        if (e==-1L) {
            addEdge(srcId, dstId, time);
        }
        else {
            ++_nEdgesUpdated;
            _aepm.addHistory(e, time, true, true);
            _avpm.addHistory(iter.getSrcVertexId(), time, true, false, e, true);
            _avpm.addHistory(iter.getDstVertexId(), time, true, false, e, false);
        }
    }


    private void addEdge(long srcId, long dstId, long time) {
        Edge e = _rap.getEdge();
        e.incRefCount();

        long edgeId = _nextFreeEdgeId;

        e.init(edgeId, true, time);

        e.resetEdgeData(srcId, dstId, false, false);

        VertexPartition p = _avpm.getPartitionForVertex(srcId);
        long outgoingPtr = p.addOutgoingEdgeToList(e.getSrcVertex(), e.getLocalId(), e.getDstVertex(), false);
        p.addHistory(srcId, time, true, false, e.getLocalId(), true);

        p = _avpm.getPartitionForVertex(dstId);
        long incomingPtr = p.addIncomingEdgeToList(e.getDstVertex(), e.getLocalId(), e.getSrcVertex());
        p.addHistory(dstId, time, true, false, e.getLocalId(), false);

        _nextEdgePartition.addEdge(e, incomingPtr, outgoingPtr);
        _nextEdgePartition.addHistory(e.getLocalId(), time, true, true);

        e.decRefCount();

        ++_nEdgesAdded;

        if (++_nextFreeEdgeId >= _lastFreeEdgeId) {
            _nextFreeEdgeId = _aepm.getNextFreeEdgeId();
            int partitionId = _aepm.getPartitionId(_nextFreeEdgeId);
            _nextEdgePartition = _aepm.getPartitionAndLoad(partitionId);
            _lastFreeEdgeId = (partitionId + 1L) * _aepm.PARTITION_SIZE;
        }
    }


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
            br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file), BUFFER_SIZE)), BUFFER_SIZE);
        }
        else {
            br = new BufferedReader(new InputStreamReader(new FileInputStream(file)), BUFFER_SIZE);
        }
        String line;
        StringBuilder tmp = new StringBuilder();

        System.out.println(new Date() + ": Starting load");


        long SGBID[];
        long DGBID[];
        long TIME[];

        if (BATCH_EDGES) {
            SGBID = new long[EDGE_BATCH_SIZE];
            DGBID = new long[EDGE_BATCH_SIZE];
            TIME = new long[EDGE_BATCH_SIZE];
        }

        int batchSize = 0;


        int nLines = 0;
        long then = System.currentTimeMillis();
        while ((line = br.readLine())!=null) {
            ++nLines;
            if (nLines % 1024==0) {
                System.out.println(nLines);
            }

            String[] fields = line.split(",");

            String src = fields[0];
            String dst = fields[1];

            long srcGlobalId = _rap.getGlobalEntityIdStore().getGlobalNodeId(src);
            long dstGlobalId = _rap.getGlobalEntityIdStore().getGlobalNodeId(dst);

            long time = Long.parseLong(fields[2]);

            int srcWorker = Math.abs((int)(srcGlobalId % N_LOAD_THREADS));
            int dstWorker = Math.abs((int)(dstGlobalId % N_LOAD_THREADS));

            queueVertex(dstWorker, dstGlobalId, dst, time);
            queueVertex(srcWorker, srcGlobalId, src, time);

            if (BATCH_EDGES) {
                int bs = batchSize;
                SGBID[bs] = srcGlobalId;
                DGBID[bs] = dstGlobalId;
                TIME[bs] = time;

                if (++batchSize >= EDGE_BATCH_SIZE) {
                    for (int j = 0; j < EDGE_BATCH_SIZE; ++j) {
                        srcWorker = Math.abs((int) (SGBID[j] % N_LOAD_THREADS));

                        queueEdge(srcWorker, SGBID[j], DGBID[j], TIME[j]);
                    }

                    batchSize = 0;
                }
            }
            else {
                queueEdge(srcWorker, srcGlobalId, dstGlobalId, time);
            }
        }

        if (BATCH_EDGES && batchSize!=0) {
            for (int i=0; i<batchSize; ++i) {
                int srcWorker = Math.abs((int)(SGBID[i] % N_LOAD_THREADS));

                queueEdge(srcWorker, SGBID[i], DGBID[i], TIME[i]);
            }
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

        for (int i=0; i<N_LOAD_THREADS; ++i) {
            _disruptors[i].halt();
        }

        long now = System.currentTimeMillis();
        double rate = nLines / (((double)(now-then)) / 1000.0d);
        System.out.println(new Date() + ": Ending load: rate=" + rate + " per second, dummy=");

        System.out.println(_rap.getStatistics());
        System.out.println("Total time: " + (now-then) + "ms");
    }


    private void queueVertex(int worker, long globalId, String name, long time) {
        RingBuffer<AddVertexEvent> q = _queues[worker];

        long sequenceId = q.next();
        AddVertexEvent event = q.get(sequenceId);
        event.initAddVertex(globalId, name, time);
        q.publish(sequenceId);
    }


    private void queueEdge(int worker, long srcGlobalId, long dstGlobalId, long time) {
        RingBuffer<AddVertexEvent> q = _queues[worker];

        long sequenceId = q.next();
        AddVertexEvent event = q.get(sequenceId);
        event.initAddEdge(srcGlobalId, dstGlobalId, time);
        q.publish(sequenceId);
    }



    private class Worker implements EventHandler<AddVertexEvent> {
        private final int _id;
        private long _lastVertexId = -1L;
        private long _endVertexId = -1L;
        private long _lastEdgeId = -1L;
        private long _endEdgeId = -1L;
        private EdgePartition _lastEdgePartition = null;
        private VertexPartition _lastVertexPartition = null;


        private final VertexIterator _vertexIter2;
        //private final LocalEntityIdStore _leis;

        public Worker(int id) {
            _id = id;
            _vertexIter2 = _rap.getNewAllVerticesIterator();
            //_leis = _rap.getLocalEntityIdStore();
            getNextVertexId();
        }


        private void getNextVertexId() {
            int partId = _avpm.getNewPartitionId();
            _lastVertexId = partId * _avpm.PARTITION_SIZE;
            _endVertexId = _lastVertexId + _avpm.PARTITION_SIZE;
            _lastVertexPartition = _avpm.getPartition(partId);
        }


        @Override
        public void onEvent(AddVertexEvent av, long l, boolean b) throws Exception {
            if (av._dstGlobalId == -1L) {
                addVertex(av);
            }
            else {
                addEdge(av);
            }
        }


        private void addEdge(AddVertexEvent av) {
            long srcId = _leis.getLocalNodeId(av._globalId);
            long dstId;
            while ((dstId = _leis.getLocalNodeId(av._dstGlobalId)) == -1L) {
                Thread.yield();
                //LockSupport.parkNanos(1L);
            }

            // Check if edge already exists...
            long e = -1L;
            _vertexIter2.reset(srcId);
            EdgeIterator iter = _vertexIter2.findAllOutgoingEdges(dstId, false);
            if (iter.hasNext()) {
                e = iter.next();
            }

            ///System.out.println(av._globalId + "," + av._dstGlobalId + ", " + (e!=null));

            if (e == -1L) {
                long edgeId = addEdge(srcId, dstId, av._time);
            }
            else {
                _aepm.addHistory(e, av._time, true, true);
                _avpm.addHistory(iter.getSrcVertexId(), av._time, true, false, e, true);
                _avpm.addHistory(iter.getDstVertexId(), av._time, true, false, e, false);
            }
        }



        private long addEdge(long srcId, long dstId, long time) {
            if (_lastEdgeId == -1L || _lastEdgeId >= _endEdgeId) {
                int partId = _aepm.getNewPartitionId();
                _lastEdgeId = partId * _aepm.PARTITION_SIZE;
                _endEdgeId = _lastEdgeId + _aepm.PARTITION_SIZE;
                _lastEdgePartition = _aepm.getPartition(partId);
            }

            Edge e = _rap.getEdge();
            e.incRefCount();

            e.init(_lastEdgeId, true, time);
            e.resetEdgeData(srcId, dstId, false, false);

            VertexPartition p = _avpm.getPartitionForVertex(srcId);
            long outgoingPtr = p.addOutgoingEdgeToList(e.getSrcVertex(), e.getLocalId(), e.getDstVertex(), false);
            p.addHistory(srcId, time, true, false, e.getLocalId(), true);

            p = _avpm.getPartitionForVertex(dstId);
            long incomingPtr = p.addIncomingEdgeToList(e.getDstVertex(), e.getLocalId(), e.getSrcVertex());
            p.addHistory(dstId, time, true, false, e.getLocalId(), false);

            _lastEdgePartition.addEdge(e, incomingPtr, outgoingPtr);
            _lastEdgePartition.addHistory(e.getLocalId(), time, true, true);

            e.decRefCount();

            return _lastEdgeId++;
        }



        private void addVertex(AddVertexEvent av) {
            if (_lastVertexId >= _endVertexId) {
                getNextVertexId();
            }

            long localId = _leis.getLocalNodeId(av._globalId);
            if (localId == -1L) {
                createVertex(_lastVertexId, av._globalId, av._name, av._time).decRefCount();
                ++_lastVertexId;
            }
        }


        private Vertex createVertex(long localId, long globalId, String name, long time) {
            Vertex v = _rap.getVertex();
            v.incRefCount();
            v.reset(localId, globalId, true, time);
            v.getField(NAME_FIELD).set(name);
            _lastVertexPartition.addVertex(v);
            _lastVertexPartition.addHistory(localId, time, true, true, -1L, false);
            return v;
        }
    }


    private static class AddVertexEvent {
        public final static EventFactory EVENT_FACTORY = () -> new AddVertexEvent();

        protected long _globalId;
        protected String _name;
        protected long _time;
        protected long _dstGlobalId;


        public void initAddVertex(long globalId, String name, long time) {
            _globalId = globalId;
            _name = name;
            _time = time;
            _dstGlobalId = -1L;
        }


        public void initAddEdge(long srcGlobalId, long dstGlobalId, long time) {
            _globalId = srcGlobalId;
            _name = null;
            _dstGlobalId = dstGlobalId;
            _time = time;
        }
    }


    private Disruptor<AddVertexEvent> buildDisruptor() {
        ThreadFactory threadFactory = DaemonThreadFactory.INSTANCE;
        WaitStrategy waitStrategy = new YieldingWaitStrategy();
        //WaitStrategy waitStrategy = new BusySpinWaitStrategy();
        //WaitStrategy waitStrategy = new BlockingWaitStrategy();
        Disruptor<AddVertexEvent> disruptor = new Disruptor<>(AddVertexEvent.EVENT_FACTORY, QUEUE_SIZE, threadFactory, ProducerType.SINGLE, waitStrategy);
        return disruptor;
    }
}
