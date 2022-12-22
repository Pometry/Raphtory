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
    private static final int BUFFER_SIZE = 64 * 1024;
    private static final int N_LOAD_THREADS = 8;
    private static final int QUEUE_SIZE = 32768 * 2 ;


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
    private final int NODEID_FIELD;
    private final int PRICE_PROPERTY;
    private final VertexIterator _vertexIter;
    private final VersionedEntityPropertyAccessor _priceVEPA;
    private final LocalEntityIdStore _leis;

    private long _nextFreeVertexId;
    private long _lastFreeVertexId;
    private VertexPartition _nextVertexPartition;

    private long _nextFreeEdgeId;
    private long _lastFreeEdgeId;
    private EdgePartition _nextEdgePartition;


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
            loader.loadMT(RaphtoryInput + "/alphabay_sorted.csv");

            //rap.getVertexMgr().saveFiles();
            //rap.getEdgeMgr().saveFiles();
        }


        //rap = new RaphtoryArrowPartition(cfg);
        //loader = new AlphaBayLoader(rap);
        //loader.load(RaphtoryInput + "/alphabay_sorted.csv");

        for (int i=0; i<0; ++i) {
            System.out.println("\n\n\n");
            loader.degreeAlgoNoWindowMT();
            loader.degreeAlgoNoWindow();

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
        _leis = _rap.getLocalEntityIdStore();

        _vertexIter = _rap.getNewAllVerticesIterator();

        _priceVEPA = _rap.getEdgePropertyAccessor(PRICE_PROPERTY);
    }



    public void degreeAlgoNoWindow() throws Exception {
        System.out.println(new Date() + ": NoWindow Degrees starting");
        VertexIterator vi = _rap.getNewAllVerticesIterator();
        int nVertices = 0;
        int nEdges = 0;
        Long2IntOpenHashMap inDegreeMap = new Long2IntOpenHashMap(16384);
        Long2IntOpenHashMap outDegreeMap = new Long2IntOpenHashMap(16384);
        Long2IntOpenHashMap degreeMap = new Long2IntOpenHashMap(16384);


        long then = System.currentTimeMillis();
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

            int nTotal = 0;
            ei = vi.getAllEdges();
            while (ei.hasNext()) {
                ei.next();
                ++nTotal;
            }
            degreeMap.put(vId, nTotal);
            nEdges += nTotal;
        }

        /*
        OutputStream output = new BufferedOutputStream(new FileOutputStream("degree_output.csv"), BUFFER_SIZE);
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
            catch (Exception e) {
                // NOP
            }
        });

        output.flush();
        output.close();
*/

        System.out.println(new Date() + ": NoWindow Degrees ended");
        System.out.println("nVertices: " + nVertices + ", nEdges=" + nEdges);
        System.out.println("Total time: " + (System.currentTimeMillis() - then) + "ms");
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

            int nTotal = 0;
            ei = vi.getAllEdges();
            while (ei.hasNext()) {
                ei.next();
                ++nTotal;
            }
            degreeMap.put(vId, nTotal);
            nEdges += nTotal;
        }

        /*
        OutputStream output = new BufferedOutputStream(new FileOutputStream("degree_output.csv"), BUFFER_SIZE);
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

        System.out.println(new Date() + ": Normal Degrees starting");
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
        OutputStream output = new BufferedOutputStream(new FileOutputStream("degree_output.csv"), BUFFER_SIZE);
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
        System.out.println("Normal Took: " + (now-then)/1000000.0d + "ms");
        System.out.println(new Date() + ": Degrees ended");
        System.out.println("nVertices: " + nVertices + ", nEdges=" + nEdges);
    }



    public void degreeAlgoTestHack() throws Exception {
        //long time = 1325264638L;
        long time = Long.MAX_VALUE;

        System.out.println(new Date() + ": Hack Degrees starting");
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
        OutputStream output = new BufferedOutputStream(new FileOutputStream("degree_output.csv"), BUFFER_SIZE);
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
        System.out.println("Hack Took: " + (now-then)/1000000.0d + "ms");
        System.out.println(new Date() + ": Degrees ended");
        System.out.println("nVertices: " + nVertices + ", nEdges=" + nEdges);
    }


    public void degreeAlgoMT() throws Exception {
        final long start = Long.MIN_VALUE;
        final long end = Long.MAX_VALUE;

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
        System.out.println("nVertices: " + theTotalVertices + ", nEdges=" + theTotalEdges);
    }


    public void degreeAlgoNoWindowMT() throws Exception {
        System.out.println(new Date() + ": NoWindowMT Degrees starting");
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
        System.out.println("NoWindowMT Took: " + (now-then)/1000000.0d + "ms");

        System.out.println(new Date() + ": Degrees ended");
        System.out.println("nVertices: " + theTotalVertices + ", nEdges=" + theTotalEdges);
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

            if (true) {
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
        }

        long now = System.currentTimeMillis();
        double rate = _rap.getStatistics().getNVertices() / (((double)(now-then)) / 1000.0d);
        System.out.println(new Date() + ": Ending load: rate=" + rate + " per second");
        System.out.println("Total time: " + (now-then) + "ms");

        System.out.println(_rap.getStatistics());
        System.out.println("nEdgesAdded=" + _nEdgesAdded + ", nEdgesUpdated=" + _nEdgesUpdated);
    }


    private void addVertex(long globalId, long nodeId, long time) {
        long localId = _leis.getLocalNodeId(globalId);
        if (localId==-1L) {
            long id = _avpm.getNextFreeVertexId();
            Vertex v = createVertex(id, globalId, nodeId, time);
            v.decRefCount();
        }
    }


    private Vertex createVertex(long localId, long globalId, long nodeId, long time) {
        Vertex v = _rap.getVertex();
        v.incRefCount();
        v.reset(localId, globalId, true, time);
        v.getField(NODEID_FIELD).set(nodeId);
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


    private void addOrUpdateEdge(long src, long dst, long time, long price) {
        long srcId = _leis.getLocalNodeId(src);
        long dstId = _leis.getLocalNodeId(dst);

        // Check if edge already exists...
        _vertexIter.reset(srcId);
        EdgeIterator iter = _vertexIter.findAllOutgoingEdges(dstId, false);
        long e = -1L;
        if (iter.hasNext()) {
            e = iter.next();
        }

        if (e==-1L) {
            addEdge(srcId, dstId, time, price);
        }
        else if (true) {
            ++_nEdgesUpdated;
            _priceVEPA.reset();
            _priceVEPA.setHistory(true, time).set(price);
            _aepm.addProperty(e, PRICE_PROPERTY, _priceVEPA);
            _aepm.addHistory(e, time, true, true);
            _avpm.addHistory(iter.getSrcVertexId(), time, true, false, e, true);
            _avpm.addHistory(iter.getDstVertexId(), time, true, false, e, false);
        }
    }


    private void addEdge(long srcId, long dstId, long time, long price) {
        Edge e = _rap.getEdge();
        e.incRefCount();

        if (false) {
            long edgeId = _aepm.getNextFreeEdgeId();

            e.init(edgeId, true, time);
            e.resetEdgeData(srcId, dstId, false, false);
            e.getProperty(PRICE_PROPERTY).set(price);

            _aepm.addEdge(e, -1L, -1L);

            EdgePartition ep = _aepm.getPartition(_aepm.getPartitionId(e.getLocalId()));
            ep.addHistory(e.getLocalId(), time, true, true);

            VertexPartition p = _avpm.getPartitionForVertex(srcId);
            ep.setOutgoingEdgePtrByEdgeId(e.getLocalId(), p.addOutgoingEdgeToList(e.getSrcVertex(), e.getLocalId(), e.getDstVertex(), false));
            p.addHistory(srcId, time, true, false, e.getLocalId(), true);

            p = _avpm.getPartitionForVertex(dstId);
            ep.setIncomingEdgePtrByEdgeId(e.getLocalId(), p.addIncomingEdgeToList(e.getDstVertex(), e.getLocalId(), e.getSrcVertex()));
            p.addHistory(dstId, time, true, false, e.getLocalId(), false);

            e.decRefCount();

            ++_nEdgesAdded;
        }
        else {
            long edgeId = _nextFreeEdgeId;

            e.init(edgeId, true, time);

            e.resetEdgeData(srcId, dstId, false, false);
            e.getProperty(PRICE_PROPERTY).set(price);

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

        final int BATCH = 4096;
        long SGBID[] = new long[BATCH];
        long DGBID[] = new long[BATCH];
        long TIME[] = new long[BATCH];
        long PRICE[] = new long[BATCH];
        int batchSize = 0;


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

            if (true) {
                int bs = batchSize;
                SGBID[bs] = srcGlobalId;
                DGBID[bs] = dstGlobalId;
                TIME[bs] = time;
                PRICE[bs] = price;

                if (++batchSize >= BATCH) {
                    for (int i = 0; i < BATCH; ++i) {
                        srcWorker = Math.abs((int) (SGBID[i] % N_LOAD_THREADS));

                        queueEdge(srcWorker, SGBID[i], DGBID[i], TIME[i], PRICE[i]);
                    }

                    batchSize = 0;
                }
            }
            else {
                queueEdge(srcWorker, srcGlobalId, dstGlobalId, time, price);
            }
        }

        if (true && batchSize!=0) {
            for (int i=0; i<batchSize; ++i) {
                int srcWorker = Math.abs((int)(SGBID[i] % N_LOAD_THREADS));

                queueEdge(srcWorker, SGBID[i], DGBID[i], TIME[i], PRICE[i]);
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
        //System.out.println("nEdgesAdded=" + AI_NEDGES_ADDED.get() + ", nEdgesUpdated=" + AI_NEDGES_UPDATED);
    }


    private void queueVertex(int worker, long globalId, long nodeId, long time) {
        RingBuffer<AddVertexEvent> q = _queues[worker];

        long sequenceId = q.next();
        AddVertexEvent event = q.get(sequenceId);
        event.initAddVertex(globalId, nodeId, time);
        q.publish(sequenceId);
    }


    private void queueEdge(int worker, long srcGlobalId, long dstGlobalId, long time, long price) {
        RingBuffer<AddVertexEvent> q = _queues[worker];

        long sequenceId = q.next();
        AddVertexEvent event = q.get(sequenceId);
        event.initAddEdge(srcGlobalId, dstGlobalId, time, price);
        q.publish(sequenceId);
    }



    private final AtomicInteger AI_NEDGES_ADDED = new AtomicInteger(0);
    private final AtomicInteger AI_NEDGES_UPDATED = new AtomicInteger(0);

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
                long edgeId = addEdge(srcId, dstId, av._time, av._price);
            }
            else {
                _priceVEPA.reset();
                _priceVEPA.setHistory(true, av._time).set(av._price);
                _aepm.addProperty(e, PRICE_PROPERTY, _priceVEPA);
                _aepm.addHistory(e, av._time, true, true);
                _avpm.addHistory(iter.getSrcVertexId(), av._time, true, false, e, true);
                _avpm.addHistory(iter.getDstVertexId(), av._time, true, false, e, false);

                //e.decRefCount();
                //AI_NEDGES_UPDATED.incrementAndGet();
            }
        }



        private long addEdge(long srcId, long dstId, long time, long price) {
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
            e.getProperty(PRICE_PROPERTY).set(price);

            VertexPartition p = _avpm.getPartitionForVertex(srcId);
            long outgoingPtr = p.addOutgoingEdgeToList(e.getSrcVertex(), e.getLocalId(), e.getDstVertex(), false);
            p.addHistory(srcId, time, true, false, e.getLocalId(), true);

            p = _avpm.getPartitionForVertex(dstId);
            long incomingPtr = p.addIncomingEdgeToList(e.getDstVertex(), e.getLocalId(), e.getSrcVertex());
            p.addHistory(dstId, time, true, false, e.getLocalId(), false);

            _lastEdgePartition.addEdge(e, incomingPtr, outgoingPtr);
            _lastEdgePartition.addHistory(e.getLocalId(), time, true, true);

            e.decRefCount();

            //AI_NEDGES_ADDED.incrementAndGet();

            return _lastEdgeId++;
        }



        private void addVertex(AddVertexEvent av) {
            if (_lastVertexId >= _endVertexId) {
                getNextVertexId();
            }

            long localId = _leis.getLocalNodeId(av._globalId);
            if (localId == -1L) {
                createVertex(_lastVertexId, av._globalId, av._nodeId, av._time).decRefCount();
                ++_lastVertexId;
            }
        }


        private Vertex createVertex(long localId, long globalId, long nodeId, long time) {
            Vertex v = _rap.getVertex();
            v.incRefCount();
            v.reset(localId, globalId, true, time);
            v.getField(NODEID_FIELD).set(nodeId);
            _lastVertexPartition.addVertex(v);
            _lastVertexPartition.addHistory(localId, time, true, true, -1L, false);
            return v;
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

        public void initVertexAndEdge(long srcGlobalId, long nodeId, long time, long dstGlobalId, long price) {
            _globalId = srcGlobalId;
            _nodeId = nodeId;
            _time = time;
            _dstGlobalId = dstGlobalId;
            _price = price;
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
