package com.raphtory.arrowcore;

import com.raphtory.arrowcore.implementation.*;
import com.raphtory.arrowcore.model.Edge;
import com.raphtory.arrowcore.model.PropertySchema;
import com.raphtory.arrowcore.model.Vertex;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
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


    public static void main(String[] args) throws Exception {
        RaphtoryArrowPartition.RaphtoryArrowPartitionConfig cfg = new RaphtoryArrowPartition.RaphtoryArrowPartitionConfig();
        cfg._propertySchema = new AlphaBaySchema();
        cfg._arrowDir = "E:/tmp/alphabay";
        cfg._raphtoryPartitionId = 0;
        cfg._nRaphtoryPartitions = 1;
        cfg._nLocalEntityIdMaps = 128;
        cfg._localEntityIdMapSize = 1024;
        cfg._syncIDMap = false;
        cfg._edgePartitionSize = 512 * 1024;
        cfg._vertexPartitionSize = 256 * 1024;

        RaphtoryArrowPartition rap = new RaphtoryArrowPartition(cfg);

        AlphaBayLoader loader = new AlphaBayLoader(rap);
        if (new File("E:/tmp/alphabay/vertex-p0.rap").exists()) {
            rap.getVertexMgr().loadFiles();
            rap.getEdgeMgr().loadFiles();
        }
        else {
            loader.load("D:/home/jatinder/Pometry/public/Raphtory/alphabay_sorted.csv.gz");
            rap.getVertexMgr().saveFiles();
        }

        loader.degreeAlgoMT();
        System.exit(0);
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
        //VertexIterator vi = _rap.getNewWindowedVertexIterator(Long.MIN_VALUE, Long.MAX_VALUE);
        VertexIterator vi = _rap.getNewAllVerticesIterator();
        int nVertices = 0;
        int nEdges = 0;
        Long2IntOpenHashMap inDegreeMap = new Long2IntOpenHashMap(16384);
        Long2IntOpenHashMap outDegreeMap = new Long2IntOpenHashMap(16384);
        Long2IntOpenHashMap degreeMap = new Long2IntOpenHashMap(16384);


        while (vi.hasNext()) {
            long vId = vi.next();
            ++nVertices;

            EdgeIterator ei;

            inDegreeMap.put(vId, vi.getNIncomingEdges());
            outDegreeMap.put(vId, vi.getNOutgoingEdges());

            /*
            int nIncoming = 0;
            ei = vi.getOutgoingEdges();
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
            */

            int nTotal = 0;
            ei = vi.getAllEdges();
            while (ei.hasNext()) {
                ei.next();
                ++nTotal;
            }
            degreeMap.put(vId, nTotal);
            nEdges += nTotal;
        }

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

        System.out.println(new Date() + ": Degrees ended");
        System.out.println("nVertices: " + nVertices + ", nEdges=" + nEdges);
    }



    public void degreeAlgoMT() throws Exception {
        System.out.println(new Date() + ": Degrees starting");
        //VertexIterator vi = _rap.getNewWindowedVertexIterator(Long.MIN_VALUE, Long.MAX_VALUE);
        VertexIterator.MTAllVerticesManager avm = _rap.getNewMTAllVerticesManager(RaphtoryThreadPool.THREAD_POOL);

        Long2IntOpenHashMap[] inDegreesMap = new Long2IntOpenHashMap[_rap.getVertexMgr().nPartitions()];
        Long2IntOpenHashMap[] outDegreesMap = new Long2IntOpenHashMap[_rap.getVertexMgr().nPartitions()];
        Long2IntOpenHashMap[] degreesMap = new Long2IntOpenHashMap[_rap.getVertexMgr().nPartitions()];

        for (int i=0; i<_rap.getVertexMgr().nPartitions(); ++i) {
            inDegreesMap[i] = new Long2IntOpenHashMap(16384);
            outDegreesMap[i] = new Long2IntOpenHashMap(16384);
            degreesMap[i] = new Long2IntOpenHashMap(16384);
        }

        avm.start((id, vi) -> {
            Long2IntOpenHashMap inDegreeMap = inDegreesMap[id];
            Long2IntOpenHashMap outDegreeMap = inDegreesMap[id];
            Long2IntOpenHashMap degreeMap = inDegreesMap[id];

            while (vi.hasNext()) {
                long vId = vi.next();

                EdgeIterator ei;

                inDegreeMap.put(vId, vi.getNIncomingEdges());
                outDegreeMap.put(vId, vi.getNOutgoingEdges());

                /*
                int nIncoming = 0;
                ei = vi.getOutgoingEdges();
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
                */

                int nTotal = 0;
                ei = vi.getAllEdges();
                while (ei.hasNext()) {
                    ei.next();
                    ++nTotal;
                }
                degreeMap.put(vId, nTotal);
            }
        });

        avm.waitTilComplete();

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

        System.out.println(new Date() + ": Degrees ended");
    }


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

        long then = System.currentTimeMillis();
        while ((line = br.readLine())!=null) {
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
        double rate = _rap.getStatistics().getNHistoryPoints() / (((double)(now-then)) / 1000.0d);
        System.out.println(new Date() + ": Ending load: rate=" + rate + " per second");

        System.out.println(_rap.getStatistics());
    }


    private void addVertex(long globalId, long nodeId, long time) {
        long localId = _rap.getLocalEntityIdStore().getLocalNodeId(globalId);
        if (localId==-1L) {
            Vertex v = createVertex(_rap.getVertexMgr().getNextFreeVertexId(), globalId, nodeId, time);
            v.decRefCount();
        }
    }


    private Vertex createVertex(long localId, long globalId, long nodeId, long time) {
        Vertex v = _rap.getVertex();
        v.incRefCount();
        v.reset(localId, globalId, true, time);
        v.getField(NODEID_FIELD).set(nodeId);
        _avpm.addVertex(v);
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

        e.init(_rap.getEdgeMgr().getNextFreeEdgeId(), true, time);

        e.resetEdgeData(srcId, dstId, false, false);
        e.getProperty(PRICE_PROPERTY).set(price);

        _rap.getEdgeMgr().addEdge(e, -1L, -1L);
        EdgePartition ep = _rap.getEdgeMgr().getPartition(_rap.getEdgeMgr().getPartitionId(e.getLocalId()));
        ep.addHistory(e.getLocalId(), time, true, true);

        VertexPartition p = _rap.getVertexMgr().getPartitionForVertex(srcId);
        ep.setOutgoingEdgePtrByEdgeId(e.getLocalId(), p.addOutgoingEdgeToList(e.getSrcVertex(), e.getLocalId(), e.getDstVertex(), false));
        p.addHistory(srcId, time, true, false, e.getLocalId(), true);

        p = _rap.getVertexMgr().getPartitionForVertex(dstId);
        ep.setIncomingEdgePtrByEdgeId(e.getLocalId(), p.addIncomingEdgeToList(e.getDstVertex(), e.getLocalId()));
        p.addHistory(dstId, time, true, false, e.getLocalId(), false);

        e.decRefCount();
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



    /*

    private final int N_LOAD_THREADS = 8;

    public void loadMT(String file) throws Exception {
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

        long then = System.currentTimeMillis();
        while ((line = br.readLine())!=null) {
            String[] fields = line.split(",");

            long srcGlobalId = _rap.getGlobalEntityIdStore().getGlobalNodeId(fields[3]);
            long src = Long.parseLong(fields[3]);

            long dstGlobalId = _rap.getGlobalEntityIdStore().getGlobalNodeId(fields[4]);
            long dst = Long.parseLong(fields[4]);

            long time = Long.parseLong(fields[5]);
            long price = Long.parseLong(fields[7]);

            int srcWorker = Math.abs((int)(srcGlobalId % N_LOAD_THREADS));
            int dstWorker = Math.abs((int)(dstGlobalId % N_LOAD_THREADS));

            addVertex(srcWorker, srcGlobalId, src, time);
            addVertex(dstWorker, dstGlobalId, dst, time);
            addOrUpdateEdge(srcGlobalId, dstGlobalId, time, price);
        }

        long now = System.currentTimeMillis();
        double rate = _rap.getStatistics().getNHistoryPoints() / (((double)(now-then)) / 1000.0d);
        System.out.println(new Date() + ": Ending load: rate=" + rate + " per second");

        System.out.println(_rap.getStatistics());
    }

     */
}