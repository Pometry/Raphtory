package com.raphtory.arrowcore;

import com.raphtory.arrowcore.implementation.*;
import com.raphtory.arrowcore.model.Edge;
import com.raphtory.arrowcore.model.PropertySchema;
import com.raphtory.arrowcore.model.Vertex;
import net.openhft.chronicle.core.util.StringUtils;

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
        cfg._arrowDir = "/tmp";
        cfg._raphtoryPartitionId = 0;
        cfg._nRaphtoryPartitions = 1;
        cfg._nLocalEntityIdMaps = 128;
        cfg._localEntityIdMapSize = 1024;
        cfg._syncIDMap = false;

        RaphtoryArrowPartition rap = new RaphtoryArrowPartition(cfg);

        AlphaBayLoader loader = new AlphaBayLoader(rap);
        loader.load("D:\\home\\jatinder\\Pometry\\public\\Raphtory\\alphabay_sorted.csv.gz");
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


    public void load2(String file) throws Exception {
        InputStream br;

        if (file.endsWith(".gz")) {
            br = new BufferedInputStream(new GZIPInputStream(new BufferedInputStream(new FileInputStream(file), 65536), 65536), 65536);
        }
        else {
            br = new BufferedInputStream(new FileInputStream(file), 65536);
        }
        String line;
        StringBuilder tmp = new StringBuilder();

        System.out.println(new Date() + ": Starting load");

        int[] fields = new int[8];

        long then = System.currentTimeMillis();

        int count = 0;
        StringBuilder lineSB = new StringBuilder();
        while (readLine(br, lineSB)) {
            // Extract the 2 fields...
            for (int i = 0; i < fields.length; ++i) {
                if (i == 0) {
                    fields[i] = 0;
                }
                else {
                    int index = lineSB.indexOf(",", fields[i - 1]) + 1;
                    fields[i] = index;
                }
            }


            copy(lineSB, fields[3], fields[4] - 1 - fields[3], tmp);
            long src = StringUtils.parseLong(tmp, 10);

            copy(lineSB, fields[4], fields[5] - 1 - fields[4], tmp);
            long dst = StringUtils.parseLong(tmp, 10);

            copy(lineSB, fields[5], fields[6] - 1 - fields[5], tmp);
            long time = StringUtils.parseLong(tmp, 10);

            copy(lineSB, fields[7], lineSB.length() - fields[7], tmp);
            long price = StringUtils.parseLong(tmp, 10);

            addVertex(src, time);
            addVertex(dst, time);
            addOrUpdateEdge(src, dst, time, price);
        }

        long now = System.currentTimeMillis();
        double rate = _rap.getStatistics().getNHistoryPoints() / (((double)(now-then)) / 1000.0d);
        System.out.println(new Date() + ": Ending load: rate=" + rate + " per second");

        System.out.println(_rap.getStatistics());
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

            long src = Long.parseLong(fields[3]);
            long dst = Long.parseLong(fields[4]);
            long time = Long.parseLong(fields[5]);
            long price = Long.parseLong(fields[7]);

            //tmp.setLength(0);
            //tmp.append(fields[3]);
            //long srcId = _rap.getGlobalEntityIdStore().getGlobalNodeId(tmp);

            //tmp.setLength(0);
            //tmp.append(fields[4]);
            //long dstId = _rap.getGlobalEntityIdStore().getGlobalNodeId(tmp);

            addVertex(src, time);
            addVertex(dst, time);
            addOrUpdateEdge(src, dst, time, price);
        }

        long now = System.currentTimeMillis();
        double rate = _rap.getStatistics().getNHistoryPoints() / (((double)(now-then)) / 1000.0d);
        System.out.println(new Date() + ": Ending load: rate=" + rate + " per second");

        System.out.println(_rap.getStatistics());
    }


    private void addVertex(long globalId, long time) {
        long localId = _rap.getLocalEntityIdStore().getLocalNodeId(globalId);
        if (localId==-1L) {
            Vertex v = createVertex(_rap.getVertexMgr().getNextFreeVertexId(), globalId, time);
            v.decRefCount();
        }
    }


    private Vertex createVertex(long localId, long globalId, long time) {
        Vertex v = _rap.getVertex();
        v.incRefCount();
        v.reset(localId, globalId, true, time);
        v.getField(NODEID_FIELD).set(globalId);
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
}