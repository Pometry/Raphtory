/* Copyright (C) Pometry Ltd - All Rights Reserved.
 *
 * This file is proprietary and confidential. Unauthorised
 * copying of this file, via any medium is strictly prohibited.
 *
 */

package com.raphtory.bitcoin;

import com.raphtory.arrowcore.implementation.*;
import com.raphtory.arrowcore.model.Edge;
import com.raphtory.arrowcore.model.Vertex;
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
 * Sample program to load bitcoin data in a reasonably
 * efficient manner, using the arrow layer only.
 */
public class MTBitcoinLoader {
    // Thread-pool to process updates
    private static final int PARALLELISM_LEVEL = Runtime.getRuntime().availableProcessors();
    private static final RaphtoryThreadPool _arrowPool = new RaphtoryThreadPool(PARALLELISM_LEVEL, PARALLELISM_LEVEL);

    private static final int BUFFER_SIZE = 1 * 1024 * 1024; // File buffering
    private static final int BATCH_SIZE = 32768;            // Processing batch size

    // Some counts
    private static final AtomicLong _N_VERTICES = new AtomicLong(0);
    private static final AtomicLong _N_TXHASHES = new AtomicLong(0);
    private static final AtomicLong _N_EDGES = new AtomicLong(0);


    private enum BitcoinFileType {
        ADDRESS, TXHASH, RECEIVED, SENT
    }



    private static class EdgeUpdate {
        private BitcoinFileType _mode;
        private Edge _edge = null;
        private long _globalAddressChainId = -1L;
        private long _globalTxHashId = -1L;
        private long _magicValue = -1L;
        private StringBuilder _txHash = new StringBuilder();
        private StringBuilder _addressChain = new StringBuilder();
        private boolean _isHalt = false;
        private long _amountRaw = -1L;
        private long _timestamp = -1L;
        private long _localNodeId;
        private long _localEdgeId;

        private void reset() {
            _edge = null;
            _txHash.setLength(0);
            _addressChain.setLength(0);
            _globalAddressChainId = -1L;
            _globalTxHashId = -1L;
            _isHalt = false;
            _amountRaw = -1L;
            _timestamp = -1L;
            _localNodeId = -1L;
            _localEdgeId = -1L;
            _magicValue = -1L;
        }
    }


    private class BatchTask implements Runnable {
        private final EdgeUpdate[] _batch = new EdgeUpdate[BATCH_SIZE];
        private Future<?> _task = null;
        private int _batchSize = 0;
        private RaphtoryThreadPool _pool;


        private BatchTask() {
            for (int i = 0; i < _batch.length; ++i) {
                _batch[i] = new EdgeUpdate();
            }
        }


        public synchronized void submit(RaphtoryThreadPool pool) {
            _pool = pool;
            _task = pool.submitTask(this);
        }


        public synchronized void waitTilComplete() {
            if (_task != null) {
                _pool.waitTilComplete(_task);
                _task = null;
            }
        }


        @Override
        public void run() {
            switch (_batch[0]._mode) {
                case ADDRESS:
                    processAddresses(_batch, _batchSize);
                    break;

                case TXHASH:
                    processTxHashes(_batch, _batchSize);
                    break;

                case RECEIVED:
                    processReceivedBatch(_batch, _batchSize);
                    break;

                case SENT:
                    processSentBatch(_batch, _batchSize);
                    break;
            }
        }
    }




    private int _nFilesRead = 0;
    private long _nRowsRead = 0L;
    private final VertexPartitionManager _avpm;
    private final EdgePartitionManager _aepm;
    private final GlobalEntityIdStore _globalEntityIdStore;
    private final LocalEntityIdStore _localEntityIdStore;
    private int _nextBatch = 0;
    private BatchTask[] _batches = new BatchTask[]{new BatchTask(), new BatchTask()};
    private ArrayList<Future<?>> _readTasks = new ArrayList<>();
    private final RaphtoryArrowPartition _rap;

    // Field IDs
    private final int VFIELD_ADDRESS_CHAIN;
    private final int VFIELD_TX_HASH;
    private final int VFIELD_IS_HALT;
    private final int EFIELD_IS_RCVD;
    private final int EFIELD_AMOUNT;
    private final int VPROPERTY_MAGIC;
    private final int EPROPERTY_MAGIC;


    public MTBitcoinLoader(RaphtoryArrowPartition rap) {
        _rap = rap;
        _aepm = _rap.getEdgeMgr();
        _avpm = _rap.getVertexMgr();
        _globalEntityIdStore = _rap.getGlobalEntityIdStore();
        _localEntityIdStore = _rap.getLocalEntityIdStore();

        VFIELD_ADDRESS_CHAIN = _rap.getVertexFieldId("address_chain");
        VFIELD_TX_HASH = _rap.getVertexFieldId("transaction_hash");
        VFIELD_IS_HALT = _rap.getVertexFieldId("is_halt");
        VPROPERTY_MAGIC = _rap.getVertexPropertyId("magic_vertex");

        EFIELD_AMOUNT = _rap.getEdgeFieldId("amount");
        EFIELD_IS_RCVD = _rap.getEdgeFieldId("is_received");
        EPROPERTY_MAGIC = _rap.getEdgePropertyId("magic_edge");
    }


    private boolean readLine(BufferedInputStream br, StringBuilder sb) throws IOException {
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



    public void loadAddresses(String filename) throws IOException {
        StringBuilder tmpSB = new StringBuilder();

        BufferedInputStream br;
        if (filename.endsWith(".csv.gz")) {
            br = new BufferedInputStream(new GZIPInputStream(new FileInputStream(filename), BUFFER_SIZE), BUFFER_SIZE);
        }
        else {
            br = new BufferedInputStream(new FileInputStream(filename), BUFFER_SIZE);
        }

        int partitionId = -1;
        long baseLocalId = 0L;
        int nextIndex = 0;

        StringBuilder line = new StringBuilder(1024);
        int[] fields = new int[2];

        int batchSize = 0;
        BatchTask batch = getNextFreeBatch();

        while (readLine(br, line)) {
            if ((++_nRowsRead & 0x7ffff) == 0) {
                System.out.println(new Date() + ": Vertices read: " + _N_VERTICES.get());
            }

            // Extract the 2 fields...
            for (int i=0; i<fields.length; ++i) {
                if (i == 0) {
                    fields[i] = 0;
                }
                else {
                    int index = line.indexOf(",", fields[i - 1]) + 1;
                    fields[i] = index;
                }
            }

            // Setup the edge-update record
            EdgeUpdate eu = batch._batch[batchSize++];
            eu._mode = BitcoinFileType.ADDRESS;

            copy(line, fields[0], fields[1] - 1 - fields[0], eu._addressChain);
            eu._globalAddressChainId = _globalEntityIdStore.getGlobalNodeId(eu._addressChain);

            StringBuilder sb = copy(line, fields[1], line.length()-fields[1], tmpSB);
            eu._isHalt = sb.charAt(0)=='t' || sb.charAt(0)=='T';
            eu._timestamp = -1L;

            if (partitionId==-1) {
                partitionId = _avpm.getNewPartitionId();
                baseLocalId = (long)partitionId * _avpm.PARTITION_SIZE;
                nextIndex = 0;
            }

            // Assign a local-id for this node
            eu._localNodeId = baseLocalId + nextIndex++;
            _localEntityIdStore.setMapping(eu._globalAddressChainId, eu._localNodeId);
            eu._magicValue = eu._localNodeId * 2;

            _N_VERTICES.getAndIncrement();

            if (batchSize==batch._batch.length || nextIndex>=_avpm.PARTITION_SIZE) {
                // process batch
                BatchTask nextBatch = getNextFreeBatch();
                sendBatch(batch, batchSize);
                batchSize = 0;
                batch = nextBatch;
            }

            if (nextIndex>=_avpm.PARTITION_SIZE) {
                partitionId = -1;
            }
        }

        if (batchSize!=0) {
            // Send the fnal partial batch, if required.
            BatchTask nextBatch = getNextFreeBatch();
            sendBatch(batch, batchSize);
        }

        for (int i=0; i<_batches.length; ++i) {
            _batches[i].waitTilComplete();
        }

        System.out.println("N_VERTICES=" + _N_VERTICES.get());

        _batches = null;
        _readTasks = null;
    }


    public void loadTXHashes(String filename) throws IOException {
        BufferedInputStream br;
        if (filename.endsWith(".csv.gz")) {
            br = new BufferedInputStream(new GZIPInputStream(new FileInputStream(filename), BUFFER_SIZE), BUFFER_SIZE);
        }
        else {
            br = new BufferedInputStream(new FileInputStream(filename), BUFFER_SIZE);
        }

        int partitionId = -1;
        long baseLocalId = 0L;
        int nextIndex = 0;

        StringBuilder line = new StringBuilder(1024);
        int batchSize = 0;
        BatchTask batch = getNextFreeBatch();

        while (readLine(br, line)) {
            if ((++_nRowsRead & 0x7ffff) == 0) {
                System.out.println(new Date() + ": TXHashes read: " + _N_TXHASHES.get());
            }

            long globalTxHashId = _globalEntityIdStore.getGlobalNodeId(line);
            long prevLocalId = _localEntityIdStore.setMapping(globalTxHashId, baseLocalId + nextIndex);
            if (prevLocalId!=-1L) {
                continue;
            }

            if (partitionId==-1) {
                partitionId = _avpm.getNewPartitionId();
                baseLocalId = (long)partitionId * _avpm.PARTITION_SIZE;
                nextIndex = 0;
            }

            // Build the edge-update record
            EdgeUpdate eu = batch._batch[batchSize++];
            eu._mode = BitcoinFileType.TXHASH;
            eu._txHash.setLength(0);
            eu._txHash.append(line);
            eu._globalTxHashId = globalTxHashId;
            eu._timestamp = -1L;
            eu._localNodeId = baseLocalId + nextIndex++; // Allocate the local-id
            eu._magicValue = eu._localNodeId * 2;
            eu._isHalt = false;

            _N_TXHASHES.getAndIncrement();

            if (batchSize==batch._batch.length || nextIndex>=_avpm.PARTITION_SIZE) {
                // Process a complete batch
                BatchTask nextBatch = getNextFreeBatch();
                sendBatch(batch, batchSize);
                batchSize = 0;
                batch = nextBatch;
            }

            if (nextIndex>=_avpm.PARTITION_SIZE) {
                partitionId = -1;
            }
        }

        if (batchSize!=0) {
            // Process the final partial batch, if required
            getNextFreeBatch();
            sendBatch(batch, batchSize);
        }

        for (int i=0; i<_batches.length; ++i) {
            _batches[i].waitTilComplete();
        }

        System.out.println("N_TXHASHES=" + _N_TXHASHES.get());

        _batches = null;
        _readTasks = null;
    }


    public void loadReceived(String filename) throws IOException {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        BatchTask batch = getNextFreeBatch();
        int batchSize = 0;

        System.out.println("Loading: " + filename);

        BufferedInputStream br;
        if (filename.endsWith(".csv.gz")) {
            br = new BufferedInputStream(new GZIPInputStream(new FileInputStream(filename), BUFFER_SIZE), BUFFER_SIZE);
        }
        else {
            br = new BufferedInputStream(new FileInputStream(filename), BUFFER_SIZE);
        }

        int partitionId = -1;
        long baseLocalId = 0L;
        int nextIndex = 0;

        StringBuilder line = new StringBuilder(1024);
        int[] fields = new int[5];
        StringBuilder valueSB = new StringBuilder();

        while (readLine(br, line)) {
            if ((++_nRowsRead & 0x7ffff) == 0) {
                System.out.println(new Date() + ": Edges read: " + _N_EDGES);
            }

            // Split into fields
            for (int i=0; i<fields.length; ++i) {
                if (i == 0) {
                    fields[i] = 0;
                }
                else {
                    int index = line.indexOf(",", fields[i - 1]) + 1;
                    fields[i] = index;
                }
            }

            // Setup the edge-update record
            EdgeUpdate eu = batch._batch[batchSize++];
            eu._mode = BitcoinFileType.RECEIVED;

            copy(line, fields[0], fields[1] - 1 - fields[0], eu._txHash);
            eu._globalTxHashId = _globalEntityIdStore.getGlobalNodeId(eu._txHash);

            copy(line, fields[1], fields[2] - 1 - fields[1], eu._addressChain);
            eu._globalAddressChainId = _globalEntityIdStore.getGlobalNodeId(eu._addressChain);

            StringBuilder v = copy(line, fields[2], fields[3] - 1 - fields[2], valueSB);
            eu._amountRaw = StringUtils.parseLong(v,10);

            v = copy(line, fields[4], line.length() - fields[4], valueSB);
            //System.out.println(v);
            eu._timestamp = LocalDateTime.parse(v, formatter).toEpochSecond(ZoneOffset.UTC) * 1000L;

            if (partitionId==-1) {
                partitionId = _aepm.getNewPartitionId();
                baseLocalId = (long)partitionId * _aepm.PARTITION_SIZE;
                nextIndex = 0;
            }
            eu._localEdgeId = baseLocalId + nextIndex++;
            eu._magicValue = eu._localEdgeId * 2;

            _N_EDGES.incrementAndGet();

            //System.out.println(eu);
            if (batchSize==batch._batch.length || nextIndex>=_aepm.PARTITION_SIZE) {
                // Prcess the batch
                BatchTask nextBatch = getNextFreeBatch();
                sendBatch(batch, batchSize);
                batchSize = 0;
                batch = nextBatch;
            }

            if (nextIndex>=_aepm.PARTITION_SIZE) {
                partitionId = -1;
            }
        }

        if (batchSize != 0) {
            // Process the final partial batch, if required.
            getNextFreeBatch();
            sendBatch(batch, batchSize);
        }

        System.out.println("N_EDGES: " + _N_EDGES.get());
    }


    public void loadSent(String filename) throws IOException {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        BatchTask batch = getNextFreeBatch();
        int batchSize = 0;

        System.out.println("Loading: " + filename);

        BufferedInputStream br;
        if (filename.endsWith(".csv.gz")) {
            br = new BufferedInputStream(new GZIPInputStream(new FileInputStream(filename), BUFFER_SIZE), BUFFER_SIZE);
        }
        else {
            br = new BufferedInputStream(new FileInputStream(filename), BUFFER_SIZE);
        }

        int partitionId = -1;
        long baseLocalId = 0L;
        int nextIndex = 0;

        StringBuilder line = new StringBuilder(1024);
        int[] fields = new int[5];
        StringBuilder valueSB = new StringBuilder();

        while (readLine(br, line)) {
            if ((++_nRowsRead & 0x7ffff) == 0) {
                System.out.println(new Date() + ": Edges read: " + _N_EDGES);
            }

            // Generate the fields
            for (int i=0; i<fields.length; ++i) {
                if (i == 0) {
                    fields[i] = 0;
                }
                else {
                    int index = line.indexOf(",", fields[i - 1]) + 1;
                    fields[i] = index;
                }
            }

            // Set up the edge-update record
            EdgeUpdate eu = batch._batch[batchSize++];
            eu._mode = BitcoinFileType.SENT;

            copy(line, fields[0], fields[1] - 1 - fields[0], eu._addressChain);
            eu._globalAddressChainId = _globalEntityIdStore.getGlobalNodeId(eu._addressChain);

            copy(line, fields[1], fields[2] - 1 - fields[1], eu._txHash);
            eu._globalTxHashId = _globalEntityIdStore.getGlobalNodeId(eu._txHash);

            StringBuilder v = copy(line, fields[2], fields[3] - 1 - fields[2], valueSB);
            eu._amountRaw = StringUtils.parseLong(v,10);

            v = copy(line, fields[4], line.length() - fields[4], valueSB);
            //System.out.println(v);
            eu._timestamp = LocalDateTime.parse(v, formatter).toEpochSecond(ZoneOffset.UTC) * 1000L;

            if (partitionId==-1) {
                partitionId = _aepm.getNewPartitionId();
                baseLocalId = (long)partitionId * _aepm.PARTITION_SIZE;
                nextIndex = 0;
            }
            eu._localEdgeId = baseLocalId + nextIndex++;
            eu._magicValue = eu._localEdgeId * 2;

            _N_EDGES.incrementAndGet();

            //System.out.println(eu);
            if (batchSize==batch._batch.length || nextIndex>=_aepm.PARTITION_SIZE) {
                // Process the batch
                BatchTask nextBatch = getNextFreeBatch();
                sendBatch(batch, batchSize);
                batchSize = 0;
                batch = nextBatch;
            }

            if (nextIndex>=_aepm.PARTITION_SIZE) {
                partitionId = -1;
            }
        }

        if (batchSize != 0) {
            // Process the final, partial batch if required.
            getNextFreeBatch();
            sendBatch(batch, batchSize);
        }

        System.out.println("N_EDGES: " + _N_EDGES.get());
    }


    // There can only be 1 batch being processed per arrow file...
    // We allow a batch to be built in parallel to the previous
    // batch being processed -- but the new batch must wait until
    // the previous one has completed processing before it's submitted.
    private BatchTask getNextFreeBatch() {
        int thisBatch = _nextBatch;
        BatchTask batch = _batches[thisBatch];

        _nextBatch = (_nextBatch+1) % _batches.length;

        batch.waitTilComplete();
        return batch;
    }


    private void sendBatch(BatchTask batch, int batchSize) {
        batch._batchSize = batchSize;
        batch.submit(_arrowPool);
    }


    // Process a batch of address records
    private void processAddresses(EdgeUpdate[] batch, int batchSize) {
        for (int i=0; i<batchSize; ++i) {
            EdgeUpdate eu = batch[i];

            long globalAddressChainId = eu._globalAddressChainId;
            boolean isLocal = isVertexLocal(globalAddressChainId);

            if (!isLocal) {
                eu.reset();
                continue;
            }

            long localVertexId = eu._localNodeId;

            // Sources are *not* expected to exist!
            Vertex src = _avpm.getVertex(localVertexId);
            if (src == null) {
                src = _rap.getVertex();
                src.incRefCount();

                src.reset(localVertexId, globalAddressChainId, true, eu._timestamp);
                src.getField(VFIELD_ADDRESS_CHAIN).set(eu._addressChain);
                src.getField(VFIELD_IS_HALT).set(eu._isHalt);

                src.getProperty(VPROPERTY_MAGIC).setHistory(true, System.currentTimeMillis()).set(eu._magicValue);

                _avpm.addVertex(src);
            }
            else {
                // A pre-existing source may only have the halt-flag updated.
                src.getField(VFIELD_IS_HALT).set(eu._isHalt);
                _avpm.addVertex(src);
            }

            src.decRefCount();

            eu.reset();
        }
    }


    // Process a txhash batch
    private void processTxHashes(EdgeUpdate[] batch, int batchSize) {
        for (int i=0; i<batchSize; ++i) {
            EdgeUpdate eu = batch[i];

            long globalTxHashId = eu._globalTxHashId;
            boolean isLocal = isVertexLocal(globalTxHashId);

            if (!isLocal) {
                eu.reset();
                continue;
            }

            long localVertexId = eu._localNodeId;

            // Sources are not expected to exist
            Vertex src = _avpm.getVertex(localVertexId); // MT
            if (src == null) {
                src = _rap.getVertex();
                src.incRefCount();

                src.reset(localVertexId, globalTxHashId, true, eu._timestamp);
                src.getField(VFIELD_TX_HASH).set(eu._txHash);
                src.getField(VFIELD_IS_HALT).set(eu._isHalt);
                src.getProperty(VPROPERTY_MAGIC).setHistory(true, System.currentTimeMillis()).set(eu._magicValue);

                _avpm.addVertex(src); // MT
            }
            else {
                // A pre-existing source may only have the halt-flag updated.
                src.getField(VFIELD_IS_HALT).set(eu._isHalt);
                _avpm.addVertex(src); // MT
            }

            src.decRefCount();

            eu.reset();
        }
    }


    // Process a batch of received transactions
    private void processReceivedBatch(EdgeUpdate[] batch, int batchSize) {
        int nMissingTXH = 0;
        int nMissingAC = 0;

        for (int i=0; i<batchSize; ++i) {
            EdgeUpdate eu = batch[i];

            long globalTxHashVertexId = eu._globalTxHashId;
            boolean txHashIsLocal = isVertexLocal(globalTxHashVertexId);

            long globalAddressChainVertexId = eu._globalAddressChainId;
            boolean addressChainIsLocal = isVertexLocal(globalAddressChainVertexId);

            if (!txHashIsLocal && !addressChainIsLocal) {
                continue;
            }

            long localTxHashVertexId = _localEntityIdStore.getLocalNodeId(globalTxHashVertexId);
            if (localTxHashVertexId==-1L) {
                ++nMissingTXH;
                //System.out.println("'" + eu._txHash + "'");
            }

            long localAddressChainVertexId = _localEntityIdStore.getLocalNodeId(globalAddressChainVertexId);
            if (localAddressChainVertexId==-1L) {
                ++nMissingAC;
                //System.out.println("'" + eu._addressChain + "'");
            }

            if (localAddressChainVertexId==-1L || localTxHashVertexId==-1L) {
                // Missing data...
                continue;
            }

            // Add the actual edge...
            Edge edge = getNewEdge(eu._localEdgeId, eu._timestamp);
            edge.resetEdgeData(-1L, -1L, -1L, -1L, false, false);
            edge.getField(EFIELD_IS_RCVD).set(true);
            edge.getField(EFIELD_AMOUNT).set(eu._amountRaw);
            edge.getProperty(EPROPERTY_MAGIC).set(eu._magicValue);
            edge.incRefCount();
            eu._edge = edge;
            eu._edge.resetEdgeData(txHashIsLocal ? localTxHashVertexId : globalTxHashVertexId, addressChainIsLocal ? localAddressChainVertexId : globalAddressChainVertexId, -1L, -1L, !txHashIsLocal, !addressChainIsLocal);
            _aepm.addEdge(eu._edge, -1L, -1L);
            _aepm.addHistory(eu._localEdgeId, eu._timestamp, true, true);

            if (txHashIsLocal) {
                VertexPartition p = _avpm.getPartition(_avpm.getPartitionId(localTxHashVertexId));
                long prevListPtr;

                // Add the new edge to the tx-hash node
                synchronized (p) {
                    prevListPtr = p.addOutgoingEdgeToList(eu._edge.getSrcVertex(), eu._edge.getLocalId(), eu._edge.getDstVertex());
                    p.addHistory(localTxHashVertexId, eu._timestamp, true, false, eu._edge.getLocalId(), true); // MT
                }

                // Update the prev outgoing ptr on the new edge
                _aepm.setOutgoingEdgePtr(eu._edge.getLocalId(), prevListPtr);
            }

            if (addressChainIsLocal) {
                VertexPartition p = _avpm.getPartition(_avpm.getPartitionId(localAddressChainVertexId));
                long prevListPtr;

                // Add the new edge to the address-chain node
                synchronized (p) {
                    prevListPtr = p.addIncomingEdgeToList(localAddressChainVertexId, eu._edge.getLocalId());
                    p.addHistory(localAddressChainVertexId, eu._timestamp, true, false, eu._edge.getLocalId(), false); // MT
                }

                // Update the prev incoming ptr on the new edge
                _aepm.setIncomingEdgePtr(eu._edge.getLocalId(), prevListPtr);
            }
        }

        for (int i=0; i<batch.length; ++i) {
            if (batch[i]._edge != null) {
                batch[i]._edge.decRefCount();
            }
            batch[i].reset();
        }

        if (nMissingTXH!=0 || nMissingAC!=0) {
            System.out.println("RECEIVED: missing TXH=" + nMissingTXH + ", missing AC=" + nMissingAC + " of " + (batchSize * 2) + "!");
        }
    }


    // Process a batch of sent transactions
    private void processSentBatch(EdgeUpdate[] batch, int batchSize) {
        int nMissingTXH = 0;
        int nMissingAC = 0;

        for (int i=0; i<batchSize; ++i) {
            EdgeUpdate eu = batch[i];

            long globalTxHashVertexId = eu._globalTxHashId;
            boolean txHashIsLocal = isVertexLocal(globalTxHashVertexId);

            long globalAddressChainVertexId = eu._globalAddressChainId;
            boolean addressChainIsLocal = isVertexLocal(globalAddressChainVertexId);

            if (!txHashIsLocal && !addressChainIsLocal) {
                continue;
            }

            long localTxHashVertexId = _localEntityIdStore.getLocalNodeId(globalTxHashVertexId);
            if (localTxHashVertexId==-1L) {
                ++nMissingTXH;
            }

            long localAddressChainVertexId = _localEntityIdStore.getLocalNodeId(globalAddressChainVertexId);
            if (localAddressChainVertexId==-1L) {
                ++nMissingAC;
            }

            if (localAddressChainVertexId==-1L || localTxHashVertexId==-1L) {
                // Missing data...
                continue;
            }

            // Add the actual edge...
            Edge edge = getNewEdge(eu._localEdgeId, eu._timestamp);
            edge.resetEdgeData(-1L, -1L, -1L, -1L, false, false);
            edge.getField(EFIELD_IS_RCVD).set(false);
            edge.getField(EFIELD_AMOUNT).set(eu._amountRaw);
            edge.getProperty(EPROPERTY_MAGIC).set(eu._magicValue);
            edge.incRefCount();
            eu._edge = edge;
            eu._edge.resetEdgeData(addressChainIsLocal ? localAddressChainVertexId : globalAddressChainVertexId, txHashIsLocal ? localTxHashVertexId : globalTxHashVertexId, -1L, -1L, !addressChainIsLocal, !txHashIsLocal);
            _aepm.addEdge(eu._edge, -1L, -1L);
            _aepm.addHistory(eu._localEdgeId, eu._timestamp, true, true);

            if (txHashIsLocal) {
                VertexPartition p = _avpm.getPartition(_avpm.getPartitionId(localTxHashVertexId));
                long prevListPtr;

                // Add the new edge to the tx-hash node
                synchronized (p) {
                    prevListPtr = p.addIncomingEdgeToList(localTxHashVertexId, eu._edge.getLocalId());
                    p.addHistory(localTxHashVertexId, eu._timestamp, true, false, eu._edge.getLocalId(), false); // MT
                }

                // Update the incoming prev ptr on the new edge
                _aepm.setIncomingEdgePtr(eu._edge.getLocalId(), prevListPtr);
            }

            if (addressChainIsLocal) {
                VertexPartition p = _avpm.getPartition(_avpm.getPartitionId(localAddressChainVertexId));
                long prevListPtr;

                // Add the new edge to the address-chain node
                synchronized (p) {
                    prevListPtr = p.addOutgoingEdgeToList(eu._edge.getSrcVertex(), eu._edge.getLocalId(), eu._edge.getDstVertex());
                    p.addHistory(localAddressChainVertexId, eu._timestamp, true, false, eu._edge.getLocalId(), true); // MT
                }

                // Update the outgoing prev ptr on the new edge
                _aepm.setOutgoingEdgePtr(eu._edge.getLocalId(), prevListPtr);
            }
        }

        for (int i=0; i<batch.length; ++i) {
            if (batch[i]._edge != null) {
                batch[i]._edge.decRefCount();
            }
            batch[i].reset();
        }

        if (nMissingTXH!=0 || nMissingAC!=0) {
            System.out.println("SENT missing TXH=" + nMissingTXH + ", missing AC=" + nMissingAC + " of " + (batchSize * 2) + "!");
        }
    }


    // Create a new edge
    private Edge getNewEdge(long edgeId, long time) {
        Edge it = _rap.getEdge();
        it.init(edgeId, true, time);
        return it;
    }


    private boolean isVertexLocal(long id) {
        return true;
    }


    // Load and process the files in question
    private void loadAllFiles(File dir, String name, int nFilesToLoad) throws Exception {
        for (File f : dir.listFiles()) {
            if (nFilesToLoad > 0 && _nFilesRead >= nFilesToLoad) {
                return;
            }

            if (f.isDirectory()) {
                loadAllFiles(f, name, nFilesToLoad);
            }

            String filename = f.getName();
            if (filename.startsWith(name) && (filename.endsWith(".csv") || filename.endsWith(".csv.gz"))) {
                if (filename.startsWith("address")) {
                    _readTasks.add(RaphtoryThreadPool.THREAD_POOL.submitTask(() -> {
                        try {
                            new MTBitcoinLoader(_rap).loadAddresses(f.getAbsolutePath());
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                        }
                    }));

                    ++_nFilesRead;
                }
                else if (filename.startsWith("txhash")) {
                    _readTasks.add(RaphtoryThreadPool.THREAD_POOL.submitTask(() -> {
                        try {
                            new MTBitcoinLoader(_rap).loadTXHashes(f.getAbsolutePath());
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                        }
                    }));

                    ++_nFilesRead;
                }
                else if (filename.startsWith("received")) {
                    _readTasks.add(RaphtoryThreadPool.THREAD_POOL.submitTask(() -> {
                        try {
                            new MTBitcoinLoader(_rap).loadReceived(f.getAbsolutePath());
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                        }
                    }));

                    ++_nFilesRead;
                }
                else if (filename.startsWith("sent")) {
                    _readTasks.add(RaphtoryThreadPool.THREAD_POOL.submitTask(() -> {
                        try {
                            new MTBitcoinLoader(_rap).loadSent(f.getAbsolutePath());
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                        }
                    }));

                    ++_nFilesRead;
                }
            }
        }
    }


    // Simple function to log the global and local node-ids for debug purposes
    private void dumpGlobalAndLocalNodeIds(String dst, String mode) throws IOException {
        long then = System.currentTimeMillis();
        long nNodes = 0L;

        BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream(new File(dst, mode+"-NODE-IDS.txt")), 4*1024*1024);
        StringBuilder sb = new StringBuilder();

        VertexIterator.AllVerticesIterator iter = _rap.getNewAllVerticesIterator();
        while (iter.hasNext()) {
            long localNodeId = iter.next();
            long globalNodeId = iter.getGlobalVertexId();

            if (_localEntityIdStore.getLocalNodeId(globalNodeId) != localNodeId) {
                System.out.println("NODE MAP INCORRECT");
            }
            ++nNodes;

            sb.setLength(0);
            sb.append(globalNodeId).append(", ").append(localNodeId).append(", ");
            Vertex v = iter.getVertex();

            sb.append(v.getField(VFIELD_ADDRESS_CHAIN).getString()).append(", ").append(v.getField(VFIELD_TX_HASH).getString());
            sb.append("\n");
            v.decRefCount();
            os.write(sb.toString().getBytes());
        }

        long now = System.currentTimeMillis();
        System.out.println(new Date() + ": SYNC IDs: " + nNodes + " nodes in " + (now-then) + "ms");
        os.close();
    }


    public static void main(String[] args) throws Exception {
        if (args == null || args.length < 1) {
            System.err.println("Usage: BitcoinLoader BITCOIN_SRC_DIR ARROW_DST_DIR");
            System.err.println("Note, the dst dir should be empty - and no files will be created!");
            System.exit(1);
        }

        String src = args[0];
        String dst = args[1];

        RaphtoryArrowPartition.RaphtoryArrowPartitionConfig cfg = new RaphtoryArrowPartition.RaphtoryArrowPartitionConfig();
        cfg._propertySchema = new BitcoinSchema();
        cfg._arrowDir = dst;
        cfg._raphtoryPartitionId = 0;
        cfg._nRaphtoryPartitions = 1;
        cfg._nLocalEntityIdMaps = 128;
        cfg._localEntityIdMapSize = 1 * 1024 * 1024;
        cfg._syncIDMap = true;

        RaphtoryArrowPartition rap = new RaphtoryArrowPartition(cfg);

        EdgePartitionManager emgr = rap.getEdgeMgr();
        VertexPartitionManager vmgr = rap.getVertexMgr();
        GlobalEntityIdStore globalEntityIdStore = rap.getGlobalEntityIdStore();
        LocalEntityIdStore localEntityIdStore = rap.getLocalEntityIdStore();

        MTBitcoinLoader loader = new MTBitcoinLoader(rap);

        System.out.println(new Date() + ": BITCOIN LOADING: STARTING");

        File dir = new File(src);

        File testFile = new File(dst, "vertex-p0.rap");
        if (!testFile.exists()) {
            loader._nFilesRead = 0;
            loader.loadAllFiles(dir, "address", 0);
            RaphtoryThreadPool.THREAD_POOL.waitTilComplete(loader._readTasks);
            loader._readTasks.clear();
            System.out.println(new Date() + ": FINISHED PROCESSING ADDRESSES");

            loader._nFilesRead = 0;
            loader.loadAllFiles(dir, "txhash", 0);
            RaphtoryThreadPool.THREAD_POOL.waitTilComplete(loader._readTasks);
            loader._readTasks.clear();
            System.out.println(new Date() + ": FINISHED PROCESSING TXHASHES");

            //loader.dumpGlobalAndLocalNodeIds("CREATING");

            loader._nFilesRead = 0;
            loader._readTasks.clear();
            loader.loadAllFiles(dir, "received", 0);
            RaphtoryThreadPool.THREAD_POOL.waitTilComplete(loader._readTasks);
            loader._nFilesRead = 0;
            loader.loadAllFiles(dir, "sent", 0);
            RaphtoryThreadPool.THREAD_POOL.waitTilComplete(loader._readTasks);

            System.out.println(new Date() + ": BITCOIN LOADING: FINISHED");
        }
        else {
            vmgr.loadFiles();
            System.out.println(new Date() + ": FINISHED LOADING VERTICES");
            loader.dumpGlobalAndLocalNodeIds(dst, "READING");

            emgr.loadFiles();
            System.out.println(new Date() + ": FINISHED LOADING EDGES");
        }

        System.out.println(new Date() + ": BITCOIN SAVING: STARTED");
        vmgr.saveFiles();
        vmgr.close();

        emgr.saveFiles();
        emgr.close();
        System.out.println(new Date() + ": BITCOIN SAVING: ENDED");

        localEntityIdStore.close();
        globalEntityIdStore.close();

        System.exit(0);
    }
}