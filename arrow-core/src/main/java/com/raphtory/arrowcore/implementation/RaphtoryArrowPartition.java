/* Copyright (C) Pometry Ltd - All Rights Reserved.
 *
 * This file is proprietary and confidential. Unauthorised
 * copying of this file, via any medium is strictly prohibited.
 *
 */

package com.raphtory.arrowcore.implementation;

import com.raphtory.arrowcore.model.Edge;
import com.raphtory.arrowcore.model.PropertySchema;
import com.raphtory.arrowcore.model.Vertex;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.File;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class holds all of the major components that make up
 * a Raphtory Arrow Partition, such as a vertex manager, the schema
 * in use etc.
 *<p>
 * This is the main entry point into using Arrow data with Raphtory.
 */
public class RaphtoryArrowPartition {
    /**
     * Configuration details for the RaphtoryArrowPartition.
     */
    public static class RaphtoryArrowPartitionConfig {
        /**
         * Schema description
         */
        public PropertySchema _propertySchema;

        /**
         * Location of arrow files
         */
        public String _arrowDir;

        /**
         * This instance's Raphtory partition-id.
         * (Raphtory normally runs with 1 instance per machine)
         */
        public int _raphtoryPartitionId;

        /**
         * The total number of Raphtory partitions
         */
        public int _nRaphtoryPartitions;

        /**
         * The number of entity-id maps to create.
         * The higher the number the higher the level of concurrency you *may* achieve,
         * but it's not sensible to go way over the number of cores on the machine.
         *
         * Should be a power of 2.
         */
        public int _nLocalEntityIdMaps = 256;

        /**
         * The initial size of each local-entity-id map.
         *
         * Should be a power of 2.
         *
         * TODO: Investigate how useful this value actually is.
         */
        public int _localEntityIdMapSize = 1024;

        /**
         * If true, then the local-entity-id map will be set on startup.
         *
         * Many applications don't require the mapping of global-id to
         * local-id and so this is optional.
         */
        public boolean _syncIDMap = true;

        /**
         * Maximum number of vertices per arrow file
         */
        public int _vertexPartitionSize = 1024 * 1024;

        /**
         * Maximum number of edges per arrow file
         */
        public int _edgePartitionSize = 2 * 1024 * 1024;

        /**
         * Enable or disable arrow bounds checking
         */
        public boolean _disableBoundsCheck = true;
    }


    private final ThreadLocal<ArrayList<Vertex>> _vertexPool = ThreadLocal.withInitial(ArrayList::new);
    private final ThreadLocal<ArrayList<Edge>> _edgePool = ThreadLocal.withInitial(ArrayList::new);

    private final ThreadLocal<EntityFieldAccessor[]> _vertexAccessorsPool = ThreadLocal.withInitial(this::createVertexEntityFieldAccessors);
    private final ThreadLocal<EntityFieldAccessor[]> _edgeAccessorsPool = ThreadLocal.withInitial(this::createEdgeEntityFieldAccessors);


    protected final PropertySchema _propertySchema;
    private final EdgePartitionManager _emgr;
    private final VertexPartitionManager _vmgr;

    private final GlobalEntityIdStore _globalEntityIdStore;
    private final LocalEntityIdStore _localEntityIdStore;

    private final int _nRaphtoryPartitions;
    private final int _raphtoryPartitionId;

    protected final Schema _arrowEdgeSchema;
    protected final Schema _arrowVertexSchema;
    protected final boolean _syncIDMap;

    protected final int _vertexPartitionSize;
    protected final int _vertexPartitionSize_shift;
    protected final int _vertexPartitionSize_divisor;
    protected final int _vertexPartitionSize_mask;

    protected final int _edgePartitionSize;
    protected final int _edgePartitionSize_shift;
    protected final int _edgePartitionSize_divisor;
    protected final int _edgePartitionSize_mask;

    private final RaphtoryStatistics _stats;


    /**
     * Creates a new RaphtoryArrowPartition configured accordingly.
     *
     * @param config the configuration to use
     */
    public RaphtoryArrowPartition(RaphtoryArrowPartitionConfig config) {
        _propertySchema = config._propertySchema;
        _raphtoryPartitionId = config._raphtoryPartitionId;
        _nRaphtoryPartitions = config._nRaphtoryPartitions;

        System.getProperties().setProperty("arrow.enable_unsafe_memory_access", config._disableBoundsCheck ? "true" : "false");
        System.getProperties().setProperty("arrow.enable_null_check_for_get", config._disableBoundsCheck ? "false" : "true");

        _syncIDMap = config._syncIDMap;
        _globalEntityIdStore = new GlobalEntityIdStore();
        _localEntityIdStore = new LocalEntityIdStore(config._arrowDir, config._nLocalEntityIdMaps, config._localEntityIdMapSize);

        _arrowEdgeSchema = createEdgeSchema();
        _arrowVertexSchema = createVertexSchema();

        _vertexPartitionSize = roundUpToPowerOfTwo(config._vertexPartitionSize);
        _vertexPartitionSize_shift = Integer.numberOfTrailingZeros(_vertexPartitionSize);
        _vertexPartitionSize_divisor = 1 << _vertexPartitionSize_shift;
        _vertexPartitionSize_mask = _vertexPartitionSize_divisor - 1;

        _edgePartitionSize = roundUpToPowerOfTwo(config._edgePartitionSize);
        _edgePartitionSize_shift = Integer.numberOfTrailingZeros(_edgePartitionSize);
        _edgePartitionSize_divisor = 1 << _edgePartitionSize_shift;
        _edgePartitionSize_mask = _edgePartitionSize_divisor - 1;

        _emgr = new EdgePartitionManager(this, new File(config._arrowDir), RaphtoryThreadPool.THREAD_POOL);
        _vmgr = new VertexPartitionManager(this, new File(config._arrowDir), RaphtoryThreadPool.THREAD_POOL);

        _stats = new RaphtoryStatistics(this);
    }


    /**
     * Closes this Raphtory arrow partition, closing all
     * vertex and edge arrow files, along with the
     * id stores.
     */
    public void close() {
        _emgr.saveFiles();
        _vmgr.saveFiles();
        _emgr.close();
        _vmgr.close();
        _globalEntityIdStore.close();
        _localEntityIdStore.close();
    }


    /**
     * Creates the Arrow schema for edges.
     *<p>
     * This consists of the base edge schema fields, any user-defined fields and then
     * the linked-list pointers to the heads of the history lists for each user-defined
     * property.
     *
     * @return the new edge schema
     */
    private Schema createEdgeSchema() {
        List<Field> fields = EdgeArrowStore.createEdgeFields();

        ArrayList<NonversionedField> moreFields = _propertySchema.nonversionedEdgeProperties();
        if (moreFields != null) {
            int n = moreFields.size();
            for (int i = 0; i < n; ++i) {
                NonversionedField f = moreFields.get(i);
                f.setParentSchemaFieldIndex(fields.size());
                f.addToSchema(fields);
            }
        }

        ArrayList<VersionedProperty> properties = _propertySchema.versionedEdgeProperties();
        if (properties != null) {
            int n = properties.size();
            for (int i = 0; i < n; ++i) {
                VersionedProperty f = properties.get(i);
                f.setPrevPtrFieldIndexInEntity(fields.size());
                Field prevPtrField = new Field("prop_prev_ptr_" + i, new FieldType(false, new ArrowType.Int(32, true), null), null);
                fields.add(prevPtrField);
            }
        }

        return new Schema(fields);
    }


    /**
     * Creates the Arrow schema for vertices.
     *<p>
     * This consists of the base vertex schema fields, any user-defined fields and then
     * the linked-list pointers to the heads of the history lists for each user-defined
     * property.
     *
     * @return the new vertex schema
     */
    private Schema createVertexSchema() {
        ArrayList<Field> fields = VertexArrowStore.createVertexFields();

        ArrayList<NonversionedField> moreFields = _propertySchema.nonversionedVertexProperties();
        if (moreFields != null) {
            int n = moreFields.size();
            for (int i = 0; i < n; ++i) {
                NonversionedField f = moreFields.get(i);
                f.setParentSchemaFieldIndex(fields.size());
                f.addToSchema(fields);
            }
        }

        ArrayList<VersionedProperty> properties = _propertySchema.versionedVertexProperties();
        if (properties != null) {
            int n = properties.size();
            for (int i=0; i<n; ++i) {
                VersionedProperty f = properties.get(i);
                f.setPrevPtrFieldIndexInEntity(fields.size());
                Field prevPtrField = new Field("prop_prev_ptr_" + i, new FieldType(false, new ArrowType.Int(32, true), null), null);
                fields.add(prevPtrField);
            }
        }

        return new Schema(fields);
    }


    /**
     * @return the property schema in use
     */
    public PropertySchema getPropertySchema() {
        return _propertySchema;
    }


    /**
     * @return the edge partition manager in use
     */
    public EdgePartitionManager getEdgeMgr() {
        return _emgr;
    }


    /**
     * @return the vertex partition manager in use
     */
    public VertexPartitionManager getVertexMgr() {
        return _vmgr;
    }


    /**
     * @return the global-entity-id store in use
     */
    public GlobalEntityIdStore getGlobalEntityIdStore() {
        return _globalEntityIdStore;
    }


    /**
     * @return the local-entity-id store in use
     */
    public LocalEntityIdStore getLocalEntityIdStore() {
        return _localEntityIdStore;
    }


    /**
     * @return the number of Raphtory partitions in use
     */
    public int getNRaphtoryPartitions() {
        return _nRaphtoryPartitions;
    }


    /**
     * @return this partitions Raphtory partition-id
     */
    public int getRaphtoryPartitionId() {
        return _raphtoryPartitionId;
    }


    /**
     * An array of EntityFieldAccessors is required to access fields.
     * This function returns a thread-local value suitable for
     * inspecting vertices.
     *<p>
     * This thread-local is not used by the inner library and so is
     * completely available for user-code to make use of.
     *
     * @return the vertex field accessors
     */
    public EntityFieldAccessor[] getTmpVertexEntityFieldAccessors() {
        return _vertexAccessorsPool.get();
    }


    /**
     * An array of EntityFieldAccessors is required to access fields.
     * This function returns a thread-local value suitable for
     * inspecting edges.
     *<p>
     * This thread-local is not used by the inner library and so is
     * completely available for user-code to make use of.
     *
     * @return the edge field accessors
     */
    public EntityFieldAccessor[] getTmpEdgeEntityFieldAccessors() {
        return _edgeAccessorsPool.get();
    }


    /**
     * Creates a schema property accessor from a property field
     *
     * @param root the Arrow VectorSchemaRoot to use (can be null)
     * @param field the field in question
     *
     * @return a new schema property accessor for that field
     */
    protected VersionedPropertyStore createSchemaPropertyAccessor(VectorSchemaRoot root, VersionedProperty field) {
        return field.getNewSchemaFieldAccessor(root);
    }


    /**
     * Creates a set of schema field accessors from a list of fields
     *
     * @param root the Arrow VectorSchemaRoot to use (can be null)
     * @param fields the list of fields in question
     *
     * @return an array of schema field accessors
     */
    protected SchemaFieldAccessor[] createSchemaFieldAccessors(VectorSchemaRoot root, ArrayList<NonversionedField> fields) {
        if (fields == null || fields.size() == 0) {
            return null;
        }

        int n = fields.size();
        SchemaFieldAccessor[] fas = new SchemaFieldAccessor[n];

        for (int i=0; i<fas.length; ++i) {
            NonversionedField f = fields.get(i);
            fas[i] = f.getNewSchemaFieldAccessor(root);
        }

        return fas;
    }


    /**
     * @return a new array of entity field accessors for a vertex
     */
    protected EntityFieldAccessor[] createVertexEntityFieldAccessors() {
        return createEntityFieldAccessors(_propertySchema.nonversionedVertexProperties());
    }


    /**
     * @return a new array of entity field accessors for an edge
     */
    protected EntityFieldAccessor[] createEdgeEntityFieldAccessors() {
        return createEntityFieldAccessors(_propertySchema.nonversionedEdgeProperties());
    }


    /**
     * Creates an array of entity-field-accessors for the supplied list of fields
     *
     * @param fields the list in quetion
     *
     * @return a new array of entity-field-accessors
     */
    protected EntityFieldAccessor[] createEntityFieldAccessors(ArrayList<NonversionedField> fields) {
        if (fields == null || fields.size() == 0) {
            return null;
        }

        int n = fields.size();
        EntityFieldAccessor[] fas = new EntityFieldAccessor[n];

        for (int i = 0; i < n; ++i) {
            NonversionedField f = fields.get(i);
            fas[i] = f.getNewEntityFieldAccessor();
        }

        return fas;
    }


    /**
     * Creates an Arrow schema customized for a single vertex property (incl history)
     *
     * @param id the id of the property in question
     *
     * @return a new Arrow Schema for that property
     */
    protected Schema getVertexPropertySchema(int id) {
        VersionedProperty f = _propertySchema.versionedVertexProperties().get(id);

        List<Field> fields = PropertyStore.createBasePropertySchema();

        f.addToSchema(fields);

        return new Schema(fields);
    }


    /**
     * Creates an Arrow schema customized for a single edge property (incl history)
     *
     * @param id the id of the property in question
     *
     * @return a new Arrow Schema for that property
     */
    protected Schema getEdgePropertySchema(int id) {
        VersionedProperty f = _propertySchema.versionedEdgeProperties().get(id);

        List<Field> fields = PropertyStore.createBasePropertySchema();

        f.addToSchema(fields);

        return new Schema(fields);
    }


    /**
     * @return a new array of entity property accessors for a vertex
     */
    protected VersionedEntityPropertyAccessor[] createVertexEntityPropertyAccessors() {
        return createEntityPropertyAccessors(_propertySchema.versionedVertexProperties());
    }


    /**
     * @return a new array of entity property accessors for an edge
     */
    protected VersionedEntityPropertyAccessor[] createEdgeEntityPropertyAccessors() {
        return createEntityPropertyAccessors(_propertySchema.versionedEdgeProperties());
    }


    /**
     * Creates an array of entity-property-accessors for the supplied list of properties
     *
     * @param props the list in quetion
     *
     * @return a new array of entity-property-accessors
     */
    protected VersionedEntityPropertyAccessor[] createEntityPropertyAccessors(ArrayList<VersionedProperty> props) {
        if (props == null) {
            return null;
        }

        int n = props.size();

        VersionedEntityPropertyAccessor[] accessors = new VersionedEntityPropertyAccessor[n];
        for (int i=0; i<n; ++i) {
            accessors[i] = props.get(i).getNewEntityFieldAccessor(i);
        }

        return accessors;
    }


    /**
     * Creates a new entity-property-accessor for the specified vertex property
     *
     * @param propertyId the property in question
     *
     * @return a new entity-property-accessor for that property
     */
    public VersionedEntityPropertyAccessor getVertexPropertyAccessor(int propertyId) {
        return _propertySchema.versionedVertexProperties().get(propertyId).getNewEntityFieldAccessor(propertyId);
    }


    /**
     * Creates a new entity-property-accessor for the specified edge property
     *
     * @param propertyId the property in question
     *
     * @return a new entity-property-accessor for that property
     */
    public VersionedEntityPropertyAccessor getEdgePropertyAccessor(int propertyId) {
        return _propertySchema.versionedEdgeProperties().get(propertyId).getNewEntityFieldAccessor(propertyId);
    }


    /**
     * @param id the id of the vertex property in quetion
     *
     * @return the vertex property field for the supplied id
     */
    protected VersionedProperty getVertexProperty(int id) {
        return _propertySchema.versionedVertexProperties().get(id);
    }


    /**
     * @param id the id of the edge property in quetion
     *
     * @return the edge property field for the supplied id
     */
    protected VersionedProperty getEdgeProperty(int id) {
        return _propertySchema.versionedEdgeProperties().get(id);
    }


    /**
     * Returns the vertex property-id for the supplied property name
     *
     * @param name the name of the property
     *
     * @return the property id
     *
     * @throws IllegalArgumentException if the property can't be found
     */
    public int getVertexPropertyId(String name) throws IllegalArgumentException {
        ArrayList<VersionedProperty> fields = _propertySchema.versionedVertexProperties();
        int n = fields.size();
        for (int i=0; i<n; ++i) {
            if (fields.get(i).name().equals(name)) {
                return i;
            }
        }

        throw new IllegalArgumentException("invalid field");
    }


    /**
     * Returns the edge property-id for the supplied property name
     *
     * @param name the name of the property
     *
     * @return the property id
     *
     * @throws IllegalArgumentException if the property can't be found
     */
    public int getEdgePropertyId(String name) throws IllegalArgumentException {
        ArrayList<VersionedProperty> fields = _propertySchema.versionedEdgeProperties();
        int n = fields.size();
        for (int i = 0; i < n; ++i) {
            if (fields.get(i).name().equals(name)) {
                return i;
            }
        }

        throw new IllegalArgumentException("invalid field");
    }


    /**
     * This function returns a Vertex configured for this schema. The Vertex
     * may be taken from a pool or created, however it's reference count will
     * be zero.
     *
     * @return the new Vertex
     */
    public Vertex getVertex() {
        ArrayList<Vertex> pool = _vertexPool.get();
        int n = pool.size();
        if (n > 0) {
            Vertex it = pool.remove(n - 1);
            return it;
        }

        Vertex it = new Vertex(this, createVertexEntityFieldAccessors(), createVertexEntityPropertyAccessors());
        return it;
    }


    /**
     * Returns the Vertex back to the pool, after clearing it.
     *<p>
     * The reference count should be zero before this function
     * is invoked.
     *
     * @param v the vertex in question
     */
    public void putVertex(Vertex v) {
        if (v.getRefCount() == 0) {
            v.recycle();
            v.clear();

            ArrayList<Vertex> pool = _vertexPool.get();
            pool.add(v);
        }
    }


    /**
     * This function returns an Edge configured for this schema. The Edge
     * may be taken from a pool or created, however it's reference count will
     * be zero.
     *
     * @return the new Edge
     */
    public Edge getEdge() {
        ArrayList<Edge> pool = _edgePool.get();
        int n = pool.size();
        if (n > 0) {
            Edge it = pool.remove(n - 1);
            return it;
        }

        Edge it = new Edge(this, createEdgeEntityFieldAccessors(), createEdgeEntityPropertyAccessors());
        return it;
    }


    /**
     * Returns the Edge back to the pool, after clearing it.
     *<p>
     * The reference count should be zero before this function
     * is invoked.
     *
     * @param e the edge in question
     */
    public void putEdge(Edge e) {
        if (e.getRefCount() == 0) {
            e.recycle();
            e.clear();
            ArrayList<Edge> pool = _edgePool.get();
            pool.add(e);
        }
    }


    /**
     * This function maps a vertex field name to its id.
     *
     * @param name the vertex field
     *
     * @return the id of the vertex field
     *
     * @throws IllegalArgumentException if the field can't be found
     */
    public int getVertexFieldId(String name) throws IllegalArgumentException {
        ArrayList<NonversionedField> fields = _propertySchema.nonversionedVertexProperties();
        int n = fields.size();
        for (int i = 0; i < n; ++i) {
            if (fields.get(i).name().equals(name)) {
                return i;
            }
        }

        throw new IllegalArgumentException("invalid field");
    }


    /**
     * This function maps an edge field name to its id.
     *
     * @param name the edge field
     *
     * @return the id of the edge field
     *
     * @throws IllegalArgumentException if the field can't be found
     */
    public int getEdgeFieldId(String name) {
        ArrayList<NonversionedField> fields = _propertySchema.nonversionedEdgeProperties();
        int n = fields.size();
        for (int i = 0; i < n; ++i) {
            if (fields.get(i).name().equals(name)) {
                return i;
            }
        }

        throw new IllegalArgumentException("invalid field");
    }


    /**
     * Returns a new single-threaded iterator over all vertices.
     *
     * @return a new iterator - iterating over all vertices
     */
    public VertexIterator.AllVerticesIterator getNewAllVerticesIterator() {
        return new VertexIterator.AllVerticesIterator(_vmgr);
    }


    /**
     * Returns a new single-threaded vertex iterator that iterates over
     * all vertices within the specified time window
     *
     * @param minTime the start time (inclusive)
     * @param maxTime the end time (inclusive)
     *
     * @return a new iterator
     */
    public VertexIterator.WindowedVertexIterator getNewWindowedVertexIterator(long minTime, long maxTime) {
        return new VertexIterator.WindowedVertexIterator(_vmgr, minTime, maxTime);
    }


    /**
     * Returns a new single-threaded iterator over all edges.
     *
     * @return a new iterator - iterating over all edges
     */
    public EdgeIterator.AllEdgesIterator getNewAllEdgesIterator() {
        return new EdgeIterator.AllEdgesIterator(_emgr);
    }


    /**
     * Returns a new single-threaded vertex iterator that iterates over
     * all edges within the specified time window
     *
     * @param minTime the start time (inclusive)
     * @param maxTime the end time (inclusive)
     *
     * @return a new iterator
     */
    public EdgeIterator.WindowedEdgeIterator getNewWindowedEdgeIterator(long minTime, long maxTime) {
        return new EdgeIterator.WindowedEdgeIterator(_emgr, minTime, maxTime);
    }


    /**
     * Returns a new Vertices Manager that manages multi-threaded iteration over all vertices.
     *
     * @param threadPool the thread pool to use
     *
     * @return a new Vertices Manager
     */
    public VertexIterator.MTAllVerticesManager getNewMTAllVerticesManager(RaphtoryThreadPool threadPool) {
        VertexIterator.MTAllVerticesManager it = new VertexIterator.MTAllVerticesManager();
        it.init(_vmgr, threadPool);
        return it;
    }


    /**
     * Returns a new Windowed Vertices Manager that manages multi-threaded iteration over windowed vertices.
     *
     * @param threadPool the thread pool to use
     * @param startTime the window start time (inclusive)
     * @param endTime the window end time (inclusive)
     *
     * @return a new Vertices Manager
     */
    public VertexIterator.MTWindowedVertexManager getNewMTWindowedVertexManager(RaphtoryThreadPool threadPool, long startTime, long endTime) {
        VertexIterator.MTWindowedVertexManager it = new VertexIterator.MTWindowedVertexManager();
        it.init(_vmgr, threadPool, startTime, endTime);
        return it;
    }


    /**
     * Returns a new Edges Manager that manages multi-threaded iteration over all edges.
     *
     * @param threadPool the thread pool to use
     *
     * @return a new Edges Manager
     */
    public EdgeIterator.MTAllEdgesManager getNewMTAllEdgesManager(RaphtoryThreadPool threadPool) {
        EdgeIterator.MTAllEdgesManager it = new EdgeIterator.MTAllEdgesManager();
        it.init(_emgr, threadPool);
        return it;
    }


    public VertexHistoryIterator.WindowedVertexHistoryIterator getNewVertexHistoryIterator() {
        return getNewVertexHistoryIterator(Long.MIN_VALUE, Long.MAX_VALUE);
    }


    public VertexHistoryIterator.WindowedVertexHistoryIterator getNewVertexHistoryIterator(long minTime, long maxTime) {
        return new VertexHistoryIterator.WindowedVertexHistoryIterator(_vmgr, minTime, maxTime);
    }


    public VertexHistoryIterator.WindowedVertexHistoryIterator getNewVertexHistoryIterator(long vertexId, long minTime, long maxTime) {
        return new VertexHistoryIterator.WindowedVertexHistoryIterator(_vmgr, vertexId, minTime, maxTime);
    }


    public EdgeHistoryIterator.WindowedEdgeHistoryIterator getNewEdgeHistoryIterator(long minTime, long maxTime) {
        return new EdgeHistoryIterator.WindowedEdgeHistoryIterator(_emgr, minTime, maxTime);
    }


    public EdgeHistoryIterator.WindowedEdgeHistoryIterator getNewEdgeHistoryIterator(long edgeId, long minTime, long maxTime) {
        return new EdgeHistoryIterator.WindowedEdgeHistoryIterator(_emgr, edgeId, minTime, maxTime);
    }



    public void takeSnapshot(long minTime, long maxTime) {
        VertexIterator.MTWindowedVertexManager mgr = new VertexIterator.MTWindowedVertexManager();
        mgr.init(getVertexMgr(), RaphtoryThreadPool.THREAD_POOL, minTime, maxTime);

        int nPartitions = getVertexMgr().nPartitions();
        final LongArrayList[] vertexIds = new LongArrayList[nPartitions];
        final LongArrayList[] timestamps = new LongArrayList[nPartitions];
        for (int i=0; i<nPartitions; ++i) {
            vertexIds[i] = new LongArrayList(1024);
            timestamps[i] = new LongArrayList(1024);
        }

        AtomicLong edgeCount = new AtomicLong();
        AtomicLong vertexCount = new AtomicLong();

        long then = System.currentTimeMillis();
        mgr.start((pid, iter) -> {
            VertexIterator.WindowedVertexHistoryIterator wvi = (VertexIterator.WindowedVertexHistoryIterator)iter;

            LongArrayList vIds = vertexIds[pid];
            LongArrayList times = timestamps[pid];

            int nEdges = 0;
            int nVertices = 0;

            while (wvi.hasNext()) {
                vIds.add(wvi.next());
                times.add(wvi.getModificationTime());
                ++nVertices;

                EdgeIterator ei = wvi.getAllEdges();
                while (ei.hasNext()) {
                    ei.next();
                    ++nEdges;
                    // Store edge info somewhere?
                }
            }

            vertexCount.addAndGet(nVertices);
            edgeCount.addAndGet(nEdges);
        });
        mgr.waitTilComplete();

        long now = System.currentTimeMillis();
        System.out.println("From: " + new Date(minTime) + " took " + (now-then) + "ms, vc=" + vertexCount.get() + ", ec=" + edgeCount.get());
    }


    protected static int roundUpToPowerOfTwo(int v) {
        int i = Integer.highestOneBit(v);
        return v>i ? i<<1 : i;
    }


    public RaphtoryStatistics getStatistics() {
        return _stats;
    }


    /**
     * Dumps the graph for debugging purposes only!
     */
    public void dump() {
        PrintStream out = System.out;

        out.println("Raphtory Arrow Partition " + _raphtoryPartitionId);

        VertexIterator iter = getNewAllVerticesIterator();
        while (iter.hasNext()) {
            iter.next();
            out.println(iter.getGlobalVertexId());
        }
        out.println("\n");


        iter = getNewAllVerticesIterator();
        while (iter.hasNext()) {
            iter.next();

            out.println(iter.getGlobalVertexId() + " -> nEdges=" + (iter.getNOutgoingEdges() + iter.getNIncomingEdges()));

            EdgeIterator edges = iter.getIncomingEdges();

            int edgeNum = 0;
            while (edges.hasNext()) {
                long edgeId = edges.next();

                out.println("    I " + edgeNum + " -> " + edgeId +
                            ", f=" + getGlobalId(edges.getSrcVertexId(), edges.isSrcVertexLocal()) +
                            ", t=" + getGlobalId(edges.getDstVertexId(), edges.isDstVertexLocal()));

                ++edgeNum;
            }

            edges = iter.getOutgoingEdges();
            while (edges.hasNext()) {
                long edgeId = edges.next();

                out.println("    O " + edgeNum + " -> " + edgeId +
                            ", f=" + getGlobalId(edges.getSrcVertexId(), edges.isSrcVertexLocal()) +
                            ", t=" + getGlobalId(edges.getDstVertexId(), edges.isDstVertexLocal()));

                ++edgeNum;
            }

            out.println();
        }

        out.flush();
        System.out.println(new Date() + ": allVerticesIterator finished");
    }


    private long getGlobalId(long id, boolean isLocal) {
        if (!isLocal) {
            return id;
        }

        return _vmgr.getGlobalVertexId(id);
    }
}
