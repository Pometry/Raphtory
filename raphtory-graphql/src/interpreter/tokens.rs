//! Interned GraphQL identifier strings — the SDL type names, field names, and
//! argument names the planner matches on. Centralizing them keeps `resolve_op`
//! free of bare string literals and gives one place to audit/rename identifiers
//! against `schema.graphql`.

/// GraphQL object type names (the `parent_type` keys in `resolve_op`).
pub mod ty {
    pub const QUERY_ROOT: &str = "QueryRoot";
    pub const GRAPH: &str = "Graph";
    pub const NODE: &str = "Node";
    pub const EDGE: &str = "Edge";
    pub const NODES: &str = "Nodes";
    pub const EDGES: &str = "Edges";
    pub const PATH_FROM_NODE: &str = "PathFromNode";
    pub const HISTORY: &str = "History";
    pub const HISTORY_TIMESTAMP: &str = "HistoryTimestamp";
    pub const HISTORY_EVENT_ID: &str = "HistoryEventId";
    pub const HISTORY_DATE_TIME: &str = "HistoryDateTime";
    pub const EVENT_TIME: &str = "EventTime";
    pub const PROPERTIES: &str = "Properties";
    pub const METADATA: &str = "Metadata";
    pub const TEMPORAL_PROPERTIES: &str = "TemporalProperties";
    pub const PROPERTY: &str = "Property";
    pub const TEMPORAL_PROPERTY: &str = "TemporalProperty";
}

/// GraphQL field names.
pub mod field {
    pub const GRAPH: &str = "graph";
    pub const NODES: &str = "nodes";
    pub const NODE: &str = "node";
    pub const EDGES: &str = "edges";
    pub const EDGE: &str = "edge";
    pub const LAYER: &str = "layer";
    pub const LAYERS: &str = "layers";
    pub const EXCLUDE_LAYER: &str = "excludeLayer";
    pub const EXCLUDE_LAYERS: &str = "excludeLayers";
    pub const DEFAULT_LAYER: &str = "defaultLayer";
    pub const WINDOW: &str = "window";
    pub const AT: &str = "at";
    pub const AFTER: &str = "after";
    pub const BEFORE: &str = "before";
    pub const LATEST: &str = "latest";
    pub const SNAPSHOT_AT: &str = "snapshotAt";
    pub const SNAPSHOT_LATEST: &str = "snapshotLatest";
    pub const SHRINK_WINDOW: &str = "shrinkWindow";
    pub const SHRINK_START: &str = "shrinkStart";
    pub const SHRINK_END: &str = "shrinkEnd";
    pub const VALID: &str = "valid";
    pub const SUBGRAPH: &str = "subgraph";
    pub const SUBGRAPH_NODE_TYPES: &str = "subgraphNodeTypes";
    pub const EXCLUDE_NODES: &str = "excludeNodes";
    pub const EARLIEST_TIME: &str = "earliestTime";
    pub const LATEST_TIME: &str = "latestTime";
    pub const START: &str = "start";
    pub const END: &str = "end";
    pub const FIRST_UPDATE: &str = "firstUpdate";
    pub const LAST_UPDATE: &str = "lastUpdate";
    pub const COUNT_NODES: &str = "countNodes";
    pub const COUNT_EDGES: &str = "countEdges";
    pub const COUNT_TEMPORAL_EDGES: &str = "countTemporalEdges";
    pub const UNIQUE_LAYERS: &str = "uniqueLayers";
    pub const HAS_NODE: &str = "hasNode";
    pub const HAS_EDGE: &str = "hasEdge";
    pub const LIST: &str = "list";
    pub const PAGE: &str = "page";
    pub const COUNT: &str = "count";
    pub const SORTED: &str = "sorted";
    pub const ID: &str = "id";
    pub const NAME: &str = "name";
    pub const HISTORY: &str = "history";
    pub const NEIGHBOURS: &str = "neighbours";
    pub const IN_NEIGHBOURS: &str = "inNeighbours";
    pub const OUT_NEIGHBOURS: &str = "outNeighbours";
    pub const IN_EDGES: &str = "inEdges";
    pub const OUT_EDGES: &str = "outEdges";
    pub const IN_COMPONENT: &str = "inComponent";
    pub const OUT_COMPONENT: &str = "outComponent";
    pub const NODE_TYPE: &str = "nodeType";
    pub const DEGREE: &str = "degree";
    pub const IN_DEGREE: &str = "inDegree";
    pub const OUT_DEGREE: &str = "outDegree";
    pub const EDGE_HISTORY_COUNT: &str = "edgeHistoryCount";
    pub const IS_ACTIVE: &str = "isActive";
    pub const PROPERTIES: &str = "properties";
    pub const METADATA: &str = "metadata";
    pub const SRC: &str = "src";
    pub const DST: &str = "dst";
    pub const NBR: &str = "nbr";
    pub const EXPLODE: &str = "explode";
    pub const EXPLODE_LAYERS: &str = "explodeLayers";
    pub const DELETIONS: &str = "deletions";
    pub const IS_VALID: &str = "isValid";
    pub const IS_DELETED: &str = "isDeleted";
    pub const IS_SELF_LOOP: &str = "isSelfLoop";
    pub const LAYER_NAMES: &str = "layerNames";
    pub const TIMESTAMPS: &str = "timestamps";
    pub const EVENT_ID: &str = "eventId";
    pub const DATETIMES: &str = "datetimes";
    pub const TIMESTAMP: &str = "timestamp";
    pub const DATETIME: &str = "datetime";
    pub const VALUES: &str = "values";
    pub const TEMPORAL: &str = "temporal";
    pub const KEY: &str = "key";
    pub const AS_STRING: &str = "asString";
    pub const VALUE: &str = "value";
}

/// GraphQL field-argument names.
pub mod arg {
    pub const NAME: &str = "name";
    pub const SRC: &str = "src";
    pub const DST: &str = "dst";
    pub const LAYER: &str = "layer";
    pub const NAMES: &str = "names";
    pub const NODES: &str = "nodes";
    pub const NODE_TYPES: &str = "nodeTypes";
    pub const START: &str = "start";
    pub const END: &str = "end";
    pub const TIME: &str = "time";
    pub const PATH: &str = "path";
    pub const KEYS: &str = "keys";
    pub const FORMAT_STRING: &str = "formatString";
    pub const SELECT: &str = "select";
    pub const LIMIT: &str = "limit";
    pub const OFFSET: &str = "offset";
    pub const PAGE_INDEX: &str = "pageIndex";
    pub const SORT_BYS: &str = "sortBys";
}
