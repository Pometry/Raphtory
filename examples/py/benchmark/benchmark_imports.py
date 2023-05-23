RaphtoryBench = None
KuzuBench = None
NetworkXBench = None
Neo4jBench = None
GraphToolBench = None
MemgraphBench = None
CozoDBBench = None

try:
    from raphtory_bench import RaphtoryBench
    RaphtoryBench = RaphtoryBench()
except ImportError as e:
    pass

try:
    from kuzu_bench import KuzuBench
    KuzuBench = KuzuBench()
except ImportError:
    pass

try:
    from networkx_bench import NetworkXBench
    NetworkXBench = NetworkXBench()
except ImportError as e:
    pass

try:
    from neo4j_bench import Neo4jBench
    Neo4jBench = Neo4jBench()
except ImportError:
    pass

try:
    from graphtool_bench import GraphToolBench
    GraphToolBench = GraphToolBench
except ImportError:
    pass

try:
    from memgraph_bench import MemgraphBench
    MemgraphBench = MemgraphBench()
except ImportError:
    pass

try:
    from cozo_bench import CozoDBBench
    CozoDBBench = CozoDBBench()
except ImportError:
    pass