from typing import Generic, Optional, TypeVar

from .raphtory import Graph, Node

H = TypeVar("H")
Y = TypeVar("Y")

# TODO: Is H always Node?
class AlgorithmResult(Generic[H, Y], object):
    def get(self, key: H) -> Optional[Y]: ...
    def get_all(self) -> dict[Node, Y]: ...
    def get_all_values(self) -> list[Y]: ...
    def get_all_with_names(self) -> dict[H, Y]: ...
    def group_by(self): ...  # TODO: Define return type
    def max(self) -> tuple[Node, Y]: ...
    def median(self) -> tuple[Node, Y]: ...
    def sort_by_node(self, reverse: Optional[bool] = False) -> list[tuple[Node, Y]]: ...
    def sort_by_node_name(
        self, reverse: Optional[bool] = False
    ) -> list[tuple[Node, Y]]: ...
    def sort_by_value(
        self, reverse: Optional[bool] = False
    ) -> list[tuple[Node, Y]]: ...
    def to_string(self) -> str: ...
    def top_k(
        self,
        k: int,
        percentage: Optional[bool] = False,
        reverse: Optional[bool] = False,
    ) -> list[tuple[Node, Y]]: ...

# ---- Centrality ----

def degree_centrality(
    g: Graph, threads: Optional[int] = None
) -> AlgorithmResult[str, float]: ...
def pagerank(
    g: Graph,
    iter_count: int = 20,
    max_diff: Optional[float] = None,
    use_l2_norm: bool = True,
    damping_factor: float = 0.85,
) -> AlgorithmResult[str, float]: ...
def hits(
    g: Graph, iter_count: int = 20, threads: Optional[int] = None
) -> AlgorithmResult[str, tuple[float, float]]: ...
def betweenness_centrality(
    g: Graph, k: Optional[int] = None, normalized: Optional[bool] = True
) -> AlgorithmResult[str, float]: ...

# ---- Community Detection ----

def weakly_connected_components(
    g: Graph, iter_count: int = 9223372036854775807
) -> AlgorithmResult[str, int]: ...
