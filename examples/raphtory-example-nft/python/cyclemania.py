from pyraphtory.algorithm import PyAlgorithm
from pyraphtory.graph import TemporalGraph, Row, Table
from pyraphtory.vertex import Vertex
from dataclasses import dataclass

CYCLES_FOUND: str = "CYCLES_FOUND"


@dataclass(frozen=True)
class Sale:
    buyer: str
    price_usd: float
    time: int
    tx_hash: str
    nft_id: str


@dataclass(frozen=False)
class Cycle:
    sales: list[Sale]


class CycleMania(PyAlgorithm):
    def __init__(self):
        pass

    def __call__(self, graph: TemporalGraph) -> TemporalGraph:
        def step(v: Vertex):
            if v.type() != "NFT":
                v[CYCLES_FOUND] = []
                return
            all_cycles = []
            all_purchases = sorted(v.explode_in_edges(), key=lambda e: e.timestamp(), reverse=True)
            purchasers = list(map(lambda e:
                                  Sale(
                                      buyer=e.get_property_or_else("buyer_address", "_UNKNOWN_"),
                                      price_usd=e.get_property_or_else("price_USD", 0.0),
                                      time=e.timestamp,
                                      tx_hash=e.get_property_or_else("transaction_hash", ""),
                                      nft_id=e.get_property_or_else("token_id", "_UNKNOWN_")),
                                  all_purchases))
            if len(purchasers) > 2:
                buyers_seen = {}
                for pos, item_sale in enumerate(purchasers):
                    buyer_id = item_sale.buyer
                    if buyer_id not in buyers_seen:
                        buyers_seen[buyer_id] = pos
                    else:
                        prev_pos = buyers_seen[buyer_id]
                        prev_price = purchasers[pos].price_usd
                        current_price = item_sale.price_usd
                        buyers_seen[buyer_id] = pos
                        if prev_price < current_price:
                            all_cycles.append(Cycle(purchasers[prev_pos:pos + 1]))

            if len(all_cycles):
                v[CYCLES_FOUND] = all_cycles
            else:
                v[CYCLES_FOUND] = []

        return graph.step(step)

    def tabularise(self, graph: TemporalGraph):
        def explodeme(v: Vertex):
            vertex_type = v.type()
            if vertex_type == "NFT" and len(v[CYCLES_FOUND]):
                nft_id = v.id()
                cycles_found = v[CYCLES_FOUND]
                nft_collection = v.get_property_or_else('collection', '_UNKNOWN_')
                nft_category = v.get_property_or_else('category', '_UNKNOWN_')
                # for single_cycle in cycles_found:


        return graph.select(lambda v: explodeme(v))
