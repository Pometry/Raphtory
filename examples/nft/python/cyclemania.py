from pyraphtory.algorithm import PyAlgorithm
from pyraphtory.graph import TemporalGraph, Row, Table
from pyraphtory.vertex import Vertex
import pyraphtory.scala.collection

CYCLES_FOUND: str = "CYCLES_FOUND"

class CycleMania(PyAlgorithm):
    def __call__(self, graph: TemporalGraph) -> TemporalGraph:
        def step(v: Vertex):
            if v.type() != "NFT":
                v[CYCLES_FOUND] = []
                return
            all_cycles = []
            all_purchases = sorted(v.explode_in_edges(), key=lambda e: e.timestamp())
            purchasers = list(map(lambda e:
                                  dict(buyer=e.get_property_or_else("buyer_address", "_UNKNOWN_"),
                                       price_usd=float(e.get_property_or_else("price_usd", 0.0)),
                                       time=e.timestamp(),
                                       tx_hash=e.get_property_or_else("transaction_hash", ""),
                                       nft_id=e.get_property_or_else("token_id", "_UNKNOWN_")),
                                  all_purchases))
            if len(purchasers) > 2:
                buyers_seen = {}
                for pos, item_sale in enumerate(purchasers):
                    buyer_id = item_sale['buyer']
                    if buyer_id not in buyers_seen:
                        buyers_seen[buyer_id] = pos
                    else:
                        prev_pos = buyers_seen[buyer_id]
                        prev_price = purchasers[prev_pos]['price_usd']
                        current_price = item_sale['price_usd']
                        buyers_seen[buyer_id] = pos
                        if prev_price < current_price:
                            all_cycles.append(purchasers[prev_pos:pos + 1])
            if len(all_cycles):
                v[CYCLES_FOUND] = all_cycles
            else:
                v[CYCLES_FOUND] = []

        return graph.reduced_view().step(step)

    def tabularise(self, graph: TemporalGraph):
        def get_cycles(v: Vertex):
            vertex_type = v.type()
            rows_found = [Row()]
            if vertex_type == "NFT" and len(v[CYCLES_FOUND]):
                nft_id = str(v.id())
                cycles_found = v[CYCLES_FOUND]
                nft_collection = v.get_property_or_else('collection', '_UNKNOWN_')
                nft_category = v.get_property_or_else('category', '_UNKNOWN_')
                rows_found = list(map(lambda single_cycle:
                                      Row(
                                          nft_id,
                                          nft_collection,
                                          nft_category,
                                          len(single_cycle),
                                          dict(buyer=str(single_cycle[0]['buyer']),
                                               profit_usd=float(single_cycle[len(single_cycle) - 1]['price_usd']) -
                                                          float(single_cycle[0]['price_usd']),
                                               cycle=single_cycle)
                                      ), cycles_found))
            return rows_found

        return graph.explode_select(lambda v: get_cycles(v)).filter(lambda row: len(row.get_values()) > 0)