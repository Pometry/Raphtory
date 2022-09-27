from pyraphtory.algorithm import PyAlgorithm
from pyraphtory.graph import TemporalGraph, Row, Table
from pyraphtory.vertex import Vertex
from dataclasses import dataclass
import pyraphtory.scala.collection

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


@dataclass(frozen=True)
class CycleData:
    buyer: str
    profit_usd: float
    cycle: Cycle


class CycleManiaj(PyAlgorithm):
    def __call__(self, graph: TemporalGraph) -> TemporalGraph:
        def step(v: Vertex):
            if v.type() != "NFT":
                v[CYCLES_FOUND] = []
                return
            all_cycles = []
            all_purchases = sorted(v.explode_in_edges(), key=lambda e: e.timestamp())

            purchasers = list(map(lambda e:
                                  {
                                      "buyer":e.get_property_or_else("buyer_address", "_UNKNOWN_"),
                                      "price_usd":float(e.get_property_or_else("price_usd", 0.0)),
                                      "time":e.timestamp(),
                                      "tx_hash":e.get_property_or_else("transaction_hash", ""),
                                      "nft_id":e.get_property_or_else("token_id", "_UNKNOWN_")},
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
                        # print(f"prev {prev_price} : current: {current_price}")
                        if prev_price < current_price:
                            # print(f"Money Cycle found, item from {buyer_id} to {pos}")
                            all_cycles.append([purchasers[prev_pos:pos + 1]])
            if len(all_cycles):
                print("found cycles. Saved to vertex")
                print(all_cycles)
                v[CYCLES_FOUND] = all_cycles
                print("saved")
            else:
                v[CYCLES_FOUND] = []

        return graph.reduced_view().step(step)

    # def tabularise(self, graph: TemporalGraph) -> Table:
    #     return graph.select(lambda v: Row(v['PURCHASE']))

    def tabularise(self, graph: TemporalGraph):
        def get_cycles(v: Vertex):
            vertex_type = v.type()
            rows_found = [Row()]
            if vertex_type == "NFT" and len(v[CYCLES_FOUND]):
                nft_id = v.id()
                cycles_found = v[CYCLES_FOUND]
                nft_collection = v.get_property_or_else('collection', '_UNKNOWN_')
                nft_category = v.get_property_or_else('category', '_UNKNOWN_')
                # for single_cycle in cycles_found:
                rows_found = list(map(lambda single_cycle:
                                      Row(
                                          nft_id,
                                          nft_collection,
                                          nft_category,
                                          len(single_cycle.sales),
                                          CycleData(
                                              buyer=single_cycle.sales[0].buyer,
                                              profit_usd=single_cycle.sales[-1].price_usd - single_cycle.sales[
                                                  0].price_usd,
                                              cycle=single_cycle
                                          )
                                      ), cycles_found))
            return rows_found
        return graph.explode_select(lambda v: get_cycles(v))


qp = graph \
    .at(at_time) \
    .past() \
    .execute(CycleManiaj()) \
    .write_to(FileSink('/tmp/raphtory_nft_python', format = JsonFormat()))
