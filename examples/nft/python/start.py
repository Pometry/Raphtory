from cyclemania import CycleMania
import time
from calendar import timegm
from pyraphtory.graph import Row
from pyraphtory.context import PyRaphtory
from pyraphtory.builder import *
import csv

def get_date_price(self, eth_historic_csv="/tmp/ETH-USD.csv"):
    date_price_map = {}
    with open(eth_historic_csv) as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',')
        for row in reader:
            date_price_map[row['Date']] = (float(row['High']) + float(row['Low'])) / 2
    return date_price_map


def main():
    pr = PyRaphtory(logging=False).open()
    graph = pr.new_graph()
    filename = '/Users/haaroony/Documents/nft/Data_API_clean_nfts_ETH_only_1k.csv'
    # load the data into a graph
    with open(filename, 'r') as csvfile:
        datareader = csv.reader(csvfile)
        for file_line in datareader:
            # Skip Header
            if file_line[0] == "Smart_contract":
                continue
            # Seller details
            seller_address = file_line[3]
            seller_address_hash = graph.assign_id(seller_address)
            # Buyer details
            buyer_address = file_line[5]
            buyer_address_hash = graph.assign_id(buyer_address)
            # Transaction details
            datetime_str = file_line[13]
            timestamp_utc = time.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")
            timestamp = timegm(timestamp_utc)
            tx_hash = file_line[2]
            token_id_str = file_line[1]
            token_id_hash = graph.assign_id(token_id_str)

            crypto = file_line[8]
            if crypto != 'ETH':
                continue

            if file_line[9] == "":
                price_usd = get_date_price(datetime_str[0:10], '/Users/haaroony/Documents/nft/ETH-USD.csv')
            else:
                price_usd = float(file_line[9])

            # NFT Details
            collection_cleaned = file_line[14]
            market = file_line[11]
            category = file_line[15]

            #  add buyer node
            graph.add_vertex(
                timestamp,
                buyer_address_hash,
                Properties(ImmutableProperty("address", buyer_address)),
                Type("Wallet")
            )

            # add seller node
            graph.add_vertex(
                timestamp,
                seller_address_hash,
                Properties(ImmutableProperty("address", seller_address)),
                Type("Wallet")
            )

            # Add node for NFT
            graph.add_vertex(
                timestamp,
                token_id_hash,
                Properties(
                    ImmutableProperty("id", token_id_str),
                    ImmutableProperty("collection", collection_cleaned),
                    ImmutableProperty("category", category)
                ),
                Type("NFT")
            )

            # Creating a bipartite graph,
            # add edge between buyer and nft
            graph.add_edge(
                timestamp,
                buyer_address_hash,
                token_id_hash,
                Properties(
                    StringProperty("transaction_hash", tx_hash),
                    StringProperty("crypto", crypto),
                    StringProperty("price_usd", str(price_usd)),
                    StringProperty("market", market),
                    StringProperty("token_id", token_id_str),
                    StringProperty("buyer_address", buyer_address)
                ),
                Type("Purchase")
            )

    # test to see if the dataframe works
    df = graph \
        .select(lambda vertex: Row(vertex.name(), vertex.degree(), vertex.out_degree(), vertex.in_degree())) \
        .to_df(["name", "degree", "out_degree", "in_degree"])
    print(df)
    print("Done")

    at_time = 1575147060
    cols = ['nft_id',
            'nft_collection',
            'nft_category',
            'num_sales',
            'cycle_data']
    graph\
        .at(at_time)\
        .past()\
        .execute(CycleMania())\
        .to_df(cols)

if __name__ == "__main__":
    main()
