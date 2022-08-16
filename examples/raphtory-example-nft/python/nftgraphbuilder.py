import time
from calendar import timegm
import csv
from pyraphtory.builder import BaseBuilder, ImmutableProperty, Properties, Type, StringProperty, DoubleProperty


class NFTGraphBuilder(BaseBuilder):
    def __init__(self):
        super(NFTGraphBuilder, self).__init__()

    def get_date_price(self, eth_historic_csv="/tmp/ETH-USD.csv"):
        date_price_map = {}
        with open(eth_historic_csv) as csvfile:
            reader = csv.DictReader(csvfile, delimiter=',')
            for row in reader:
                date_price_map[row['Date']] = (float(row['High']) + float(row['Low'])) / 2
        return date_price_map

    def parse_tuple(self, line: str):
        file_line = list(map(str.strip, line.split(",")))
        # Skip Header
        if file_line[0] == "Smart_contract":
            return
        # Seller details
        seller_address = file_line[3]
        seller_address_hash = self.assign_id(seller_address)
        # Buyer details
        buyer_address = file_line[5]
        buyer_address_hash = self.assign_id(buyer_address)
        # Transaction details
        datetime_str = file_line[13]
        timestamp_utc = time.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")
        timestamp = timegm(timestamp_utc)
        tx_hash = file_line[2]
        token_id_str = file_line[1]
        token_id_hash = self.assign_id(token_id_str)

        crypto = file_line[8]
        if crypto != 'ETH':
            return

        if file_line[9] == "":
            price_usd = self.get_date_price(datetime_str[0:10], '/Users/haaroony/Documents/nft/ETH-USD.csv')
        else:
            price_usd = float(file_line[9])

        # NFT Details
        collection_cleaned = file_line[14]
        market = file_line[11]
        category = file_line[15]

        #  add buyer node
        self.add_vertex(
            timestamp,
            buyer_address_hash,
            Properties(ImmutableProperty("address", buyer_address)),
            Type("Wallet")
        )

        # add seller node
        self.add_vertex(
            timestamp,
            seller_address_hash,
            Properties(ImmutableProperty("address", seller_address)),
            Type("Wallet")
        )

        # Add node for NFT
        self.add_vertex(
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
        self.add_edge(
            timestamp,
            buyer_address_hash,
            token_id_hash,
            Properties(
                StringProperty("transaction_hash", tx_hash),
                StringProperty("crypto", crypto),
                DoubleProperty("price_usd", price_usd),
                StringProperty("market", market),
                StringProperty("token_id", token_id_str),
                StringProperty("buyer_address", buyer_address)
            ),
            Type("Purchase")
        )
