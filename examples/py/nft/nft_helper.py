import json
import numpy as np
import csv

def load_json(filename):
    data = []
    with open(filename, 'r') as f:
        for r in f.readlines():
            r_json = json.loads(r)
            if r_json['row'][4]['buyer']=='0x0000000000000000000000000000000000000000':
                continue
            else:
                data.append(r_json)
    print("amount of data %i"  % len(data))
    return data


# For making CDFs and CCDFs
def cdf(listlike, normalised=True):
    data = np.array(listlike)
    N = len(listlike)

    x = np.sort(data)
    if (normalised):
        y = np.arange(N)/float(N-1)
    else:
        y = np.arange(N)
    return x, y

def ccdf(listlike, normalised=True):
    x, y = cdf(listlike,normalised)
    if normalised:
        return x, 1.0-y
    else:
        return x, len(listlike)-y
    
    
def setup_date_prices(eth_historic_csv="/tmp/ETH-USD.csv"):
    date_price_map = {}
    with open(eth_historic_csv) as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',')
        for row in reader:
            date_price_map[row['Date']] = (float(row['High']) + float(row['Low'])) / 2
    return date_price_map