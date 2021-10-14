import networkx as nx
from os import listdir
from os.path import isfile, join
import pandas as pd

# Get file paths
source_dir = "/tmp/taint_output/com.raphtory.dev.ethereum.analysis.TaintAlgorithm_1634118612681"
files = [source_dir+"/"+f for f in listdir(source_dir) if "partition" in f]
print("found "+str(len(files))+" files")

# Load all the files
g = nx.DiGraph()
c=0
unique_txs = set([])
for f in files:
    c+=1
    print("Reading file "+f)
    file_handler = open(f, 'r')
    lines = file_handler.readlines()
    for line in lines:
        split = line.split(",")
        to_address = line[1]
        start_of_list = line.find("List(")
        if start_of_list == -1:
            print("skip")
            continue
        from_txs = line[start_of_list:].replace("List(", "")[:-1].split(")")
        for row in from_txs:
            if "tainted" in row:
                print(row)
                row = row.replace(")", "").replace("(", "").replace(" ", "").split(",")
                from_address = row[-1]
                unique_txs.add(row[1])
                g.add_edge(from_address, to_address)
print("Complete")
print("Found "+str(g.number_of_nodes())+" nodes")
print("Found "+str(g.number_of_edges())+" edges")
print("Found "+str(len(unique_txs))+" txs")

print("Drawing...")
import matplotlib.pyplot as plt
f = plt.figure()
nx.draw(g, ax=f.add_subplot(111))
f.savefig("graph.png")

#31575000000,0xc0f1aecbad7212ff19905fc6a03f32ac8d752ed3,true,List((tainted,0x4e5210ad5923d6a731e4a34a6274c5b69a8a1e30b4d977bd1158c39ceafe962b,1574938742,9999.999778196308,0xf4678169b1eb0c141fd99942ac02191145fefe3f), (tainted,0x49898da7332c7787150ec9ce76b00e0ce89a6c016f6484bc4db8ac794f39d5be,1574938630,1000.0000200408773,0xf4678169b1eb0c141fd99942ac02191145fefe3f))