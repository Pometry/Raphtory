import pandas as pd

from raphtory_bench import RaphtoryBench
from kuzu_bench import KuzuBench
from networkx_bench import NetworkXBench
from neo4j_bench import Neo4jBench
from graphtool_bench import GraphToolBench
from memgraph_bench import MemgraphBench
from cozo_bench import CozoDBBench
import time
import shutil
import gzip
import requests
import os

fns = ['setup', 'degree', 'out_neighbours', 'page_rank', 'connected_components']


# Display menu and get user's choice
def display_menu():
    print("Benchmark Options:")
    print("0. Run All")
    print("1. Download Data")
    print("2. Run Raphtory Benchmark")
    print("3. Run GraphTool Benchmark")
    print("4. Run Kuzu Benchmark")
    print("5. Run NetworkX Benchmark")
    print("6. Run Neo4j Benchmark")
    print("7. Run Memgraph Benchmark")
    print("8. Run CozoDB Benchmark")
    print("9. Exit")
    choice = int(input("Enter your choice: "))
    return choice


def setup():
    return {
        0: 'ALL',
        1: 'DOWNLOAD',
        2: RaphtoryBench(),
        3: GraphToolBench(),
        4: KuzuBench(),
        5: NetworkXBench(),
        6: Neo4jBench(),
        7: MemgraphBench(),
        8: CozoDBBench()
    }


def run_benchmark(choice):
    driver = setup()[choice]
    times = []
    for fn in fns:
        print("** Running " + fn + "...")
        start_time = time.time()
        getattr(driver, fn)()
        end_time = time.time()
        print(fn + " time: " + str(end_time - start_time))
        times.append(end_time - start_time)
    return driver.name(), times


def run_all():
    print("** Running all benchmarks...")
    results = {}
    for key in setup().keys():
        if key == 0:
            continue
        print("** Running benchmark " + str(key) + "...")
        name, result = run_benchmark(key)
        results[name] = result
    return results


def print_table(data):
    if len(data) == 0:
        return
    df = pd.DataFrame.from_dict(data, orient='index', columns=fns)
    # Extract header keys from the first inner dictionary
    print(df.to_string(index=True, justify='left'))


def dl_file(url, path):
    # Download the first file
    print("Downloading " + url + "...")
    r = requests.get(url, stream=True)
    if r.status_code == 200:
        with open(path, 'wb') as f:
            f.write(r.raw.read())
    else:
        print("Error downloading data")
        return


def create_directory(directory_path):
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        print(f"Directory '{directory_path}' created successfully.")
    else:
        print(f"Directory '{directory_path}' already exists.")


def download_data():
    # Download the data
    print("Downloading data...")
    urls = {
        'simple-profiles.csv.gz': 'https://raw.githubusercontent.com/Raphtory/Data/main/simple-profiles.csv.gz',
        'simple-relationships.csv.gz': 'https://media.githubusercontent.com/media/Raphtory/Data/main/simple-relationships.csv.gz'
    }

    # make the data directory
    create_directory('data')

    for name, url in urls.items():
        dl_file(url, 'data/' + name)

    # Unzip the files
    for name in urls.keys():
        print("Unzipping " + name + "...")
        with gzip.open('data/' + name, 'rb') as f_in:
            with open('data/' + name[:-3], 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        print("Done unzipping " + name + "...")
        os.remove('data/' + name)

    # return the file paths
    return 'data/simple-profiles.csv', 'data/simple-relationships.csv'


def main():
    results = {}
    try:
        while True:
            choice = display_menu()
            if choice not in setup():
                print(str(choice) + " not found. Exiting...")
                break
            if choice == 0:
                results = run_all()
            elif choice == 1:
                download_data()
            else:
                name, result = run_benchmark(choice)
                results[name] = result
    except Exception as e:
        print("Error: " + str(e))
    finally:
        print_table(results)


if __name__ == "__main__":
    main()
