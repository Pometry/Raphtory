import argparse

import pandas as pd

# packages = {
#     'raphtory_bench': 'RaphtoryBench',
#     'kuzu_bench':  'KuzuBench',
#     'networkx_bench': 'NetworkXBench',
#     'neo4j_bench':  'Neo4jBench',
#     'graphtool_bench': 'GraphToolBench',
#     'memgraph_bench': 'MemgraphBench',
#     'cozo_bench':  'CozoDBBench',
# }
#
# for package, classx in packages.items():
#     try:
#         exec(f'from {package} import {classx}')
#     except ImportError:
#         continue

# from raphtory_bench import RaphtoryBench
# from kuzu_bench import KuzuBench
# from networkx_bench import NetworkXBench
# from neo4j_bench import Neo4jBench
# from graphtool_bench import GraphToolBench
# from memgraph_bench import MemgraphBench
# from cozo_bench import CozoDBBench

from benchmark_imports import *
import time
import shutil
import gzip
import requests
import os

fns = ['setup', 'degree', 'out_neighbours', 'page_rank', 'connected_components']


def process_arguments():
    parser = argparse.ArgumentParser(description='benchmark args')
    parser.add_argument('-d', '--docker', type=bool, help='Launch with docker containers (default: False)',
                        default=False)
    parser.add_argument('-s', '--save', type=bool, help='Save results to file (default: False)', default=False)
    parser.add_argument('-b', '--bench', type=str, help="""
    Run specific benchmark,
    default: Goes to Menu (if docker runs all),
    all: Run All
    download: Download Data
    r: Run Raphtory Benchmark
    gt: Run GraphTool Benchmark
    k: Run Kuzu Benchmark
    nx: Run NetworkX Benchmark
    neo: Run Neo4j Benchmark
    mem: Run Memgraph Benchmark
    cozo: Run CozoDB Benchmark
    exit: Exit
    """, default='menu')
    argsx = parser.parse_args()
    return argsx


# Display menu and get user's choice
def display_menu():
    print("Benchmark Options:")
    print("all: Run All")
    print("download: Download Data")
    print("r: Run Raphtory Benchmark")
    print("gt: Run GraphTool Benchmark")
    print("k: Run Kuzu Benchmark")
    print("nx: Run NetworkX Benchmark")
    print("neo: Run Neo4j Benchmark")
    print("mem: Run Memgraph Benchmark")
    print("cozo: Run CozoDB Benchmark")
    print("exit: Exit")
    choice = int(input("Enter your choice: "))
    return choice


def setup():
    return {
        'all': 'ALL',
        'download': 'DOWNLOAD',
        'r': RaphtoryBench,
        'gt': GraphToolBench,
        'k': KuzuBench,
        'nx': NetworkXBench,
        'neo': Neo4jBench,
        'mem': MemgraphBench,
        'cozo': CozoDBBench
    }


def run_benchmark(choice, save=False):
    driver = setup()[choice]
    print("** Running for " + driver.name() + "...")
    times = []
    for fn in fns:
        print("** Running " + fn + "...")
        start_time = time.time()
        getattr(driver, fn)()
        end_time = time.time()
        print(fn + " time: " + str(end_time - start_time))
        times.append(end_time - start_time)
    filename = ''
    if save:
        filename = '/tmp/bench-' + driver.name() + str(int(time.time())) + '.csv'
        pd.DataFrame([times], columns=fns).to_csv(filename, index=False)
    return driver.name(), times, filename


def run_all():
    print("** Running all benchmarks...")
    results = {}
    for key in setup().keys():
        if key == 'menu' or key == 'all' or key == 'download':
            continue
        print("** Running benchmark " + str(key) + "...")
        name, result, filename = run_benchmark(key)
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


def run_benchmark_docker(bench):
    results = {}
    benchmarks_to_run = []
    if bench == 'menu':
        for key in setup().keys():
            if key == 'menu' or key == 'all' or key == 'download':
                continue
            benchmarks_to_run.append(key)
    else:
        benchmarks_to_run.append(bench)
    for key in benchmarks_to_run:
        print("** Running dockerized benchmark " + str(key) + "...")
        driver = setup()[key]
        print("** Running for " + driver.name() + "...")
        print("Starting docker container...")
        exit_code, logs = driver.start_docker()
        print("Docker container exited with non-zero code " + str(exit_code))
        print("Logs: " + logs)
    else:
        pass


def main(docker, bench, save):
    print("Welcome to the Raphtory Benchmarking Tool")
    results = {}
    try:
        if docker:
            print("Running dockerised benchmarking...")
            run_benchmark_docker(bench)
        else:
            print("Running local benchmarking...")
            if bench == 'menu':
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
                        name, result, filename = run_benchmark(choice, save)
                        results[name] = result
            else:
                name, result, filename = run_benchmark(bench, save)
                results[name] = result
    except Exception as e:
        print("Error: " + str(e))
        raise e
    finally:
        print_table(results)


if __name__ == "__main__":
    args = process_arguments()
    main(args.docker, args.bench, args.save)
