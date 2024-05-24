import argparse
import pandas as pd
from benchmark_imports import *
import time
import shutil
import gzip
import requests
import os
from io import StringIO

fns = ["setup", "degree", "out_neighbours", "page_rank", "connected_components"]


def process_arguments():
    parser = argparse.ArgumentParser(description="benchmark args")
    parser.add_argument(
        "--docker",
        action=argparse.BooleanOptionalAction,
        help="Launch with docker containers, --no-docker to run locally",
        default=True,
    )
    parser.add_argument(
        "-b",
        "--bench",
        type=str,
        help="""
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
    """,
        default="menu",
    )
    return parser.parse_args()


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
        "all": "ALL",
        "download": "DOWNLOAD",
        "r": RaphtoryBench,
        "gt": GraphToolBench,
        "k": KuzuBench,
        "nx": NetworkXBench,
        "neo": Neo4jBench,
        "mem": MemgraphBench,
        "cozo": CozoDBBench,
    }


def run_benchmark(choice, docker=False):
    benchmarks_to_run = []
    if choice.lower() == "all":
        for key in setup().keys():
            if key == "menu" or key == "all" or key == "download":
                continue
            benchmarks_to_run.append(key)
    elif choice == "download" or choice == "menu":
        return
    elif choice in setup().keys():
        benchmarks_to_run.append(choice)

    results = {}
    print("Running benchmarks: " + str(benchmarks_to_run))
    for key in benchmarks_to_run:
        driver = setup()[key]
        print(key)
        print(setup())
        if docker:
            print("** Running dockerized benchmark " + str(key) + "...")
            print("Starting docker container...")
            exit_code, logs = driver.start_docker()
            print("Docker container exited with code " + str(exit_code))
            print("Logs: " + logs)
            results[driver.name()] = logs
        else:
            print("** Running for " + driver.name() + "...")
            times = ""
            fn_header = ""
            for fn in fns:
                print("** Running " + fn + "...")
                start_time = time.time()
                getattr(driver, fn)()
                end_time = time.time()
                print(fn + " time: " + str(end_time - start_time))
                fn_header += fn + ","
                if driver.name() == "Neo4j" and fn == "setup":
                    # take away 15 seconds for the sleep time when restarting the database
                    end_time = end_time - 50
                times += str(end_time - start_time) + ","
            fn_header = fn_header[:-1]
            times = times[:-1]
            results[driver.name()] = fn_header + "\n" + times
            pd.DataFrame([times.split(",")], columns=fns).to_csv(
                "/tmp/bench-" + driver.name() + "-" + str(time.time()) + ".csv"
            )
    return results


def print_table(data):
    if len(data) == 0:
        return
    _data = {}
    for key, value in data.items():
        _data[key] = pd.read_csv(StringIO(value))
    merged_df = pd.concat([df.assign(key=key) for key, df in _data.items()])
    col = merged_df.pop("key")
    if "Unnamed: 0" in merged_df.columns:
        merged_df.drop("Unnamed: 0", axis=1, inplace=True)
    merged_df.insert(0, "System", col)
    print(merged_df.to_string(index=False, justify="left"))


def dl_file(url, path):
    # Download the first file
    print("Downloading " + url + "...")
    r = requests.get(url, stream=True)
    if r.status_code == 200:
        with open(path, "wb") as f:
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
        "simple-profiles.csv.gz": "https://osf.io/download/w2xns/",
        "simple-relationships.csv.gz": "https://osf.io/download/nbq6h/",
    }

    # make the data directory
    create_directory("data")

    for name, url in urls.items():
        dl_file(url, "data/" + name)

    # Unzip the files
    for name in urls.keys():
        print("Unzipping " + name + "...")
        with gzip.open("data/" + name, "rb") as f_in:
            with open("data/" + name[:-3], "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        print("Done unzipping " + name + "...")
        os.remove("data/" + name)

    # return the file paths
    return "data/simple-profiles.csv", "data/simple-relationships.csv"


def main(docker, choice):
    print("Welcome to the Raphtory Benchmarking Tool")
    results = {}
    try:
        if choice == "menu":
            choice = display_menu()
        if choice == "download" or choice == 1:
            download_data()
        elif choice == "exit" or choice not in setup():
            print(str(choice) + ". Exiting...")
        else:
            results = run_benchmark(choice, docker)
    except Exception as ex:
        print("Error: " + str(ex))
        raise ex
    finally:
        print_table(results)


if __name__ == "__main__":
    args = process_arguments()
    main(args.docker, args.bench)
