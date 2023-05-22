import pandas as pd

from raphtory_bench import RaphtoryBench
from kuzu_bench import KuzuBench
from networkx_bench import NetworkXBench
from neo4j_bench import Neo4jBench
from graphtool_bench import GraphToolBench
from memgraph_bench import MemgraphBench
from cozo_bench import CozoDBBench
import time

fns = ['setup', 'degree', 'out_neighbours', 'page_rank', 'connected_components']


# Display menu and get user's choice
def display_menu():
    print("Benchmark Options:")
    print("0. Run All")
    print("1. Run Raphtory Benchmark")
    print("2. Run GraphTool Benchmark")
    print("3. Run Kuzu Benchmark")
    print("4. Run NetworkX Benchmark")
    print("5. Run Neo4j Benchmark")
    print("6. Run Memgraph Benchmark")
    print("7. Run CozoDB Benchmark")
    print("8. Exit")
    choice = int(input("Enter your choice: "))
    return choice


def setup():
    return {
        0: 'ALL',
        1: RaphtoryBench(),
        2: GraphToolBench(),
        3: KuzuBench(),
        4: NetworkXBench(),
        5: Neo4jBench(),
        6: MemgraphBench(),
        7: CozoDBBench()
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
            else:
                name, result = run_benchmark(choice)
                results[name] = result
    except Exception as e:
        print("Error: " + str(e))
    finally:
        print_table(results)


if __name__ == "__main__":
    main()
