from pathlib import Path
import pandas as pd

FLATTENED_FILE = "/Users/arien/RustroverProjects/Raphtory/dataset_tests/flattened_data.parquet"
DATASET_DIR = "/Users/arien/Downloads"

def flatten_dataframes_append():
    dfs = []
    flattened_file = Path(FLATTENED_FILE)
    dataset_dir = Path(DATASET_DIR)

    if flattened_file.exists():
        dfs.append(pd.read_parquet(flattened_file))

    def get_addr(v):
        if v is not None:
            return v[0]["address"]
    files = list(dataset_dir.glob("*.snappy.parquet"))
    num_files = len(files)
    for i in range(num_files):
        fp = files[i]
        print(f"Processing file {i}/{num_files}: {fp}")
        df = pd.read_parquet(fp)
        df = pd.DataFrame({
            "block_timestamp": df["block_timestamp"],
            "inputs_address": df["inputs"].apply(get_addr),
            "outputs_address": df["outputs"].apply(get_addr),
        })
        df = df.dropna(subset=["block_timestamp", "inputs_address", "outputs_address"])
        dfs.append(df)

    out = pd.concat(dfs, ignore_index=True)
    print(f"Total: {len(out)} rows")
    out.to_parquet(FLATTENED_FILE, index=False, compression="snappy")


if __name__ == "__main__":
    flatten_dataframes_append()