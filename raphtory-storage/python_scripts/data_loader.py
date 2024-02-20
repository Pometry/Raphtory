from pathlib import Path
import polars as pl
from tqdm import tqdm

base = Path.cwd()
files = sorted((base / "edges").iterdir())
out_dir = base / "edges_parquet"
out_dir.mkdir(exist_ok=True)

offset = 0
for file in tqdm(files):
    df = pl.read_csv(file, separator=" ", has_header=False, new_columns=["src", "dst"])
    num_rows = df.shape[0]
    df = df.with_columns([pl.col("src").alias("src_hash"), pl.col("dst").alias("dst_hash"), pl.int_range(start=offset, end=offset + num_rows, dtype=pl.Int64).alias("time")])
    offset += num_rows
    out_file = out_dir / (file.stem + ".snappy.parquet")
    df = df.cast({"src": pl.UInt64, "dst": pl.UInt64, "src_hash": pl.Int64, "dst_hash": pl.Int64, "time": pl.Int64})
    df.write_parquet(out_file, compression="snappy")
    print(f"processed {file}")
