from functools import reduce
from typing import List, Tuple
from pyspark.sql import *
from pyspark.sql.functions import lit, concat, col
import os.path
import argparse

def read_dataframe(spark: SparkSession, path: str) -> DataFrame:
    return (spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", "|")
    .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
    .load(path))

def make_edge_dataframes_org_and_place(spark: SparkSession, directories: List[str], workdir: str) -> List[Tuple[str, DataFrame]]:
    edge_dataframes: List[Tuple[str, DataFrame]] = []

    # For each directory in the filtered list
    for directory in directories:
        # Get a list of all subdirectories in the directory
        subdirectories = [
            sd
            for sd in os.listdir(os.path.join(workdir, directory))
            if os.path.isdir(os.path.join(workdir, directory, sd))
        ]

        # Filter out subdirectories that don't contain an underscore or contain 'Organisation' or 'Place'
        subdirectories = [
            sd
            for sd in subdirectories
            if "_" in sd and ("Organisation" in sd or "Place" in sd)
        ]

        # For each subdirectory in the filtered list
        for subdirectory in subdirectories:
            print("Loading data from {}/{}".format(directory, subdirectory))
            # Extract the first and last parts of the subdirectory name
            src_type, layer, dst_type = subdirectory.split("_")

            # Construct a glob pattern for '.csv.gz' files in the subdirectory
            file_pattern = os.path.join(workdir, directory, subdirectory, "*.csv.gz")

            src_path = os.path.join(workdir, directory, src_type, "*.csv.gz")
            dst_path = os.path.join(workdir, directory, dst_type, "*.csv.gz")

            # Load the files into a DataFrame using Spark
            read_dataframe(spark, src_path).createOrReplaceTempView("src")

            read_dataframe(spark, dst_path).createOrReplaceTempView("dst")

            read_dataframe(spark, file_pattern).createOrReplaceTempView("edge")

            if src_type == dst_type:
                src_type = "{}1".format(src_type)
                dst_type = "{}2".format(dst_type)

            df = spark.sql("""
                SELECT concat(src.type,'/',src.id) as src, concat(dst.type,'/',dst.id) as dst
                FROM edge
                JOIN src ON edge.{}Id = src.id
                JOIN dst ON edge.{}Id = dst.id
                ORDER BY src, dst
            """.format(src_type, dst_type))

            df = df.withColumn("time", lit(0))
            
            df.show()

            df.printSchema()

            edge_dataframes.append((layer, df))

    return edge_dataframes


def make_edge_dataframes(
    directories: List[str], workdir: str
) -> List[Tuple[str, DataFrame]]:
    edge_dataframes: List[Tuple[str, DataFrame]] = []

    # For each directory in the filtered list
    for directory in directories:
        # Get a list of all subdirectories in the directory
        subdirectories = [
            sd
            for sd in os.listdir(os.path.join(workdir, directory))
            if os.path.isdir(os.path.join(workdir, directory, sd))
        ]

        # Filter out subdirectories that don't contain an underscore or contain 'Organisation' or 'Place'
        subdirectories = [
            sd
            for sd in subdirectories
            if "_" in sd and "Organisation" not in sd and "Place" not in sd
        ]

        # For each subdirectory in the filtered list
        for subdirectory in subdirectories:
            print("Loading data from {}/{}".format(directory, subdirectory))
            # Extract the first and last parts of the subdirectory name
            src_type, layer, dst_type = subdirectory.split("_")

            # Construct a glob pattern for '.csv.gz' files in the subdirectory
            file_pattern = os.path.join(workdir, directory, subdirectory, "*.csv.gz")

            # Load the files into a DataFrame using Spark
            df = read_dataframe(spark, file_pattern)

            # Find the first column that ends in 'Id' and call it 'src'
            src = next(col for col in df.columns if col.endswith("Id"))

            # Find the second column that ends in 'Id' and call it 'dst'
            dst = next(col for col in df.columns if col.endswith("Id") and col != src)

            # Make a 'src' column that contains concatenated 'src_type/src' and a 'dst' column that contains 'dst_type/dst'
            df = df.withColumn("src", concat(lit(src_type), lit("/"), col(src))).drop(
                src
            )
            df = df.withColumn("dst", concat(lit(dst_type), lit("/"), col(dst))).drop(
                dst
            )

            for col_name in df.columns:
                if "date" in col_name.lower():
                    df = df.withColumnRenamed(col_name, "time")

            # Add a time column if it doesn't exist and fill it with 0
            if "time" not in df.columns:
                df = df.withColumn("time", lit(0))

            # Sort the DataFrame by 'src' and 'dst' if they contain the time column then sort by that too
            df = df.orderBy(["src", "dst", "time"])

            df.show()

            # check the schema for the column type, it should be i64 or long
            df.printSchema()

            # Add the DataFrame to the list
            edge_dataframes.append((layer, df))

    return edge_dataframes


def make_node_dataframes(directories: List[str], workdir: str) -> List[DataFrame]:
    node_dataframes: List[DataFrame] = []
    # For each directory in the filtered list
    for directory in directories:
        # Get a list of all subdirectories in the directory
        subdirectories = [
            sd
            for sd in os.listdir(os.path.join(workdir, directory))
            if os.path.isdir(os.path.join(workdir, directory, sd))
        ]

        # Filter out subdirectories that contain an underscore
        subdirectories = [sd for sd in subdirectories if "_" not in sd]

        # For each subdirectory in the filtered list
        for subdirectory in subdirectories:
            print("Loading data from {}/{}".format(directory, subdirectory))
            # Construct a glob pattern for '.csv.gz' files in the subdirectory
            file_pattern = os.path.join(workdir, directory, subdirectory, "*.csv.gz")

            # Load the files into a DataFrame using Spark
            df = read_dataframe(spark, file_pattern)

            # If the DataFrame doesn't have a column named 'type', add it
            if "type" not in df.columns:
                df = df.withColumn("type", lit(subdirectory))

            # Add a 'gid' column that is the concatenation of 'type' and 'id' columns
            df = df.withColumn("gid", concat(col("type"), lit("/"), col("id"))).drop(
                "id"
            )

            # Add the DataFrame to the list
            node_dataframes.append(df)
    return node_dataframes


if __name__ == "__main__":
    # Initialize argument parser
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--workdir", default=os.path.dirname(os.path.realpath(__file__))
    )
    args = parser.parse_args()

    # Initialize SparkSession
    spark = SparkSession.builder.config("spark.driver.memory", "64g").getOrCreate()

    # Define the workdir
    workdir = args.workdir

    # Get a list of all directories in the workdir
    directories = [
            d for d in os.listdir(workdir) if os.path.isdir(os.path.join(workdir, d))
    ]

    org_place_edge_lists:List[Tuple[str, DataFrame]] = make_edge_dataframes_org_and_place(spark, directories, workdir)

    # write each dataframe in ldbc_edges directory
    for layer, df in org_place_edge_lists:
        df.write.mode("overwrite").parquet(os.path.join(workdir, "ldbc_edges", layer))

    edge_dataframes: List[Tuple[str, DataFrame]] = make_edge_dataframes(directories, workdir)

    # write each dataframe in ldbc_edges directory
    for layer, df in edge_dataframes:
        df.write.mode("overwrite").parquet(os.path.join(workdir, "ldbc_edges", layer))

    node_dataframes: List[DataFrame] = make_node_dataframes(directories, workdir)

    print("Loaded {} DataFrames".format(len(node_dataframes)))
    # now Union all the dataframes make sure the columns that are not in all dataframes are filled with null
    df: DataFrame = reduce(
        lambda x, y: x.unionByName(y, allowMissingColumns=True), node_dataframes
    )
    df.orderBy(["gid"]).write.mode("overwrite").parquet(
        os.path.join(workdir, "ldbc_nodes")
    )
