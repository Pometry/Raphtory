from pathlib import Path

import polars as pl
import pandas as pd
import pyarrow as pa
from raphtory import Graph, PersistentGraph, PropType
import pytest

try:
    import fireducks.pandas as fpd
except ModuleNotFoundError:
    fpd = None


def _collect_edges(g: Graph):
    return sorted((e.history.t[0], e.src.id, e.dst.id, e["value"]) for e in g.edges)


@pytest.mark.parametrize("graph_type", [Graph, PersistentGraph])
def test_load_edges_from_polars_df(graph_type):
    df = pl.DataFrame(
        {
            "time": [1, 2, 3],
            "src": [1, 2, 3],
            "dst": [2, 3, 4],
            "value": [10.0, 20.0, 30.0],
        }
    )

    g_to_pandas = graph_type()
    g_to_pandas.load_edges_from_pandas(
        df=df.to_pandas(), time="time", src="src", dst="dst", properties=["value"]
    )

    g_from_df = graph_type()
    g_from_df.load_edges_from_df(
        data=df, time="time", src="src", dst="dst", properties=["value"]
    )

    expected = [(1, 1, 2, 10.0), (2, 2, 3, 20.0), (3, 3, 4, 30.0)]
    assert _collect_edges(g_to_pandas) == _collect_edges(g_from_df)
    assert _collect_edges(g_to_pandas) == expected
    assert _collect_edges(g_from_df) == expected

def test_different_data_sources():
    nodes_list = []

    ######### PARQUET #########
    parquet_dir_path_str = str(Path(__file__).parent) + "/data/btc_dataset/parquet_directory"
    parquet_file_path_str = str(Path(__file__).parent) + "/data/btc_dataset/flattened_data.parquet"
    # test path string for parquet file
    g = Graph()
    g.load_nodes(data=parquet_file_path_str, time="block_timestamp", id="inputs_address")
    nodes_list.append(sorted(g.nodes.id.collect()))
    del g

    # test Path object for parquet file
    file_path_obj = Path(parquet_file_path_str)
    g = Graph()
    g.load_nodes(data=file_path_obj, time="block_timestamp", id="inputs_address")
    nodes_list.append(sorted(g.nodes.id.collect()))
    del g

    # test path string for parquet directory
    g = Graph()
    g.load_nodes(data=parquet_dir_path_str, time="block_timestamp", id="inputs_address")
    nodes_list.append(sorted(g.nodes.id.collect()))
    del g

    # test Path object for parquet directory
    dir_path_obj = Path(parquet_dir_path_str)
    g = Graph()
    g.load_nodes(data=dir_path_obj, time="block_timestamp", id="inputs_address")
    nodes_list.append(sorted(g.nodes.id.collect()))
    del g

    ######### CSV #########
    csv_dir_path_str = str(Path(__file__).parent) + "/data/btc_dataset/csv_directory"
    csv_file_path_str = str(Path(__file__).parent) + "/data/btc_dataset/flattened_data.csv"
    # test path string for CSV file
    g = Graph()
    g.load_nodes(data=csv_file_path_str, time="block_timestamp", id="inputs_address")
    nodes_list.append(sorted(g.nodes.id.collect()))
    del g

    # test Path object for CSV file
    file_path_obj = Path(csv_file_path_str)
    g = Graph()
    g.load_nodes(data=file_path_obj, time="block_timestamp", id="inputs_address")
    nodes_list.append(sorted(g.nodes.id.collect()))
    del g

    # test path string for CSV directory
    g = Graph()
    g.load_nodes(data=csv_dir_path_str, time="block_timestamp", id="inputs_address")
    nodes_list.append(sorted(g.nodes.id.collect()))
    del g

    # test Path object for CSV directory
    dir_path_obj = Path(csv_dir_path_str)
    g = Graph()
    g.load_nodes(data=dir_path_obj, time="block_timestamp", id="inputs_address")
    nodes_list.append(sorted(g.nodes.id.collect()))
    del g

    ######### mixed directory #########
    mixed_dir_path_str = str(Path(__file__).parent) + "/data/btc_dataset/mixed_directory"
    # test path string
    g = Graph()
    g.load_nodes(data=mixed_dir_path_str, time="block_timestamp", id="inputs_address")
    nodes_list.append(sorted(g.nodes.id.collect()))
    del g

    # test Path object
    g = Graph()
    g.load_nodes(data=Path(mixed_dir_path_str), time="block_timestamp", id="inputs_address")
    nodes_list.append(sorted(g.nodes.id.collect()))
    del g

    ######### arrow_c_stream #########
    # test pandas
    df_pd = pd.read_parquet(parquet_file_path_str)
    g = Graph()
    g.load_nodes(data=df_pd, time="block_timestamp", id="inputs_address")
    nodes_list.append(sorted(g.nodes.id.collect()))
    del g, df_pd

    # test polars
    df_pl = pl.read_parquet(parquet_file_path_str)
    g = Graph()
    g.load_nodes(data=df_pl, time="block_timestamp", id="inputs_address")
    nodes_list.append(sorted(g.nodes.id.collect()))
    del g, df_pl

    # sanity check, make sure we ingested the same number of nodes each time
    print(f"Number of tests ran: {len(nodes_list)}")
    for i in range(len(nodes_list)-1):
        assert nodes_list[0] == nodes_list[i+1], f"Nodes list assertion failed at item i={i}"

def test_schema_casting():
    # time/id as regular ints (I64), value column as explicit int32
    df = pd.DataFrame(
        {
            "time": pd.Series([1, 2, 3], dtype="int64"),
            "id": pd.Series([10, 20, 30], dtype="int64"),
            "val_i32": pd.Series([1, 2, 3], dtype="int32"),
        }
    )
    g = Graph()
    # No casting
    g.load_nodes(
        data=df,
        time="time",
        id="id",
        properties=["val_i32"],
    )
    n_prop_dtype = g.node(10).properties.get_dtype_of("val_i32")
    assert n_prop_dtype == PropType.i32()
    del g, n_prop_dtype

    # Cast the val_i32 column to I64 using PropType.i64()
    g = Graph()
    g.load_nodes(
        data=df,
        time="time",
        id="id",
        properties=["val_i32"],
        schema=[("val_i32", PropType.i64())],
    )
    n_prop_dtype = g.node(10).properties.get_dtype_of("val_i32")
    assert n_prop_dtype == PropType.i64()
    del g, n_prop_dtype

    # Cast the val_i32 column to I64 using PyArrow int64 DataType
    g = Graph()
    g.load_nodes(
        data=df,
        time="time",
        id="id",
        properties=["val_i32"],
        schema=[("val_i32", pa.int64())],
    )
    n_prop_dtype = g.node(10).properties.get_dtype_of("val_i32")
    assert n_prop_dtype == PropType.i64()


def test_list_schema_casting():
    table = pa.Table.from_pydict(
        {
            "time": pa.array([1, 2, 3], type=pa.int64()),
            "id": pa.array([10, 20, 30], type=pa.int64()),
            "val_list_i32": pa.array(
                [[1, 2], [3, 4], [5, 6]],
                type=pa.list_(pa.int32()),
            ),
        }
    )

    # No casting
    g = Graph()
    g.load_nodes(data=table, time="time", id="id", properties=["val_list_i32"])
    n_prop_dtype = g.node(10).properties.get_dtype_of("val_list_i32")
    assert n_prop_dtype == PropType.list(PropType.i32())
    del g, n_prop_dtype

    # Cast the val_list_i32 column to I64 using PropType.list(PropType.i64())
    g = Graph()
    g.load_nodes(
        data=table,
        time="time",
        id="id",
        properties=["val_list_i32"],
        schema=[("val_list_i32", PropType.list(PropType.i64()))],
    )
    n_prop_dtype = g.node(10).properties.get_dtype_of("val_list_i32")
    assert n_prop_dtype == PropType.list(PropType.i64())
    del g, n_prop_dtype

    # Cast the val_list_i32 column to I64 using PyArrow list<int64> DataType
    g = Graph()
    g.load_nodes(
        data=table,
        time="time",
        id="id",
        properties=["val_list_i32"],
        schema=[("val_list_i32", pa.list_(pa.int64()))],
    )
    n_prop_dtype = g.node(10).properties.get_dtype_of("val_list_i32")
    assert n_prop_dtype == PropType.list(PropType.i64())

def test_schema_casting_dict():
    # time/id as regular ints (I64), value column as explicit int32
    df = pd.DataFrame(
        {
            "time": pd.Series([1, 2, 3], dtype="int64"),
            "id": pd.Series([10, 20, 30], dtype="int64"),
            "val_i32": pd.Series([1, 2, 3], dtype="int32"),
        }
    )

    # schema casting as list
    g_list = Graph()
    g_list.load_nodes(
        data=df,
        time="time",
        id="id",
        properties=["val_i32"],
        schema=[("val_i32", PropType.i64())],
    )
    dtype_list = [g_list.node(10).properties.get_dtype_of("val_i32")]
    del g_list

    # schema casting as dict using PropType
    g_dict_proptype = Graph()
    g_dict_proptype.load_nodes(
        data=df,
        time="time",
        id="id",
        properties=["val_i32"],
        schema={"val_i32": PropType.i64()},
    )
    dtype_list.append(g_dict_proptype.node(10).properties.get_dtype_of("val_i32"))
    del g_dict_proptype

    # schema casting as dict using pyarrow DataType
    g_dict_pa = Graph()
    g_dict_pa.load_nodes(
        data=df,
        time="time",
        id="id",
        properties=["val_i32"],
        schema={"val_i32": pa.int64()},
    )
    dtype_list.append(g_dict_pa.node(10).properties.get_dtype_of("val_i32"))
    del g_dict_pa

    for dtype in dtype_list:
        assert dtype == PropType.i64()

def test_nested_schema_casting():
    # types to make sure the table is built properly and test the types
    struct_type_i32 = pa.struct(
        [
            pa.field("a", pa.int32()),
            pa.field("b", pa.int32()),
        ]
    )
    struct_type_i64 = pa.struct(
        [
            pa.field("a", pa.int64()),
            pa.field("b", pa.int64()),
        ]
    )

    table = pa.Table.from_pydict(
        {
            "time": pa.array([1, 2, 3], type=pa.int64()),
            "id": pa.array([10, 20, 30], type=pa.int64()),
            "val_struct": pa.array(
                [
                    {"a": 1, "b": 10},
                    {"a": 2, "b": 20},
                    {"a": 3, "b": 30},
                ],
                type=struct_type_i32,
            ),
        }
    )

    # no casting
    g = Graph()
    g.load_nodes(
        data=table,
        time="time",
        id="id",
        properties=["val_struct"],
    )
    d_type_no_cast = g.node(10).properties.get_dtype_of("val_struct")
    del g

    assert d_type_no_cast == struct_type_i32
    assert d_type_no_cast == PropType.map({"a": PropType.i32(), "b": PropType.i32()})
    # also check PropType.map of pyarrow types, mix and match
    assert d_type_no_cast == PropType.map({"a": pa.int32(), "b": pa.int32()})

    # schema is a PropType.map(...) inside a dict
    g = Graph()
    g.load_nodes(
        data=table,
        time="time",
        id="id",
        properties=["val_struct"],
        schema={
            "val_struct": PropType.map(
                {
                    "a": PropType.i64(),
                    "b": PropType.i64(),
                }
            )
        },
    )
    dtype_proptype = g.node(10).properties.get_dtype_of("val_struct")
    del g

    assert dtype_proptype == struct_type_i64
    assert dtype_proptype == PropType.map({"a": PropType.i64(), "b": PropType.i64()})
    # also check PropType.map of pyarrow types, mix and match
    assert dtype_proptype == PropType.map({"a": pa.int64(), "b": pa.int64()})

    # schema is a PropType.map(...) with mixed pyarrow and PropType types
    g = Graph()
    g.load_nodes(
        data=table,
        time="time",
        id="id",
        properties=["val_struct"],
        schema={
            "val_struct": PropType.map(
                {
                    "a": pa.int64(),
                    "b": pa.int64(),
                }
            )
        },
    )
    dtype_mixed = g.node(10).properties.get_dtype_of("val_struct")
    del g

    assert dtype_mixed == struct_type_i64
    assert dtype_mixed == PropType.map({"a": PropType.i64(), "b": PropType.i64()})
    # also check PropType.map of pyarrow types, mix and match
    assert dtype_mixed == PropType.map({"a": pa.int64(), "b": pa.int64()})

    # schema is defined using pyarrow
    g = Graph()
    g.load_nodes(
        data=table,
        time="time",
        id="id",
        properties=["val_struct"],
        schema={"val_struct": struct_type_i64},
    )
    dtype_pyarrow = g.node(10).properties.get_dtype_of("val_struct")
    del g

    assert dtype_pyarrow == dtype_proptype
    assert dtype_pyarrow == struct_type_i64
    assert dtype_pyarrow == PropType.map({"a": PropType.i64(), "b": PropType.i64()})
    # also check PropType.map of pyarrow types, mix and match
    assert dtype_pyarrow == PropType.map({"a": pa.int64(), "b": pa.int64()})

def _btc_root() -> Path:
    return Path(__file__).parent / "data" / "btc_dataset"

def _csv_expected_earliest_dt(paths: list[Path]):
    df = pd.concat([pd.read_csv(p) for p in paths], ignore_index=True)
    return pd.to_datetime(df["block_timestamp"], utc=True).min().to_pydatetime()

def _parquet_expected_earliest_dt(paths: list[Path]):
    df = pd.concat([pd.read_parquet(p) for p in paths], ignore_index=True)
    return pd.to_datetime(df["block_timestamp"], utc=True).min().to_pydatetime()

@pytest.mark.parametrize("schema_value", [PropType.datetime(), pa.timestamp("ms", tz="UTC")])
def test_casting_btc_csv_file(schema_value):
    csv_path = _btc_root() / "flattened_data.csv"
    expected_earliest = _csv_expected_earliest_dt([csv_path])

    # Pick a node id from the file
    df = pd.read_csv(csv_path)
    some_node_id = df["inputs_address"].iloc[0]

    g = Graph()
    g.load_nodes(
        data=str(csv_path),
        time="block_timestamp",
        id="inputs_address",
        properties=["block_timestamp"],
        schema={"block_timestamp": schema_value},
    )

    dtype = g.node(some_node_id).properties.get_dtype_of("block_timestamp")
    assert dtype == PropType.datetime()
    assert dtype == pa.timestamp("ms", tz="UTC")
    assert g.earliest_time.dt == expected_earliest

@pytest.mark.parametrize("schema_value", [PropType.datetime(), pa.timestamp("ms", tz="UTC")])
def test_casting_btc_csv_directory(schema_value):
    csv_dir = _btc_root() / "csv_directory"
    csv_paths = sorted(p for p in csv_dir.iterdir() if p.suffix == ".csv")
    expected_earliest = _csv_expected_earliest_dt(csv_paths)

    df0 = pd.read_csv(csv_paths[0])
    some_node_id = df0["inputs_address"].iloc[0]

    g = Graph()
    g.load_nodes(
        data=str(csv_dir),
        time="block_timestamp",
        id="inputs_address",
        properties=["block_timestamp"],
        schema={"block_timestamp": schema_value},
    )

    dtype = g.node(some_node_id).properties.get_dtype_of("block_timestamp")
    assert dtype == PropType.datetime()
    assert dtype == pa.timestamp("ms", tz="UTC")
    assert g.earliest_time.dt == expected_earliest

@pytest.mark.parametrize("schema_value", [PropType.datetime(), pa.timestamp("ms", tz="UTC")])
def test_casting_btc_parquet_file(schema_value):
    pq_path = _btc_root() / "flattened_data.parquet"
    expected_earliest = _parquet_expected_earliest_dt([pq_path])

    df = pd.read_parquet(pq_path)
    some_node_id = df["inputs_address"].iloc[0]

    g = Graph()
    g.load_nodes(
        data=str(pq_path),
        time="block_timestamp",
        id="inputs_address",
        properties=["block_timestamp"],
        schema={"block_timestamp": schema_value},
    )

    dtype = g.node(some_node_id).properties.get_dtype_of("block_timestamp")
    assert dtype == PropType.datetime()
    assert dtype == pa.timestamp("ms", tz="UTC")
    assert g.earliest_time.dt == expected_earliest

@pytest.mark.parametrize("schema_value", [PropType.datetime(), pa.timestamp("ms", tz="UTC")])
def test_casting_btc_parquet_directory(schema_value):
    pq_dir = _btc_root() / "parquet_directory"
    pq_paths = sorted(p for p in pq_dir.iterdir() if p.suffix == ".parquet")
    expected_earliest = _parquet_expected_earliest_dt(pq_paths)

    df0 = pd.read_parquet(pq_paths[0])
    some_node_id = df0["inputs_address"].iloc[0]

    g = Graph()
    g.load_nodes(
        data=str(pq_dir),
        time="block_timestamp",
        id="inputs_address",
        properties=["block_timestamp"],
        schema={"block_timestamp": schema_value},
    )

    dtype = g.node(some_node_id).properties.get_dtype_of("block_timestamp")
    assert dtype == PropType.datetime()
    assert dtype == pa.timestamp("ms", tz="UTC")
    assert g.earliest_time.dt == expected_earliest

@pytest.mark.parametrize("schema_value", [PropType.datetime(), pa.timestamp("ms", tz="UTC")])
def test_casting_btc_mixed_directory(schema_value):
    mixed_dir = _btc_root() / "mixed_directory"
    csv_paths = sorted(p for p in mixed_dir.iterdir() if p.suffix == ".csv")
    pq_paths = sorted(p for p in mixed_dir.iterdir() if p.suffix == ".parquet")

    # Compute expected earliest across both formats
    expected_csv = _csv_expected_earliest_dt(csv_paths)
    expected_pq = _parquet_expected_earliest_dt(pq_paths)
    expected_earliest = min(expected_csv, expected_pq)

    # Use an id from one of the files
    some_node_id = pd.read_csv(csv_paths[0])["inputs_address"].iloc[0]

    g = Graph()
    g.load_nodes(
        data=str(mixed_dir),
        time="block_timestamp",
        id="inputs_address",
        properties=["block_timestamp"],
        schema={"block_timestamp": schema_value},
    )

    dtype = g.node(some_node_id).properties.get_dtype_of("block_timestamp")
    assert dtype == PropType.datetime()
    assert dtype == pa.timestamp("ms", tz="UTC")
    assert g.earliest_time.dt == expected_earliest

def test_malformed_files():
    malformed_dir = _btc_root() / "malformed_files"

    with pytest.raises(Exception) as missing_col_error:
        g = Graph()
        g.load_nodes(
            data=malformed_dir / "missing_col.parquet",
            time="block_timestamp",
            id="inputs_address",
            properties=["block_timestamp"],
        )
    assert "columns are not present" in str(missing_col_error.value)
    assert "block_timestamp" in str(missing_col_error.value)

    with pytest.raises(Exception) as malformed_timestamp_error:
        g = Graph()
        g.load_nodes(
            data=malformed_dir / "timestamp_malformed.parquet",
            time="block_timestamp",
            id="inputs_address",
            properties=["block_timestamp"],
            schema={"block_timestamp": pa.timestamp("ms", tz="UTC")}
        )
    print(f"Error: {malformed_timestamp_error.value}")

if fpd:
    import pandas

    @pytest.mark.parametrize("graph_type", [Graph, PersistentGraph])
    def test_load_edges_from_fireducks_df(graph_type):
        # FireDucks DataFrame (pandas-compatible API)
        df = fpd.DataFrame(
            {
                "time": [1, 2, 3],
                "src": [1, 2, 3],
                "dst": [2, 3, 4],
                "value": [10.0, 20.0, 30.0],
            }
        )

        g = graph_type()
        g.load_edges_from_df(
            data=df, time="time", src="src", dst="dst", properties=["value"]
        )
        assert [(1, 1, 2, 10.0), (2, 2, 3, 20.0), (3, 3, 4, 30.0)] == _collect_edges(g)

    @pytest.mark.parametrize("graph_type", [Graph, PersistentGraph])
    def test_fireducks_matches_pandas_for_same_edges(graph_type):
        df_fireducks = fpd.DataFrame(
            {
                "time": [1, 2, 3],
                "src": [1, 2, 3],
                "dst": [2, 3, 4],
                "value": [10.0, 20.0, 30.0],
            }
        )
        df_pandas = pandas.DataFrame(
            {
                "time": [1, 2, 3],
                "src": [1, 2, 3],
                "dst": [2, 3, 4],
                "value": [10.0, 20.0, 30.0],
            }
        )

        g_fireducks = graph_type()
        g_fireducks.load_edges_from_df(
            data=df_fireducks, time="time", src="src", dst="dst", properties=["value"]
        )

        g_pandas = graph_type()
        g_pandas.load_edges_from_pandas(
            df=df_pandas, time="time", src="src", dst="dst", properties=["value"]
        )

        expected = [(1, 1, 2, 10.0), (2, 2, 3, 20.0), (3, 3, 4, 30.0)]

        assert _collect_edges(g_fireducks) == _collect_edges(g_pandas)
        assert _collect_edges(g_fireducks) == expected
        assert _collect_edges(g_pandas) == expected
