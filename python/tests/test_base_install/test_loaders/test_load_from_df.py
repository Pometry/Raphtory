from pathlib import Path

import pandas as pd
import pyarrow as pa
import pytest
from raphtory import Graph, PersistentGraph, PropType


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
    return Path(__file__).parent.parent.parent / "data" / "btc_dataset"


def _csv_expected_earliest_dt(paths: list[Path]):
    df = pd.concat([pd.read_csv(p) for p in paths], ignore_index=True)
    return pd.to_datetime(df["block_timestamp"], utc=True).min().to_pydatetime()


def _parquet_expected_earliest_dt(paths: list[Path]):
    df = pd.concat([pd.read_parquet(p) for p in paths], ignore_index=True)
    return pd.to_datetime(df["block_timestamp"], utc=True).min().to_pydatetime()


@pytest.mark.parametrize(
    "schema_value", [PropType.datetime(), pa.timestamp("ms", tz="UTC")]
)
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


@pytest.mark.parametrize(
    "schema_value", [PropType.datetime(), pa.timestamp("ms", tz="UTC")]
)
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


@pytest.mark.parametrize(
    "schema_value", [PropType.datetime(), pa.timestamp("ms", tz="UTC")]
)
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


@pytest.mark.parametrize(
    "schema_value", [PropType.datetime(), pa.timestamp("ms", tz="UTC")]
)
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


@pytest.mark.parametrize(
    "schema_value", [PropType.datetime(), pa.timestamp("ms", tz="UTC")]
)
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


def test_malformed_files_and_directory():
    empty_dir = _btc_root() / "empty_directory"
    with pytest.raises(
        Exception,
        match="Paths must either point to a Parquet/CSV file, or a directory containing Parquet/CSV files",
    ):
        g = Graph()
        g.load_nodes(
            data=empty_dir,
            time="block_timestamp",
            id="inputs_address",
            properties=["outputs_address"],
        )

    malformed_dir = _btc_root() / "malformed_files"
    for malformed_file in malformed_dir.iterdir():
        # couldn't create a parquet file malformed with an extra column in a row
        if "extra_field" in malformed_file.name:
            with pytest.raises(
                Exception,
                match="incorrect number of fields for line 2, expected 3 got 4",
            ):
                g = Graph()
                g.load_nodes(
                    data=malformed_file,
                    time="block_timestamp",
                    id="inputs_address",
                    properties=["outputs_address"],
                )

        if "impossible_date" in malformed_file.name:
            with pytest.raises(Exception) as e:
                g = Graph()
                g.load_nodes(
                    data=malformed_file,
                    time="block_timestamp",
                    id="inputs_address",
                    properties=["outputs_address"],
                )
            assert ("Error during parsing of time string" in str(e.value)) or (
                "Error parsing timestamp from '2025-99-99 99:99:99'" in str(e.value)
            )

        if "missing_field" in malformed_file.name:
            g = Graph()
            g.load_nodes(
                data=malformed_file,
                time="block_timestamp",
                id="inputs_address",
                properties=["outputs_address"],
            )
            n = g.node("bc1qabc")
            assert n.history[0] == "2025-11-10 00:28:09"
            assert n.properties.get("outputs_address") is None
            with pytest.raises(Exception, match="'No such property'"):
                n.properties["outputs_address"]

        if "missing_id_col" in malformed_file.name:
            with pytest.raises(
                Exception,
                match="columns are not present within the dataframe: inputs_address",
            ):
                g = Graph()
                g.load_nodes(
                    data=malformed_file,
                    time="block_timestamp",
                    id="inputs_address",
                    properties=["outputs_address"],
                )

        if "missing_prop_col" in malformed_file.name:
            with pytest.raises(
                Exception,
                match="columns are not present within the dataframe: outputs_address",
            ):
                g = Graph()
                g.load_nodes(
                    data=malformed_file,
                    time="block_timestamp",
                    id="inputs_address",
                    properties=["outputs_address"],
                )

        if "missing_timestamp_col" in malformed_file.name:
            with pytest.raises(
                Exception,
                match="columns are not present within the dataframe: block_timestamp",
            ):
                g = Graph()
                g.load_nodes(
                    data=malformed_file,
                    time="block_timestamp",
                    id="inputs_address",
                    properties=["outputs_address"],
                )

        if "null_id.csv" in malformed_file.name:
            with pytest.raises(Exception, match="Null not supported as node id"):
                g = Graph()
                g.load_nodes(
                    data=malformed_file,
                    time="block_timestamp",
                    id="inputs_address",
                    properties=["outputs_address"],
                )

        # in parquet, null value gets interpreted as Float64
        if "null_id.parquet" in malformed_file.name:
            with pytest.raises(
                Exception, match="Float64 not supported as node id type"
            ):
                g = Graph()
                g.load_nodes(
                    data=malformed_file,
                    time="block_timestamp",
                    id="inputs_address",
                    properties=["outputs_address"],
                )

        if "null_timestamp.csv" in malformed_file.name:
            with pytest.raises(Exception, match="Null not supported for time column"):
                g = Graph()
                g.load_nodes(
                    data=malformed_file,
                    time="block_timestamp",
                    id="inputs_address",
                    properties=["outputs_address"],
                )

        if "null_timestamp.parquet" in malformed_file.name:
            with pytest.raises(Exception, match="Missing value for timestamp"):
                g = Graph()
                g.load_nodes(
                    data=malformed_file,
                    time="block_timestamp",
                    id="inputs_address",
                    properties=["outputs_address"],
                )

        if "out_of_range_timestamp" in malformed_file.name:
            with pytest.raises(
                Exception, match="'999999999999999999999' is not a valid datetime"
            ):
                g = Graph()
                g.load_nodes(
                    data=malformed_file,
                    time="block_timestamp",
                    id="inputs_address",
                    properties=["outputs_address"],
                )

        # not applicable to csv
        if "semicolon_delimiter" in malformed_file.name:
            with pytest.raises(
                Exception,
                match="the following columns are not present within the dataframe",
            ):
                g = Graph()
                g.load_nodes(
                    data=malformed_file,
                    time="block_timestamp",
                    id="inputs_address",
                    properties=["outputs_address"],
                )
            g = Graph()

            g.load_nodes(
                data=malformed_file,
                time="block_timestamp",
                id="inputs_address",
                properties=["outputs_address"],
                csv_options={"delimiter": ";"},
            )
            assert g.node("bc1qabc").history[0] == "2025-11-10 00:28:09"

        if "timestamp_malformed" in malformed_file.name:
            with pytest.raises(Exception, match="Missing value for timestamp"):
                g = Graph()
                g.load_nodes(
                    data=malformed_file,
                    time="block_timestamp",
                    id="inputs_address",
                    properties=["block_timestamp"],
                    schema={"block_timestamp": pa.timestamp("ms", tz="UTC")},
                )


def _two_edges_table():
    return pa.table({"src": ["a", "b"], "dst": ["b", "c"], "ts": [1, 2]})


def test_load_edges_from_duckdb_relation():
    duckdb = pytest.importorskip("duckdb")
    rel = duckdb.query("SELECT * FROM (VALUES ('a','b',1),('b','c',2)) t(src,dst,ts)")
    g = Graph()
    g.load_edges(data=rel, src="src", dst="dst", time="ts")
    assert g.count_edges() == 2
    assert g.count_temporal_edges() == 2


def test_load_edges_asks_for_len_before_exporting_the_stream():
    calls = []

    class Recording:
        def __init__(self, table):
            self._table = table

        def __arrow_c_stream__(self, requested_schema=None):
            calls.append("__arrow_c_stream__")
            return self._table.__arrow_c_stream__(requested_schema)

        def __len__(self):
            calls.append("__len__")
            return self._table.num_rows

    g = Graph()
    g.load_edges(data=Recording(_two_edges_table()), src="src", dst="dst", time="ts")
    assert g.count_edges() == 2
    # a lazy producer re-executes on len(), which drains a stream exported before it
    assert calls == ["__len__", "__arrow_c_stream__"]


@pytest.mark.parametrize("claimed_len", [0, 1, 1000])
def test_load_edges_does_not_trust_a_lying_len(claimed_len):
    class LyingLen:
        def __init__(self, table):
            self._table = table

        def __arrow_c_stream__(self, requested_schema=None):
            return self._table.__arrow_c_stream__(requested_schema)

        def __len__(self):
            return claimed_len

    g = Graph()
    g.load_edges(data=LyingLen(_two_edges_table()), src="src", dst="dst", time="ts")
    assert g.count_edges() == 2


def test_load_edges_survives_a_len_that_raises():
    class RaisingLen:
        def __init__(self, table):
            self._table = table

        def __arrow_c_stream__(self, requested_schema=None):
            return self._table.__arrow_c_stream__(requested_schema)

        def __len__(self):
            raise TypeError("no length here")

    g = Graph()
    g.load_edges(data=RaisingLen(_two_edges_table()), src="src", dst="dst", time="ts")
    assert g.count_edges() == 2


def test_load_nodes_and_metadata_from_a_lying_len():
    class LyingLen:
        def __init__(self, table):
            self._table = table

        def __arrow_c_stream__(self, requested_schema=None):
            return self._table.__arrow_c_stream__(requested_schema)

        def __len__(self):
            return 0

    nodes = pa.table({"id": ["a", "b"], "ts": [1, 2], "m": ["x", "y"]})
    g = Graph()
    g.load_nodes(data=LyingLen(nodes), id="id", time="ts")
    assert g.count_nodes() == 2
    g.load_node_metadata(data=LyingLen(nodes), id="id", metadata=["m"])
    assert g.node("a").metadata.get("m") == "x"
    g.load_edges(data=LyingLen(_two_edges_table()), src="src", dst="dst", time="ts")
    edges = pa.table({"src": ["a"], "dst": ["b"], "m": ["e"]})
    g.load_edge_metadata(data=LyingLen(edges), src="src", dst="dst", metadata=["m"])
    assert g.edge("a", "b").metadata.get("m") == "e"
