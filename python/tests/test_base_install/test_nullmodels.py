import numpy as np
import pandas as pd
from raphtory import nullmodels


def test_shuffle_column_keeps_every_value_and_the_dtype():
    df = pd.DataFrame(
        {
            "src": list("abcdef"),
            "time": pd.array([10, 20, 30, 40, 50, None], dtype="Int64"),
        }
    )
    df = df[df.index > 1]  # not the default index

    shuffled = nullmodels.shuffle_column(df, col_name="time")

    assert sorted(shuffled["time"].dropna().tolist()) == [30, 40, 50]
    assert shuffled["time"].isna().sum() == 1
    assert shuffled["time"].dtype == df["time"].dtype
    assert shuffled["src"].tolist() == df["src"].tolist()


def test_shuffle_multiple_columns_shuffles_every_column():
    np.random.seed(0)
    df = pd.DataFrame({"src": range(50), "dst": range(50, 100)})
    original = df.copy()

    shuffled = nullmodels.shuffle_multiple_columns(df, col_names=["src", "dst"])

    for column in ["src", "dst"]:
        assert sorted(shuffled[column]) == sorted(original[column])
        assert shuffled[column].tolist() != original[column].tolist()
    pd.testing.assert_frame_equal(df, original)
