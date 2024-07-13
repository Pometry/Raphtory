"""
Generate randomised reference models for a temporal graph edgelist
"""

import pandas as pd


def shuffle_column(
    graph_df: pd.DataFrame, col_number=None, col_name=None, inplace=False
):
    """
    Returns an edgelist with a given column shuffled. Exactly one of col_number or col_name should be specified.

    Args:
        graph_df (pd.DataFrame): The input DataFrame representing the timestamped edgelist.
        col_number (int, optional): The column number to shuffle. Default is None.
        col_name (str, optional): The column name to shuffle. Default is None.
        inplace (bool, optional): If True, shuffles the column in-place. Otherwise, creates a copy of the DataFrame. Default is False.

    Returns:
        pd.DataFrame: The shuffled DataFrame with the specified column.

    Raises:
        AssertionError: If neither col_number nor col_name is provided.
        AssertionError: If both col_number and col_name are provided.

    """
    assert (
        col_number is not None or col_name is not None
    ), f"No column number or name provided."
    assert not (
        col_name is not None and col_number is not None
    ), f"Cannot have both a column number and a column name."

    if inplace:
        df = graph_df
    else:
        df = graph_df.copy()

    no_events = len(df)

    if col_number is not None:
        col = df[df.columns[col_number]].sample(n=no_events)
        col.reset_index(inplace=True, drop=True)
        df[df.columns[col_number]] = col
    if col_name is not None:
        col = df[col_name].sample(n=no_events)
        col.reset_index(inplace=True, drop=True)
        df[col_name] = col
    return df


def shuffle_multiple_columns(
    graph_df: pd.DataFrame,
    col_numbers: list = None,
    col_names: list = None,
    inplace=False,
):
    """
    Returns an edgelist with given columns shuffled. Exactly one of col_numbers or col_names should be specified.

    Args:
        graph_df (pd.DataFrame): The input DataFrame representing the graph.
        col_numbers (list, optional): The list of column numbers to shuffle. Default is None.
        col_names (list, optional): The list of column names to shuffle. Default is None.
        inplace (bool, optional): If True, shuffles the columns in-place. Otherwise, creates a copy of the DataFrame. Default is False.

    Returns:
        pd.DataFrame: The shuffled DataFrame with the specified columns.

    Raises:
        AssertionError: If neither col_numbers nor col_names are provided.
        AssertionError: If both col_numbers and col_names are provided.

    """
    assert (
        col_numbers is not None or col_names is not None
    ), f"No column numbers or names provided."
    assert not (
        col_names is not None and col_numbers is not None
    ), f"Cannot have both column numbers and column names."

    if col_numbers is not None:
        for n in col_numbers:
            df = shuffle_column(graph_df, col_number=n, inplace=inplace)
    if col_names is not None:
        for name in col_names:
            df = shuffle_column(graph_df, col_name=name)
    return df


def permuted_timestamps_model(
    graph_df: pd.DataFrame,
    time_col: int = None,
    time_name: str = None,
    inplace=False,
    sorted=False,
):
    """
    Returns a DataFrame with the time column shuffled.

    Args:
        graph_df (pd.DataFrame): The input DataFrame representing the graph.
        time_col (int, optional): The column number of the time column to shuffle. Default is None.
        time_name (str, optional): The column name of the time column to shuffle. Default is None.
        inplace (bool, optional): If True, shuffles the time column in-place. Otherwise, creates a copy of the DataFrame. Default is False.
        sorted (bool, optional): If True, sorts the DataFrame by the shuffled time column. Default is False.

    Returns:
        pd.DataFrame or None: The shuffled DataFrame with the time column, or None if inplace=True.

    """
    shuffled_df = shuffle_column(graph_df, time_col, time_name, inplace)

    if sorted:
        shuffled_df.sort_values(
            by=time_name if time_name else shuffled_df.columns[time_col], inplace=True
        )

    if inplace:
        return
    else:
        return shuffled_df
