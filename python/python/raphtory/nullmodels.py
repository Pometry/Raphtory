"""
Generate null models for a graph.
"""

import pandas as pd

def shuffle_column(graph_df:pd.DataFrame, col_number=None, col_name=None, inplace=False):
    """
    returns a dataframe with a given column shuffled
    """
    assert col_number is not None or col_name is not None, f"No column number or name provided."
    assert not (col_name is not None and col_number is not None), f"Cannot have both a column number and a column name."

    if inplace:
        df = graph_df
    else:
        df = graph_df.copy()

    no_events = len(df)

    if col_number is not None:
        col = df[[col_number]].sample(n=no_events)
        col.reset_index(inplace=True,drop=True)
        df[[col_number]] = col
    if col_name is not None:
        col = df[col_name].sample(n=no_events)
        col.reset_index(inplace=True,drop=True)
    
    return df

def shuffle_multiple_columns(graph_df:pd.DataFrame, col_numbers:list=None, col_names:list=None, inplace=False):
    """
    returns a dataframe with a given columns shuffled.
    """
    assert col_numbers is not None or col_names is not None, f"No column numbers or names provided."
    assert not (col_names is not None and col_numbers is not None), f"Cannot have both column numbers and column names."

    if col_numbers is not None:
        for n in col_numbers:
            df = shuffle_column(graph_df, col_number=n, inplace=inplace)
    if col_names is not None:
        for name in col_names:
            df = shuffle_column(graph_df, col_name=name)
    
    return df
    
def permuted_timestamps_model(graph_df:pd.DataFrame, time_col:int=None, time_name:str=None, inplace=False, sorted=False):
    """
    returns a dataframe with the time column shuffled
    """
    shuffled_df = shuffle_column(graph_df, time_col, time_name, inplace)

    if sorted:
        shuffled_df.sort_values(by=time_name if time_name else shuffled_df.columns[time_col], inplace=True)
    
    if inplace:
        return
    else:
        return shuffled_df