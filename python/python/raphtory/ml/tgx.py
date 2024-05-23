def df_to_tgx_edgelist(my_df, time_col='time', src_col='source', dst_col='destination'):
    """
    Converts a DataFrame representing an edge list to a TemporalGraphX edge list.

    Args:
        my_df (DataFrame): The input DataFrame containing edge information.
        time_col (str, optional): The column name representing time information. Defaults to 'time'.
        src_col (str, optional): The column name representing the source node IDs. Defaults to 'source'.
        dst_col (str, optional): The column name representing the destination node IDs. Defaults to 'destination'.

    Returns:
        dict: The TemporalGraphX edge list, where each time is a key mapped to a dictionary
              containing (src, dst) tuples as keys and their corresponding frequencies.
              The edge list is to be loaded by TGX as a continuous time dynamic graph (CTDG).


    Notes:
        - TGX library [TGX](https://complexdata-mila.github.io/TGX/)
        - The input DataFrame should contain columns for time, source, and destination.
    """

    from collections import Counter

    edgelist = { k:Counter() for k in my_df[time_col].values}

    for idx,row in my_df.iterrows():
        edgelist[row[time_col]][(row[src_col], row[dst_col])] +=1

    edgelist = { idx:dict(cnt) for idx, cnt in edgelist.items()}

    return edgelist

def to_tgx_edgelist(G, shift_t_zero=False, to_seconds=False):
    """
    Converts a Raphthory Graph to a TGX edge list.

    Args:
        G (object): The Raphthory Graph object.
        shift_t_zero (bool, optional): If True, shifts time zero to the earliest event timestamp. Defaults to False.
        to_seconds (bool, optional): If True, converts time to seconds, by diving by 1000. Defaults to False.

    Returns:
        dict: A dictionary where each time is a key mapped to a dictionary
              containing (src, dst) tuples as keys and their corresponding frequencies.
              The edge list is to be loaded by TGX as a continuous time dynamic graph (CTDG).

    Notes:
        - TGX library [TGX](https://complexdata-mila.github.io/TGX/)
        - If shift_t_zero is True, time zero is shifted to the earliest event timestamp.
        - If to_seconds is True, time is converted to seconds.

    """
    df = G.edges.to_df(explode=True, include_property_history=False)

    print(df.columns)

    # they may need to be turned into seconds
    if to_seconds:
        df["update_history"] =  df["update_history"]/1000

    # you may need to make the start from zero
    if shift_t_zero:
        df["update_history"] = (df["update_history"] - min(df["update_history_exploded"])).astype(int)

    df = df[["src","dst","update_history",]]
    #print(df.shape)

    edgelist = df_to_tgx_edgelist(df, time_col = 'update_history', src_col = 'src', dst_col = 'dst')

    return edgelist


def to_tgx_ctdg(G, shift_t_zero=False, to_seconds=False):
    """
    Converts a Raphthory Graph to a continuous time dynamic graph (CTDG) in TemporalGraphX (TGX) format.

    Args:
        G (object): The Raphthory Graph object.
        shift_t_zero (bool, optional): If True, shifts time zero to the earliest event timestamp. Defaults to False.
        to_seconds (bool, optional): If True, converts time to seconds. Defaults to False.

    Returns:
        object: The continuous time dynamic graph (CTDG) in TGX format.

    Notes:
        - TGX library [TGX](https://complexdata-mila.github.io/TGX/)
        - The graph is to be loaded by TGX as a continuous time dynamic graph (CTDG) object.
        - If shift_t_zero is True, time zero is shifted to the earliest event timestamp.
        - If to_seconds is True, time is converted to seconds.
    """

    import tgx as tgx_og

    edgelist = to_tgx_edgelist(G, shift_t_zero = False, to_seconds = False)

    ctdg = tgx_og.Graph(edgelist=edgelist)

    return ctdg

def from_tgx_graph(tgx_graph):
    """
    Converts a dynamic graph in TemporalGraphX (TGX) format to a Raphthory Graph.

    Args:
        tgx_graph (object): A dynamic graph object from TGX library.

    Returns:
        object: The Raphthory Graph object.

    Notes:
        - TGX library [TGX](https://complexdata-mila.github.io/TGX/)
        - This function supports both continuous time dynamic graph (CTDG) and discrete time dynamic graph (DTDG).
        - The input graph is assumed to be in TGX graph object.
        - The output graph is a Raphthory Graph object.
    """

    import raphtory as rp

    tgx_dataset = tgx_graph.export_full_data()

    G = rp.Graph()
    for src, dst, t in zip(tgx_dataset['sources'],tgx_dataset['destinations'],tgx_dataset['timestamps']):
        G.add_edge(timestamp=t, src=src,dst=dst)

    return G