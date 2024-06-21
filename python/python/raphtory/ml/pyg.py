def from_pyg(data):
    """
    Convert PyTorch Geometric (PyG) Data object to Raphtory Graph object.

    Args:
        data: PyTorch Geometric Data object to be converted.

    Returns:
        Graph: Converted Raphtory Graph object.

    Raises:
        ValueError: If the input data is invalid or cannot be converted.

    Example:
        >>> from torch_geometric.data import Data
        >>> from raphtory import Graph
        >>> pyg_data = Data(...)  # Obtain or create a PyG Data object
        >>> raph_graph = pyg_to_raph(pyg_data)
    """

    import torch
    from torch_geometric.data import Data, TemporalData
    import raphtory as rp

    assert type(data) == type(Data()), f"The passed object (type {type(data)}) is not of type {type(Data())}. If TemporalData, use pyg_temp_to_raph"

    g = rp.Graph()

    # add nodes first, with a loop
    #print("Nodes")
    for i in range(data.num_nodes):
        g.add_node(id = i, timestamp=0)

    if 'x' in data:
        # adding x as a list for each node
        #print("Features")
        for i,x in enumerate(data.x.tolist()):
            g.node(i).add_constant_properties(properties={"x":x})

    #print("Edges")
    for e in data.edge_index.T.tolist():
        g.add_edge(timestamp=0, src=e[0],dst=e[1])

    # ADD MASKS IF AVAILABLE, WE SHOULD PASS THEM AS GRAPH FEATURES
    graph_properties = {}

    if 'y' in data:
        graph_properties['y'] = data.y.tolist()

    if 'train_mask' in data:
        graph_properties['train_mask'] = data.train_mask.tolist()

    if 'val_mask' in data:
        graph_properties['val_mask'] = data.val_mask.tolist()

    if 'test_mask' in data:
        graph_properties['test_mask'] = data.test_mask.tolist()

    # We assume we have just static masks or labels.
    g.add_property(timestamp = 0, properties = graph_properties)

    #print(g)
    # Checks
    assert data.num_nodes == g.count_nodes()
    assert data.num_edges == g.count_edges()

    return g


#def raph_to_pyg(G, re_index = False, nodelist = None, node_props_to_save = ['x'], edge_props_to_save = []):
def to_pyg(G, re_index=False, nodelist=None, node_props_to_save=['x'], edge_props_to_save=[]):
    """
    Converts a Raphtory graph to PyG (PyTorch Geometric) format.

    Args:
        G (object): The Raphtory graph object.
        re_index (bool, optional): If True, re-indexes node indices. Defaults to False.
        nodelist (list, optional): A list of node indices to include. If None, includes all nodes. Defaults to None.
        node_props_to_save (list, optional): A list of node properties to save. Defaults to ['x'].
        edge_props_to_save (list, optional): A list of edge properties to save. Defaults to [].

    Returns:
        tuple: If re_index is True, returns a tuple containing:
            - d (object): The PyG graph representation.
            - mapping_node (dict): A dictionary mapping original node indices to new indices.
        If re_index is False, returns:
            - d (object): The PyG graph representation.

    Notes:
        - This function is intended for use with Raphthory graph objects.
        - PyG refers to PyTorch Geometric.
        - 'x' usually denotes node features.

    """

    import torch
    from torch_geometric.data import Data
    import pickle
    from tqdm import tqdm
    import os
    import raphtory as rp

    d = Data()
    # If the ids are not from 0 to N, we need a new id. If we do, we may want to store the previous one.
    if re_index:
        if nodelist is None:
            nodelist = list(G.nodes.id.sorted())
        mapping_node = dict(zip(nodelist, (i for i in range(len(nodelist)) ) ))
    else:
        mapping_node = dict(zip(G.nodes.id,G.nodes.id)) # returns the same id

    d.num_nodes = G.count_nodes()

    file_exists = os.path.exists(f'{G.count_nodes()}.pickle')

    if file_exists:
        # Deserialize (unpickle) the graph
        with open(f'{G.count_nodes()}.pickle', 'rb') as f:
            d = pickle.load(f)
            if re_index:
                return d, mapping_node
            return d
    else:
        edges = list(map(lambda x: (mapping_node[x[0]],mapping_node[x[1]]),sorted(zip(G.edges.src.id,G.edges.dst.id),key=lambda l:l[0])))
        src, dst = zip(*edges)

        d.edge_index = torch.tensor([dst, src]) # directions get flipped otherwise

        # Now features
        #print('Node Props')

        if node_props_to_save == 'all':
            node_props_to_save = G.nodes.properties.keys()

        if node_props_to_save is None:
            node_props_to_save = []

        for k in tqdm(node_props_to_save):
            node_props = G.nodes.properties.get(k).collect()
            props_tensor = torch.tensor(node_props)
            d[k] = props_tensor#.astype('float32')

        if edge_props_to_save == 'all':
            edge_props_to_save = G.edges.properties.keys()

        if edge_props_to_save is None:
            edge_props_to_save = []

        for k in edge_props_to_save:
            edge_props = G.edges.properties.get(k).collect()
            props_tensor = torch.tensor(edge_props)
            d[k] = props_tensor

        # Machine learning masks and labels
        g_keys = G.properties.keys()
        if 'y' in g_keys:
            d.y = torch.tensor(G.properties['y'])
        if 'train_mask' in g_keys:
            d.train_mask = torch.tensor(G.properties['train_mask'])
        if 'val_mask' in g_keys:
            d.val_mask = torch.tensor(G.properties['val_mask'])
        if 'test_mask' in g_keys:
            d.test_mask = torch.tensor(G.properties['test_mask'])

        # Serialize (pickle) the graph
        with open(f'{G.count_nodes()}.pickle', 'wb') as f:
            pickle.dump(d, f)

        if re_index:
            return d, mapping_node

        return d
