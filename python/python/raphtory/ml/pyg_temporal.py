
def from_pyg_temporal(data):
    """
    Converts a TemporalData object from PyTorch Geometric to a Raphthory Graph object.

    Args:
        data (object): The TemporalData object from PyTorch Geometric.

    Returns:
        object: The Raphthory Graph object.

    Notes:
        - This function is intended for use with PyTorch Geometric TemporalData objects.
        - 'g' typically refers to a graph object.

    """

    import torch
    from torch_geometric.data import Data,TemporalData
    import raphtory as rp

    assert type(data) == type(TemporalData()), f"The passed object (type {type(data)}) is not of type {type(TemporalData()) }. If Data, use pyg_to_raph"

    g = rp.Graph()

    src_list = data.src.tolist()
    dst_list = data.dst.tolist()
    time_list = data.t.tolist()

    # add nodes first
    #print("Nodes")
    for i in range(data.num_nodes):
        g.add_node(id = i, timestamp=0)

    if 'x' in data:
        # adding x as a list for each node
        #print("Features")
        for i,x in enumerate(data.x.tolist()):
            g.edge(i).add_constant_properties(properties={"x":x})

    #print("Edges")

    if 'msg' not in data:
        for src,dst,t in zip(src_list,dst_list,time_list):
            g.add_edge(
                timestamp=t,
                src=src,
                dst=dst
            )
    else:
        msg_list = data.msg.tolist()
        for idx,src,dst,t,msg in zip(range(data.num_edges),src_list,dst_list,time_list,msg_list):
            g.add_edge(
                timestamp=t,
                src=src,
                dst=dst,
                properties = {"msg": msg,"e_idx":idx}
            )

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

    assert data.num_nodes == g.count_nodes()
    assert data.num_edges == g.count_temporal_edges()

    return g


#def raph_to_pyg_temp(G, re_index = False, nodelist = None, node_props_to_save = [], edge_props_to_save = ['msg']):
def to_pyg_temporal(G, re_index=False, nodelist=None, node_props_to_save=[], edge_props_to_save=['msg']):
    """
    Converts a Raphthory Graph to a temporalData object in PyTorch Geometric format.

    Args:
        G (object): The Raphtory Graph object.
        re_index (bool, optional): If True, re-indexes node indices. Defaults to False.
        nodelist (list, optional): A list of node indices to include. If None, includes all nodes. Defaults to None.
        node_props_to_save (list, optional): A list of node properties to save. Defaults to [].
        edge_props_to_save (list, optional): A list of edge properties to save. Defaults to ['msg'].

    Returns:
        tuple: If re_index is True, returns a tuple containing:
            - d (object): The PyTorch Geometric temporalData object.
            - mapping_node (dict): A dictionary mapping original node indices to new indices.
        If re_index is False, returns:
            - d (object): The PyTorch Geometric temporalData object.

    Notes:
        - This function is intended for use with Raphthory Graph objects.
        - PyG refers to PyTorch Geometric.
        - 'msg' usually denotes edge features.

    """

    import torch
    from torch_geometric.data import TemporalData

    import raphtory as rp

    d = TemporalData()

    if re_index:
        if nodelist is None:
            nodelist = sorted(G.nodes.id, key=int)
        mapping_node = dict(zip(nodelist, (i for i in range(len(nodelist)) ) ))
    else:
        mapping_node = dict(zip(G.nodes.id,G.nodes.id)) # returns the same id

    #d.num_nodes = len(mapping_node)
    #print(d.num_nodes)

    # Just explode the edges and we are good
    src = []
    dst = []
    ts = []

    if "e_idx" in G.edges.properties.keys():
        events = sorted(G.edges.explode(), key = lambda e : (e.properties["e_idx"]))
    else:
        events = sorted(G.edges.explode(), key = lambda e : (e.earliest_time, e.src.id,e.dst.id))

    i = 0
    for e in events:

        src.append(mapping_node[e.src.id])
        dst.append(mapping_node[e.dst.id])
        ts.append(e.earliest_time)

    d.src = torch.tensor(src)
    d.dst = torch.tensor(dst)
    d.t = torch.tensor(ts)


    if node_props_to_save == 'all':
        node_props_to_save = G.nodes.properties.keys()

    if node_props_to_save is None:
        node_props_to_save = []

    for k in node_props_to_save:
        if k in G.nodes.properties.keys():
            node_props = G.nodes.properties.get(k).collect()
            props_tensor = torch.tensor(node_props)
            d[k] = props_tensor

    if edge_props_to_save == 'all':
        edge_props_to_save = G.edges.properties.keys()

    if edge_props_to_save is None:
        edge_props_to_save = []



    if ('msg' in edge_props_to_save) and ('msg' in G.edges.properties.keys()):
        #print("Edges msgs")
        msgs = []
        for e in events:
            msgs.append(e.properties["msg"])
        d.msg = torch.tensor(msgs)

    for k in edge_props_to_save:
        if k == 'msg':
            continue

        if k in G.edges.properties.keys():
            edge_props = G.edges.properties.get(k).collect()
            props_tensor = torch.tensor(edge_props)
            d[k] = props_tensor

    # Machine learning masks and labels

    if 'y' in G.properties.keys():
        d.y = torch.tensor(G.properties['y'])
    if 'train_mask' in G.properties.keys():
        d.train_mask = torch.tensor(G.properties['train_mask'])
    if 'val_mask' in G.properties.keys():
        d.val_mask = torch.tensor(G.properties['val_mask'])
    if 'test_mask' in G.properties.keys():
        d.test_mask = torch.tensor(G.properties['test_mask'])

    if d.num_nodes < G.count_nodes():
        """
        edge case: Data and TemporalData will guess the number of nodes from the edge_index or x
        https://github.com/pyg-team/pytorch_geometric/issues/190
        Hence they added the option to overwrite num_nodes
        https://pytorch-geometric.readthedocs.io/en/latest/generated/torch_geometric.data.Data.html#torch_geometric.data.Data.num_nodes
        but it does not work like that for TemporalData
        https://github.com/pyg-team/pytorch_geometric/blob/master/torch_geometric/data/temporal.py#L202
        it only looks at the src and dst ...
        
        patch: for now we will save the info
        
        """
        num_nodes = G.count_nodes()
        d.total_nodes = torch.ones(num_nodes)

    #assert d.num_nodes == G.count_nodes(), f"{d.num_nodes}, {G.count_nodes()}"
    assert d.num_edges == G.count_temporal_edges(), f"{d.num_edges}, {G.count_temporal_edges()}"


    if re_index:
        return d, mapping_node

    return d
