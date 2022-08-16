from pyraphtory.algorithms.degree import Degree

if __name__ == "__main__":
    from pathlib import Path
    from pyraphtory.context import PyRaphtory

    pr = PyRaphtory(
        spout_input=Path('/Users/haaroony/Documents/nft/Data_API_clean_nfts_ETH_only_1k.csv'),
        builder_script=Path('/Users/haaroony/Documents/Raphtory-pyraphtory3/examples/raphtory-example-nft/python/nftgraphbuilder.py'),
        builder_class='NFTGraphBuilder',
        mode='batch',
        logging=False).open()

    rg = pr.graph()

    tb = rg.at(1575158373).past().execute(Degree())
    df = tb.write_to_dataframe(['name', 'in', 'out', 'deg'])
    print(df)
    print("Done")
