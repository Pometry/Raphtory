

    conda create --name gt -c conda-forge graph-tool
    conda activate gt
    conda install -c conda-forge neo4j-python-driver
    conda install networkx pandas maturin
    pip install kuzu gqlalchemy tqdm

### then install raphtory with
    maturin develop -r