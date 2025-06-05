# GraphQL Benchmark

This is a short script used to quickly benchmark the raphtory graph-ql server

1. Run `generate_graph.py` to generate the graph (you should already download the reddit graph and place the tsv file here
   
    http://web.archive.org/web/20201107005944/http://snap.stanford.edu/data/soc-redditHyperlinks-title.tsv
   
    http://snap.stanford.edu/data/soc-redditHyperlinks-title.tsv

2. Run `run_graphql.py` to start the server (make sure you have raphtory installed in python)

3. Run `artillery run benchmark.yml` to run the benchmark, uses https://www.npmjs.com/package/artillery (install via npm install -g artillery)


